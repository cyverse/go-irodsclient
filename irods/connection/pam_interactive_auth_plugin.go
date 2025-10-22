package connection

import (
	"encoding/json"
	"fmt"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/types"
	jsonpatch "github.com/evanphx/json-patch"
	log "github.com/sirupsen/logrus"
	"github.com/xeipuuv/gojsonpointer"
	"golang.org/x/term"
)

const (
	PAM_INTERACTIVE_AUTH_PERFORM_RUNNING           string = "running"
	PAM_INTERACTIVE_AUTH_PERFORM_READY             string = "ready"
	PAM_INTERACTIVE_AUTH_PERFORM_WAITING           string = "waiting"
	PAM_INTERACTIVE_AUTH_PERFORM_WAITING_PW        string = "waiting_pw"
	PAM_INTERACTIVE_AUTH_PERFORM_RESPONSE          string = "response"
	PAM_INTERACTIVE_AUTH_PERFORM_NEXT              string = "next"
	PAM_INTERACTIVE_AUTH_PERFORM_ERROR             string = "error"
	PAM_INTERACTIVE_AUTH_PERFORM_TIMEOUT           string = "timeout"
	PAM_INTERACTIVE_AUTH_PERFORM_AUTHENTICATED     string = "authenticated"
	PAM_INTERACTIVE_AUTH_PERFORM_NOT_AUTHENTICATED string = "not_authenticated"
	PAM_INTERACTIVE_AUTH_PERFORM_NATIVE_AUTH       string = "perform_native_auth"
)

type PAMInteractiveInputHandler func() (string, error)

type PAMInteractiveAuthPlugin struct {
	BaseIRODSAuthPlugin
	requireSecureConnection  bool
	getInputHandler          PAMInteractiveInputHandler
	getSensitiveInputHandler PAMInteractiveInputHandler
}

func NewPAMInteractiveAuthPlugin(requireSecureConnection bool) *PAMInteractiveAuthPlugin {
	plugin := &PAMInteractiveAuthPlugin{
		requireSecureConnection: requireSecureConnection,
	}

	plugin.getInputHandler = plugin.getInputFromClientStdin
	plugin.getSensitiveInputHandler = plugin.getPasswordFromClientStdin

	plugin.initialize()
	return plugin
}

func NewPAMInteractiveAuthPluginWithHandlers(requireSecureConnection bool, getInputHandler PAMInteractiveInputHandler, getSensitiveInputHandler PAMInteractiveInputHandler) *PAMInteractiveAuthPlugin {
	plugin := &PAMInteractiveAuthPlugin{
		requireSecureConnection:  requireSecureConnection,
		getInputHandler:          getInputHandler,
		getSensitiveInputHandler: getSensitiveInputHandler,
	}

	plugin.initialize()
	return plugin
}

func (plugin *PAMInteractiveAuthPlugin) initialize() {
	plugin.AddOperation(AUTH_CLIENT_START, plugin.AuthClientStart)
	plugin.AddOperation(AUTH_CLIENT_AUTH_REQUEST, plugin.clientRequest)
	plugin.AddOperation(AUTH_CLIENT_AUTH_RESPONSE, plugin.clientResponse)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_RUNNING, plugin.stepGeneric)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_READY, plugin.stepGeneric)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_NEXT, plugin.stepClientNext)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_RESPONSE, plugin.stepGeneric)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_WAITING, plugin.stepWaiting)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_WAITING_PW, plugin.stepWaitingPw)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_ERROR, plugin.stepError)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_TIMEOUT, plugin.stepTimeout)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_AUTHENTICATED, plugin.stepAuthenticated)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_NOT_AUTHENTICATED, plugin.stepNotAuthenticated)
	plugin.AddOperation(PAM_INTERACTIVE_AUTH_PERFORM_NATIVE_AUTH, plugin.performNativeAuth)
}

func (plugin *PAMInteractiveAuthPlugin) GetName() string {
	return "pam_interactive"
}

func (plugin *PAMInteractiveAuthPlugin) AuthClientStart(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	responseContext := requestContext.GetCopy()

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_REQUEST)

	responseContext.Set("pdirty", false)
	responseContext.Set("pstate", map[string]interface{}{})

	responseContext.Set("user_name", conn.account.ProxyUser)
	responseContext.Set("zone_name", conn.account.ProxyZone)

	// don't leak user's plaintext password
	responseContext.Remove("password")

	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) clientRequest(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	logger := log.WithFields(log.Fields{})

	if plugin.requireSecureConnection {
		if !conn.isSSLSocket {
			return nil, errors.Wrapf(types.NewAuthError(conn.account), "PAM password authentication requires secure connection")
		}
	} else {
		if !conn.isSSLSocket {
			logger.Warn("using insecure channel for authentication. password will be visible on the network.")
		}
	}

	reqContext := requestContext.GetCopy()

	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_REQUEST)

	responseContext, err := plugin.Request(conn, reqContext)
	if err != nil {
		return nil, err
	}

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_RESPONSE)
	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) clientResponse(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	if !requestContext.Has("user_name") {
		return nil, errors.Wrapf(types.NewAuthError(conn.account), "missing required field (user_name) in auth request")
	}

	if !requestContext.Has("zone_name") {
		return nil, errors.Wrapf(types.NewAuthError(conn.account), "missing required field (zone_name) in auth request")
	}

	reqContext := requestContext.GetCopy()
	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)

	return plugin.Request(conn, reqContext)
}

func (plugin *PAMInteractiveAuthPlugin) stepClientNext(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	reqContext := requestContext.GetCopy()
	if msgMap, ok := reqContext.GetMap("msg"); ok && msgMap != nil {
		if promptVal, ok2 := msgMap["prompt"]; ok2 {
			if prompt, ok3 := promptVal.(string); ok3 {
				fmt.Printf("%s", prompt)
			}
		}
	}

	err := plugin.patchState(reqContext)
	if err != nil {
		return nil, err
	}

	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)

	return plugin.Request(conn, reqContext)
}

func (plugin *PAMInteractiveAuthPlugin) stepWaiting(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	reqContext := requestContext.GetCopy()

	retrieved, err := plugin.retrieveEntry(reqContext)
	if err != nil {
		return nil, err
	}

	if retrieved {
		reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)
		err := plugin.patchState(reqContext)
		if err != nil {
			return nil, err
		}

		return plugin.Request(conn, reqContext)
	}

	defaultValue, err := plugin.getDefaultValue(reqContext)
	if err != nil {
		return nil, err
	}

	input, err := plugin.getInputHandler()
	if err != nil {
		return nil, err
	}

	if len(input) > 0 {
		reqContext.Set("resp", input)
	} else {
		reqContext.Set("resp", defaultValue)
	}

	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)
	err = plugin.patchState(reqContext)
	if err != nil {
		return nil, err
	}

	return plugin.Request(conn, reqContext)
}

func (plugin *PAMInteractiveAuthPlugin) stepWaitingPw(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	reqContext := requestContext.GetCopy()

	retrieved, err := plugin.retrieveEntry(reqContext)
	if err != nil {
		return nil, err
	}

	if retrieved {
		reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)
		err := plugin.patchState(reqContext)
		if err != nil {
			return nil, err
		}

		return plugin.Request(conn, reqContext)
	}

	defaultValue, err := plugin.getDefaultValue(reqContext)
	if err != nil {
		return nil, err
	}

	input, err := plugin.getSensitiveInputHandler()
	if err != nil {
		return nil, err
	}

	if len(input) > 0 {
		reqContext.Set("resp", input)
	} else {
		reqContext.Set("resp", defaultValue)
	}

	err = plugin.patchState(reqContext)
	if err != nil {
		return nil, err
	}

	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)

	return plugin.Request(conn, reqContext)
}

func (plugin *PAMInteractiveAuthPlugin) stepError(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	fmt.Printf("error\n")
	responseContext := requestContext.GetCopy()
	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE)
	conn.loggedIn = false
	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) stepTimeout(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	fmt.Printf("timeout\n")
	responseContext := requestContext.GetCopy()
	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE)
	conn.loggedIn = false
	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) stepAuthenticated(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	responseContext := requestContext.GetCopy()
	responseContext.Set(AUTH_NEXT_OPERATION, PAM_INTERACTIVE_AUTH_PERFORM_NATIVE_AUTH)
	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) stepNotAuthenticated(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	responseContext := requestContext.GetCopy()
	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE)
	conn.loggedIn = false
	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) stepGeneric(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	reqContext := requestContext.GetCopy()

	err := plugin.patchState(reqContext)
	if err != nil {
		return nil, err
	}

	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)
	return plugin.Request(conn, reqContext)
}

func (plugin *PAMInteractiveAuthPlugin) performNativeAuth(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	responseContext := requestContext.GetCopy()

	// Remove PAM password to avoid sending it over the network
	responseContext.Remove(AUTH_PASSWORD_KEY)

	input := NewIRODSAuthContext()
	requestResult, _ := responseContext.GetString("request_result")
	input.Set("password", requestResult)

	nativeAuthPlugin := NewNativeAuthPlugin()
	err := AuthenticateClient(conn, nativeAuthPlugin, input)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to perform native auth as part of PAM password auth")
	}

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE)

	// The native auth plugin sets this on success, so this isn't necessary.
	// We'll set it anyway to align with the C++ implementation, just to be on
	// the safe side.
	conn.loggedIn = true

	return responseContext, nil
}

func (plugin *PAMInteractiveAuthPlugin) getInputFromClientStdin() (string, error) {
	userInput := ""
	_, err := fmt.Scanln(&userInput)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return "", errors.Wrapf(newErr, "failed to get user input")
	}

	return userInput, nil
}

func (plugin *PAMInteractiveAuthPlugin) getPasswordFromClientStdin() (string, error) {
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return "", errors.Wrapf(newErr, "failed to get user password input")
	}

	return string(bytePassword), nil
}

func (plugin *PAMInteractiveAuthPlugin) patchState(requestContext *IRODSAuthContext) error {
	if !requestContext.Has("patch") {
		return nil
	}

	var patchList []map[string]interface{}
	if patchArray, ok := requestContext.GetMapArray("patch"); ok {
		patchList = patchArray
	}

	for _, patch := range patchList {
		op := patch["op"]
		opStr := op.(string)
		if opStr == "add" || opStr == "replace" {
			if _, ok2 := patch["value"]; !ok2 {
				value, _ := requestContext.GetString("resp")
				patch["value"] = value
			}
		}
	}

	var pstate map[string]interface{}
	if pstateItem, ok := requestContext.GetMap("pstate"); ok {
		pstate = pstateItem
	}
	if pstate == nil {
		pstate = map[string]interface{}{}
	}

	patchJson, err := json.Marshal(patchList)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return errors.Wrapf(newErr, "failed to marshal patch data to JSON")
	}

	pstateJson, err := json.Marshal(pstate)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return errors.Wrapf(newErr, "failed to marshal pstate to JSON")
	}

	patch, err := jsonpatch.DecodePatch(patchJson)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return errors.Wrapf(newErr, "failed to decode patch JSON")
	}

	modifiedJson, err := patch.Apply(pstateJson)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return errors.Wrapf(newErr, "failed to apply patch to pstate JSON")
	}

	newPstate := map[string]interface{}{}
	err = json.Unmarshal(modifiedJson, &newPstate)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(nil))
		return errors.Wrapf(newErr, "failed to unmarshal modified pstate JSON")
	}

	requestContext.Set("pstate", newPstate)
	requestContext.Set("pdirty", true)

	if msg, ok := requestContext.GetMap("msg"); ok && msg != nil {
		delete(msg, "patch")
	}

	return nil
}

func (plugin *PAMInteractiveAuthPlugin) getDefaultValue(requestContext *IRODSAuthContext) (string, error) {
	defaultPath := ""
	if msg, ok := requestContext.GetMap("msg"); ok && msg != nil {
		if defaultPathObj, ok2 := msg["default_path"]; ok2 {
			if defaultPathString, ok3 := defaultPathObj.(string); ok3 {
				defaultPath = defaultPathString
			}
		}
	}

	if len(defaultPath) > 0 {
		// use default path to find default value from pstate
		if pstate, ok := requestContext.GetMap("pstate"); ok && pstate != nil {
			pointer, err := gojsonpointer.NewJsonPointer(defaultPath)
			if err != nil {
				newErr := errors.Join(err, types.NewAuthError(nil))
				return "", errors.Wrapf(newErr, "failed to create json pointer for default path")
			}

			pointerValue, _, err := pointer.Get(pstate)
			if err != nil {
				newErr := errors.Join(err, types.NewAuthError(nil))
				return "", errors.Wrapf(newErr, "failed to get value from pstate for default path")
			}

			if pointValueString, ok2 := pointerValue.(string); ok2 {
				return pointValueString, nil
			}
		}
	}

	return "", nil
}

func (plugin *PAMInteractiveAuthPlugin) retrieveEntry(requestContext *IRODSAuthContext) (bool, error) {
	retrievePath := ""
	found := false
	if msg, ok := requestContext.GetMap("msg"); ok && msg != nil {
		if retrieve, ok2 := msg["retrieve"]; ok2 {
			if retrieveString, ok3 := retrieve.(string); ok3 {
				retrievePath = retrieveString
				found = true
			}
		}
	}

	if found {
		requestContext.Set("resp", "")

		if len(retrievePath) > 0 {
			if pstate, ok := requestContext.GetMap("pstate"); ok && pstate != nil {
				pointer, err := gojsonpointer.NewJsonPointer(retrievePath)
				if err != nil {
					newErr := errors.Join(err, types.NewAuthError(nil))
					return false, errors.Wrapf(newErr, "failed to create json pointer for retrieve path")
				}

				pointerValue, _, err := pointer.Get(pstate)
				if err != nil {
					newErr := errors.Join(err, types.NewAuthError(nil))
					return false, errors.Wrapf(newErr, "failed to get value from pstate for retrieve path")
				}

				if pointValueString, ok2 := pointerValue.(string); ok2 {
					requestContext.Set("resp", pointValueString)
					return true, nil
				}
			}
		}

		return true, nil
	}

	return false, nil
}
