package connection

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

const (
	PAM_PASSWORD_AUTH_PERFORM_NATIVE_AUTH string = "perform_native_auth"
)

type PAMPasswordAuthPlugin struct {
	BaseIRODSAuthPlugin
	requireSecureConnection bool
}

func NewPAMPasswordAuthPlugin(requireSecureConnection bool) *PAMPasswordAuthPlugin {
	plugin := &PAMPasswordAuthPlugin{
		requireSecureConnection: requireSecureConnection,
	}

	plugin.initialize()
	return plugin
}

func (plugin *PAMPasswordAuthPlugin) initialize() {
	plugin.AddOperation(AUTH_CLIENT_START, plugin.AuthClientStart)
	plugin.AddOperation(AUTH_CLIENT_AUTH_REQUEST, plugin.clientRequest)
	plugin.AddOperation(PAM_PASSWORD_AUTH_PERFORM_NATIVE_AUTH, plugin.performNativeAuth)
}

func (plugin *PAMPasswordAuthPlugin) GetName() string {
	return "pam_password"
}

func (plugin *PAMPasswordAuthPlugin) AuthClientStart(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	responseContext := requestContext.GetCopy()

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_REQUEST)

	responseContext.Set("user_name", conn.account.ProxyUser)
	responseContext.Set("zone_name", conn.account.ProxyZone)

	password, _ := requestContext.GetString("password")
	responseContext.Set(AUTH_PASSWORD_KEY, password)

	// don't leak user's plaintext password
	responseContext.Remove("password")

	return responseContext, nil
}

func (plugin *PAMPasswordAuthPlugin) clientRequest(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
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

	responseContext.Set(AUTH_NEXT_OPERATION, PAM_PASSWORD_AUTH_PERFORM_NATIVE_AUTH)

	if !responseContext.Has("request_result") {
		return nil, errors.Wrapf(types.NewAuthError(conn.account), "missing request result in PAM password auth response")
	}

	return responseContext, nil
}

func (plugin *PAMPasswordAuthPlugin) performNativeAuth(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
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
