package connection

import (
	"encoding/json"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

const (
	AUTH_CLIENT_START         string = "auth_client_start"
	AUTH_AGENT_START          string = "auth_agent_start"
	AUTH_ESTABLISH_CONTEXT    string = "auth_establish_context"
	AUTH_CLIENT_AUTH_REQUEST  string = "auth_client_auth_request"
	AUTH_AGENT_AUTH_REQUEST   string = "auth_agent_auth_request"
	AUTH_CLIENT_AUTH_RESPONSE string = "auth_client_auth_response"
	AUTH_AGENT_AUTH_RESPONSE  string = "auth_agent_auth_response"
	AUTH_AGENT_AUTH_VERIFY    string = "auth_agent_auth_verify"

	AUTH_FLOW_COMPLETE  string = "authentication_flow_complete"
	AUTH_NEXT_OPERATION string = "next_operation"

	// TODO This one may not be necessary. It is used in the C++ implementations
	// to control whether the plugins prompt the user for input. That is something
	// the developer can implement ahead of time, I think. Then again, the plugin
	// may be designed to prompt the client at certain times.
	AUTH_FORCE_PASSWORD_PROMPT string = "force_password_prompt"

	// Client Options
	AUTH_USER_KEY     string = "a_user"
	AUTH_SCHEME_KWY   string = "a_scheme"
	AUTH_TTL_KEY      string = "a_ttl"
	AUTH_PASSWORD_KEY string = "a_pw"
	AUTH_RESPONSE_KEY string = "a_resp"
)

type IRODSAuthContext struct {
	context map[string]interface{}
}

func NewIRODSAuthContext() *IRODSAuthContext {
	return &IRODSAuthContext{
		context: map[string]interface{}{},
	}
}

func (ctx *IRODSAuthContext) Set(key string, value interface{}) {
	if ctx.context == nil {
		ctx.context = make(map[string]interface{})
	}
	ctx.context[key] = value
}

func (ctx *IRODSAuthContext) Remove(key string) {
	delete(ctx.context, key)
}

func (ctx *IRODSAuthContext) Get(key string) (interface{}, bool) {
	value, ok := ctx.context[key]
	return value, ok
}

func (ctx *IRODSAuthContext) GetString(key string) (string, bool) {
	value, ok := ctx.context[key]
	if !ok {
		return "", false
	}

	if value == nil {
		return "", false
	}

	strValue, ok := value.(string)
	if !ok {
		return "", false
	}

	return strValue, true
}

func (ctx *IRODSAuthContext) GetMap(key string) (map[string]interface{}, bool) {
	value, ok := ctx.context[key]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, false
	}

	mapValue, ok := value.(map[string]interface{})
	if !ok {
		return nil, false
	}

	return mapValue, true
}

func (ctx *IRODSAuthContext) GetObjectArray(key string) ([]interface{}, bool) {
	value, ok := ctx.context[key]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, false
	}

	arrayValue, ok := value.([]interface{})
	if !ok {
		return nil, false
	}

	return arrayValue, true
}

func (ctx *IRODSAuthContext) GetMapArray(key string) ([]map[string]interface{}, bool) {
	value, ok := ctx.context[key]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, false
	}

	arrayValue, ok := value.([]interface{})
	if !ok {
		return nil, false
	}

	var result []map[string]interface{}
	for _, item := range arrayValue {
		if mapItem, ok := item.(map[string]interface{}); ok {
			result = append(result, mapItem)
		}
	}

	return result, true
}

func (ctx *IRODSAuthContext) GetJSON(key string) ([]byte, bool) {
	value, ok := ctx.context[key]
	if !ok {
		return nil, false
	}

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}

	return jsonBytes, true
}

func (ctx *IRODSAuthContext) Has(key string) bool {
	_, ok := ctx.context[key]
	return ok
}

func (ctx *IRODSAuthContext) CopyTo(newContext *IRODSAuthContext) {
	if newContext.context == nil {
		newContext.context = map[string]interface{}{}
	}

	for k, v := range ctx.context {
		newContext.context[k] = v
	}
}

func (ctx *IRODSAuthContext) GetCopy() *IRODSAuthContext {
	copy := NewIRODSAuthContext()
	ctx.CopyTo(copy)
	return copy
}

type IRODSAuthPluginOperationFunc func(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error)

type IRODSAuthPlugin interface {
	AddOperation(name string, operation IRODSAuthPluginOperationFunc)
	Execute(conn *IRODSConnection, operationName string, requestContext *IRODSAuthContext) (*IRODSAuthContext, error)
	Request(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error)

	// plugin must implement this
	GetName() string
	AuthClientStart(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error)
}

type BaseIRODSAuthPlugin struct {
	operations map[string]IRODSAuthPluginOperationFunc

	// base will not implement following APIs
	//Initialize()
	//GetName() string
	//AuthClientStart(conn *IRODSConnection)
}

func (plugin *BaseIRODSAuthPlugin) AddOperation(name string, operation IRODSAuthPluginOperationFunc) {
	if plugin.operations == nil {
		plugin.operations = map[string]IRODSAuthPluginOperationFunc{}
	}
	plugin.operations[name] = operation
}

func (plugin *BaseIRODSAuthPlugin) Execute(conn *IRODSConnection, operationName string, context *IRODSAuthContext) (*IRODSAuthContext, error) {
	operation, ok := plugin.operations[operationName]
	if !ok {
		return nil, types.NewAuthOperationNotFoundError(operationName)
	}

	return operation(conn, context)
}

func (plugin *BaseIRODSAuthPlugin) Request(conn *IRODSConnection, context *IRODSAuthContext) (*IRODSAuthContext, error) {
	timeout := conn.GetOperationTimeout()

	authRequest := message.NewIRODSMessageNewAuthPluginRequest(context.context)

	authResponse := message.IRODSMessageNewAuthPluginResponse{}
	err := conn.RequestAndCheck(authRequest, &authResponse, nil, timeout)
	if err != nil {
		newErr := errors.Join(types.NewAuthError(conn.account), err)
		return nil, errors.Wrapf(newErr, "failed to auth with new auth plugin")
	}

	return &IRODSAuthContext{context: authResponse.AuthContext}, nil
}
