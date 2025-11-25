package connection

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

const (
	NATIVE_AUTH_CHALLENGE_LEN int = 64
	NATIVE_AUTH_RESPONSE_LEN  int = 16
)

type NativeAuthPlugin struct {
	BaseIRODSAuthPlugin
}

func NewNativeAuthPlugin() *NativeAuthPlugin {
	plugin := &NativeAuthPlugin{}
	plugin.initialize()
	return plugin
}

func (plugin *NativeAuthPlugin) initialize() {
	plugin.AddOperation(AUTH_CLIENT_START, plugin.AuthClientStart)
	plugin.AddOperation(AUTH_ESTABLISH_CONTEXT, plugin.establishContext)
	plugin.AddOperation(AUTH_CLIENT_AUTH_REQUEST, plugin.clientRequest)
	plugin.AddOperation(AUTH_CLIENT_AUTH_RESPONSE, plugin.clientResponse)

}

func (plugin *NativeAuthPlugin) GetName() string {
	return "native"
}

func (plugin *NativeAuthPlugin) AuthClientStart(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	responseContext := requestContext.GetCopy()

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_REQUEST)

	responseContext.Set("user_name", conn.account.ProxyUser)
	responseContext.Set("zone_name", conn.account.ProxyZone)

	return responseContext, nil
}

func (plugin *NativeAuthPlugin) establishContext(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	logger := log.WithFields(log.Fields{})

	responseContext := requestContext.GetCopy()

	requestResult, _ := requestContext.GetString("request_result")

	logger.Debugf("request result string = %q", requestResult)

	// Compute the client signature and store it in the connection
	conn.clientSignature = plugin.generateClientSignature([]byte(requestResult))
	logger.Debugf("client signature = %q", conn.clientSignature)

	// if the anonymous user is used, no need to append password
	password, _ := requestContext.GetString("password")

	authResponse := plugin.generateAuthResponse([]byte(requestResult), password)
	logger.Debugf("auth response = %q", authResponse)

	// don't leak user's plaintext password
	responseContext.Remove("password")

	responseContext.Set("digest", authResponse)
	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_RESPONSE)

	return responseContext, nil
}

func (plugin *NativeAuthPlugin) clientRequest(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	reqContext := requestContext.GetCopy()

	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_REQUEST)

	responseContext, err := plugin.Request(conn, reqContext)
	if err != nil {
		return nil, err
	}

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_ESTABLISH_CONTEXT)
	return responseContext, nil
}

func (plugin *NativeAuthPlugin) clientResponse(conn *IRODSConnection, requestContext *IRODSAuthContext) (*IRODSAuthContext, error) {
	if !requestContext.Has("digest") || !requestContext.Has("user_name") || !requestContext.Has("zone_name") {
		return nil, types.NewAuthFlowError("missing required fields (digest, user_name, zone_name) in auth request")
	}

	reqContext := requestContext.GetCopy()
	reqContext.Set(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE)

	responseContext, err := plugin.Request(conn, reqContext)
	if err != nil {
		return nil, err
	}

	conn.loggedIn = true

	responseContext.Set(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE)
	return responseContext, nil
}

func (plugin *NativeAuthPlugin) generateClientSignature(challenge []byte) string {
	signaturePart := []byte{}
	if len(challenge) > 16 {
		signaturePart = challenge[:16]
	}

	return hex.EncodeToString(signaturePart)
}

func (plugin *NativeAuthPlugin) generateAuthResponse(challenge []byte, password string) string {
	paddedPassword := make([]byte, common.MaxPasswordLength)
	copy(paddedPassword, []byte(password))

	m := md5.New()
	m.Write(challenge[:NATIVE_AUTH_CHALLENGE_LEN])
	if len(password) > 0 {
		m.Write(paddedPassword)
	}

	encodedPassword := m.Sum(nil)

	// replace 0x00 to 0x01
	for idx := 0; idx < len(encodedPassword); idx++ {
		if encodedPassword[idx] == 0 {
			encodedPassword[idx] = 1
		}
	}

	b64encodedPassword := base64.StdEncoding.EncodeToString(encodedPassword[:NATIVE_AUTH_RESPONSE_LEN])
	return b64encodedPassword
}
