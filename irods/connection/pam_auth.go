package connection

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

func AuthenticatePAMWithPassword(conn *IRODSConnection, password string) error {
	logger := log.WithFields(log.Fields{})

	timeout := conn.GetOperationTimeout()

	// Check whether ssl has already started
	if _, ok := conn.socket.(*tls.Conn); !ok {
		newErr := types.NewConnectionError()
		return errors.Wrapf(newErr, "connection should be using SSL")
	}

	ttl := conn.account.PamTTL
	if ttl < 0 {
		ttl = 0 // server decides
	}

	pamPassword := getSafePAMPassword(password)

	userKV := fmt.Sprintf("a_user=%s", conn.account.ProxyUser)
	passwordKV := fmt.Sprintf("a_pw=%s", pamPassword)
	ttlKV := fmt.Sprintf("a_ttl=%s", strconv.Itoa(ttl))

	authContext := strings.Join([]string{userKV, passwordKV, ttlKV}, ";")

	useDedicatedPAMApi := true
	if conn.requireNewPamAuth() {
		useDedicatedPAMApi = strings.ContainsAny(pamPassword, ";=") || len(authContext) >= 1024+64
	}

	// authenticate
	pamToken := ""

	if useDedicatedPAMApi {
		logger.Debugf("use dedicated PAM api")

		pamAuthRequest := message.NewIRODSMessagePamAuthRequest(conn.account.ProxyUser, password, ttl)
		pamAuthResponse := message.IRODSMessagePamAuthResponse{}
		err := conn.RequestAndCheck(pamAuthRequest, &pamAuthResponse, nil, timeout)
		if err != nil {
			newErr := errors.Join(err, types.NewAuthError(conn.account))
			return errors.Wrapf(newErr, "failed to receive a PAM token")
		}

		pamToken = pamAuthResponse.GeneratedPassword
	} else {
		logger.Debugf("use auth plugin api: scheme %q", string(types.AuthSchemePAM))

		pamAuthRequest := message.NewIRODSMessageAuthPluginRequest(string(types.AuthSchemePAM), authContext)
		pamAuthResponse := message.IRODSMessageAuthPluginResponse{}
		err := conn.RequestAndCheck(pamAuthRequest, &pamAuthResponse, nil, timeout)
		if err != nil {
			newErr := errors.Join(err, types.NewAuthError(conn.account))
			return errors.Wrapf(newErr, "failed to receive a PAM token")
		}

		pamToken = string(pamAuthResponse.GeneratedPassword)
	}

	// save irods generated password for possible future use
	conn.account.PAMToken = pamToken

	// we do not login here.
	// connection will be disconnected and reconnected afterword
	return nil
}

func AuthenticatePAMWithToken(conn *IRODSConnection, token string) error {
	// Check whether ssl has already started
	if _, ok := conn.socket.(*tls.Conn); !ok {
		newErr := types.NewConnectionError()
		return errors.Wrapf(newErr, "connection should be using SSL")
	}

	// same as native auth
	return AuthenticateNative(conn, token)
}

func getSafePAMPassword(password string) string {
	// For certain characters in the pam password, if they need escaping with '\' then do so.
	replacer := strings.NewReplacer("@", "\\@", "=", "\\=", "&", "\\&", ";", "\\;")
	return replacer.Replace(password)
}
