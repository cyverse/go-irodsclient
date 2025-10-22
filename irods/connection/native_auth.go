package connection

import (
	"encoding/hex"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/auth"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

func AuthenticateNative(conn *IRODSConnection, password string) error {
	timeout := conn.GetOperationTimeout()

	authRequest := message.NewIRODSMessageAuthRequest()
	authChallenge := message.IRODSMessageAuthChallengeResponse{}
	err := conn.RequestAndCheck(authRequest, &authChallenge, nil, timeout)
	if err != nil {
		return errors.Join(err, types.NewAuthFlowError("failed to receive authentication challenge message body"))
	}

	challengeBytes, err := authChallenge.GetChallenge()
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(conn.account))
		return errors.Wrapf(newErr, "failed to get authentication challenge")
	}

	// save client signature
	conn.clientSignature = generateClientSignature(challengeBytes)

	encodedPassword := auth.GenerateAuthResponse(challengeBytes, password)

	authResponse := message.NewIRODSMessageAuthResponse(encodedPassword, conn.account.ProxyUser, conn.account.ProxyZone)
	authResult := message.IRODSMessageAuthResult{}
	err = conn.RequestAndCheck(authResponse, &authResult, nil, timeout)
	if err != nil {
		newErr := errors.Join(err, types.NewAuthError(conn.account))
		return errors.Wrapf(newErr, "received irods authentication error")
	}

	conn.loggedIn = true

	return nil
}

// generateClientSignature generates a client signature from auth challenge
func generateClientSignature(challenge []byte) string {
	if len(challenge) > 16 {
		challenge = challenge[:16]
	}

	signature := hex.EncodeToString(challenge)
	return signature
}
