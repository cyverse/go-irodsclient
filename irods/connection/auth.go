package connection

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

func AuthenticateClient(conn *IRODSConnection, authPlugin IRODSAuthPlugin, requestContext *IRODSAuthContext) error {
	logger := log.WithFields(log.Fields{})

	logger.Debug("authentication start")

	if authPlugin == nil {
		return types.NewAuthFlowError("no authentication plugin provided")
	}

	nextOp := AUTH_CLIENT_START

	requestContext.Set("scheme", authPlugin.GetName())
	requestContext.Set(AUTH_NEXT_OPERATION, nextOp)

	logger.Debugf("initial context: %v", requestContext)

	for {
		responseContext, err := authPlugin.Execute(conn, nextOp, requestContext)
		if err != nil {
			return errors.Join(err, types.NewAuthFlowError("authentication plugin execution failed"))
		}

		logger.Debugf("server response context: %v", responseContext)

		if conn.IsLoggedIn() {
			break
		}

		authNextOperation, ok := responseContext.Get(AUTH_NEXT_OPERATION)
		if !ok {
			return types.NewAuthFlowError("authentication response did not return next operation")
		}

		if authNextOperationString, ok := authNextOperation.(string); ok {
			nextOp = authNextOperationString
		}

		if len(nextOp) == 0 || nextOp == AUTH_FLOW_COMPLETE {
			// invalid
			return types.NewAuthFlowError("authentication did not complete successfully")
		}

		requestContext = responseContext
	}

	logger.Debug("authentication complete")
	return nil
}
