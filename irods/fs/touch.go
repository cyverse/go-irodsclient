package fs

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// Touch create an empty data object or update timestamp for the path
func Touch(conn *connection.IRODSConnection, path string, resource string, noCreate bool, replicaNumber *int, referencePath string, secondsSinceEpoch *int) error {
	if conn == nil || !conn.IsConnected() {
		return errors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	//if len(resource) == 0 {
	//	account := conn.GetAccount()
	//	resource = account.DefaultResource
	//}

	request := message.NewIRODSMessageTouchRequest(path)
	if noCreate {
		request.SetNoCreate(noCreate)
	}

	if replicaNumber != nil {
		replicaNumberVal := *replicaNumber
		request.SetReplicaNumber(replicaNumberVal)
	}

	if replicaNumber != nil && len(resource) > 0 {
		request.SetLeafResourceName(resource)
	}

	if len(referencePath) > 0 {
		request.SetReference(referencePath)
	}

	if len(referencePath) == 0 && secondsSinceEpoch != nil {
		secondsSinceEpochVal := *secondsSinceEpoch
		request.SetSecondsSinceEpoch(secondsSinceEpochVal)
	}

	response := message.IRODSMessageTouchResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the data object for path %q", path)
		} else if types.GetIRODSErrorCode(err) == common.SYS_UNMATCHED_API_NUM {
			// not supported
			newErr := errors.Join(err, types.NewAPINotSupportedError(common.TOUCH_APN))
			return errors.Wrapf(newErr, "failed to find the data object for path %q", path)
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the collection for path %q", path)
		}

		return errors.Wrapf(err, "failed to touch data object for path %q", path)
	}

	return nil
}
