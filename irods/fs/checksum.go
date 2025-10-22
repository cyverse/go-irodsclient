package fs

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// GetDataObjectChecksum returns a data object checksum for the path
func GetDataObjectChecksum(conn *connection.IRODSConnection, path string, resource string) (*types.IRODSChecksum, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForStat(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageChecksumRequest(path, resource)
	response := message.IRODSMessageChecksumResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return nil, errors.Wrapf(newErr, "failed to find the data object for path %q", path)
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", path)
		}

		return nil, errors.Wrapf(err, "failed to get data object checksum")
	}

	checksum, err := types.CreateIRODSChecksum(response.Checksum)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create iRODS checksum")
	}

	return checksum, nil
}
