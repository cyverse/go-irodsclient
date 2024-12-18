package fs

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// GetDataObjectChecksum returns a data object checksum for the path
func GetDataObjectChecksum(conn *connection.IRODSConnection, path string, resource string) (*types.IRODSChecksum, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
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
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return nil, xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return nil, xerrors.Errorf("failed to get data object checksum: %w", err)
	}

	checksum, err := types.CreateIRODSChecksum(response.Checksum)
	if err != nil {
		return nil, xerrors.Errorf("failed to create iRODS checksum: %w", err)
	}

	return checksum, nil
}
