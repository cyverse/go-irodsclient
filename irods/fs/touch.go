package fs

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// Touch create an empty data object or update timestamp for the path
func Touch(conn *connection.IRODSConnection, path string, resource string, noCreate bool) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageTouchRequest(path, noCreate, resource)
	response := message.IRODSMessageTouchResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.SYS_UNMATCHED_API_NUM {
			// not supported
			return xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewAPINotSupportedError(common.TOUCH_APN))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return xerrors.Errorf("failed to touch: %w", err)
	}

	return nil
}
