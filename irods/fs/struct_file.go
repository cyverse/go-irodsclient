package fs

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

// ExtractStructFile extracts a struct file for the path
func ExtractStructFile(conn *connection.IRODSConnection, path string, target string, resource string, dataType types.DataType, force bool, bulkReg bool) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	switch dataType {
	case types.TAR_FILE_DT, types.GZIP_TAR_DT, types.BZIP2_TAR_DT, types.ZIP_FILE_DT:
		// pass
	default:
		return xerrors.Errorf("failed to extract content from unsupported data type %s", dataType)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageExtractStructFileRequest(path, target, resource, dataType, force, bulkReg)
	response := message.IRODSMessageRemoveDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("received extract struct file error: %w", err)
	}
	return nil
}
