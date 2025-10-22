package fs

import (
	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// ExtractStructFile extracts a struct file for the path
func ExtractStructFile(conn *connection.IRODSConnection, path string, target string, resource string, dataType types.DataType, force bool, bulkReg bool) error {
	if conn == nil || !conn.IsConnected() {
		return errors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	switch dataType {
	case types.TAR_FILE_DT, types.GZIP_TAR_DT, types.BZIP2_TAR_DT, types.ZIP_FILE_DT:
		// pass
	default:
		return errors.Errorf("failed to extract content from unsupported data type %q", dataType)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageExtractStructFileRequest(path, target, resource, dataType, force, bulkReg)
	response := message.IRODSMessageExtractStructFileResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetLongResponseOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the data object for path %q", path)
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the collection for path %q", path)
		}

		return errors.Wrapf(err, "received extract struct file error")
	}
	return nil
}
