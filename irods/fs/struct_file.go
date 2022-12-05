package fs

import (
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// ExtractStructFile extracts a struct file for the path
func ExtractStructFile(conn *connection.IRODSConnection, path string, target string, resource string, dataType types.DataType, force bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	switch dataType {
	case types.TAR_FILE_DT, types.GZIP_TAR_DT, types.BZIP2_TAR_DT, types.ZIP_FILE_DT:
		// pass
	default:
		return fmt.Errorf("cannot extract content from unsupported data type %s", dataType)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageExtractStructFileRequest(path, target, resource, dataType, force)
	response := message.IRODSMessageRemoveDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object (struct file) to extract")
	}
	return err
}
