package fs

import (
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

func Checksum(conn *connection.IRODSConnection, irodsPath string) (string, error) {
	if conn == nil || !conn.IsConnected() {
		return "", xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	fileOpenMode := types.FileOpenMode("r")
	resource := conn.GetAccount().DefaultResource
	flag := fileOpenMode.GetFlag()
	request := &message.ChecksumRequest{
		Path:          irodsPath,
		CreateMode:    0,
		OpenFlags:     flag,
		Offset:        0,
		Size:          -1,
		Threads:       0,
		OperationType: 0,
		KeyVals:       message.IRODSMessageSSKeyVal{},
	}

	if len(resource) > 0 {
		request.KeyVals.Add(string(common.DEST_RESC_NAME_KW), resource)
	}

	response := message.ChecksumResponse{}

	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return "", types.NewFileNotFoundErrorf("could not find a data object")
		}

		return "", err
	}

	return response.Checksum, nil
}
