package fs

import (
	"encoding/base64"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"
)

type HashType string

const (
	SHA256 = "SHA256"
	SHA512 = "SHA512"
	MD5    = "MD5"
)

func Checksum(conn *connection.IRODSConnection, irodsPath string) (HashType, []byte, error) {
	if conn == nil || !conn.IsConnected() {
		return "", nil, xerrors.Errorf("connection is nil or disconnected")
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
			return "", nil, types.NewFileNotFoundErrorf("could not find a data object")
		}

		return "", nil, err
	}

	return splitChecksum(response.Checksum)
}

func splitChecksum(checksum string) (HashType, []byte, error) {
	sp := strings.Split(checksum, ":")
	if len(sp) != 2 {
		return "", nil, xerrors.Errorf("unexpected checksum: %v", string(checksum))
	}
	inHashType := sp[0]
	hash, err := base64.StdEncoding.DecodeString(sp[1])
	var hashType HashType

	if inHashType == "sha2" && len(hash) == 256/8 {
		hashType = SHA256
	} else if inHashType == "sha2" && len(hash) == 512/8 {
		hashType = SHA512
	} else if strings.ToLower(string(inHashType)) == "md5" {
		hashType = MD5
	} else {
		return "", nil, xerrors.Errorf("unknown hash type: %s", hashType)
	}
	return hashType, hash, err
}
