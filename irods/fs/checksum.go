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
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError())
		}
		return nil, xerrors.Errorf("failed to get data object checksum: %w", err)
	}

	algorithm, checksum, err := splitChecksum(response.Checksum)
	if err != nil {
		return nil, xerrors.Errorf("failed to split data object checksum: %w", err)
	}

	return &types.IRODSChecksum{
		Algorithm: algorithm,
		Checksum:  checksum,
	}, nil
}

func splitChecksum(checksumString string) (types.ChecksumAlgorithm, []byte, error) {
	sp := strings.Split(checksumString, ":")
	if len(sp) != 2 {
		return types.ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unexpected checksum: %v", string(checksumString))
	}

	algorithm := sp[0]
	checksum, err := base64.StdEncoding.DecodeString(sp[1])
	if err != nil {
		return types.ChecksumAlgorithmUnknown, nil, xerrors.Errorf("failed to base64 decode checksum: %v", err)
	}

	switch strings.ToLower(algorithm) {
	case "sha2":
		if len(checksum) == 256/8 {
			return types.ChecksumAlgorithmSHA256, checksum, nil
		} else if len(checksum) == 512/8 {
			return types.ChecksumAlgorithmSHA512, checksum, nil
		} else {
			return types.ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
		}
	case "sha256":
		if len(checksum) == 256/8 {
			return types.ChecksumAlgorithmSHA256, checksum, nil
		} else {
			return types.ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
		}
	case "sha512":
		if len(checksum) == 512/8 {
			return types.ChecksumAlgorithmSHA512, checksum, nil
		} else {
			return types.ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
		}
	case "md5":
		return types.ChecksumAlgorithmMD5, checksum, nil
	default:
		return types.ChecksumAlgorithmUnknown, nil, xerrors.Errorf("unknown checksum algorithm: %s len %d", algorithm, len(checksum))
	}
}
