package fs

import (
	"bytes"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/types"
)

func TestSetLocalChecksumDoesNotOverwriteIRODSAlgorithm(t *testing.T) {
	result := &FileTransferResult{
		IRODSCheckSumAlgorithm: types.ChecksumAlgorithmSHA256,
	}
	localChecksum := []byte{1, 2, 3}

	setLocalChecksum(result, types.ChecksumAlgorithmMD5, localChecksum)

	if result.IRODSCheckSumAlgorithm != types.ChecksumAlgorithmSHA256 {
		t.Fatalf("iRODS checksum algorithm was overwritten: %q", result.IRODSCheckSumAlgorithm)
	}
	if result.LocalCheckSumAlgorithm != types.ChecksumAlgorithmMD5 {
		t.Fatalf("local checksum algorithm = %q, want %q", result.LocalCheckSumAlgorithm, types.ChecksumAlgorithmMD5)
	}
	if !bytes.Equal(result.LocalCheckSum, localChecksum) {
		t.Fatalf("local checksum = %v, want %v", result.LocalCheckSum, localChecksum)
	}
}
