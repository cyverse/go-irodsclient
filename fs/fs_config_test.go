package fs

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/types"
)

func TestNewFileSystemAcceptsNilConfig(t *testing.T) {
	// An invalid account makes session creation return before any network I/O. The assertion here
	// is that nil config follows the documented default-config path instead of panicking first.
	account := &types.IRODSAccount{}

	filesystem, err := NewFileSystem(account, nil)
	if err == nil {
		if filesystem != nil {
			filesystem.Release()
		}
		t.Fatal("expected invalid account error")
	}
	if filesystem != nil {
		t.Fatal("filesystem was returned for an invalid account")
	}
}
