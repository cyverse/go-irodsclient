package testcases

import (
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/stretchr/testify/assert"
)

func TestFSCache(t *testing.T) {
	setup()

	t.Run("test MakeDir", testMakeDir)

	shutdown()
}

func testMakeDir(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	fsConfig := fs.NewFileSystemConfigWithDefault("go-irodsclient-test")

	fs, err := fs.NewFileSystem(account, fsConfig)
	assert.NoError(t, err)
	defer fs.Release()

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

	for i := 0; i < 10; i++ {
		newdir := fmt.Sprintf("%s/test_dir_%d", homedir, i)
		err = fs.MakeDir(newdir, false)
		assert.NoError(t, err)

		entries, err := fs.List(homedir)
		assert.NoError(t, err)

		found := false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID)
			if entry.Path == newdir {
				// okay
				found = true
				break
			}
		}

		assert.True(t, found)
	}
}
