package testcases

import (
	"fmt"
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/stretchr/testify/assert"
)

func TestFSCache(t *testing.T) {
	setup()
	defer shutdown()

	t.Run("test MakeDir", testMakeDir)
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

		// create test
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

		exist := fs.ExistsDir(newdir)
		assert.True(t, exist)

		// delete test
		err = fs.RemoveDir(newdir, true, true)
		assert.NoError(t, err)

		entries, err = fs.List(homedir)
		assert.NoError(t, err)

		found = false
		for _, entry := range entries {
			assert.NotEmpty(t, entry.ID)
			if entry.Path == newdir {
				// found removed dir?
				found = true
				break
			}
		}

		assert.False(t, found)
	}
}
