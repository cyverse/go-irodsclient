package testcases

import (
	"testing"
)

func TestIRODSEnvironment(t *testing.T) {
	//t.Run("test LoadCurrentUserEnv", testLoadCurrentUserEnv)
	//t.Run("test SaveEnv", testSaveEnv)
}

/*
func testLoadCurrentUserEnv(t *testing.T) {
	account, err := icommands.CreateAccountFromCurrentUserAndHome()
	assert.NoError(t, err)
	assert.Equal(t, "iychoi", account.ClientUser)
}

func testSaveEnv(t *testing.T) {
	account, err := server.GetLocalAccount()
	assert.NoError(t, err)

	homeDir, err := os.UserHomeDir()
	assert.NoError(t, err)

	newEnvDir := filepath.Join(homeDir, ".irods2")
	err = icommands.SaveAccountToDir(newEnvDir, 0, account)
	assert.NoError(t, err)
}
*/
