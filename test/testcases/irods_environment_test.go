package testcases

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/cyverse/go-irodsclient/utils/icommands"
	"github.com/stretchr/testify/assert"
)

func TestIRODSEnvironment(t *testing.T) {
	t.Run("test SaveAndLoadEnv", testSaveAndLoadEnv)
	t.Run("test SaveAndLoadEnvSession", testSaveAndLoadEnvSession)
}

func testSaveAndLoadEnv(t *testing.T) {
	account, err := server.GetLocalAccount()
	assert.NoError(t, err)

	homeDir, err := os.UserHomeDir()
	assert.NoError(t, err)

	newEnvDir := filepath.Join(homeDir, ".irods2")
	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(newEnvDir, 0, account)
	assert.NoError(t, err)

	err = envMgr.Save()
	assert.NoError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager(newEnvDir, 0)
	assert.NoError(t, err)

	envMgr2.Load()

	env2 := envMgr2.GetEnvironment()
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.Zone)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.GetPassword())

	err = os.RemoveAll(newEnvDir)
	assert.NoError(t, err)
}

func testSaveAndLoadEnvSession(t *testing.T) {
	account, err := server.GetLocalAccount()
	assert.NoError(t, err)

	homeDir, err := os.UserHomeDir()
	assert.NoError(t, err)

	newEnvDir := filepath.Join(homeDir, ".irods2")
	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(newEnvDir, 0, account)
	assert.NoError(t, err)

	testWorkingDir := "/test/working/dir"

	envMgr.GetSession().CurrentWorkingDir = testWorkingDir

	err = envMgr.Save()
	assert.NoError(t, err)
	err = envMgr.SaveSession()
	assert.NoError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager(newEnvDir, 0)
	assert.NoError(t, err)

	envMgr2.Load()

	env2 := envMgr2.GetEnvironment()
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.Zone)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.GetPassword())

	sess2 := envMgr2.GetSession()
	t.Logf("%v", sess2)
	assert.Equal(t, testWorkingDir, sess2.CurrentWorkingDir)

	err = os.RemoveAll(newEnvDir)
	assert.NoError(t, err)
}
