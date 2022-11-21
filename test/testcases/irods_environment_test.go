package testcases

import (
	"os"
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

	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(account)
	assert.NoError(t, err)

	envMgr.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	err = envMgr.SaveEnvironment()
	assert.NoError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager()
	assert.NoError(t, err)

	envMgr2.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	err = envMgr2.Load(os.Getppid())
	assert.NoError(t, err)

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.Zone)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Password)

	err = os.RemoveAll("~/.irods2")
	assert.NoError(t, err)
}

func testSaveAndLoadEnvSession(t *testing.T) {
	account, err := server.GetLocalAccount()
	assert.NoError(t, err)

	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(account)
	assert.NoError(t, err)

	envMgr.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	testWorkingDir := "/test/working/dir"

	envMgr.Session.CurrentWorkingDir = testWorkingDir

	err = envMgr.SaveEnvironment()
	assert.NoError(t, err)
	err = envMgr.SaveSession(os.Getppid())
	assert.NoError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager()
	assert.NoError(t, err)

	envMgr2.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	envMgr2.Load(os.Getppid())

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.Zone)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Password)

	sess2 := envMgr2.Session
	//t.Logf("%v", sess2)
	assert.Equal(t, testWorkingDir, sess2.CurrentWorkingDir)

	err = os.RemoveAll("~/.irods2")
	assert.NoError(t, err)
}
