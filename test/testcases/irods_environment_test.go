package testcases

import (
	"os"
	"path"
	"testing"

	"github.com/cyverse/go-irodsclient/icommands"
	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/stretchr/testify/assert"
)

func TestIRODSEnvironment(t *testing.T) {
	t.Run("test SaveAndLoadEnv", testSaveAndLoadEnv)
	t.Run("test SaveAndLoadEnvSession", testSaveAndLoadEnvSession)
	t.Run("test ConfiguredAuthFilePath", testConfiguredAuthFilePath)
}

func testSaveAndLoadEnv(t *testing.T) {
	account, err := server.GetLocalAccount()
	failError(t, err)

	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(account)
	failError(t, err)

	envMgr.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	err = envMgr.SaveEnvironment()
	failError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager()
	failError(t, err)

	envMgr2.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	err = envMgr2.Load(os.Getppid())
	failError(t, err)

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.Zone)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Password)

	err = os.RemoveAll("~/.irods2")
	failError(t, err)
}

func testConfiguredAuthFilePath(t *testing.T) {
	account, err := server.GetLocalAccount()
	failError(t, err)

	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(account)
	failError(t, err)

	// Create a safe temporary directory in TMPDIR
	dir, err := os.MkdirTemp("", ".irods")
	defer func(d string) {
		e := os.RemoveAll(d)
		if e != nil {
			failError(t, e)
		}
	}(dir)

	failError(t, err)

	envFilePath := path.Join(dir, "irods_environment.json")
	authFilePath := path.Join(dir, "configured_irodsA")

	err = envMgr.SetEnvironmentFilePath(envFilePath)
	failError(t, err)

	envMgr.Environment.AuthenticationFile = authFilePath
	err = envMgr.SaveEnvironment()
	failError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager()
	failError(t, err)

	err = envMgr2.SetEnvironmentFilePath(envFilePath)
	failError(t, err)

	err = envMgr2.Load(os.Getppid())
	failError(t, err)

	assert.Equal(t, authFilePath, envMgr2.Environment.AuthenticationFile, "Configured auth file path should be loaded")
	assert.Equal(t, authFilePath, envMgr2.GetPasswordFilePath(), "Configured auth file path should be returned")
}

func testSaveAndLoadEnvSession(t *testing.T) {
	account, err := server.GetLocalAccount()
	failError(t, err)

	envMgr, err := icommands.CreateIcommandsEnvironmentManagerFromIRODSAccount(account)
	failError(t, err)

	envMgr.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	testWorkingDir := "/test/working/dir"

	envMgr.Session.CurrentWorkingDir = testWorkingDir

	err = envMgr.SaveEnvironment()
	failError(t, err)
	err = envMgr.SaveSession(os.Getppid())
	failError(t, err)

	envMgr2, err := icommands.CreateIcommandsEnvironmentManager()
	failError(t, err)

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
	failError(t, err)
}
