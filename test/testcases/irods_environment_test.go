package testcases

import (
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/config"
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

	// save
	envMgr, err := config.NewICommandsEnvironmentManager()
	failError(t, err)

	envMgr.FromIRODSAccount(account)

	envMgr.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	err = envMgr.SaveEnvironment()
	failError(t, err)

	// load
	envMgr2, err := config.NewICommandsEnvironmentManager()
	failError(t, err)

	envMgr2.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	err = envMgr2.Load()
	failError(t, err)

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.ZoneName)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Environment.Password)

	err = os.RemoveAll("~/.irods2")
	failError(t, err)
}

func testSaveAndLoadEnvSession(t *testing.T) {
	account, err := server.GetLocalAccount()
	failError(t, err)

	envMgr, err := config.NewICommandsEnvironmentManager()
	failError(t, err)

	envMgr.FromIRODSAccount(account)

	envMgr.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	testWorkingDir := "/test/working/dir"

	envMgr.Session.CurrentWorkingDir = testWorkingDir

	err = envMgr.SaveEnvironment()
	failError(t, err)

	err = envMgr.SaveSession()
	failError(t, err)

	envMgr2, err := config.NewICommandsEnvironmentManager()
	failError(t, err)

	envMgr2.SetEnvironmentFilePath("~/.irods2/irods_environment.json")

	envMgr2.Load()

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.ZoneName)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Environment.Password)

	sess2 := envMgr2.Session
	assert.Equal(t, testWorkingDir, sess2.CurrentWorkingDir)

	err = os.RemoveAll("~/.irods2")
	failError(t, err)
}

func testConfiguredAuthFilePath(t *testing.T) {
	account, err := server.GetLocalAccount()
	failError(t, err)

	envMgr, err := config.NewICommandsEnvironmentManager()
	failError(t, err)

	envMgr.FromIRODSAccount(account)

	// Create a safe temporary directory in TMPDIR
	dir, err := os.MkdirTemp("", ".irods")
	defer func(d string) {
		e := os.RemoveAll(d)
		if e != nil {
			failError(t, e)
		}
	}(dir)

	failError(t, err)

	t.Logf("temp dir: %s\n", dir)

	err = envMgr.SetEnvironmentDirPath(dir)
	failError(t, err)

	t.Logf("env dir: %s\n", envMgr.EnvironmentDirPath)
	t.Logf("env file: %s\n", envMgr.EnvironmentFilePath)
	t.Logf("session file: %s\n", envMgr.SessionFilePath)
	t.Logf("pass file: %s\n", envMgr.PasswordFilePath)

	err = envMgr.SaveEnvironment()
	failError(t, err)

	envMgr2, err := config.NewICommandsEnvironmentManager()
	failError(t, err)

	err = envMgr2.SetEnvironmentDirPath(dir)
	failError(t, err)

	err = envMgr2.Load()
	failError(t, err)

	t.Logf("env dir: %s\n", envMgr2.EnvironmentDirPath)
	t.Logf("env file: %s\n", envMgr2.EnvironmentFilePath)
	t.Logf("session file: %s\n", envMgr2.SessionFilePath)
	t.Logf("pass file: %s\n", envMgr2.PasswordFilePath)

	assert.Equal(t, envMgr.Environment.AuthenticationFile, envMgr2.Environment.AuthenticationFile, "Configured auth file path should be loaded")
	assert.Equal(t, account.Password, envMgr2.Environment.Password, "Password should be loaded")
}
