package testcases

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cyverse/go-irodsclient/config"
	"github.com/stretchr/testify/assert"
)

func getUtilEnvironmentTest() Test {
	return Test{
		Name: "Util_Environment",
		Func: utilEnvironmentTest,
	}
}

func utilEnvironmentTest(t *testing.T, test *Test) {
	t.Run("SaveAndLoadEnvironment", testSaveAndLoadEnvironment)
	t.Run("SaveAndLoadSession", testSaveAndLoadSession)
	t.Run("LoadFilePaths", testLoadFilePaths)
}

func testSaveAndLoadEnvironment(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	// save
	envMgr, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr.FromIRODSAccount(account)

	tempPath := t.TempDir()
	envFilePath := filepath.Join(tempPath, "irods_environment.json")

	err = envMgr.SetEnvironmentFilePath(envFilePath)
	FailError(t, err)

	err = envMgr.SaveEnvironment()
	FailError(t, err)

	// load
	envMgr2, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	err = envMgr2.SetEnvironmentFilePath(envFilePath)
	FailError(t, err)

	err = envMgr2.Load()
	FailError(t, err)

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host, "loaded host should match source account host")
	assert.Equal(t, account.Port, env2.Port, "loaded port should match source account port")
	assert.Equal(t, account.ClientZone, env2.ZoneName, "loaded zone should match source account zone")
	assert.Equal(t, account.ClientUser, env2.Username, "loaded username should match source account user")
	assert.Equal(t, account.Password, envMgr2.Environment.Password, "loaded password should match source account password")
}

func testSaveAndLoadSession(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	// save
	envMgr, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr.FromIRODSAccount(account)

	tempPath := t.TempDir()
	envFilePath := filepath.Join(tempPath, "irods_environment.json")

	err = envMgr.SetEnvironmentFilePath(envFilePath)
	FailError(t, err)

	// set working data in session
	envMgr.Session.CurrentWorkingDir = "/test/working/dir"

	err = envMgr.SaveEnvironment()
	FailError(t, err)

	err = envMgr.SaveSession()
	FailError(t, err)

	// load
	envMgr2, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	err = envMgr2.SetEnvironmentFilePath(envFilePath)
	FailError(t, err)

	err = envMgr2.Load()
	FailError(t, err)

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host, "reloaded host should match source account host")
	assert.Equal(t, account.Port, env2.Port, "reloaded port should match source account port")
	assert.Equal(t, account.ClientZone, env2.ZoneName, "reloaded zone should match source account zone")
	assert.Equal(t, account.ClientUser, env2.Username, "reloaded username should match source account user")
	assert.Equal(t, account.Password, envMgr2.Environment.Password, "reloaded password should match source account password")

	assert.Equal(t, envMgr.Session.CurrentWorkingDir, envMgr2.Session.CurrentWorkingDir, "reloaded working dir should match saved working dir")
}

func testLoadFilePaths(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	account, err := server.GetAccount()
	FailError(t, err)

	envMgr, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr.FromIRODSAccount(account)

	tempPath := t.TempDir()

	FailError(t, err)

	err = envMgr.SetEnvironmentDirPath(tempPath)
	FailError(t, err)

	assert.Equal(t, tempPath, envMgr.EnvironmentDirPath, "environment dir path should be set value")
	assert.Equal(t, filepath.Join(tempPath, "irods_environment.json"), envMgr.EnvironmentFilePath, "environment file path should be temp+irods_environment.json")
	assert.Equal(t, filepath.Join(tempPath, ".irodsA"), envMgr.PasswordFilePath, "password file path should be temp+.irodsA")
	assert.Equal(t, fmt.Sprintf("%s.%d", envMgr.EnvironmentFilePath, envMgr.PPID), envMgr.SessionFilePath, "session file path should be environment file + PPID")

	err = envMgr.SaveEnvironment()
	FailError(t, err)

	envMgr2, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	err = envMgr2.SetEnvironmentDirPath(tempPath)
	FailError(t, err)

	err = envMgr2.Load()
	FailError(t, err)

	assert.Equal(t, envMgr.EnvironmentDirPath, envMgr2.EnvironmentDirPath, "reloaded environment dir should match first manager")
	assert.Equal(t, envMgr.EnvironmentFilePath, envMgr2.EnvironmentFilePath, "reloaded environment file path should match first manager")
	assert.Equal(t, envMgr.PasswordFilePath, envMgr2.PasswordFilePath, "reloaded password file path should match first manager")
	assert.Equal(t, envMgr.SessionFilePath, envMgr2.SessionFilePath, "reloaded session file path should match first manager")

	assert.Equal(t, envMgr.Environment.AuthenticationFile, envMgr2.Environment.AuthenticationFile, "reloaded auth file should match first manager")
	assert.Equal(t, account.Password, envMgr2.Environment.Password, "reloaded password should match source account")
}
