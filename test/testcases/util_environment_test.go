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
	server := test.GetServer()

	account := server.GetAccountCopy()

	// save
	envMgr, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr.FromIRODSAccount(account)

	tempPath := t.TempDir()
	envFilePath := filepath.Join(tempPath, "irods_environment.json")

	envMgr.SetEnvironmentFilePath(envFilePath)

	err = envMgr.SaveEnvironment()
	FailError(t, err)

	// load
	envMgr2, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr2.SetEnvironmentFilePath(envFilePath)

	err = envMgr2.Load()
	FailError(t, err)

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.ZoneName)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Environment.Password)
}

func testSaveAndLoadSession(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()

	// save
	envMgr, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr.FromIRODSAccount(account)

	tempPath := t.TempDir()
	envFilePath := filepath.Join(tempPath, "irods_environment.json")

	envMgr.SetEnvironmentFilePath(envFilePath)

	// set working data in session
	envMgr.Session.CurrentWorkingDir = "/test/working/dir"

	err = envMgr.SaveEnvironment()
	FailError(t, err)

	err = envMgr.SaveSession()
	FailError(t, err)

	// load
	envMgr2, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr2.SetEnvironmentFilePath(envFilePath)

	envMgr2.Load()

	env2 := envMgr2.Environment
	assert.Equal(t, account.Host, env2.Host)
	assert.Equal(t, account.Port, env2.Port)
	assert.Equal(t, account.ClientZone, env2.ZoneName)
	assert.Equal(t, account.ClientUser, env2.Username)
	assert.Equal(t, account.Password, envMgr2.Environment.Password)

	assert.Equal(t, envMgr.Session.CurrentWorkingDir, envMgr2.Session.CurrentWorkingDir)
}

func testLoadFilePaths(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	account := server.GetAccountCopy()

	envMgr, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	envMgr.FromIRODSAccount(account)

	tempPath := t.TempDir()

	FailError(t, err)

	err = envMgr.SetEnvironmentDirPath(tempPath)
	FailError(t, err)

	assert.Equal(t, tempPath, envMgr.EnvironmentDirPath)
	assert.Equal(t, filepath.Join(tempPath, "irods_environment.json"), envMgr.EnvironmentFilePath)
	assert.Equal(t, filepath.Join(tempPath, ".irodsA"), envMgr.PasswordFilePath)
	assert.Equal(t, fmt.Sprintf("%s.%d", envMgr.EnvironmentFilePath, envMgr.PPID), envMgr.SessionFilePath)

	err = envMgr.SaveEnvironment()
	FailError(t, err)

	envMgr2, err := config.NewICommandsEnvironmentManager()
	FailError(t, err)

	err = envMgr2.SetEnvironmentDirPath(tempPath)
	FailError(t, err)

	err = envMgr2.Load()
	FailError(t, err)

	assert.Equal(t, envMgr.EnvironmentDirPath, envMgr2.EnvironmentDirPath)
	assert.Equal(t, envMgr.EnvironmentFilePath, envMgr2.EnvironmentFilePath)
	assert.Equal(t, envMgr.PasswordFilePath, envMgr2.PasswordFilePath)
	assert.Equal(t, envMgr.SessionFilePath, envMgr2.SessionFilePath)

	assert.Equal(t, envMgr.Environment.AuthenticationFile, envMgr2.Environment.AuthenticationFile)
	assert.Equal(t, account.Password, envMgr2.Environment.Password)
}
