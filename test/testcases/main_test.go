package testcases

import (
	"path"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/rs/xid"

	log "github.com/sirupsen/logrus"
)

type Test struct {
	Name               string
	Func               func(t *testing.T, test *Test)
	DoNotCreateHomeDir bool
	Versions           []string // if empty, run for all versions

	// runtime info
	currentTestHomename string
	currentServer       *server.IRODSServer
}

func (test *Test) GetTestHomeDir() (string, error) {
	homeDir, err := test.currentServer.GetHomeDir()
	if err != nil {
		log.Errorf("Failed to get home directory: %+v", err)
		return "", errors.Wrapf(err, "failed to get home directory")
	}

	return path.Join(homeDir, test.currentTestHomename), nil
}

func (test *Test) MakeTestHomeDir() error {
	fs, err := test.currentServer.GetFileSystem()
	if err != nil {
		return errors.Wrapf(err, "failed to create a new filesystem")
	}
	defer fs.Release()

	homeDir, err := test.GetTestHomeDir()
	if err != nil {
		return errors.Wrapf(err, "failed to get test home directory")
	}

	err = fs.MakeDir(homeDir, true)
	if err != nil {
		return errors.Wrapf(err, "failed to make a home directory %q", homeDir)
	}

	return nil
}

func (test *Test) GetCurrentServer() *server.IRODSServer {
	return test.currentServer
}

func (test *Test) checkRunForVersion(version string) bool {
	// if version is not specified, run for all versions
	if len(test.Versions) == 0 {
		return true
	}

	for _, v := range test.Versions {
		if v == version {
			return true
		}
	}

	return false
}

func (test *Test) ResetForTest(server *server.IRODSServer) error {
	// setup new UUID as home name
	test.currentTestHomename = makeTestUUID()
	test.currentServer = server

	// create home directory
	if !test.DoNotCreateHomeDir {
		err := test.MakeTestHomeDir()
		if err != nil {
			return err
		}
	}

	return nil
}

func makeTestUUID() string {
	return xid.New().String()
}

var (
	currentTest *Test
)

func GetCurrentTest() *Test {
	return currentTest
}

func testMainForServer(t *testing.T, server *server.IRODSServer, tests []Test) {
	serverInfo := server.GetInfo()
	t.Logf("Testing for server %q", serverInfo.Name)

	testFunc := func(t *testing.T) {
		err := server.Start()
		FailError(t, err)

		defer func() {
			err = server.Stop()
			FailError(t, err)

			currentTest = nil
		}()

		// run here
		for _, test := range tests {
			if test.checkRunForVersion(serverInfo.Version) {
				// update current
				currentTest = &test

				// setup
				err = test.ResetForTest(server)
				FailError(t, err)

				testFunc := func(t *testing.T) {
					test.Func(t, &test)
				}

				t.Run(test.Name, testFunc)
			}
		}
	}

	t.Run(serverInfo.Name, testFunc)
}

func TestLocalMain(t *testing.T) {
	t.Log("Running all test cases...")

	tests := []Test{}

	// Add all test cases here
	tests = append(tests, getUtilEncodingTest())
	tests = append(tests, getTypeDurationTest())
	tests = append(tests, getUtilErrorTest())
	tests = append(tests, getUtilEnvironmentTest())
	tests = append(tests, getUtilPasswordObfuscationTest())
	tests = append(tests, getLowlevelConnectionTest())
	tests = append(tests, getLowlevelSessionTest())
	tests = append(tests, getLowlevelProcessTest())
	tests = append(tests, getLowlevelUserTest())
	tests = append(tests, getLowlevelLockTest())
	tests = append(tests, getLowlevelFileTransferTest())
	tests = append(tests, getHighlevelFilesystemTest())
	tests = append(tests, getHighlevelFilesystemCacheTest())
	tests = append(tests, getHighlevelFileTransferTest())
	tests = append(tests, getHighlevelTicketTest())

	// local test servers
	for _, serverInfo := range server.GetTestIRODSServerInfos() {
		server := server.NewIRODSServer(serverInfo)

		testMainForServer(t, server, tests)
	}
}

func TestProductionMain(t *testing.T) {
	/*
		t.Log("Running all test cases...")

		tests := []Test{}

		// Add all test cases here
		//tests = append(tests, getLowlevelConnectionTest())
		tests = append(tests, getLowlevelSessionTest())

		// production server
		for _, ver := range server.GetProductionIRODSVersions() {
			testMainForVersion(t, ver, true, tests)
		}
	*/
}
