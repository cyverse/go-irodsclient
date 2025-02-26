package testcases

import (
	"path"
	"testing"

	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/rs/xid"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

type Test struct {
	Name         string
	Func         func(t *testing.T, test *Test)
	testHomeName string
	Versions     []server.IRODSTestServerVersion // if empty, run for all versions
	server       *server.IRODSTestServer
}

func (test *Test) GetTestHomeDir() string {
	return path.Join(test.server.GetHomeDir(), test.testHomeName)
}

func (test *Test) MakeTestHomeDir() error {
	logger := log.WithFields(log.Fields{
		"package":  "testcases",
		"struct":   "Test",
		"function": "MakeTestHomeDir",
	})

	fs, err := test.server.GetFilesystem()
	if err != nil {
		return xerrors.Errorf("failed to create a new filesystem: %w", err)
	}
	defer fs.Release()

	homedir := test.GetTestHomeDir()

	err = fs.MakeDir(homedir, true)
	if err != nil {
		return xerrors.Errorf("failed to make a home directory %q: %w", homedir, err)
	}

	logger.Infof("Created test home directory: %s", homedir)

	return nil
}

func (test *Test) GetServer() *server.IRODSTestServer {
	return test.server
}

func checkRunForVersion(testFunc Test, version server.IRODSTestServerVersion) bool {
	if len(testFunc.Versions) == 0 {
		return true
	}

	for _, v := range testFunc.Versions {
		if v == version {
			return true
		}
	}

	return false
}

func FailError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%+v", err)
		t.FailNow()
	}
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

func testMainForVersion(t *testing.T, ver server.IRODSTestServerVersion, tests []Test) {
	t.Logf("Testing for version: %s", ver)

	verFunc := func(t *testing.T) {
		irodsServer, err := server.NewIRODSServer(ver)
		FailError(t, err)

		err = irodsServer.Start()
		FailError(t, err)

		defer func() {
			//time.Sleep(1 * time.Hour)

			if irodsServer != nil {
				irodsServer.Stop()
				currentTest = nil
			}
		}()

		// run here
		for _, test := range tests {
			if checkRunForVersion(test, ver) {
				currentTest = &test

				// setup
				test.testHomeName = makeTestUUID()
				test.server = irodsServer

				// create home directory
				err = test.MakeTestHomeDir()
				FailError(t, err)

				testFunc := func(t *testing.T) {
					test.Func(t, &test)
				}

				t.Run(test.Name, testFunc)
			}
		}
	}

	t.Run(string(ver), verFunc)
}

func TestMain(t *testing.T) {
	t.Log("Running all test cases...")

	versions := server.GetIRODSVersions()
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
	tests = append(tests, getLowlevelUpdownTest())
	tests = append(tests, getHighlevelFilesystemTest())
	tests = append(tests, getHighlevelFilesystemCacheTest())

	for _, ver := range versions {
		testMainForVersion(t, ver, tests)
	}
}
