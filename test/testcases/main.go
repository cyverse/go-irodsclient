package testcases

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/test/server"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var (
	account   *types.IRODSAccount
	testFiles []string
	testDirs  []string
)

func setup() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setup",
	})

	var err error
	account, err = server.StartServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func shutdown() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "shutdown",
	})

	err := server.StopServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func GetTestAccount() *types.IRODSAccount {
	accountCpy := *account
	return &accountCpy
}

func GetTestFiles() []string {
	return testFiles
}

func GetTestDirs() []string {
	return testDirs
}

func testPrepareSamples(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	assert.NoError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection()
	assert.NoError(t, err)

	homedir := fmt.Sprintf("/%s/home/%s", account.ClientZone, account.ClientUser)

	collection, err := fs.GetCollection(conn, homedir)
	assert.NoError(t, err)
	assert.NotEmpty(t, collection.ID)

	testval := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // 62

	numFiles := 10
	numDirs := 10
	baseRecordNum := 100

	// create random files
	for i := 0; i < numFiles; i++ {
		recordNum := i * baseRecordNum
		filesize := recordNum * len(testval)
		filename := fmt.Sprintf("test_file_%d.bin", filesize)
		buf := make([]byte, filesize)

		for j := 0; j < recordNum; j++ {
			startIdx := j * len(testval)
			copy(buf[startIdx:startIdx+len(testval)], testval)
		}

		err = ioutil.WriteFile(filename, buf, 0666)
		assert.NoError(t, err)

		irodsPath := homedir + "/" + filename
		err = fs.UploadDataObject(sess, filename, irodsPath, "", false)
		assert.NoError(t, err)

		testFiles = append(testFiles, irodsPath)

		err = os.Remove(filename)
		assert.NoError(t, err)
	}

	// create random directories
	for i := 0; i < numDirs; i++ {
		dirname := fmt.Sprintf("test_dir_%d", i)

		irodsPath := homedir + "/" + dirname
		err = fs.CreateCollection(conn, irodsPath, true)
		assert.NoError(t, err)

		testDirs = append(testDirs, irodsPath)
	}

	err = sess.ReturnConnection(conn)
	assert.NoError(t, err)
}
