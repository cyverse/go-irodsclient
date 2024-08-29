package testcases

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"testing"

	irods_fs "github.com/cyverse/go-irodsclient/fs"
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

	err := server.StartServer()
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	account, err = server.GetLocalAccount()
	if err != nil {
		logger.Error(err)
		panic(err)
	}
}

func setup_existing() {
	logger := log.WithFields(log.Fields{
		"package":  "test",
		"function": "setup_existing",
	})

	var err error
	account, err = server.GetLocalAccount()
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

	// empty global variables
	account = nil
	testFiles = []string{}
	testDirs = []string{}

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

func failError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%+v", err)
		t.FailNow()
	}
}

func GetTestApplicationName() string {
	return "go-irodsclient-test"
}

func GetTestFileSystemConfig() *irods_fs.FileSystemConfig {
	fsConfig := irods_fs.NewFileSystemConfig(GetTestApplicationName())
	fsConfig.AddressResolver = server.AddressResolver
	return fsConfig
}

func GetTestSessionConfig() *session.IRODSSessionConfig {
	fsConfig := GetTestFileSystemConfig()
	return fsConfig.ToIOSessionConfig()
}

func makeFixedContentTestDataBuf(size int64) []byte {
	testval := "abcdefghijklmnopqrstuvwxyz"

	// fill
	dataBuf := make([]byte, size)
	writeLen := 0
	for writeLen < len(dataBuf) {
		copy(dataBuf[writeLen:], testval)
		writeLen += len(testval)
	}
	return dataBuf
}

func makeRandomContentTestDataBuf(size int64) []byte {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	// fill
	dataBuf := make([]byte, size)
	for i := range dataBuf {
		dataBuf[i] = letters[rand.Intn(len(letters))]
	}
	return dataBuf
}

func createLocalTestFile(name string, size int64) (string, error) {
	// fill
	dataBuf := makeFixedContentTestDataBuf(1024)

	f, err := os.CreateTemp("", name)
	if err != nil {
		return "", err
	}

	tempPath := f.Name()

	defer f.Close()

	totalWriteLen := int64(0)
	for totalWriteLen < size {
		writeLen, err := f.Write(dataBuf)
		if err != nil {
			os.Remove(tempPath)
			return "", err
		}

		totalWriteLen += int64(writeLen)
	}

	return tempPath, nil
}

func getHomeDir(testID string) string {
	account := GetTestAccount()
	return fmt.Sprintf("/%s/home/%s/%s", account.ClientZone, account.ClientUser, testID)
}

func makeHomeDir(t *testing.T, testID string) {
	account := GetTestAccount()
	account.ClientServerNegotiation = false

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(testID)
	err = fs.CreateCollection(conn, homedir, true)
	failError(t, err)
}

func prepareSamples(t *testing.T, testID string) {
	account := GetTestAccount()
	account.ClientServerNegotiation = false

	sessionConfig := GetTestSessionConfig()

	sess, err := session.NewIRODSSession(account, sessionConfig)
	failError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection()
	failError(t, err)

	homedir := getHomeDir(testID)

	collection, err := fs.GetCollection(conn, homedir)
	failError(t, err)
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

		err = os.WriteFile(filename, buf, 0666)
		failError(t, err)

		irodsPath := homedir + "/" + filename
		err = fs.UploadDataObject(sess, filename, irodsPath, "", false, nil, nil)
		failError(t, err)

		conn, err := sess.AcquireConnection()
		failError(t, err)

		sha1sum := sha1.New()
		_, err = sha1sum.Write([]byte(irodsPath))
		failError(t, err)

		hashBytes := sha1sum.Sum(nil)
		hashString := hex.EncodeToString(hashBytes)

		err = fs.AddDataObjectMeta(conn, irodsPath, &types.IRODSMeta{
			Name:  "hash",
			Value: hashString,
		})
		failError(t, err)

		err = fs.AddDataObjectMeta(conn, irodsPath, &types.IRODSMeta{
			Name:  "tag",
			Value: "test",
		})
		failError(t, err)

		sess.ReturnConnection(conn)

		testFiles = append(testFiles, irodsPath)

		err = os.Remove(filename)
		failError(t, err)
	}

	// create random directories
	for i := 0; i < numDirs; i++ {
		dirname := fmt.Sprintf("test_dir_%d", i)

		irodsPath := homedir + "/" + dirname
		err = fs.CreateCollection(conn, irodsPath, true)
		failError(t, err)

		testDirs = append(testDirs, irodsPath)
	}

	err = sess.ReturnConnection(conn)
	failError(t, err)
}
