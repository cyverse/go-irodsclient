package testcases

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func getLowlevelSessionTest() Test {
	return Test{
		Name: "Lowlevel_Session",
		Func: lowlevelSessionTest,
	}
}

func lowlevelSessionTest(t *testing.T, test *Test) {
	t.Run("Session", testSession)
	t.Run("testMaxConnectionsShared", testMaxConnectionsShared)
	t.Run("testMaxConnectionsNotShared", testMaxConnectionsNotShared)
	t.Run("ConnectionMetrics", testConnectionMetrics)
}

func testSession(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection(true)
	FailError(t, err)

	homeDir := test.GetTestHomeDir()

	collection, err := fs.GetCollection(conn, homeDir)
	FailError(t, err)

	assert.Equal(t, homeDir, collection.Path)
	assert.NotEmpty(t, collection.ID)

	err = sess.ReturnConnection(conn)
	FailError(t, err)
}

func testMaxConnectionsShared(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	homeDir := test.GetTestHomeDir()

	connections := []*connection.IRODSConnection{}

	for i := 0; i < 15; i++ {
		conn, err := sess.AcquireConnection(true)
		FailError(t, err)

		collection, err := fs.GetCollection(conn, homeDir)
		FailError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homeDir, collection.Path)
		assert.NotEmpty(t, collection.ID)
	}

	assert.Equal(t, sess.GetConfig().ConnectionMaxNumber, sess.GetConnectionsTotal())

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		FailError(t, err)
	}
}

func testMaxConnectionsNotShared(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	homeDir := test.GetTestHomeDir()

	config := sess.GetConfig()
	connections, err := sess.AcquireConnectionsMulti(15, false)
	if 15 < config.ConnectionMaxNumber {
		FailError(t, err)
	} else {
		assert.Error(t, err)
		assert.True(t, types.IsConnectionPoolFullError(err))
	}

	assert.Equal(t, config.ConnectionMaxNumber, len(connections))
	assert.Equal(t, config.ConnectionMaxNumber, sess.GetConnectionsTotal())

	for _, conn := range connections {
		collection, err := fs.GetCollection(conn, homeDir)
		FailError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homeDir, collection.Path)
		assert.NotEmpty(t, collection.ID)
	}

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		FailError(t, err)
	}
}

func testConnectionMetrics(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	sessionConfig := sess.GetConfig()

	metrics := sess.GetMetrics()
	if metrics != nil {
		assert.Equal(t, uint64(sess.GetConfig().ConnectionInitNumber), metrics.GetConnectionsOpened())
		assert.Equal(t, uint64(0), metrics.GetConnectionsOccupied())
	}

	homeDir := test.GetTestHomeDir()

	connections := []*connection.IRODSConnection{}

	for i := 0; i < 15; i++ {
		conn, err := sess.AcquireConnection(true)
		FailError(t, err)

		collection, err := fs.GetCollection(conn, homeDir)
		FailError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homeDir, collection.Path)
		assert.NotEmpty(t, collection.ID)
	}

	assert.Equal(t, sessionConfig.ConnectionMaxNumber, sess.GetConnectionsTotal())
	assert.Equal(t, uint64(sessionConfig.ConnectionMaxNumber), metrics.GetConnectionsOpened())
	assert.Equal(t, uint64(sessionConfig.ConnectionMaxNumber), metrics.GetConnectionsOccupied())

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		FailError(t, err)
	}

	assert.Equal(t, uint64(sessionConfig.ConnectionMaxIdleNumber), metrics.GetConnectionsOpened())
	assert.Equal(t, uint64(0), metrics.GetConnectionsOccupied())
}
