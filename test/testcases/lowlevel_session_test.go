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
	server := test.GetCurrentServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection(true)
	FailError(t, err)

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	collection, err := fs.GetCollection(conn, homeDir)
	FailError(t, err)

	assert.Equal(t, homeDir, collection.Path, "collection path from GetCollection should match requested path")
	assert.NotEmpty(t, collection.ID, "collection should have assigned ID from iRODS")

	err = sess.ReturnConnection(conn)
	FailError(t, err)
}

func testMaxConnectionsShared(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	connections := []*connection.IRODSConnection{}

	for i := 0; i < 15; i++ {
		// allow shared
		conn, err := sess.AcquireConnection(true)
		FailError(t, err)

		collection, err := fs.GetCollection(conn, homeDir)
		FailError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homeDir, collection.Path, "shared collection path should match home directory")
		assert.NotEmpty(t, collection.ID, "collection should have assigned ID")
	}

	connMap := map[*connection.IRODSConnection]bool{}
	for _, conn := range connections {
		connMap[conn] = true
	}

	assert.Equal(t, 15, len(connections), "requested connections list should contain 15 entries")
	assert.Equal(t, sess.GetConfig().ConnectionMaxNumber, len(connMap), "unique shared connections should not exceed pool max")
	assert.Equal(t, sess.GetConfig().ConnectionMaxNumber, sess.GetOpenConnections(), "session open connections should match unique connections returned")

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		FailError(t, err)
	}
}

func testMaxConnectionsNotShared(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	config := sess.GetConfig()
	connections, err := sess.AcquireConnectionsMulti(15, false)
	assert.Error(t, err, "acquiring 15 exclusive connections should fail when max is less")
	assert.True(t, types.IsConnectionPoolFullError(err), "error should indicate connection pool exhausted")

	assert.LessOrEqual(t, len(connections), config.ConnectionMaxNumber, "exclusive connections acquired should not exceed configured max")
	assert.Equal(t, config.ConnectionMaxNumber, sess.GetOpenConnections(), "session should have max connections in use after exhaustion")

	for _, conn := range connections {
		collection, err := fs.GetCollection(conn, homeDir)
		FailError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homeDir, collection.Path, "collection accessed via exhausted pool should match home dir")
		assert.NotEmpty(t, collection.ID, "collection accessed via exhausted pool should have ID")
	}

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		FailError(t, err)
	}
}

func testConnectionMetrics(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	sess, err := server.GetSession()
	FailError(t, err)
	defer sess.Release()

	sessionConfig := sess.GetConfig()

	metrics := sess.GetMetrics()
	if metrics != nil {
		assert.Equal(t, uint64(sess.GetConfig().ConnectionInitNumber), metrics.GetConnectionsOpened(), "initial opened connections should equal init number")
		assert.Equal(t, uint64(0), metrics.GetConnectionsOccupied(), "initially occupied connections should be zero")
	}

	homeDir, err := test.GetTestHomeDir()
	FailError(t, err)

	connections := []*connection.IRODSConnection{}

	for i := 0; i < 15; i++ {
		conn, err := sess.AcquireConnection(true)
		FailError(t, err)

		collection, err := fs.GetCollection(conn, homeDir)
		FailError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homeDir, collection.Path, "collection path during metrics test should match home dir")
		assert.NotEmpty(t, collection.ID, "collection ID during metrics test should be assigned")
	}

	assert.Equal(t, sessionConfig.ConnectionMaxNumber, sess.GetOpenConnections(), "open connections should reach max when fully acquired")
	assert.Equal(t, uint64(sessionConfig.ConnectionMaxNumber), metrics.GetConnectionsOpened(), "metrics should report max connections opened")
	assert.Equal(t, uint64(sessionConfig.ConnectionMaxNumber), metrics.GetConnectionsOccupied(), "metrics should report all max connections in use")

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		FailError(t, err)
	}

	assert.Equal(t, uint64(sessionConfig.ConnectionMaxIdleNumber), metrics.GetConnectionsOpened(), "metrics after return should show idle pool size")
	assert.Equal(t, uint64(0), metrics.GetConnectionsOccupied(), "metrics after return should show zero occupied connections")
}
