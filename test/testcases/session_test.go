package testcases

import (
	"testing"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
)

var (
	fsSessionTestID = xid.New().String()
)

func TestSession(t *testing.T) {
	setup()
	defer shutdown()

	makeHomeDir(t, fsSessionTestID)

	t.Run("test Session", testSession)
	t.Run("test many Connections", testManyConnections)
}

func testSession(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationDontCare

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	assert.NoError(t, err)
	defer sess.Release()

	// first
	conn, err := sess.AcquireConnection()
	assert.NoError(t, err)

	homedir := getHomeDir(fsSessionTestID)

	collection, err := fs.GetCollection(conn, homedir)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	assert.Equal(t, homedir, collection.Path)
	assert.NotEmpty(t, collection.ID)

	err = sess.ReturnConnection(conn)
	assert.NoError(t, err)
}

func testManyConnections(t *testing.T) {
	account := GetTestAccount()

	account.ClientServerNegotiation = false
	account.CSNegotiationPolicy = types.CSNegotiationDontCare

	sessionConfig := session.NewIRODSSessionConfigWithDefault("go-irodsclient-test")

	sess, err := session.NewIRODSSession(account, sessionConfig)
	assert.NoError(t, err)
	defer sess.Release()

	homedir := getHomeDir(fsSessionTestID)

	connections := []*connection.IRODSConnection{}

	for i := 0; i < 30; i++ {
		conn, err := sess.AcquireConnection()
		assert.NoError(t, err)

		collection, err := fs.GetCollection(conn, homedir)
		assert.NoError(t, err)

		connections = append(connections, conn)

		assert.Equal(t, homedir, collection.Path)
		assert.NotEmpty(t, collection.ID)
	}

	for _, conn := range connections {
		err = sess.ReturnConnection(conn)
		assert.NoError(t, err)
	}
}
