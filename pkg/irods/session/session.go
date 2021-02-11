package session

import (
	"log"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
	"github.com/silenceper/pool"
)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	Account          *types.IRODSAccount
	OperationTimeout time.Duration
	IdleTimeout      time.Duration
	ApplicationName  string
	ConnectionMax    int
	ConnectionPool   pool.Pool
}

// NewIRODSSession create a IRODSSession
func NewIRODSSession(account *types.IRODSAccount, operationTimeout time.Duration, idleTimeout time.Duration, connectionMax int, applicationName string) *IRODSSession {
	sess := IRODSSession{
		Account:          account,
		OperationTimeout: operationTimeout,
		IdleTimeout:      idleTimeout,
		ApplicationName:  applicationName,
		ConnectionMax:    connectionMax,
	}

	initCap := 1
	maxIdle := 1
	if connectionMax >= 15 {
		maxIdle = 10
	} else if connectionMax >= 5 {
		maxIdle = 4
	}

	poolConfig := pool.Config{
		InitialCap:  initCap,
		MaxIdle:     maxIdle,
		MaxCap:      sess.ConnectionMax,
		Factory:     sess.connOpen,
		Close:       sess.connClose,
		IdleTimeout: sess.IdleTimeout,
	}

	p, err := pool.NewChannelPool(&poolConfig)
	if err != nil {
		util.LogErrorf("Cannot create a new connection pool - %v", err)
		log.Panic(err)
	}

	sess.ConnectionPool = p
	return &sess
}

// NewIRODSSessionWithDefault create a IRODSSession with a default settings
func NewIRODSSessionWithDefault(account *types.IRODSAccount, applicationName string) *IRODSSession {
	sess := IRODSSession{
		Account:          account,
		OperationTimeout: 5 * time.Minute,
		IdleTimeout:      5 * time.Minute,
		ApplicationName:  applicationName,
		ConnectionMax:    20,
	}

	poolConfig := pool.Config{
		InitialCap:  1,
		MaxIdle:     10,
		MaxCap:      sess.ConnectionMax,
		Factory:     sess.connOpen,
		Close:       sess.connClose,
		IdleTimeout: sess.IdleTimeout,
	}

	p, err := pool.NewChannelPool(&poolConfig)
	if err != nil {
		util.LogErrorf("Cannot create a new connection pool - %v", err)
		log.Panic(err)
	}

	sess.ConnectionPool = p
	return &sess
}

func (sess *IRODSSession) connOpen() (interface{}, error) {
	// create a conenction
	conn := connection.NewIRODSConnection(sess.Account, sess.OperationTimeout, sess.ApplicationName)
	err := conn.Connect()
	if err != nil {
		util.LogErrorf("Could not connect - %v", err)
		return nil, err
	}
	return conn, nil
}

func (sess *IRODSSession) connClose(v interface{}) error {
	// close a conenction
	conn := v.(*connection.IRODSConnection)
	return conn.Disconnect()
}

// AcquireConnection returns an idle connection
func (sess *IRODSSession) AcquireConnection() (*connection.IRODSConnection, error) {
	// close a conenction
	v, err := sess.ConnectionPool.Get()
	if err != nil {
		util.LogErrorf("Could not get an idle connection - %v", err)
		return nil, err
	}

	conn := v.(*connection.IRODSConnection)
	return conn, nil
}

// ReturnConnection returns an idle connection
func (sess *IRODSSession) ReturnConnection(conn *connection.IRODSConnection) error {
	err := sess.ConnectionPool.Put(conn)
	if err != nil {
		util.LogErrorf("Could not return an idle connection - %v", err)
		return err
	}
	return nil
}

// Release releases all connections
func (sess *IRODSSession) Release() {
	sess.ConnectionPool.Release()
}

// Connections returns the number of connections in the pool
func (sess *IRODSSession) Connections() int {
	return sess.ConnectionPool.Len()
}
