package session

import (
	"log"

	"github.com/iychoi/go-irodsclient/irods/connection"
	"github.com/iychoi/go-irodsclient/irods/types"
	"github.com/iychoi/go-irodsclient/irods/util"
	"github.com/silenceper/pool"
)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	Account        *types.IRODSAccount
	Config         *IRODSSessionConfig
	ConnectionPool pool.Pool
}

// NewIRODSSession create a IRODSSession
func NewIRODSSession(account *types.IRODSAccount, config *IRODSSessionConfig) *IRODSSession {
	sess := IRODSSession{
		Account: account,
		Config:  config,
	}

	poolConfig := pool.Config{
		InitialCap:  config.ConnectionInitNumber,
		MaxIdle:     config.ConnectionMaxIdle,
		MaxCap:      config.ConnectionMax,
		Factory:     sess.connOpen,
		Close:       sess.connClose,
		IdleTimeout: config.IdleTimeout,
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
	conn := connection.NewIRODSConnection(sess.Account, sess.Config.OperationTimeout, sess.Config.ApplicationName)
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
