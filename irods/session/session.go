package session

import (
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/silenceper/pool"
)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	Account        *types.IRODSAccount
	Config         *IRODSSessionConfig
	ConnectionPool pool.Pool
}

// NewIRODSSession create a IRODSSession
func NewIRODSSession(account *types.IRODSAccount, config *IRODSSessionConfig) (*IRODSSession, error) {
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
		return nil, err
	}

	sess.ConnectionPool = p
	return &sess, nil
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

	if sess.Config.StartNewTransaction {
		// Each irods connection automatically starts a database transaction at initial setup.
		// All queries against irods using a connection will give results corresponding to the time
		// the connection was made, or since the last change using the very same connection.
		// I.e. if connections 1 and 2 are created at the same time, and connection 1 does an update,
		// connection 2 will not see it until any other change is made using connection 2.
		// The connection we get here from the connection pool might be old, and we might miss
		// changes that happened in parallel connections. We fix this by doing a rollback operation,
		// which will do nothing to the database (there are no operations staged for commit/rollback),
		// but which will close the current transaction and starts a new one - refreshing the view for
		// future queries.
		err = conn.PoorMansRollback()
		if err != nil {
			util.LogErrorf("Could not perform poor man rollback for the connection - %v", err)
			_ = sess.ReturnConnection(conn)
			return nil, err
		}
	}

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
