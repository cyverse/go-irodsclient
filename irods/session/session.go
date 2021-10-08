package session

import (
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	account        *types.IRODSAccount
	config         *IRODSSessionConfig
	connectionPool *ConnectionPool
}

// NewIRODSSession create a IRODSSession
func NewIRODSSession(account *types.IRODSAccount, config *IRODSSessionConfig) (*IRODSSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"function": "NewIRODSSession",
	})

	sess := IRODSSession{
		account: account,
		config:  config,
	}

	poolConfig := ConnectionPoolConfig{
		Account:          account,
		ApplicationName:  config.ApplicationName,
		InitialCap:       config.ConnectionInitNumber,
		MaxIdle:          config.ConnectionMaxIdle,
		MaxCap:           config.ConnectionMax,
		IdleTimeout:      config.IdleTimeout,
		OperationTimeout: config.OperationTimeout,
	}

	pool, err := NewConnectionPool(&poolConfig)
	if err != nil {
		logger.Errorf("cannot create a new connection pool - %v", err)
		return nil, err
	}

	sess.connectionPool = pool
	return &sess, nil
}

// AcquireConnection returns an idle connection
func (sess *IRODSSession) AcquireConnection() (*connection.IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "AcquireConnection",
	})

	// get a conenction
	conn, err := sess.connectionPool.Get()
	if err != nil {
		logger.Errorf("failed to get an idle connection - %v", err)
		return nil, err
	}

	if len(sess.account.Ticket) > 0 {
		// when ticket is used, we cannot use transaction since we don't have access to home dir
		return conn, nil
	}

	if sess.config.StartNewTransaction {
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
			logger.Errorf("could not perform poor man rollback for the connection - %v", err)
			_ = sess.ReturnConnection(conn)
			return nil, err
		}
	}

	return conn, nil
}

// ReturnConnection returns an idle connection
func (sess *IRODSSession) ReturnConnection(conn *connection.IRODSConnection) error {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "ReturnConnection",
	})

	err := sess.connectionPool.Return(conn)
	if err != nil {
		logger.Errorf("failed to return an idle connection - %v", err)
		return err
	}
	return nil
}

// Release releases all connections
func (sess *IRODSSession) Release() {
	sess.connectionPool.Release()
}

// Connections returns the number of connections in the pool
func (sess *IRODSSession) Connections() int {
	return sess.connectionPool.OpenConnections()
}
