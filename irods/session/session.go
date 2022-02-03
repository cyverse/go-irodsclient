package session

import (
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	account              *types.IRODSAccount
	config               *IRODSSessionConfig
	connectionPool       *ConnectionPool
	startNewTransaction  bool
	poormansRollbackFail bool
	transferMetrics      types.TransferMetrics
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
		Lifespan:         config.ConnectionLifespan,
		IdleTimeout:      config.ConnectionIdleTimeout,
		OperationTimeout: config.OperationTimeout,
	}

	pool, err := NewConnectionPool(&poolConfig)
	if err != nil {
		logger.Errorf("cannot create a new connection pool - %v", err)
		return nil, err
	}

	// transaction
	sess.startNewTransaction = config.StartNewTransaction
	sess.poormansRollbackFail = false

	// when the user is anonymous, we cannot use transaction since we don't have access to home dir
	if sess.account.ClientUser == "anonymous" {
		sess.startNewTransaction = false
		sess.poormansRollbackFail = true
	}

	// test if it can create a new transaction
	if sess.startNewTransaction {
		logger.Debugf("testing perform poor man rollback")

		conn, _, err := pool.Get()
		if err != nil {
			logger.Errorf("failed to get a test connection - %v", err)
			pool.Release()
			return nil, err
		}

		err = conn.PoorMansRollback()
		if err != nil {
			logger.Warnf("could not perform poor man rollback for the connection, disabling poor mans rollback - %v", err)
			pool.Discard(conn)
			sess.poormansRollbackFail = true
		} else {
			logger.Debugf("using poor man rollback for the connection")
			pool.Return(conn)
		}
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
	conn, isNewConn, err := sess.connectionPool.Get()
	if err != nil {
		logger.Errorf("failed to get an idle connection - %v", err)
		return nil, err
	}

	if sess.startNewTransaction && !isNewConn {
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
		if sess.poormansRollbackFail {
			// always use new connection
			sess.connectionPool.Discard(conn)

			conn, err = sess.connectionPool.GetNew()
			if err != nil {
				logger.Errorf("failed to get a new connection - %v", err)
				return nil, err
			}
		} else {
			err = conn.PoorMansRollback()
			if err != nil {
				logger.Warnf("could not perform poor man rollback for the connection, creating a new connection - %v", err)
				sess.connectionPool.Discard(conn)
				sess.poormansRollbackFail = true

				conn, err = sess.connectionPool.GetNew()
				if err != nil {
					logger.Errorf("failed to get a new connection - %v", err)
					return nil, err
				}
			}
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

	// add up metrics
	metrics := conn.GetTransferMetrics()
	sess.sumUpMetrics(&metrics)
	conn.ClearTransferMetrics()

	if sess.startNewTransaction && sess.poormansRollbackFail {
		// discard, since we cannot reuse the connection
		sess.connectionPool.Discard(conn)
		return nil
	}

	err := sess.connectionPool.Return(conn)
	if err != nil {
		logger.Errorf("failed to return an idle connection - %v", err)
		return err
	}
	return nil
}

// DiscardConnection discards a connection
func (sess *IRODSSession) DiscardConnection(conn *connection.IRODSConnection) error {

	// add up metrics
	metrics := conn.GetTransferMetrics()
	sess.sumUpMetrics(&metrics)
	conn.ClearTransferMetrics()

	sess.connectionPool.Discard(conn)
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

// sumUpMetrics adds up transfer metrics
func (sess *IRODSSession) sumUpMetrics(metrics *types.TransferMetrics) {
	if metrics == nil {
		return
	}

	sess.transferMetrics.BytesReceived += metrics.BytesReceived
	sess.transferMetrics.BytesSent += metrics.BytesSent

	sess.transferMetrics.CollectionIO.Stat += metrics.CollectionIO.Stat
	sess.transferMetrics.CollectionIO.List += metrics.CollectionIO.List
	sess.transferMetrics.CollectionIO.Create += metrics.CollectionIO.Create
	sess.transferMetrics.CollectionIO.Delete += metrics.CollectionIO.Delete
	sess.transferMetrics.CollectionIO.Rename += metrics.CollectionIO.Rename
	sess.transferMetrics.CollectionIO.Meta += metrics.CollectionIO.Meta

	sess.transferMetrics.DataObjectIO.Stat += metrics.DataObjectIO.Stat
	sess.transferMetrics.DataObjectIO.Create += metrics.DataObjectIO.Create
	sess.transferMetrics.DataObjectIO.Delete += metrics.DataObjectIO.Delete
	sess.transferMetrics.DataObjectIO.Rename += metrics.DataObjectIO.Rename
	sess.transferMetrics.DataObjectIO.Meta += metrics.DataObjectIO.Meta
	sess.transferMetrics.DataObjectIO.Read += metrics.DataObjectIO.Read
	sess.transferMetrics.DataObjectIO.Write += metrics.DataObjectIO.Write
}

// GetTransferMetrics returns transfer metrics
func (sess *IRODSSession) GetTransferMetrics() types.TransferMetrics {
	return sess.transferMetrics
}
