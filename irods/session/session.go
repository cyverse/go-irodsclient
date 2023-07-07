package session

import (
	"sync"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	account               *types.IRODSAccount
	config                *IRODSSessionConfig
	connectionPool        *ConnectionPool
	sharedConnections     map[*connection.IRODSConnection]int
	startNewTransaction   bool
	commitFail            bool
	poormansRollbackFail  bool
	supportParallelUpload bool
	metrics               metrics.IRODSMetrics
	mutex                 sync.Mutex
}

// NewIRODSSession create a IRODSSession
func NewIRODSSession(account *types.IRODSAccount, config *IRODSSessionConfig) (*IRODSSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"function": "NewIRODSSession",
	})

	sess := IRODSSession{
		account:           account,
		config:            config,
		sharedConnections: map[*connection.IRODSConnection]int{},

		// transaction
		startNewTransaction:   config.StartNewTransaction,
		commitFail:            false,
		poormansRollbackFail:  false,
		supportParallelUpload: false,

		metrics: metrics.IRODSMetrics{},

		mutex: sync.Mutex{},
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
		TcpBufferSize:    config.TcpBufferSize,
	}

	pool, err := NewConnectionPool(&poolConfig, &sess.metrics)
	if err != nil {
		return nil, xerrors.Errorf("failed to create connection pool: %w", err)
	}
	sess.connectionPool = pool

	// check connection
	logger.Debugf("testing connection")
	err = sess.checkConnection()
	if err != nil {
		return nil, xerrors.Errorf("failed to check transaction: %w", err)
	}

	return &sess, nil
}

// checkConnection checks connection
func (sess *IRODSSession) checkConnection() error {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"function": "checkConnection",
	})

	conn, _, err := sess.connectionPool.Get()
	if err != nil {
		return xerrors.Errorf("failed to get a test connection: %w", err)
	}

	conn.Lock()

	// check parallel upload
	sess.supportParallelUpload = conn.SupportParallelUpload()
	logger.Debugf("support parallel upload: %t", sess.supportParallelUpload)

	// check transaction
	// when the user is anonymous, we cannot use transaction since we don't have access to home dir
	if sess.account.ClientUser == "anonymous" {
		sess.commitFail = true
		sess.poormansRollbackFail = true
	}

	conn.Unlock()

	sess.connectionPool.Return(conn)
	return nil
}

// GetConfig returns a configuration
func (sess *IRODSSession) GetConfig() *IRODSSessionConfig {
	return sess.config
}

// GetAccount returns an account
func (sess *IRODSSession) GetAccount() *types.IRODSAccount {
	return sess.account
}

// endTransaction ends transaction
func (sess *IRODSSession) endTransaction(conn *connection.IRODSConnection) error {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "endTransaction",
	})

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

	if !sess.startNewTransaction {
		// done
		return nil
	}

	if !sess.commitFail {
		commitErr := conn.Commit()
		if commitErr == nil {
			return nil
		}

		// failed to commit
		sess.commitFail = true
		logger.WithError(commitErr).Debug("failed to commit transaction")
	}

	if !sess.poormansRollbackFail {
		// try rollback
		rollbackErr := conn.PoorMansRollback()
		if rollbackErr == nil {
			return nil
		}

		// failed to rollback
		sess.poormansRollbackFail = true
		logger.WithError(rollbackErr).Debug("failed to rollback (poorman) transaction")
	}

	return xerrors.Errorf("failed to commit/rollback transaction")
}

// AcquireConnection returns an idle connection
func (sess *IRODSSession) AcquireConnection() (*connection.IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "AcquireConnection",
	})

	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// check if there are available connections in the pool
	if sess.connectionPool.AvailableConnections() > 0 {
		// try to get it from the pool
		conn, _, err := sess.connectionPool.Get()
		// ignore error this happens when connections in the pool are all occupied
		if err != nil {
			if IsConnectionPoolFullError(err) {
				logger.WithError(err).Debug("failed to get a connection from the pool, the pool is full")
				// fall below
			}
		} else {
			// put to share
			if shares, ok := sess.sharedConnections[conn]; ok {
				shares++
				sess.sharedConnections[conn] = shares
			} else {
				sess.sharedConnections[conn] = 1
			}

			return conn, nil
		}
	}

	// failed to get connection from pool
	// find a connection from shared connection list that has minimum share count
	logger.Debug("Share an in-use connection as it cannot create a new connection")
	minShare := 0
	var minShareConn *connection.IRODSConnection
	for sharedConn, shareCount := range sess.sharedConnections {
		if minShare == 0 || shareCount < minShare {
			minShare = shareCount
			minShareConn = sharedConn
		}

		if minShare == 1 {
			// can't be smaller
			break
		}
	}

	if minShareConn == nil {
		sess.metrics.IncreaseCounterForConnectionPoolFailures(1)
		return nil, xerrors.Errorf("failed to get a shared connection, too many connections created")
	}

	// update
	minShare++
	sess.sharedConnections[minShareConn] = minShare

	return minShareConn, nil
}

// AcquireConnectionsMulti returns idle connections
func (sess *IRODSSession) AcquireConnectionsMulti(number int) ([]*connection.IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "AcquireConnectionsMulti",
	})

	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	connections := map[*connection.IRODSConnection]bool{}

	// check if there are available connections in the pool
	for i := 0; i < number; i++ {
		if sess.connectionPool.AvailableConnections() > 0 {
			// try to get it from the pool
			conn, _, err := sess.connectionPool.Get()
			if err != nil {
				if IsConnectionPoolFullError(err) {
					logger.WithError(err).Debug("failed to get a connection from the pool, the pool is full")
				}

				// fall through
				break
			} else {
				connections[conn] = true

				// put to share
				if shares, ok := sess.sharedConnections[conn]; ok {
					shares++
					sess.sharedConnections[conn] = shares
				} else {
					sess.sharedConnections[conn] = 1
				}
			}
		} else {
			break
		}
	}

	connectionsInNeed := number - len(connections)

	// failed to get connection from pool
	// find a connection from shared connection
	logger.Debug("Share an in-use connection as it cannot create a new connection")
	for connectionsInNeed > 0 {
		for sharedConn, shareCount := range sess.sharedConnections {
			shareCount++

			connections[sharedConn] = true
			sess.sharedConnections[sharedConn] = shareCount

			connectionsInNeed--
			if connectionsInNeed <= 0 {
				break
			}
		}
	}

	acquiredConnections := []*connection.IRODSConnection{}
	for conn := range connections {
		acquiredConnections = append(acquiredConnections, conn)
	}

	return acquiredConnections, nil
}

// AcquireUnmanagedConnection returns a connection that is not managed
func (sess *IRODSSession) AcquireUnmanagedConnection() (*connection.IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "AcquireUnmanagedConnection",
	})

	// create a new one
	newConn := connection.NewIRODSConnection(sess.account, sess.config.OperationTimeout, sess.config.ApplicationName)
	err := newConn.Connect()
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to irods server: %w", err)
	}

	logger.Debug("Created a new unmanaged connection")
	return newConn, nil
}

// ReturnConnection returns an idle connection with transaction close
func (sess *IRODSSession) ReturnConnection(conn *connection.IRODSConnection) error {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "IRODSSession",
		"function": "ReturnConnection",
	})

	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	if share, ok := sess.sharedConnections[conn]; ok {
		share--
		if share <= 0 {
			// no share
			delete(sess.sharedConnections, conn)

			conn.Lock()
			if conn.IsTransactionDirty() {
				err := sess.endTransaction(conn)
				if err != nil {
					conn.Unlock()

					logger.Debug(err)

					// discard, since we cannot reuse the connection
					sess.connectionPool.Discard(conn)
					return nil
				}

				// clear transaction
				conn.SetTransactionDirty(false)
			}
			conn.Unlock()

			err := sess.connectionPool.Return(conn)
			if err != nil {
				return xerrors.Errorf("failed to return an idle connection: %w", err)
			}
		} else {
			sess.sharedConnections[conn] = share
		}
	} else {
		// may be unmanged?
		if conn.IsConnected() {
			conn.Disconnect()
		}
	}

	return nil
}

// DiscardConnection discards a connection
func (sess *IRODSSession) DiscardConnection(conn *connection.IRODSConnection) error {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	if share, ok := sess.sharedConnections[conn]; ok {
		share--
		if share <= 0 {
			// no share
			delete(sess.sharedConnections, conn)

			sess.connectionPool.Discard(conn)
			return nil
		} else {
			sess.sharedConnections[conn] = share
		}
	} else {
		// may be unmanaged?
		if conn.IsConnected() {
			conn.Disconnect()
		}
	}

	return nil
}

// Release releases all connections
func (sess *IRODSSession) Release() {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// we don't disconnect connections here,
	// we will disconnect it when calling pool.Release
	sess.sharedConnections = map[*connection.IRODSConnection]int{}

	sess.connectionPool.Release()
}

// SupportParallelUpload returns if parallel upload is supported
func (sess *IRODSSession) SupportParallelUpload() bool {
	return sess.supportParallelUpload
}

// Connections returns the number of connections in the pool
func (sess *IRODSSession) ConnectionTotal() int {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.OpenConnections()
}

// GetMetrics returns metrics
func (sess *IRODSSession) GetMetrics() *metrics.IRODSMetrics {
	return &sess.metrics
}
