package session

import (
	"sync"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// TransactionFailureHandler is an handler that is called when transaction operation fails
type TransactionFailureHandler func(commitFail bool, poormansRollbackFail bool)

// IRODSSession manages connections to iRODS
type IRODSSession struct {
	account        *types.IRODSAccount
	config         *IRODSSessionConfig
	connectionPool *ConnectionPool

	sharedConnections         map[*connection.IRODSConnection]int
	startNewTransaction       bool
	commitFail                bool
	poormansRollbackFail      bool
	transactionFailureHandler TransactionFailureHandler

	lastConnectionError     error
	lastConnectionErrorTime time.Time

	supportParallelUpload    bool
	supportParallelUploadSet bool

	metrics metrics.IRODSMetrics
	mutex   sync.Mutex
}

// NewIRODSSession create a IRODSSession
func NewIRODSSession(account *types.IRODSAccount, config *IRODSSessionConfig) (*IRODSSession, error) {
	if account == nil {
		return nil, xerrors.Errorf("account is not set: %w", types.NewConnectionConfigError(nil))
	}

	// use default config if not set
	if config == nil {
		config = &IRODSSessionConfig{}
	}

	account.FixAuthConfiguration()
	err := account.Validate()
	if err != nil {
		return nil, err
	}

	config.fillDefaults()
	err = config.Validate()
	if err != nil {
		return nil, err
	}

	sess := IRODSSession{
		account:           account,
		config:            config,
		sharedConnections: map[*connection.IRODSConnection]int{},

		// transaction
		startNewTransaction:       config.StartNewTransaction,
		commitFail:                false,
		poormansRollbackFail:      false,
		transactionFailureHandler: nil,

		lastConnectionError:     nil,
		lastConnectionErrorTime: time.Time{},

		supportParallelUpload:    false,
		supportParallelUploadSet: false,

		metrics: metrics.IRODSMetrics{},

		mutex: sync.Mutex{},
	}

	// resolve host address
	poolAccount := *account
	if config.AddressResolver != nil {
		poolAccount.Host = config.AddressResolver(poolAccount.Host)
	}

	poolConfig := config.ToConnectionPoolConfig()
	poolConfig.Metrics = &sess.metrics

	pool, err := NewConnectionPool(&poolAccount, poolConfig)
	if err != nil {
		sess.lastConnectionError = err
		sess.lastConnectionErrorTime = time.Now()

		return nil, xerrors.Errorf("failed to create connection pool: %w", err)
	}
	sess.connectionPool = pool

	// set transaction config
	// when the user is anonymous, we cannot use transaction since we don't have access to home dir
	if sess.account.IsAnonymousUser() {
		sess.commitFail = true
		sess.poormansRollbackFail = true
	}

	return &sess, nil
}

// GetConfig returns a configuration
func (sess *IRODSSession) GetConfig() *IRODSSessionConfig {
	return sess.config
}

// GetAccount returns an account
func (sess *IRODSSession) GetAccount() *types.IRODSAccount {
	return sess.account
}

// IsConnectionError returns if there is a failure
func (sess *IRODSSession) GetLastConnectionError() (time.Time, error) {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.lastConnectionErrorTime, sess.lastConnectionError
}

func (sess *IRODSSession) getPendingError() error {
	if sess.lastConnectionError == nil {
		return nil
	}

	if types.IsPermanantFailure(sess.lastConnectionError) {
		return sess.lastConnectionError
	}

	// transitive error
	// check timeout
	if sess.lastConnectionErrorTime.Add(sess.config.ConnectionCreationTimeout).Before(time.Now()) {
		// passed timeout
		return nil
	}

	return sess.lastConnectionError
}

// AddConnectionUsageCallback adds connection usage callback
func (sess *IRODSSession) AddConnectionUsageCallback(callback ConnectionUsageCallback) string {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.AddUsageCallback(callback)
}

// RemoveConnectionUsageCallback removes connection usage callback
func (sess *IRODSSession) RemoveConnectionUsageCallback(id string) {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	sess.connectionPool.RemoveUsageCallback(id)
}

// IsPermanantFailure returns if there is a failure that is unfixable, permanent
func (sess *IRODSSession) IsPermanantFailure() bool {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return types.IsPermanantFailure(sess.lastConnectionError)
}

// SetTransactionFailureHandler sets transaction failure handler
func (sess *IRODSSession) SetTransactionFailureHandler(handler TransactionFailureHandler) {
	sess.transactionFailureHandler = handler
}

// SetCommitFail sets commit fail
func (sess *IRODSSession) SetCommitFail(commitFail bool) {
	sess.commitFail = commitFail
}

// SetPoormansRollbackFail sets poormans rollback fail
func (sess *IRODSSession) SetPoormansRollbackFail(poormansRollbackFail bool) {
	sess.poormansRollbackFail = poormansRollbackFail
}

// endTransaction ends transaction
func (sess *IRODSSession) endTransaction(conn *connection.IRODSConnection) error {
	logger := log.WithFields(log.Fields{})

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

		if sess.transactionFailureHandler != nil {
			sess.transactionFailureHandler(sess.commitFail, sess.poormansRollbackFail)
		}
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

		if sess.transactionFailureHandler != nil {
			sess.transactionFailureHandler(sess.commitFail, sess.poormansRollbackFail)
		}
	}

	return xerrors.Errorf("failed to commit/rollback transaction")
}

func (sess *IRODSSession) createConnectionFromPool(new bool, wait bool) (*connection.IRODSConnection, error) {
	conn, _, err := sess.connectionPool.Get(new, wait)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (sess *IRODSSession) acquireConnection(new bool, allowShared bool, wait bool) (*connection.IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"new":          new,
		"allow_shared": allowShared,
		"wait":         wait,
	})

	if allowShared {
		wait = false
	}

	// try to get it from the pool
	conn, err := sess.createConnectionFromPool(new, wait)
	if err != nil {
		if !types.IsConnectionPoolFullError(err) {
			// fail
			sess.lastConnectionError = err
			sess.lastConnectionErrorTime = time.Now()
			return nil, err
		}

		logger.WithError(err).Debug("failed to get a connection from the pool, the pool is full")

		if !allowShared {
			return nil, xerrors.Errorf("failed to get a connection from the pool, the pool is full: %w", err)
		}

		// fall below
	} else {
		// put to share
		if shares, ok := sess.sharedConnections[conn]; ok {
			shares++
			sess.sharedConnections[conn] = shares
		} else {
			sess.sharedConnections[conn] = 1
		}

		if !sess.supportParallelUploadSet {
			sess.supportParallelUpload = conn.SupportParallelUpload()
			sess.supportParallelUploadSet = true
		}

		return conn, nil
	}

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

// AcquireConnection acquires an idle connection
func (sess *IRODSSession) AcquireConnection(allowShared bool) (*connection.IRODSConnection, error) {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// return last error
	pendingErr := sess.getPendingError()
	if pendingErr != nil {
		return nil, xerrors.Errorf("failed to get a connection from the pool because pending error is found: %w", pendingErr)
	}

	conn, err := sess.acquireConnection(false, allowShared, sess.config.WaitConnection)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// AcquireNewConnection acquires a new connection
func (sess *IRODSSession) AcquireNewConnection(allowShared bool) (*connection.IRODSConnection, error) {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// return last error
	pendingErr := sess.getPendingError()
	if pendingErr != nil {
		return nil, xerrors.Errorf("failed to get a connection from the pool because pending error is found: %w", pendingErr)
	}

	conn, err := sess.acquireConnection(true, allowShared, sess.config.WaitConnection)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// AcquireConnectionsMulti acquires multiple idle connections
func (sess *IRODSSession) AcquireConnectionsMulti(number int, allowShared bool) ([]*connection.IRODSConnection, error) {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// return last error
	pendingErr := sess.getPendingError()
	if pendingErr != nil {
		return nil, xerrors.Errorf("failed to get a connection from the pool because pending error is found: %w", pendingErr)
	}

	poolFull := false
	maxConns := sess.connectionPool.GetMaxConnections()

	requestedNum := number
	if requestedNum > maxConns {
		requestedNum = maxConns
		poolFull = true
	}

	connections := []*connection.IRODSConnection{}
	for i := 0; i < requestedNum; i++ {
		conn, err := sess.acquireConnection(false, allowShared, sess.config.WaitConnection)
		if err != nil {
			// return current connections
			return connections, err
		}

		connections = append(connections, conn)
	}

	if poolFull {
		return connections, types.NewConnectionPoolFullError(number, maxConns)
	}
	return connections, nil
}

func (sess *IRODSSession) returnConnection(conn *connection.IRODSConnection) error {
	logger := log.WithFields(log.Fields{})

	if share, ok := sess.sharedConnections[conn]; ok {
		share--
		if share <= 0 {
			// no share
			delete(sess.sharedConnections, conn)

			conn.Lock()

			if conn.IsSocketFailed() {
				conn.Unlock()

				// discard, since we cannot reuse the connection
				sess.connectionPool.Discard(conn)
				return nil
			} else if conn.IsTransactionDirty() {
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
		// unknown connection
		if conn.IsConnected() {
			conn.Disconnect()
		}
	}

	return nil
}

// ReturnConnection returns an idle connection with transaction close
func (sess *IRODSSession) ReturnConnection(conn *connection.IRODSConnection) error {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.returnConnection(conn)
}

// ReturnConnectionsMulti returns multiple idle connections with transaction close
func (sess *IRODSSession) ReturnConnectionsMulti(conns []*connection.IRODSConnection) error {
	var firstErr error
	for _, conn := range conns {
		err := sess.returnConnection(conn)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// DiscardConnection discards a connection
func (sess *IRODSSession) DiscardConnection(conn *connection.IRODSConnection) {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	if share, ok := sess.sharedConnections[conn]; ok {
		share--
		if share <= 0 {
			// no share
			delete(sess.sharedConnections, conn)

			sess.connectionPool.Discard(conn)
			return
		} else {
			sess.sharedConnections[conn] = share
		}
	} else {
		// unknown connection
		if conn.IsConnected() {
			conn.Disconnect()
		}
	}
}

// Release releases all connections
func (sess *IRODSSession) Release() {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// we don't disconnect connections here,
	// we will disconnect it when calling pool.Release
	sess.sharedConnections = map[*connection.IRODSConnection]int{}

	sess.lastConnectionError = nil

	sess.connectionPool.Release()
}

// SupportParallelUpload returns if parallel upload is supported
func (sess *IRODSSession) SupportParallelUpload() bool {
	logger := log.WithFields(log.Fields{})

	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	// return last error
	pendingErr := sess.getPendingError()
	if pendingErr != nil {
		return false
	}

	if !sess.supportParallelUploadSet {
		conn, _, err := sess.connectionPool.Get(false, true)
		if err != nil {
			if !types.IsConnectionPoolFullError(err) {
				sess.lastConnectionError = err
				sess.lastConnectionErrorTime = time.Now()
			}

			return false
		}

		conn.Lock()

		// check parallel upload
		sess.supportParallelUpload = conn.SupportParallelUpload()
		logger.Debugf("support parallel upload: %t", sess.supportParallelUpload)

		conn.Unlock()

		sess.connectionPool.Return(conn) //nolint
		sess.supportParallelUploadSet = true
	}

	return sess.supportParallelUpload
}

// GetOpenConnections returns the number of connections open in the pool
func (sess *IRODSSession) GetOpenConnections() int {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.GetOpenConnections()
}

// GetMaxConnections returns the maximum number of connections in the pool
func (sess *IRODSSession) GetMaxConnections() int {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.GetMaxConnections()
}

// GetOccupiedConnections returns the number of occupied connections in the pool
func (sess *IRODSSession) GetOccupiedConnections() int {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.GetOccupiedConnections()
}

// GetIdleConnections returns total number of idle connections
func (sess *IRODSSession) GetIdleConnections() int {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.GetIdleConnections()
}

// GetAvailableConnections returns the number of available connections in the pool
func (sess *IRODSSession) GetAvailableConnections() int {
	sess.mutex.Lock()
	defer sess.mutex.Unlock()

	return sess.connectionPool.GetAvailableConnections()
}

// GetMetrics returns metrics
func (sess *IRODSSession) GetMetrics() *metrics.IRODSMetrics {
	return &sess.metrics
}

// GetRedirectionConnection returns redirection connection to resource server
func (sess *IRODSSession) GetRedirectionConnection(controlConnection *connection.IRODSConnection, redirectionInfo *types.IRODSRedirectionInfo) (*connection.IRODSResourceServerConnection, error) {
	// make a copy of redirectionInfo
	resourceServerInfo := *redirectionInfo
	if sess.config.AddressResolver != nil {
		resourceServerInfo.Host = sess.config.AddressResolver(resourceServerInfo.Host)
	}

	connConfig := &connection.IRODSResourceServerConnectionConfig{
		ConnectTimeout: sess.config.ConnectionCreationTimeout,
		TcpBufferSize:  sess.config.TcpBufferSize,
		Metrics:        &sess.metrics,
	}

	return connection.NewIRODSResourceServerConnection(controlConnection, &resourceServerInfo, connConfig)
}
