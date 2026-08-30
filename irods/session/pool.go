package session

import (
	"container/list"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/system"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/xid"

	log "github.com/sirupsen/logrus"
)

type ConnectionUsageCallback func(occupied int, idle int, max int)

// ConnectionPool is a struct for connection pool
type ConnectionPool struct {
	id      string
	account *types.IRODSAccount
	config  *ConnectionPoolConfig
	logger  *log.Entry

	idleConnections     *list.List // list of *connection.IRODSConnection
	occupiedConnections map[*connection.IRODSConnection]bool
	maxConnectionsReal  int                                // max connections can be created in reality
	callbacks           map[string]ConnectionUsageCallback // callbacks for connection usage changes
	mutex               sync.Mutex
	waitCond            *sync.Cond // condition variable for waiting
	terminateChan       chan bool
	terminated          bool
}

// NewConnectionPool creates a new ConnectionPool
func NewConnectionPool(account *types.IRODSAccount, config *ConnectionPoolConfig) (*ConnectionPool, error) {
	if account == nil {
		newErr := types.NewConnectionConfigError(nil)
		return nil, errors.Wrapf(newErr, "account is not set")
	}

	// use default config if not set
	if config == nil {
		config = &ConnectionPoolConfig{}
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

	poolID := xid.New().String()

	pool := &ConnectionPool{
		id:                  poolID,
		account:             account,
		config:              config,
		idleConnections:     list.New(),
		occupiedConnections: map[*connection.IRODSConnection]bool{},
		maxConnectionsReal:  0,
		callbacks:           map[string]ConnectionUsageCallback{},
		mutex:               sync.Mutex{},
		terminateChan:       make(chan bool),
		terminated:          false,
	}

	// set logger
	if config != nil && config.LogEntry != nil {
		logFields := log.Fields{
			"pool_id": poolID,
		}

		pool.logger = config.LogEntry.WithFields(logFields)
	} else {
		// create new logger object
		var logger *log.Logger
		if config != nil && config.Logger != nil {
			logger = config.Logger
		} else {
			logger = log.StandardLogger()
		}

		logFields := log.Fields{
			"pool_id":          poolID,
			"pool_host":        account.Host,
			"pool_client_zone": account.ClientZone,
			"pool_client_user": account.ClientUser,
		}

		if account.ClientZone != account.ProxyZone {
			logFields["pool_proxy_zone"] = account.ProxyZone
		}
		if account.ClientUser != account.ProxyUser {
			logFields["pool_proxy_user"] = account.ProxyUser
		}

		pool.logger = logger.WithFields(logFields)
	}

	// get default tcp buffer size
	if config.TcpBufferSize <= 0 {
		suggestedBufferSize, setBuffer, err := system.GetTCPBufferSize()
		if err != nil {
			pool.logger.WithError(err).Infof("failed to get system suggested buffer size. Use default.")
			// use default buffer size
		} else {
			if setBuffer && suggestedBufferSize > 0 {
				config.TcpBufferSize = suggestedBufferSize
			}
		}
	}

	pool.waitCond = sync.NewCond(&pool.mutex)

	err = pool.init()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to init connection pool")
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)

		for {
			select {
			case <-pool.terminateChan:
				ticker.Stop()
				return
			case <-ticker.C:
				pool.mutex.Lock()

				now := time.Now()
				var toDisconnect []*connection.IRODSConnection
				for {
					elem := pool.idleConnections.Front()
					if elem == nil {
						break
					}

					// if the front conn expired idle timeout, continue next
					idleConnObj := elem.Value
					if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
						if idleConn.GetLastSuccessfulAccess().Add(pool.config.IdleTimeout).Before(now) {
							// timeout
							pool.idleConnections.Remove(elem)
							toDisconnect = append(toDisconnect, idleConn)
							pool.callCallbacks()
						} else if idleConn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
							// too old
							pool.idleConnections.Remove(elem)
							toDisconnect = append(toDisconnect, idleConn)
							pool.callCallbacks()
						} else {
							break
						}
					} else {
						// unknown object, remove it
						pool.idleConnections.Remove(elem)
						pool.callCallbacks()
					}
				}

				pool.mutex.Unlock()

				for _, conn := range toDisconnect {
					conn.Disconnect() //nolint
				}
			}
		}
	}()

	return pool, nil
}

// Release releases all resources
func (pool *ConnectionPool) Release() {
	pool.mutex.Lock()

	if pool.terminated {
		pool.mutex.Unlock()
		return
	}

	pool.terminated = true

	// Collect all connections under lock, then disconnect concurrently outside lock
	connsToDisconnect := make([]*connection.IRODSConnection, 0, pool.idleConnections.Len()+len(pool.occupiedConnections))

	for pool.idleConnections.Len() > 0 {
		elem := pool.idleConnections.Front()
		if elem == nil {
			break
		}
		idleConnObj := pool.idleConnections.Remove(elem)
		if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
			connsToDisconnect = append(connsToDisconnect, idleConn)
		}
	}

	for occupiedConn := range pool.occupiedConnections {
		connsToDisconnect = append(connsToDisconnect, occupiedConn)
	}
	pool.occupiedConnections = map[*connection.IRODSConnection]bool{}

	pool.callCallbacks()
	pool.waitCond.Broadcast()
	pool.callbacks = map[string]ConnectionUsageCallback{}

	if pool.config.Metrics != nil {
		pool.config.Metrics.ClearConnections()
	}

	pool.mutex.Unlock()

	// Signal termination outside mutex to prevent deadlock with ticker goroutine
	pool.terminateChan <- true

	// Disconnect all connections concurrently
	var wg sync.WaitGroup
	for _, conn := range connsToDisconnect {
		wg.Add(1)
		go func(c *connection.IRODSConnection) {
			defer wg.Done()
			_ = c.Disconnect()
		}(conn)
	}
	wg.Wait()
}

func (pool *ConnectionPool) callCallbacks() {
	if len(pool.callbacks) == 0 {
		return
	}

	occupied := len(pool.occupiedConnections)
	idle := pool.idleConnections.Len()
	max := pool.getMaxConnectionsReal()

	// Snapshot callbacks and dispatch asynchronously to prevent reentrancy deadlock:
	// callers hold pool.mutex when invoking callCallbacks, and pool methods also
	// acquire pool.mutex, so a synchronous callback that calls any pool method deadlocks.
	cbs := make([]ConnectionUsageCallback, 0, len(pool.callbacks))
	for _, cb := range pool.callbacks {
		cbs = append(cbs, cb)
	}

	go func() {
		for _, cb := range cbs {
			cb(occupied, idle, max)
		}
	}()
}

func (pool *ConnectionPool) AddUsageCallback(callback ConnectionUsageCallback) string {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	callbackID := xid.New().String()
	pool.callbacks[callbackID] = callback

	callback(len(pool.occupiedConnections), pool.idleConnections.Len(), pool.getMaxConnectionsReal())

	return callbackID
}

func (pool *ConnectionPool) RemoveUsageCallback(id string) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	delete(pool.callbacks, id)
}

func (pool *ConnectionPool) init() error {
	pool.callCallbacks()

	// create connections
	connConfig := pool.config.ToConnectionConfig()
	connConfig.LogEntry = pool.logger

	for i := 0; i < pool.config.InitialCap; i++ {
		newConn, err := connection.NewIRODSConnection(pool.account, connConfig)
		if err != nil {
			if pool.config.Metrics != nil {
				pool.config.Metrics.IncreaseCounterForConnectionPoolFailures(1)
			}
			return errors.Wrapf(err, "failed to connect to irods server")
		}

		err = newConn.Connect()
		if err != nil {
			if pool.config.Metrics != nil {
				pool.config.Metrics.IncreaseCounterForConnectionPoolFailures(1)
			}

			if types.IsConnectionError(err) {
				// rejected?
				pool.maxConnectionsReal = i
				pool.logger.Debugf("adjusted max connections: %d", pool.maxConnectionsReal)
			}

			return errors.Wrapf(err, "failed to connect to irods server")
		}

		pool.idleConnections.PushBack(newConn)

		pool.callCallbacks()
	}

	return nil
}

func (pool *ConnectionPool) get(new bool, noConnect bool) (*connection.IRODSConnection, bool, error) {
	if pool.terminated {
		return nil, false, errors.New("connection pool is closed")
	}

	logger := pool.logger.WithFields(log.Fields{
		"new": new,
	})

	maxConn := pool.getMaxConnectionsReal()

	if len(pool.occupiedConnections) >= maxConn {
		return nil, false, types.NewConnectionPoolFullError(len(pool.occupiedConnections), maxConn)
	}

	var err error

	// check if there's idle connection
	if pool.idleConnections.Len() > 0 {
		// there's idle connection
		if new {
			// close an idle connection and create a new one
			elem := pool.idleConnections.Front()
			if elem != nil {
				idleConnObj := pool.idleConnections.Remove(elem)
				if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
					if idleConn.IsConnected() {
						_ = idleConn.Disconnect()
					}
				}

				pool.callCallbacks()

				// fall through to create a new connection
			}
		} else {
			// reuse
			// LIFO
			elem := pool.idleConnections.Back()
			if elem != nil {
				idleConnObj := pool.idleConnections.Remove(elem)
				if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
					if idleConn.IsConnected() {
						// move to occupied connections
						pool.occupiedConnections[idleConn] = true
						logger.Debug("Reuse an idle connection")

						pool.callCallbacks()

						if pool.config.Metrics != nil {
							pool.config.Metrics.IncreaseConnectionsOccupied(1)
						}
						return idleConn, false, nil
					} else {
						logger.Warn("failed to reuse an idle connection because it is already disconnected. discarding...")

						pool.callCallbacks()

						// fall through to create a new connection
					}
				}
			}
		}
	}

	// create a new if not exists
	connConfig := pool.config.ToConnectionConfig()
	connConfig.LogEntry = pool.logger

	newConn, err := connection.NewIRODSConnection(pool.account, connConfig)
	if err != nil {
		if pool.config.Metrics != nil {
			pool.config.Metrics.IncreaseCounterForConnectionPoolFailures(1)
		}
		return nil, false, errors.Wrapf(err, "failed to connect to irods server")
	}

	if !noConnect {
		err = newConn.Connect()
		if err != nil {
			if pool.config.Metrics != nil {
				pool.config.Metrics.IncreaseCounterForConnectionPoolFailures(1)
			}

			if types.IsConnectionError(err) {
				// rejected?
				pool.maxConnectionsReal = len(pool.occupiedConnections) + pool.idleConnections.Len()

				pool.callCallbacks()
				if pool.maxConnectionsReal > 0 {
					logger.Debugf("adjusted max connections: %d", pool.maxConnectionsReal)
					return nil, false, types.NewConnectionPoolFullError(len(pool.occupiedConnections), maxConn)
				}
			}

			return nil, false, errors.Wrapf(err, "failed to connect to irods server")
		}
	}

	pool.occupiedConnections[newConn] = true
	logger.Debug("Created a new connection")

	pool.callCallbacks()

	if pool.config.Metrics != nil {
		pool.config.Metrics.IncreaseConnectionsOccupied(1)
	}

	return newConn, true, nil
}

// Get gets a new or an idle connection out of the pool
// the boolean return value indicates if the returned connection is new (True) or existing idle (False)
func (pool *ConnectionPool) Get(new bool, noConnect bool, wait bool) (*connection.IRODSConnection, bool, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	for {
		conn, newConn, err := pool.get(new, noConnect)
		if err != nil && types.IsConnectionPoolFullError(err) && wait {
			// if the pool is full and wait is true, wait for a while
			pool.waitCond.Wait()
		} else {
			return conn, newConn, err
		}
	}
}

// Return returns the connection after use
func (pool *ConnectionPool) Return(conn *connection.IRODSConnection) error {
	pool.mutex.Lock()

	// find it from occupied map
	if _, ok := pool.occupiedConnections[conn]; ok {
		// delete
		delete(pool.occupiedConnections, conn)

		pool.callCallbacks()

		if pool.config.Metrics != nil {
			pool.config.Metrics.DecreaseConnectionsOccupied(1)
		}
	} else {
		pool.mutex.Unlock()
		return errors.Errorf("failed to find the connection from occupied connection list")
	}

	if !conn.IsConnected() {
		pool.logger.Warn("failed to return the connection because it is already closed. discarding...")
		pool.waitCond.Broadcast()
		pool.mutex.Unlock()
		return nil
	}

	// do not return if the connection is too old
	now := time.Now()
	if conn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
		pool.waitCond.Broadcast()
		pool.mutex.Unlock()
		_ = conn.Disconnect()
		pool.logger.Debug("Returning and destroying an old connection")
		return nil
	}

	pool.idleConnections.PushBack(conn)
	pool.callCallbacks()

	// collect excess idle connections to disconnect outside the lock
	var toDisconnect []*connection.IRODSConnection
	for pool.idleConnections.Len() > pool.config.MaxIdle {
		elem := pool.idleConnections.Front()
		if elem != nil {
			idleConnObj := pool.idleConnections.Remove(elem)
			pool.callCallbacks()
			if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
				toDisconnect = append(toDisconnect, idleConn)
			}
		}
	}

	pool.waitCond.Broadcast()
	pool.logger.Debug("Returning a connection")
	pool.mutex.Unlock()

	for _, c := range toDisconnect {
		_ = c.Disconnect()
	}

	return nil
}

// Discard discards the connection
func (pool *ConnectionPool) Discard(conn *connection.IRODSConnection) {
	pool.mutex.Lock()

	// find it from occupied map
	delete(pool.occupiedConnections, conn)
	pool.callCallbacks()

	if pool.config.Metrics != nil {
		pool.config.Metrics.DecreaseConnectionsOccupied(1)
	}

	shouldDisconnect := conn.IsConnected()
	pool.waitCond.Broadcast()
	pool.mutex.Unlock()

	if shouldDisconnect {
		_ = conn.Disconnect()
	}
}

// GetOpenConnections returns total number of connections
func (pool *ConnectionPool) GetOpenConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return len(pool.occupiedConnections) + pool.idleConnections.Len()
}

// GetOccupiedConnections returns total number of connections in use
func (pool *ConnectionPool) GetOccupiedConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return len(pool.occupiedConnections)
}

// GetIdleConnections returns total number of idle connections
func (pool *ConnectionPool) GetIdleConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return pool.idleConnections.Len()
}

// GetAvailableConnections returns connections that are available to use
func (pool *ConnectionPool) GetAvailableConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return pool.getMaxConnectionsReal() - len(pool.occupiedConnections)
}

// GetMaxConnections returns connections that can be created
func (pool *ConnectionPool) GetMaxConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return pool.getMaxConnectionsReal()
}

func (pool *ConnectionPool) getMaxConnectionsReal() int {
	if pool.maxConnectionsReal == 0 {
		return pool.config.MaxCap
	}

	if pool.maxConnectionsReal < pool.config.MaxCap {
		return pool.maxConnectionsReal
	}

	return pool.config.MaxCap
}
