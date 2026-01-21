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
	account             *types.IRODSAccount
	config              *ConnectionPoolConfig
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
	logger := log.WithFields(log.Fields{
		"application_name":       config.ApplicationName,
		"initial_cap":            config.InitialCap,
		"max_idle":               config.MaxIdle,
		"max_cap":                config.MaxCap,
		"lifespan":               config.Lifespan,
		"idle_timeout":           config.IdleTimeout,
		"connect_timeout":        config.ConnectTimeout,
		"operation_timeout":      config.OperationTimeout,
		"long_operation_timeout": config.LongOperationTimeout,
		"tcp_buffer_size":        config.TcpBufferSize,
	})

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
		logger.Error(err)
		return nil, err
	}

	// get default tcp buffer size
	if config.TcpBufferSize <= 0 {
		suggestedBufferSize, setBuffer, err := system.GetTCPBufferSize()
		if err != nil {
			logger.WithError(err).Infof("failed to get system suggested buffer size. Use default.")
			// use default buffer size
		} else {
			if setBuffer && suggestedBufferSize > 0 {
				config.TcpBufferSize = suggestedBufferSize
			}
		}
	}

	pool := &ConnectionPool{
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
							idleConn.Disconnect() //nolint

							pool.callCallbacks()
						} else if idleConn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
							// too old
							pool.idleConnections.Remove(elem)
							idleConn.Disconnect() //nolint

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
			}
		}
	}()

	return pool, nil
}

// Release releases all resources
func (pool *ConnectionPool) Release() {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.terminated {
		return
	}

	pool.terminated = true
	pool.terminateChan <- true

	for pool.idleConnections.Len() > 0 {
		elem := pool.idleConnections.Front()
		if elem == nil {
			break
		}

		idleConnObj := pool.idleConnections.Remove(elem)
		if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
			_ = idleConn.Disconnect()
		}

		pool.callCallbacks()
	}

	for occupiedConn := range pool.occupiedConnections {
		_ = occupiedConn.Disconnect()
	}

	// clear
	pool.occupiedConnections = map[*connection.IRODSConnection]bool{}

	pool.callCallbacks()

	pool.waitCond.Broadcast()

	pool.callbacks = map[string]ConnectionUsageCallback{}

	if pool.config.Metrics != nil {
		pool.config.Metrics.ClearConnections()
	}
}

func (pool *ConnectionPool) callCallbacks() {
	for _, callback := range pool.callbacks {
		callback(len(pool.occupiedConnections), pool.idleConnections.Len(), pool.getMaxConnectionsReal())
	}
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
	logger := log.WithFields(log.Fields{})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	pool.callCallbacks()

	// create connections
	connConfig := pool.config.ToConnectionConfig()

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
				logger.Debugf("adjusted max connections: %d", pool.maxConnectionsReal)
			}

			return errors.Wrapf(err, "failed to connect to irods server")
		}

		pool.idleConnections.PushBack(newConn)

		pool.callCallbacks()
	}

	return nil
}

func (pool *ConnectionPool) get(new bool, noConnect bool) (*connection.IRODSConnection, bool, error) {
	logger := log.WithFields(log.Fields{
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
	logger := log.WithFields(log.Fields{})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// find it from occupied map
	if _, ok := pool.occupiedConnections[conn]; ok {
		// delete
		delete(pool.occupiedConnections, conn)

		pool.callCallbacks()

		if pool.config.Metrics != nil {
			pool.config.Metrics.DecreaseConnectionsOccupied(1)
		}
	} else {
		// cannot find it from occupied map
		return errors.Errorf("failed to find the connection from occupied connection list")
	}

	if !conn.IsConnected() {
		logger.Warn("failed to return the connection because it is already closed. discarding...")
		pool.waitCond.Broadcast()
		return nil
	}

	// do not return if the connection is too old
	now := time.Now()
	if conn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
		_ = conn.Disconnect()
		pool.waitCond.Broadcast()
		logger.Debug("Returning and destroying an old connection")
		return nil
	}

	pool.idleConnections.PushBack(conn)

	pool.callCallbacks()

	// check maxidle
	for pool.idleConnections.Len() > pool.config.MaxIdle {
		// check front since it's old
		elem := pool.idleConnections.Front()
		if elem != nil {
			idleConnObj := pool.idleConnections.Remove(elem)
			pool.callCallbacks()

			if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
				_ = idleConn.Disconnect()
			}
		}
	}

	pool.waitCond.Broadcast()

	logger.Debug("Returning a connection")

	return nil
}

// Discard discards the connection
func (pool *ConnectionPool) Discard(conn *connection.IRODSConnection) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// find it from occupied map
	delete(pool.occupiedConnections, conn)
	pool.callCallbacks()

	if pool.config.Metrics != nil {
		pool.config.Metrics.DecreaseConnectionsOccupied(1)
	}

	if conn.IsConnected() {
		_ = conn.Disconnect()
	}

	pool.waitCond.Broadcast()
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
