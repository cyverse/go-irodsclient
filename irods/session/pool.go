package session

import (
	"container/list"
	"sync"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/metrics"
	"github.com/cyverse/go-irodsclient/irods/types"
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

// ConnectionPoolConfig is for connection pool configuration
type ConnectionPoolConfig struct {
	Account          *types.IRODSAccount
	ApplicationName  string
	InitialCap       int
	MaxIdle          int
	MaxCap           int           // output warning if total connections exceeds maxcap number
	Lifespan         time.Duration // if a connection exceeds its lifespan, the connection will die
	IdleTimeout      time.Duration // if there's no activity on a connection for the timeout time, the connection will die
	OperationTimeout time.Duration // if there's no response for the timeout time, the request will fail
	TcpBufferSize    int
}

// ConnectionPool is a struct for connection pool
type ConnectionPool struct {
	config              *ConnectionPoolConfig
	idleConnections     *list.List // list of *connection.IRODSConnection
	occupiedConnections map[*connection.IRODSConnection]bool
	metrics             *metrics.IRODSMetrics
	mutex               sync.Mutex
	terminateChan       chan bool
	terminated          bool
}

// NewConnectionPool creates a new ConnectionPool
func NewConnectionPool(config *ConnectionPoolConfig, metrics *metrics.IRODSMetrics) (*ConnectionPool, error) {
	pool := &ConnectionPool{
		config:              config,
		idleConnections:     list.New(),
		occupiedConnections: map[*connection.IRODSConnection]bool{},
		metrics:             metrics,
		mutex:               sync.Mutex{},
		terminateChan:       make(chan bool),
		terminated:          false,
	}

	err := pool.init()
	if err != nil {
		return nil, xerrors.Errorf("failed to init connection pool: %w", err)
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
						} else if idleConn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
							// too old
							pool.idleConnections.Remove(elem)
							idleConn.Disconnect() //nolint
						} else {
							break
						}
					} else {
						// unknown object, remove it
						pool.idleConnections.Remove(elem)
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
			idleConn.Disconnect()
		}
	}

	for occupiedConn := range pool.occupiedConnections {
		occupiedConn.Disconnect()
	}

	// clear
	pool.occupiedConnections = map[*connection.IRODSConnection]bool{}

	pool.metrics.ClearConnections()
}

func (pool *ConnectionPool) init() error {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// create connections
	for i := 0; i < pool.config.InitialCap; i++ {
		newConn := connection.NewIRODSConnectionWithMetrics(pool.config.Account, pool.config.OperationTimeout, pool.config.ApplicationName, pool.metrics)
		newConn.SetTCPBufferSize(pool.config.TcpBufferSize)
		err := newConn.Connect()
		if err != nil {
			pool.metrics.IncreaseCounterForConnectionPoolFailures(1)
			return xerrors.Errorf("failed to connect to irods server: %w", err)
		}

		pool.idleConnections.PushBack(newConn)
	}

	return nil
}

// Get gets a new or an idle connection out of the pool
// the boolean return value indicates if the returned connection is new (True) or existing idle (False)
func (pool *ConnectionPool) Get() (*connection.IRODSConnection, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "ConnectionPool",
		"function": "Get",
	})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if len(pool.occupiedConnections) >= pool.config.MaxCap {
		return nil, false, types.NewConnectionPoolFullError(len(pool.occupiedConnections), pool.config.MaxCap)
	}

	var err error
	// check if there's idle connection
	if pool.idleConnections.Len() > 0 {
		// there's idle connection
		// LIFO
		elem := pool.idleConnections.Back()
		if elem != nil {
			idleConnObj := pool.idleConnections.Remove(elem)
			if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
				if idleConn.IsConnected() {
					// move to occupied connections
					pool.occupiedConnections[idleConn] = true
					logger.Debug("Reuse an idle connection")

					pool.metrics.IncreaseConnectionsOccupied(1)
					return idleConn, false, nil
				}

				logger.Warn("failed to reuse an idle connection because it is already disconnected. discarding...")
			}
		}
	}

	// create a new if not exists
	newConn := connection.NewIRODSConnectionWithMetrics(pool.config.Account, pool.config.OperationTimeout, pool.config.ApplicationName, pool.metrics)
	newConn.SetTCPBufferSize(pool.config.TcpBufferSize)
	err = newConn.Connect()
	if err != nil {
		pool.metrics.IncreaseCounterForConnectionPoolFailures(1)
		return nil, false, xerrors.Errorf("failed to connect to irods server: %w", err)
	}

	pool.occupiedConnections[newConn] = true
	logger.Debug("Created a new connection")
	pool.metrics.IncreaseConnectionsOccupied(1)

	return newConn, true, nil
}

// GetNew gets a new connection out of the pool
func (pool *ConnectionPool) GetNew() (*connection.IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "ConnectionPool",
		"function": "GetNew",
	})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if len(pool.occupiedConnections) >= pool.config.MaxCap {
		return nil, types.NewConnectionPoolFullError(len(pool.occupiedConnections), pool.config.MaxCap)
	}

	// full - close an idle connection and create a new one
	if pool.idleConnections.Len() > 0 {
		// close
		elem := pool.idleConnections.Front()
		if elem != nil {
			idleConnObj := pool.idleConnections.Remove(elem)
			if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
				if idleConn.IsConnected() {
					idleConn.Disconnect()
				}
			}
		}
	}

	// create a new one
	if len(pool.occupiedConnections)+pool.idleConnections.Len() < pool.config.MaxCap {
		// create a new one
		newConn := connection.NewIRODSConnection(pool.config.Account, pool.config.OperationTimeout, pool.config.ApplicationName)
		newConn.SetTCPBufferSize(pool.config.TcpBufferSize)
		err := newConn.Connect()
		if err != nil {
			pool.metrics.IncreaseCounterForConnectionPoolFailures(1)
			return nil, xerrors.Errorf("failed to connect to irods server: %w", err)
		}

		pool.occupiedConnections[newConn] = true
		logger.Debug("Created a new connection")
		pool.metrics.IncreaseConnectionsOccupied(1)

		return newConn, nil
	}

	return nil, xerrors.Errorf("failed to create a new connection, no idle connections")
}

// Return returns the connection after use
func (pool *ConnectionPool) Return(conn *connection.IRODSConnection) error {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "ConnectionPool",
		"function": "Return",
	})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// find it from occupied map
	if _, ok := pool.occupiedConnections[conn]; ok {
		// delete
		delete(pool.occupiedConnections, conn)
		pool.metrics.DecreaseConnectionsOccupied(1)
	} else {
		// cannot find it from occupied map
		return xerrors.Errorf("failed to find the connection from occupied connection list")
	}

	if !conn.IsConnected() {
		logger.Warn("failed to return the connection because it is already closed. discarding...")
		return nil
	}

	// do not return if the connection is too old
	now := time.Now()
	if conn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
		conn.Disconnect()
		logger.Debug("Returning and destroying an old connection")
		return nil
	}

	pool.idleConnections.PushBack(conn)

	// check maxidle
	for pool.idleConnections.Len() > pool.config.MaxIdle {
		// check front since it's old
		elem := pool.idleConnections.Front()
		if elem != nil {
			idleConnObj := pool.idleConnections.Remove(elem)
			if idleConn, ok := idleConnObj.(*connection.IRODSConnection); ok {
				idleConn.Disconnect()
			}
		}
	}

	logger.Debug("Returning a connection")

	return nil
}

// Discard discards the connection
func (pool *ConnectionPool) Discard(conn *connection.IRODSConnection) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// find it from occupied map
	delete(pool.occupiedConnections, conn)

	pool.metrics.DecreaseConnectionsOccupied(1)

	if conn.IsConnected() {
		conn.Disconnect()
	}
}

// OpenConnections returns total number of connections
func (pool *ConnectionPool) OpenConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return len(pool.occupiedConnections) + pool.idleConnections.Len()
}

// OccupiedConnections returns total number of connections in use
func (pool *ConnectionPool) OccupiedConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return len(pool.occupiedConnections)
}

// IdleConnections returns total number of idle connections
func (pool *ConnectionPool) IdleConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return pool.idleConnections.Len()
}

// AvailableConnections returns connections that are available to use
func (pool *ConnectionPool) AvailableConnections() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	return pool.config.MaxCap - len(pool.occupiedConnections)
}
