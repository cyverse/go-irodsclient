package session

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/types"

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
}

// ConnectionPool is a struct for connection pool
type ConnectionPool struct {
	config              *ConnectionPoolConfig
	idleConnections     *list.List
	occupiedConnections map[*connection.IRODSConnection]bool
	mutex               sync.Mutex
	terminateChan       chan bool
	terminated          bool
}

// NewConnectionPool creates a new ConnectionPool
func NewConnectionPool(config *ConnectionPoolConfig) (*ConnectionPool, error) {
	pool := &ConnectionPool{
		config:              config,
		idleConnections:     list.New(),
		occupiedConnections: map[*connection.IRODSConnection]bool{},
		mutex:               sync.Mutex{},
		terminateChan:       make(chan bool),
		terminated:          false,
	}

	err := pool.init()
	if err != nil {
		return nil, err
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
							idleConn.Disconnect()
						} else if idleConn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
							// too old
							pool.idleConnections.Remove(elem)
							idleConn.Disconnect()
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
}

func (pool *ConnectionPool) init() error {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "ConnectionPool",
		"function": "init",
	})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// create connections
	for i := 0; i < pool.config.InitialCap; i++ {
		newConn := connection.NewIRODSConnection(pool.config.Account, pool.config.OperationTimeout, pool.config.ApplicationName)
		err := newConn.Connect()
		if err != nil {
			logger.Errorf("failed to create a new connection - %v", err)
			return err
		}

		pool.idleConnections.PushBack(newConn)
	}

	pool.warnExceedsMaxCap()
	return nil
}

func (pool *ConnectionPool) warnExceedsMaxCap() {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "ConnectionPool",
		"function": "warnExceedsMaxCap",
	})

	// do not lock here since it's used internally
	currentConn := len(pool.occupiedConnections) + pool.idleConnections.Len()
	if currentConn > pool.config.MaxCap {
		logger.Warnf("the number of opened connections %d exceeded maxCap %d", currentConn, pool.config.MaxCap)
	}
}

// Get gets a new or an idle connection out of the pool
// the boolean return value indicates if the returned conneciton is new (True) or existing idle (False)
func (pool *ConnectionPool) Get() (*connection.IRODSConnection, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "session",
		"struct":   "ConnectionPool",
		"function": "Get",
	})

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

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
					return idleConn, false, nil
				}

				logger.Warn("failed to reuse an idle connection. already disconnected. discarding.")
			}
		}
	}

	// create a new if not exists
	newConn := connection.NewIRODSConnection(pool.config.Account, pool.config.OperationTimeout, pool.config.ApplicationName)
	err = newConn.Connect()
	if err != nil {
		logger.Errorf("failed to create a new connection - %v", err)
		return nil, false, err
	}

	pool.occupiedConnections[newConn] = true
	pool.warnExceedsMaxCap()

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

	newConn := connection.NewIRODSConnection(pool.config.Account, pool.config.OperationTimeout, pool.config.ApplicationName)
	err := newConn.Connect()
	if err != nil {
		logger.Errorf("failed to create a new connection - %v", err)
		return nil, err
	}

	pool.occupiedConnections[newConn] = true
	pool.warnExceedsMaxCap()

	return newConn, nil
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
	} else {
		// cannot find it from occupied map
		logger.Error("failed to find the connection from occupied connections")
		return fmt.Errorf("failed to find the connection from occupied connections")
	}

	if !conn.IsConnected() {
		logger.Warn("failed to return the connection. already closed. discarding.")
		return nil
	}

	// do not return if the connection is too old
	now := time.Now()
	if conn.GetCreationTime().Add(pool.config.Lifespan).Before(now) {
		conn.Disconnect()
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

	pool.warnExceedsMaxCap()
	return nil
}

// Discard discards the connection
func (pool *ConnectionPool) Discard(conn *connection.IRODSConnection) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// find it from occupied map
	delete(pool.occupiedConnections, conn)

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
