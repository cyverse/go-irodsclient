package session

import (
	"fmt"
)

// ConnectionPoolFullError ...
type ConnectionPoolFullError struct {
	message string
}

// NewConnectionPoolFullError creates ConnectionPoolFullError struct
func NewConnectionPoolFullError(message string) *ConnectionPoolFullError {
	return &ConnectionPoolFullError{
		message: message,
	}
}

// NewConnectionPoolFullErrorf creates ConnectionPoolFullError struct
func NewConnectionPoolFullErrorf(format string, v ...interface{}) *ConnectionPoolFullError {
	return &ConnectionPoolFullError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *ConnectionPoolFullError) Error() string {
	return e.message
}

// IsConnectionPoolFullError evaluates if the given error is ConnectionPoolFullError
func IsConnectionPoolFullError(err error) bool {
	if _, ok := err.(*ConnectionPoolFullError); ok {
		return true
	}

	return false
}
