package session

import (
	"errors"

	"golang.org/x/xerrors"
)

var (
	connectionPoolFullError error = xerrors.New("connection pool is full")
)

// NewConnectionPoolFullError creates an error for full connection pool
func NewConnectionPoolFullError() error {
	return connectionPoolFullError
}

// IsConnectionPoolFullError evaluates if the given error is connection full error
func IsConnectionPoolFullError(err error) bool {
	return errors.Is(err, connectionPoolFullError)
}
