package types

import (
	"errors"

	"golang.org/x/xerrors"
)

var (
	fileNotFoundError error = xerrors.New("data object/collection not found")
)

// NewFileNotFoundError creates an error for file not found
func NewFileNotFoundError() error {
	return fileNotFoundError
}

// IsFileNotFoundError evaluates if the given error is file not found error
func IsFileNotFoundError(err error) bool {
	return errors.Is(err, fileNotFoundError)
}
