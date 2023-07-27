package types

import (
	"errors"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
	"golang.org/x/xerrors"
)

var (
	connectionConfigError   error = xerrors.New("connection configuration error")
	connectionError         error = xerrors.New("connection failed")
	authError               error = xerrors.New("authentication failed")
	connectionPoolFullError error = xerrors.New("connection pool is full")
	collectionNotEmptyError error = xerrors.New("collection is not empty")
	fileNotFoundError       error = xerrors.New("data object/collection not found")
)

// NewConnectionConfigError creates an error for connection config error
func NewConnectionConfigError() error {
	return connectionConfigError
}

// IsConnectionConfigError evaluates if the given error is connection config error
func IsConnectionConfigError(err error) bool {
	return errors.Is(err, connectionConfigError)
}

// NewConnectionError creates an error for connection failure
func NewConnectionError() error {
	return connectionError
}

// IsConnectionError evaluates if the given error is connection failure
func IsConnectionError(err error) bool {
	return errors.Is(err, connectionError)
}

// NewAuthError creates an error for authentication failure
func NewAuthError() error {
	return authError
}

// IsAuthError evaluates if the given error is authentication failure
func IsAuthError(err error) bool {
	return errors.Is(err, authError)
}

// NewConnectionPoolFullError creates an error for full connection pool
func NewConnectionPoolFullError() error {
	return connectionPoolFullError
}

// IsConnectionPoolFullError evaluates if the given error is connection full error
func IsConnectionPoolFullError(err error) bool {
	return errors.Is(err, connectionPoolFullError)
}

// NewCollectionNotEmptyError creates an error for collection not empty
func NewCollectionNotEmptyError() error {
	return collectionNotEmptyError
}

// IsCollectionNotEmptyError evaluates if the given error is collection not empty error
func IsCollectionNotEmptyError(err error) bool {
	return errors.Is(err, collectionNotEmptyError)
}

// NewFileNotFoundError creates an error for file not found
func NewFileNotFoundError() error {
	return fileNotFoundError
}

// IsFileNotFoundError evaluates if the given error is file not found error
func IsFileNotFoundError(err error) bool {
	return errors.Is(err, fileNotFoundError)
}

// IRODSError contains irods error information
type IRODSError struct {
	Code              common.ErrorCode
	Message           string
	ContextualMessage string
}

// NewIRODSError creates a new IRODSError
func NewIRODSError(code common.ErrorCode) *IRODSError {
	return &IRODSError{
		Code:              code,
		Message:           common.GetIRODSErrorString(code),
		ContextualMessage: "",
	}
}

// NewIRODSErrorWithString creates a new IRODSError with message
func NewIRODSErrorWithString(code common.ErrorCode, message string) *IRODSError {
	return &IRODSError{
		Code:              code,
		Message:           common.GetIRODSErrorString(code),
		ContextualMessage: message,
	}
}

// Error returns error message
func (err *IRODSError) Error() string {
	if len(err.ContextualMessage) > 0 {
		return fmt.Sprintf("%s - %s", err.Message, err.ContextualMessage)
	}
	return err.Message
}

// GetCode returns error code
func (err *IRODSError) GetCode() common.ErrorCode {
	return err.Code
}

// ToString stringifies the object
func (err *IRODSError) ToString() string {
	return fmt.Sprintf("<IRODSError %d %s %s>", err.Code, err.Message, err.ContextualMessage)
}

// IsIRODSError checks if the given error is IRODSError
func IsIRODSError(err error) bool {
	return errors.Is(err, &IRODSError{})
}

// GetIRODSErrorCode returns iRODS error code if the error is iRODSError
func GetIRODSErrorCode(err error) common.ErrorCode {
	if err == nil {
		return common.ErrorCode(0)
	}

	var irodsError *IRODSError
	if errors.As(err, &irodsError) {
		return irodsError.GetCode()
	}
	return common.ErrorCode(0)
}
