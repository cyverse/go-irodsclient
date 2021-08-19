package types

import (
	"errors"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

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
