package types

import (
	"errors"
	"fmt"

	"github.com/cyverse/go-irodsclient/irods/common"
)

// ConnectionConfigError contains connection config error information
type ConnectionConfigError struct {
	Config *IRODSAccount
}

// NewConnectionConfigError creates a connection config error
func NewConnectionConfigError(config *IRODSAccount) error {
	return &ConnectionConfigError{
		Config: config.GetRedacted(),
	}
}

// Error returns error message
func (err *ConnectionConfigError) Error() string {
	return fmt.Sprintf("connection configuration error (iRODS server: '%s:%d')", err.Config.Host, err.Config.Port)
}

// Is tests type of error
func (err *ConnectionConfigError) Is(other error) bool {
	_, ok := other.(*ConnectionConfigError)
	return ok
}

// ToString stringifies the object
func (err *ConnectionConfigError) ToString() string {
	return "<ConnectionConfigError>"
}

// IsConnectionConfigError evaluates if the given error is connection config error
func IsConnectionConfigError(err error) bool {
	return errors.Is(err, &ConnectionConfigError{})
}

// ConnectionError contains connection error information
type ConnectionError struct {
}

// NewConnectionError creates an error for connection poll full
func NewConnectionError() error {
	return &ConnectionError{}
}

// Error returns error message
func (err *ConnectionError) Error() string {
	return "connection error"
}

// Is tests type of error
func (err *ConnectionError) Is(other error) bool {
	_, ok := other.(*ConnectionError)
	return ok
}

// ToString stringifies the object
func (err *ConnectionError) ToString() string {
	return "<ConnectionError>"
}

// IsConnectionError evaluates if the given error is connection error
func IsConnectionError(err error) bool {
	return errors.Is(err, &ConnectionError{})
}

// AuthError contains auth error information
type AuthError struct {
	Config *IRODSAccount
}

// NewAuthError creates an error for auth
func NewAuthError(config *IRODSAccount) error {
	return &AuthError{
		Config: config.GetRedacted(),
	}
}

// Error returns error message
func (err *AuthError) Error() string {
	return fmt.Sprintf("authentication error (auth scheme: '%s', username: '%s', zone: '%s')", err.Config.AuthenticationScheme, err.Config.ClientUser, err.Config.ClientZone)
}

// Is tests type of error
func (err *AuthError) Is(other error) bool {
	_, ok := other.(*AuthError)
	return ok
}

// ToString stringifies the object
func (err *AuthError) ToString() string {
	return "<AuthError>"
}

// IsAuthError evaluates if the given error is authentication failure
func IsAuthError(err error) bool {
	return errors.Is(err, &AuthError{})
}

// ConnectionPoolFullError contains connection pool full error information
type ConnectionPoolFullError struct {
	Occupied int
	Max      int
}

// NewConnectionPoolFullError creates an error for connection poll full
func NewConnectionPoolFullError(requested int, max int) error {
	return &ConnectionPoolFullError{
		Occupied: requested,
		Max:      requested,
	}
}

// Error returns error message
func (err *ConnectionPoolFullError) Error() string {
	return fmt.Sprintf("connection pool is full (occupied: %d, max: %d)", err.Occupied, err.Max)
}

// Is tests type of error
func (err *ConnectionPoolFullError) Is(other error) bool {
	_, ok := other.(*ConnectionPoolFullError)
	return ok
}

// ToString stringifies the object
func (err *ConnectionPoolFullError) ToString() string {
	return "<ConnectionPoolFullError>"
}

// IsConnectionPoolFullError evaluates if the given error is connection full error
func IsConnectionPoolFullError(err error) bool {
	return errors.Is(err, &ConnectionPoolFullError{})
}

// CollectionNotEmptyError contains collection not empty error information
type CollectionNotEmptyError struct {
	Path string
}

// NewCollectionNotEmptyError creates an error for collection not empty
func NewCollectionNotEmptyError(p string) error {
	return &CollectionNotEmptyError{
		Path: p,
	}
}

// Error returns error message
func (err *CollectionNotEmptyError) Error() string {
	return fmt.Sprintf("collection not empty for path %s", err.Path)
}

// Is tests type of error
func (err *CollectionNotEmptyError) Is(other error) bool {
	_, ok := other.(*CollectionNotEmptyError)
	return ok
}

// ToString stringifies the object
func (err *CollectionNotEmptyError) ToString() string {
	return fmt.Sprintf("<CollectionNotEmptyError %s>", err.Path)
}

// IsCollectionNotEmptyError evaluates if the given error is collection not empty error
func IsCollectionNotEmptyError(err error) bool {
	return errors.Is(err, &CollectionNotEmptyError{})
}

// FileNotFoundError contains file not found error information
type FileNotFoundError struct {
	Path string
}

// NewFileNotFoundError creates an error for file not found
func NewFileNotFoundError(p string) error {
	return &FileNotFoundError{
		Path: p,
	}
}

// Error returns error message
func (err *FileNotFoundError) Error() string {
	return fmt.Sprintf("data object/collection not found for path %s", err.Path)
}

// Is tests type of error
func (err *FileNotFoundError) Is(other error) bool {
	_, ok := other.(*FileNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *FileNotFoundError) ToString() string {
	return fmt.Sprintf("<FileNotFoundError %s>", err.Path)
}

// IsFileNotFoundError checks if the given error is FileNotFoundError
func IsFileNotFoundError(err error) bool {
	return errors.Is(err, &FileNotFoundError{})
}

// FileAlreadyExistError contains file already exist error information
type FileAlreadyExistError struct {
	Path string
}

// NewFileAlreadyExistError creates an error for file already exist
func NewFileAlreadyExistError(p string) error {
	return &FileAlreadyExistError{
		Path: p,
	}
}

// Error returns error message
func (err *FileAlreadyExistError) Error() string {
	return fmt.Sprintf("data object/collection already exist for path %s", err.Path)
}

// Is tests type of error
func (err *FileAlreadyExistError) Is(other error) bool {
	_, ok := other.(*FileAlreadyExistError)
	return ok
}

// ToString stringifies the object
func (err *FileAlreadyExistError) ToString() string {
	return fmt.Sprintf("<FileAlreadyExistError %s>", err.Path)
}

// IsFileAlreadyExistError checks if the given error is FileAlreadyExistError
func IsFileAlreadyExistError(err error) bool {
	return errors.Is(err, &FileAlreadyExistError{})
}

// TicketNotFoundError contains ticket not found error information
type TicketNotFoundError struct {
	Ticket string
}

// NewTicketNotFoundError creates an error for ticket not found
func NewTicketNotFoundError(ticket string) error {
	return &TicketNotFoundError{
		Ticket: ticket,
	}
}

// Error returns error message
func (err *TicketNotFoundError) Error() string {
	return fmt.Sprintf("ticket %s not found", err.Ticket)
}

// Is tests type of error
func (err *TicketNotFoundError) Is(other error) bool {
	_, ok := other.(*TicketNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *TicketNotFoundError) ToString() string {
	return fmt.Sprintf("<TicketNotFoundError %s>", err.Ticket)
}

// IsTicketNotFoundError checks if the given error is TicketNotFoundError
func IsTicketNotFoundError(err error) bool {
	return errors.Is(err, &TicketNotFoundError{})
}

// UserNotFoundError contains user/group not found error information
type UserNotFoundError struct {
	Name string
}

// NewUserNotFoundError creates an error for user not found
func NewUserNotFoundError(name string) error {
	return &UserNotFoundError{
		Name: name,
	}
}

// Error returns error message
func (err *UserNotFoundError) Error() string {
	return fmt.Sprintf("user/group %s not found", err.Name)
}

// Is tests type of error
func (err *UserNotFoundError) Is(other error) bool {
	_, ok := other.(*UserNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *UserNotFoundError) ToString() string {
	return fmt.Sprintf("<UserNotFoundError %s>", err.Name)
}

// IsUserNotFoundError checks if the given error is UserNotFoundError
func IsUserNotFoundError(err error) bool {
	return errors.Is(err, &UserNotFoundError{})
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

// Is tests type of error
func (err *IRODSError) Is(other error) bool {
	_, ok := other.(*IRODSError)
	return ok
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

// IsPermanantFailure returns if given error is permanant failure
func IsPermanantFailure(err error) bool {
	if err == nil {
		return false
	}

	if IsAuthError(err) {
		return true
	} else if IsConnectionConfigError(err) {
		return true
	} else if IsConnectionError(err) {
		return false
	} else if IsConnectionPoolFullError(err) {
		return false
	}

	return false
}
