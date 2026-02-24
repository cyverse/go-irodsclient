package types

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
)

// ConnectionConfigError contains connection config error information
type ConnectionConfigError struct {
	Account *IRODSAccount
}

// NewConnectionConfigError creates a connection config error
func NewConnectionConfigError(account *IRODSAccount) error {
	if account == nil {
		return &ConnectionConfigError{}
	}

	return &ConnectionConfigError{
		Account: account.GetRedacted(),
	}
}

// Error returns error message
func (err *ConnectionConfigError) Error() string {
	if err.Account == nil {
		return "connection configuration error"
	}

	return fmt.Sprintf("connection configuration error (iRODS server: '%s:%d')", err.Account.Host, err.Account.Port)
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
	var connectionConfigErr *ConnectionConfigError
	return errors.As(err, &connectionConfigErr)
}

// ResourceServerConnectionConfigError contains resource server connection config error information
type ResourceServerConnectionConfigError struct {
	RedirectionInfo *IRODSRedirectionInfo
}

// NewResourceServerConnectionConfigError creates a resource server connection config error
func NewResourceServerConnectionConfigError(redirectionInfo *IRODSRedirectionInfo) error {
	if redirectionInfo == nil {
		return &ResourceServerConnectionConfigError{}
	}

	return &ResourceServerConnectionConfigError{
		RedirectionInfo: redirectionInfo,
	}
}

// Error returns error message
func (err *ResourceServerConnectionConfigError) Error() string {
	if err.RedirectionInfo == nil {
		return "resource server connection configuration error"
	}

	return fmt.Sprintf("resource server connection configuration error (iRODS server: '%s:%d')", err.RedirectionInfo.Host, err.RedirectionInfo.Port)
}

// Is tests type of error
func (err *ResourceServerConnectionConfigError) Is(other error) bool {
	_, ok := other.(*ResourceServerConnectionConfigError)
	return ok
}

// ToString stringifies the object
func (err *ResourceServerConnectionConfigError) ToString() string {
	return "<ResourceServerConnectionConfigError>"
}

// IsResourceServerConnectionConfigError evaluates if the given error is resource server connection config error
func IsResourceServerConnectionConfigError(err error) bool {
	var resourceServerConnectionConfigErr *ResourceServerConnectionConfigError
	return errors.As(err, &resourceServerConnectionConfigErr)
}

// ConnectionError contains connection error information
type ConnectionError struct {
}

// NewConnectionError creates an error for connection error
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
	var connectionErr *ConnectionError
	return errors.As(err, &connectionErr)
}

// AuthError contains auth error information
type AuthError struct {
	Config *IRODSAccount
}

// NewAuthError creates an error for auth
func NewAuthError(config *IRODSAccount) error {
	if config == nil {
		return &AuthError{}
	}

	return &AuthError{
		Config: config.GetRedacted(),
	}
}

// Error returns error message
func (err *AuthError) Error() string {
	if err.Config == nil {
		return "authentication error"
	}

	return fmt.Sprintf("authentication error (auth scheme: %q, proxy username: %q, client username: %q, client zone: %q)", err.Config.AuthenticationScheme, err.Config.ProxyUser, err.Config.ClientUser, err.Config.ClientZone)
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
	var authErr *AuthError
	return errors.As(err, &authErr)
}

// AuthOperationNotFoundError contains auth operation not found error information
type AuthOperationNotFoundError struct {
	OperationName string
}

// NewAuthOperationNotFoundError creates an auth operation not found error
func NewAuthOperationNotFoundError(operationName string) error {
	return &AuthOperationNotFoundError{
		OperationName: operationName,
	}
}

// Error returns error message
func (err *AuthOperationNotFoundError) Error() string {
	return fmt.Sprintf("auth operation not found (operation name: %q)", err.OperationName)
}

// Is tests type of error
func (err *AuthOperationNotFoundError) Is(other error) bool {
	_, ok := other.(*AuthOperationNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *AuthOperationNotFoundError) ToString() string {
	return "<AuthOperationNotFoundError>"
}

// IsAuthOperationNotFoundError evaluates if the given error is auth operation not found error
func IsAuthOperationNotFoundError(err error) bool {
	var authOperationNotFoundErr *AuthOperationNotFoundError
	return errors.As(err, &authOperationNotFoundErr)
}

// AuthFlowError contains auth flow error information
type AuthFlowError struct {
	Message string
}

// NewAuthFlowError creates an auth flow error
func NewAuthFlowError(msg string) error {
	return &AuthFlowError{
		Message: msg,
	}
}

// Error returns error message
func (err *AuthFlowError) Error() string {
	return fmt.Sprintf("auth flow error: %q", err.Message)
}

// Is tests type of error
func (err *AuthFlowError) Is(other error) bool {
	_, ok := other.(*AuthFlowError)
	return ok
}

// ToString stringifies the object
func (err *AuthFlowError) ToString() string {
	return "<AuthFlowError>"
}

// IsAuthFlowError evaluates if the given error is auth flow error
func IsAuthFlowError(err error) bool {
	var authFlowErr *AuthFlowError
	return errors.As(err, &authFlowErr)
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
	var connectionPoolFullErr *ConnectionPoolFullError
	return errors.As(err, &connectionPoolFullErr)
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
	return fmt.Sprintf("collection not empty for path %q", err.Path)
}

// Is tests type of error
func (err *CollectionNotEmptyError) Is(other error) bool {
	_, ok := other.(*CollectionNotEmptyError)
	return ok
}

// ToString stringifies the object
func (err *CollectionNotEmptyError) ToString() string {
	return fmt.Sprintf("<CollectionNotEmptyError %q>", err.Path)
}

// IsCollectionNotEmptyError evaluates if the given error is collection not empty error
func IsCollectionNotEmptyError(err error) bool {
	var collectionNotEmptyErr *CollectionNotEmptyError
	return errors.As(err, &collectionNotEmptyErr)
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
	return fmt.Sprintf("data object/collection not found for path %q", err.Path)
}

// Is tests type of error
func (err *FileNotFoundError) Is(other error) bool {
	_, ok := other.(*FileNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *FileNotFoundError) ToString() string {
	return fmt.Sprintf("<FileNotFoundError %q>", err.Path)
}

// IsFileNotFoundError checks if the given error is FileNotFoundError
func IsFileNotFoundError(err error) bool {
	var fileNotFoundErr *FileNotFoundError
	return errors.As(err, &fileNotFoundErr)
}

// ResourceNotFoundError contains resource not found error information
type ResourceNotFoundError struct {
	Name string
}

// NewResourceNotFoundError creates an error for resource not found
func NewResourceNotFoundError(name string) error {
	return &ResourceNotFoundError{
		Name: name,
	}
}

// Error returns error message
func (err *ResourceNotFoundError) Error() string {
	return fmt.Sprintf("resource not found for path %q", err.Name)
}

// Is tests type of error
func (err *ResourceNotFoundError) Is(other error) bool {
	_, ok := other.(*ResourceNotFoundError)
	return ok
}

// ToString stringifies the object
func (err *ResourceNotFoundError) ToString() string {
	return fmt.Sprintf("<ResourceNotFoundError %q>", err.Name)
}

// IsResourceNotFoundError checks if the given error is ResourceNotFoundError
func IsResourceNotFoundError(err error) bool {
	var resourceNotFoundErr *ResourceNotFoundError
	return errors.As(err, &resourceNotFoundErr)
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
	return fmt.Sprintf("data object/collection already exist for path %q", err.Path)
}

// Is tests type of error
func (err *FileAlreadyExistError) Is(other error) bool {
	_, ok := other.(*FileAlreadyExistError)
	return ok
}

// ToString stringifies the object
func (err *FileAlreadyExistError) ToString() string {
	return fmt.Sprintf("<FileAlreadyExistError %q>", err.Path)
}

// IsFileAlreadyExistError checks if the given error is FileAlreadyExistError
func IsFileAlreadyExistError(err error) bool {
	var fileAlreadyExistErr *FileAlreadyExistError
	return errors.As(err, &fileAlreadyExistErr)
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
	var ticketNotFoundErr *TicketNotFoundError
	return errors.As(err, &ticketNotFoundErr)
}

// UserNotFoundError contains user not found error information
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
	return fmt.Sprintf("user %s not found", err.Name)
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
	var userNotFoundErr *UserNotFoundError
	return errors.As(err, &userNotFoundErr)
}

// APINotSupportedError contains api not supported error information
type APINotSupportedError struct {
	APINumber common.APINumber
}

// NewAPINotSupportedError creates an error for api not supported
func NewAPINotSupportedError(apiNumber common.APINumber) error {
	return &APINotSupportedError{
		APINumber: apiNumber,
	}
}

// Error returns error message
func (err *APINotSupportedError) Error() string {
	return fmt.Sprintf("API number %d not supported", err.APINumber)
}

// Is tests type of error
func (err *APINotSupportedError) Is(other error) bool {
	_, ok := other.(*APINotSupportedError)
	return ok
}

// ToString stringifies the object
func (err *APINotSupportedError) ToString() string {
	return fmt.Sprintf("<APINotSupportedError %d>", err.APINumber)
}

// IsAPINotSupportedError checks if the given error is APINotSupportedError
func IsAPINotSupportedError(err error) bool {
	var apiNotSupportedErr *APINotSupportedError
	return errors.As(err, &apiNotSupportedErr)
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
	var irodsErr *IRODSError
	return errors.As(err, &irodsErr)
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

// IsPermanantFailure returns if given error is permanent failure
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
