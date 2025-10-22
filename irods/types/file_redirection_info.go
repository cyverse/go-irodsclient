package types

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// IRODSFileOpenRedirectionHandle contains file open redirection handle
type IRODSFileOpenRedirectionHandle struct {
	FileDescriptor int
	// Path has an absolute path to the data object
	Path            string
	Resource        string
	Threads         int
	CheckSum        string
	RedirectionInfo *IRODSRedirectionInfo
}

// IRODSRedirectionInfo contains redirection info
type IRODSRedirectionInfo struct {
	Port         int
	Cookie       int
	ServerSocket int
	WindowSize   int
	Host         string
}

// ToString stringifies the object
func (info *IRODSFileOpenRedirectionHandle) ToString() string {
	return fmt.Sprintf("<IRODSFileOpenRedirectionHandle %d %s %s %d %s %s>", info.FileDescriptor, info.Path, info.Resource, info.Threads, info.CheckSum, info.RedirectionInfo.ToString())
}

// ToString stringifies the object
func (info *IRODSRedirectionInfo) ToString() string {
	return fmt.Sprintf("<IRODSRedirectionInfo %d %d %d %d %s>", info.Port, info.Cookie, info.ServerSocket, info.WindowSize, info.Host)
}

// Validate validates redirection info
func (info *IRODSRedirectionInfo) Validate() error {
	if len(info.Host) == 0 {
		newErr := NewResourceServerConnectionConfigError(info)
		return errors.Wrapf(newErr, "empty host")
	}

	if info.Port <= 0 {
		newErr := NewResourceServerConnectionConfigError(info)
		return errors.Wrapf(newErr, "empty port")
	}

	if info.Cookie <= 0 {
		newErr := NewResourceServerConnectionConfigError(info)
		return errors.Wrapf(newErr, "empty cookie")
	}

	if info.ServerSocket <= 0 {
		newErr := NewResourceServerConnectionConfigError(info)
		return errors.Wrapf(newErr, "empty server socket")
	}

	return nil
}
