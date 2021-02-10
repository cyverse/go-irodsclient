package types

import (
	"fmt"
)

// FileNotFoundError ...
type FileNotFoundError struct {
	message string
}

// NewFileNotFoundError creates FileNotFoundError struct
func NewFileNotFoundError(message string) *FileNotFoundError {
	return &FileNotFoundError{
		message: message,
	}
}

// NewFileNotFoundErrorf creates FileNotFoundError struct
func NewFileNotFoundErrorf(format string, v ...interface{}) *FileNotFoundError {
	return &FileNotFoundError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *FileNotFoundError) Error() string {
	return e.message
}
