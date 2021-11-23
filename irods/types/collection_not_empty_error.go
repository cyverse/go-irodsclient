package types

import (
	"fmt"
)

// CollectionNotEmptyError ...
type CollectionNotEmptyError struct {
	message string
}

// NewCollectionNotEmptyError creates CollectionNotEmptyError struct
func NewCollectionNotEmptyError(message string) *CollectionNotEmptyError {
	return &CollectionNotEmptyError{
		message: message,
	}
}

// NewCollectionNotEmptyErrorf creates CollectionNotEmptyError struct
func NewCollectionNotEmptyErrorf(format string, v ...interface{}) *CollectionNotEmptyError {
	return &CollectionNotEmptyError{
		message: fmt.Sprintf(format, v...),
	}
}

func (e *CollectionNotEmptyError) Error() string {
	return e.message
}

// IsCollectionNotEmptyError evaluates if the given error is CollectionNotEmptyError
func IsCollectionNotEmptyError(err error) bool {
	if _, ok := err.(*CollectionNotEmptyError); ok {
		return true
	}

	return false
}
