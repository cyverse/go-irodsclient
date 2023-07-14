package types

import (
	"errors"

	"golang.org/x/xerrors"
)

var (
	collectionNotEmptyError error = xerrors.New("collection is not empty")
)

// NewCollectionNotEmptyError creates an error for collection not empty
func NewCollectionNotEmptyError() error {
	return collectionNotEmptyError
}

// IsCollectionNotEmptyError evaluates if the given error is collection not empty error
func IsCollectionNotEmptyError(err error) bool {
	return errors.Is(err, collectionNotEmptyError)
}
