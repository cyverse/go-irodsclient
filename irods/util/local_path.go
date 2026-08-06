package util

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

// CleanLocalPath corrects the path
func CleanLocalPath(p string) string {
	return filepath.Clean(p)
}

// ExpandLocalHomeDir expands ~/
func ExpandLocalHomeDir(path string) (string, error) {
	if len(path) == 0 {
		return "", nil
	}

	if path[0] != '~' {
		return filepath.Abs(path)
	}

	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrapf(err, "failed to get user home dir")
	}

	// resolve "~"
	if len(path) == 1 {
		return filepath.Abs(homedir)
	}

	// resolve "~/"
	if path[1] == '/' {
		path = filepath.Join(homedir, path[2:])
		return filepath.Abs(path)
	}

	return filepath.Abs(path)
}

// ExistLocalFile checks if file exists
func ExistLocalFile(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}

	if !st.IsDir() {
		return true
	}
	return false
}
