package util

import (
	"os"
	"path/filepath"

	"golang.org/x/xerrors"
)

// GetCorrectLocalPath corrects the path
func GetCorrectLocalPath(p string) string {
	return filepath.Clean(p)
}

func ExpandHomeDir(path string) (string, error) {
	if len(path) == 0 {
		return "", nil
	}

	if path[0] != '~' {
		return path, nil
	}

	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", xerrors.Errorf("failed to get user home dir: %w", err)
	}

	// resolve "~"
	if len(path) == 1 {
		return homedir, nil
	}

	// resolve "~/"
	if path[1] == '/' {
		path = filepath.Join(homedir, path[2:])
		return filepath.Clean(path), nil
	}

	return path, nil
}

func ExistFile(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}

	if !st.IsDir() {
		return true
	}
	return false
}
