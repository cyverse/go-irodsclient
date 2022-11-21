package util

import (
	"os"
	"path/filepath"
	"strings"
)

func ExpandHomeDir(path string) (string, error) {
	// resolve "~/"
	if path == "~" {
		homedir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}

		return homedir, nil
	} else if strings.HasPrefix(path, "~/") {
		homedir, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}

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
