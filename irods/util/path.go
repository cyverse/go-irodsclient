package util

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/xerrors"
)

// GetCorrectLocalPath corrects the path
func GetCorrectLocalPath(p string) string {
	return filepath.Clean(p)
}

// GetBasename returns basename (filename)
func GetBasename(p string) string {
	p = strings.TrimRight(p, string(os.PathSeparator))
	p = strings.TrimRight(p, "/")

	idx1 := strings.LastIndex(p, string(os.PathSeparator))
	idx2 := strings.LastIndex(p, "/")

	if idx1 < 0 && idx2 < 0 {
		return p
	}

	if idx1 >= idx2 {
		return p[idx1+1:]
	}
	return p[idx2+1:]
}

// GetDir returns directory part of path
func GetDir(p string) string {
	idx1 := strings.LastIndex(p, string(os.PathSeparator))
	idx2 := strings.LastIndex(p, "/")

	if idx1 < 0 && idx2 < 0 {
		return "/"
	}

	if idx1 >= idx2 {
		return p[:idx1]
	}
	return p[:idx2]
}

// Join joins path
func Join(p1 string, p2 ...string) string {
	sep := "/"

	if strings.Contains(p1, string(os.PathSeparator)) {
		sep = string(os.PathSeparator)
	} else if strings.Contains(p1, "/") {
		sep = "/"
	}

	p := []string{}
	p = append(p, p1)
	p = append(p, p2...)

	return strings.Join(p, sep)
}

// ExpandHomeDir expands ~/
func ExpandHomeDir(path string) (string, error) {
	if len(path) == 0 {
		return "", nil
	}

	if path[0] != '~' {
		return filepath.Abs(path)
	}

	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", xerrors.Errorf("failed to get user home dir: %w", err)
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

// ExistFile checks if file exists
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
