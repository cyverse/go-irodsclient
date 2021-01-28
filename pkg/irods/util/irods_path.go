package util

import (
	"path/filepath"
)

// SplitIRODSPath splits the path into dir and file
func SplitIRODSPath(path string) (string, string) {
	return filepath.Split(path)
}

// GetIRODSPathDirname returns the dir of the path
func GetIRODSPathDirname(path string) string {
	return filepath.Dir(path)
}

// GetIRODSPathFileName returns the filename of the path
func GetIRODSPathFileName(path string) string {
	return filepath.Base(path)
}
