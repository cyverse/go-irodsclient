package util

import (
	"fmt"
	"path/filepath"
	"strings"
)

// MakeIRODSPath makes the path from collection and data object
func MakeIRODSPath(collectionPath string, dataobjectName string) string {
	if strings.HasSuffix(collectionPath, "/") {
		return fmt.Sprintf("%s/%s", collectionPath[0:len(collectionPath)-1], dataobjectName)
	} else {
		return fmt.Sprintf("%s/%s", collectionPath, dataobjectName)
	}
}

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

// GetIRODSZone returns the zone of the path
func GetIRODSZone(path string) (string, error) {
	if len(path) < 1 {
		return "", fmt.Errorf("Cannot extract Zone from path - %s", path)
	}

	if path[0] != '/' {
		return "", fmt.Errorf("Cannot extract Zone from path - %s", path)
	}

	parts := strings.Split(path[1:], "/")
	if len(parts) >= 1 {
		return parts[0], nil
	}
	return "", fmt.Errorf("Cannot extract Zone from path - %s", path)
}
