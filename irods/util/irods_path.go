package util

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// MakeIRODSPath makes the path from collection and data object
func MakeIRODSPath(collectionPath string, dataobjectName string) string {
	if strings.HasSuffix(collectionPath, "/") {
		return fmt.Sprintf("%s/%s", collectionPath[0:len(collectionPath)-1], dataobjectName)
	}
	return fmt.Sprintf("%s/%s", collectionPath, dataobjectName)
}

// SplitIRODSPath splits the path into dir and file
func SplitIRODSPath(p string) (string, string) {
	return filepath.Split(p)
}

// GetIRODSPathDirname returns the dir of the path
func GetIRODSPathDirname(p string) string {
	return filepath.Dir(p)
}

// GetIRODSPathFileName returns the filename of the path
func GetIRODSPathFileName(p string) string {
	return filepath.Base(p)
}

// GetIRODSZone returns the zone of the path
func GetIRODSZone(p string) (string, error) {
	if len(p) < 1 {
		return "", fmt.Errorf("cannot extract Zone from path - %s", p)
	}

	if p[0] != '/' {
		return "", fmt.Errorf("cannot extract Zone from path - %s", p)
	}

	parts := strings.Split(p[1:], "/")
	if len(parts) >= 1 {
		return parts[0], nil
	}
	return "", fmt.Errorf("cannot extract Zone from path - %s", p)
}

// GetCorrectIRODSPath corrects the path
func GetCorrectIRODSPath(p string) string {
	if p == "" || p == "/" {
		return "/"
	}

	newPath := path.Clean(p)
	if !strings.HasPrefix(newPath, "/") {
		newPath = fmt.Sprintf("/%s", newPath)
		newPath = path.Clean(newPath)
	}
	return newPath
}
