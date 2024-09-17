package util

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/xerrors"
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
	return path.Split(p)
}

// GetIRODSPathDirname returns the dir of the path
func GetIRODSPathDirname(p string) string {
	return path.Dir(p)
}

// GetIRODSPathFileName returns the filename of the path
func GetIRODSPathFileName(p string) string {
	return path.Base(p)
}

// GetIRODSZone returns the zone of the path
func GetIRODSZone(p string) (string, error) {
	if len(p) == 0 || p[0] != '/' {
		return "", xerrors.Errorf("cannot extract Zone from path %q", p)
	}

	parts := strings.Split(p[1:], "/")
	if len(parts) >= 1 {
		return parts[0], nil
	}
	return "", xerrors.Errorf("cannot extract Zone from path %q", p)
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

// GetIRODSPathDepth returns depth of the path
// "/" returns 0
// "abc" returns -1
// "/abc" returns 0
// "/a/b" returns 1
// "/a/b/c" returns 2
func GetIRODSPathDepth(p string) int {
	if !strings.HasPrefix(p, "/") {
		return -1
	}

	cleanPath := path.Clean(p)

	if cleanPath == "/" {
		return 0
	}

	pArr := strings.Split(p[1:], "/")
	return len(pArr) - 1
}

// GetParentIRODSDirs returns all parent dirs
func GetParentIRODSDirs(p string) []string {
	parents := []string{}

	if p == "/" {
		return parents
	}

	curPath := p
	for len(curPath) > 0 && curPath != "/" {
		curDir := path.Dir(curPath)
		if len(curDir) > 0 {
			parents = append(parents, curDir)
		}

		curPath = curDir
	}

	// sort
	sort.Slice(parents, func(i int, j int) bool {
		return len(parents[i]) < len(parents[j])
	})

	return parents
}

// GetRelativeIRODSPath returns relative path
func GetRelativeIRODSPath(base string, target string) (string, error) {
	osBase := strings.ReplaceAll(base, "/", string(os.PathSeparator))
	osTarget := strings.ReplaceAll(target, "/", string(os.PathSeparator))

	rel, err := filepath.Rel(osBase, osTarget)
	if err != nil {
		return "", xerrors.Errorf("failed to calculate relative path from %q to %q: %w", osBase, osTarget, err)
	}
	return filepath.ToSlash(rel), nil
}
