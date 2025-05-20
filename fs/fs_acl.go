package fs

import (
	"fmt"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

// ListACLs returns ACLs
func (fs *FileSystem) ListACLs(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	if stat.Type == DirectoryEntry {
		return fs.ListDirACLs(path)
	} else if stat.Type == FileEntry {
		return fs.ListFileACLs(path)
	}

	return nil, xerrors.Errorf("unknown type %q", stat.Type)
}

// ListACLsForEntries returns ACLs for entries in a collection
func (fs *FileSystem) ListACLsForEntries(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	return fs.listACLsForEntries(irodsPath)
}

// ListACLsWithGroupUsers returns ACLs
func (fs *FileSystem) ListACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	accesses := []*types.IRODSAccess{}
	if stat.Type == DirectoryEntry {
		accessList, err := fs.ListDirACLsWithGroupMembers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else if stat.Type == FileEntry {
		accessList, err := fs.ListFileACLsWithGroupMembers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else {
		return nil, xerrors.Errorf("unknown type %q", stat.Type)
	}

	return accesses, nil
}

// GetDirACLInheritance returns ACL inheritance of a directory
func (fs *FileSystem) GetDirACLInheritance(path string) (*types.IRODSAccessInheritance, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// retrieve it
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	inheritance, err := irods_fs.GetCollectionAccessInheritance(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	return inheritance, nil
}

// ListDirACLs returns ACLs of a directory
func (fs *FileSystem) ListDirACLs(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check cache first
	cachedAccesses := fs.cache.GetAclCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	accesses, err := irods_fs.ListCollectionAccesses(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddAclCache(irodsPath, accesses)

	return accesses, nil
}

// ListDirACLsWithGroupMembers returns ACLs of a directory
// CAUTION: this can fail if a group contains a lot of users
func (fs *FileSystem) ListDirACLsWithGroupMembers(path string) ([]*types.IRODSAccess, error) {
	accesses, err := fs.ListDirACLs(path)
	if err != nil {
		return nil, err
	}

	newAccesses := []*types.IRODSAccess{}
	newAccessesMap := map[string]*types.IRODSAccess{}

	for _, access := range accesses {
		if access.UserType == types.IRODSUserRodsGroup {
			// retrieve all users in the group
			users, err := fs.ListGroupMembers(access.UserZone, access.UserName)
			if err != nil {
				return nil, err
			}

			for _, user := range users {
				userAccess := &types.IRODSAccess{
					Path:        access.Path,
					UserName:    user.Name,
					UserZone:    user.Zone,
					UserType:    user.Type,
					AccessLevel: access.AccessLevel,
				}

				// remove duplicates
				newAccessesMap[fmt.Sprintf("%s||%s", user.Name, access.AccessLevel)] = userAccess
			}
		} else {
			newAccessesMap[fmt.Sprintf("%s||%s", access.UserName, access.AccessLevel)] = access
		}
	}

	// convert map to array
	for _, access := range newAccessesMap {
		newAccesses = append(newAccesses, access)
	}

	return newAccesses, nil
}

// ListFileACLs returns ACLs of a file
func (fs *FileSystem) ListFileACLs(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check cache first
	cachedAccesses := fs.cache.GetAclCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	accesses, err := irods_fs.ListDataObjectAccesses(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddAclCache(irodsPath, accesses)

	return accesses, nil
}

// ListFileACLsWithGroupMembers returns ACLs of a file
func (fs *FileSystem) ListFileACLsWithGroupMembers(path string) ([]*types.IRODSAccess, error) {
	accesses, err := fs.ListFileACLs(path)
	if err != nil {
		return nil, err
	}

	newAccesses := []*types.IRODSAccess{}
	newAccessesMap := map[string]*types.IRODSAccess{}

	for _, access := range accesses {
		if access.UserType == types.IRODSUserRodsGroup {
			// retrieve all members in the group
			users, err := fs.ListGroupMembers(access.UserName, access.UserName)
			if err != nil {
				return nil, err
			}

			for _, user := range users {
				userAccess := &types.IRODSAccess{
					Path:        access.Path,
					UserName:    user.Name,
					UserZone:    user.Zone,
					UserType:    user.Type,
					AccessLevel: access.AccessLevel,
				}

				// remove duplicates
				newAccessesMap[fmt.Sprintf("%s||%s", user.Name, access.AccessLevel)] = userAccess
			}
		} else {
			newAccessesMap[fmt.Sprintf("%s||%s", access.UserName, access.AccessLevel)] = access
		}
	}

	// convert map to array
	for _, access := range newAccessesMap {
		newAccesses = append(newAccesses, access)
	}

	return newAccesses, nil
}

// ChangeACLs changes ACLs of a file or directory
func (fs *FileSystem) ChangeACLs(path string, access types.IRODSAccessLevelType, userName string, zoneName string, recursive bool, adminFlag bool) error {
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ChangeAccess(conn, path, access, userName, zoneName, recursive, adminFlag)
	if err != nil {
		return err
	}

	return nil
}

// ChangeDirACLInheritance changes ACL inheritance of a directory
func (fs *FileSystem) ChangeDirACLInheritance(path string, inherit bool, recursive bool, adminFlag bool) error {
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ChangeAccessInherit(conn, path, inherit, recursive, adminFlag)
	if err != nil {
		return err
	}

	return nil
}

// listACLsForEntries lists ACLs for entries in a collection
func (fs *FileSystem) listACLsForEntries(collPath string) ([]*types.IRODSAccess, error) {
	// check cache first
	cachedAccesses := []*types.IRODSAccess{}
	useCached := false

	cachedDirEntryPaths := fs.cache.GetDirCache(collPath)
	if cachedDirEntryPaths != nil {
		useCached = true
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			cachedAccess := fs.cache.GetAclCache(cachedDirEntryPath)
			if cachedAccess != nil {
				cachedAccesses = append(cachedAccesses, cachedAccess...)
			} else {
				useCached = false
				break
			}
		}
	}

	if useCached {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection(true)
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	// ListAccessesForSubCollections does not return Accesses for some files/dirs
	// For these files/dirs, we compare accesses we obtained to the list of files/dirs in a dir
	// and register an empty Access array to cache
	dirEntryPathsToBeAdded := []string{}
	if cachedDirEntryPaths != nil {
		dirEntryPathsToBeAdded = append(dirEntryPathsToBeAdded, cachedDirEntryPaths...)
	} else {
		// otherwise, retrieve it and add it to cache
		collections, err := irods_fs.ListSubCollections(conn, collPath)
		if err != nil {
			return nil, err
		}

		entries := []*Entry{}

		for _, coll := range collections {
			entry := NewEntryFromCollection(coll)
			entries = append(entries, entry)

			// cache it
			fs.cache.RemoveNegativeEntryCache(entry.Path)
			fs.cache.AddEntryCache(entry)
		}

		dataobjects, err := irods_fs.ListDataObjectsMasterReplica(conn, collPath)
		if err != nil {
			return nil, err
		}

		for _, dataobject := range dataobjects {
			if len(dataobject.Replicas) == 0 {
				continue
			}

			entry := NewEntryFromDataObject(dataobject)
			entries = append(entries, entry)

			// cache it
			fs.cache.RemoveNegativeEntryCache(entry.Path)
			fs.cache.AddEntryCache(entry)
		}

		// cache dir entries
		dirEntryPaths := []string{}
		for _, entry := range entries {
			dirEntryPaths = append(dirEntryPaths, entry.Path)
			dirEntryPathsToBeAdded = append(dirEntryPathsToBeAdded, entry.Path)
		}
		fs.cache.AddDirCache(collPath, dirEntryPaths)
	}

	// list access
	dirEntryPathsAdded := map[string]bool{}

	collectionAccesses, err := irods_fs.ListAccessesForSubCollections(conn, collPath)
	if err != nil {
		return nil, err
	}

	accesses := []*types.IRODSAccess{}

	accesses = append(accesses, collectionAccesses...)

	// cache it
	fs.cache.AddAclCacheMulti(collectionAccesses)

	dataobjectAccesses, err := irods_fs.ListAccessesForDataObjectsInCollection(conn, collPath)
	if err != nil {
		return nil, err
	}

	accesses = append(accesses, dataobjectAccesses...)

	// cache it
	fs.cache.AddAclCacheMulti(dataobjectAccesses)

	for _, acc := range accesses {
		dirEntryPathsAdded[acc.Path] = true
	}

	// cache missing dir entries
	for _, pathToBeAdded := range dirEntryPathsToBeAdded {
		if _, ok := dirEntryPathsAdded[pathToBeAdded]; !ok {
			// add empty one
			fs.cache.AddAclCache(pathToBeAdded, []*types.IRODSAccess{})
			dirEntryPathsAdded[pathToBeAdded] = true
		}
	}

	return accesses, nil
}
