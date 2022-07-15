package fs

import (
	"fmt"

	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
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

	return nil, fmt.Errorf("unknown type - %s", stat.Type)
}

// ListACLsForEntries returns ACLs for entries in a collection
func (fs *FileSystem) ListACLsForEntries(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	collectionEntry, err := fs.getCollection(irodsPath)
	if err != nil {
		return nil, err
	}

	collection := fs.getCollectionFromEntry(collectionEntry)

	return fs.listACLsForEntries(collection)
}

// ListACLsWithGroupUsers returns ACLs
func (fs *FileSystem) ListACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}

	accesses := []*types.IRODSAccess{}
	if stat.Type == DirectoryEntry {
		accessList, err := fs.ListDirACLsWithGroupUsers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else if stat.Type == FileEntry {
		accessList, err := fs.ListFileACLsWithGroupUsers(path)
		if err != nil {
			return nil, err
		}

		accesses = append(accesses, accessList...)
	} else {
		return nil, fmt.Errorf("unknown type - %s", stat.Type)
	}

	return accesses, nil
}

// ListDirACLs returns ACLs of a directory
func (fs *FileSystem) ListDirACLs(path string) ([]*types.IRODSAccess, error) {
	irodsPath := util.GetCorrectIRODSPath(path)

	// check cache first
	cachedAccesses := fs.cache.GetACLsCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	accesses, err := irods_fs.ListCollectionAccesses(conn, irodsPath)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddACLsCache(irodsPath, accesses)

	return accesses, nil
}

// ListDirACLsWithGroupUsers returns ACLs of a directory
// CAUTION: this can fail if a group contains a lot of users
func (fs *FileSystem) ListDirACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	accesses, err := fs.ListDirACLs(path)
	if err != nil {
		return nil, err
	}

	newAccesses := []*types.IRODSAccess{}
	newAccessesMap := map[string]*types.IRODSAccess{}

	for _, access := range accesses {
		if access.UserType == types.IRODSUserRodsGroup {
			// retrieve all users in the group
			users, err := fs.ListGroupUsers(access.UserName)
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
	cachedAccesses := fs.cache.GetACLsCache(irodsPath)
	if cachedAccesses != nil {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	collectionEntry, err := fs.getCollection(util.GetIRODSPathDirname(irodsPath))
	if err != nil {
		return nil, err
	}

	collection := fs.getCollectionFromEntry(collectionEntry)

	accesses, err := irods_fs.ListDataObjectAccesses(conn, collection, util.GetIRODSPathFileName(irodsPath))
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddACLsCache(irodsPath, accesses)

	return accesses, nil
}

// ListFileACLsWithGroupUsers returns ACLs of a file
func (fs *FileSystem) ListFileACLsWithGroupUsers(path string) ([]*types.IRODSAccess, error) {
	accesses, err := fs.ListFileACLs(path)
	if err != nil {
		return nil, err
	}

	newAccesses := []*types.IRODSAccess{}
	newAccessesMap := map[string]*types.IRODSAccess{}

	for _, access := range accesses {
		if access.UserType == types.IRODSUserRodsGroup {
			// retrieve all users in the group
			users, err := fs.ListGroupUsers(access.UserName)
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

// listACLsForEntries lists ACLs for entries in a collection
func (fs *FileSystem) listACLsForEntries(collection *types.IRODSCollection) ([]*types.IRODSAccess, error) {
	// check cache first
	cachedAccesses := []*types.IRODSAccess{}
	useCached := false

	cachedDirEntryPaths := fs.cache.GetDirCache(collection.Path)
	if cachedDirEntryPaths != nil {
		useCached = true
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			cachedAccess := fs.cache.GetACLsCache(cachedDirEntryPath)
			if cachedAccess != nil {
				cachedAccesses = append(cachedAccesses, cachedAccess...)
			} else {
				useCached = false
			}
		}
	}

	if useCached {
		return cachedAccesses, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.session.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.session.ReturnConnection(conn)

	collectionAccesses, err := irods_fs.ListAccessesForSubCollections(conn, collection.Path)
	if err != nil {
		return nil, err
	}

	accesses := []*types.IRODSAccess{}

	accesses = append(accesses, collectionAccesses...)

	// cache it
	fs.cache.AddACLsCacheMulti(collectionAccesses)

	dataobjectAccesses, err := irods_fs.ListAccessesForDataObjects(conn, collection)
	if err != nil {
		return nil, err
	}

	accesses = append(accesses, dataobjectAccesses...)

	// cache it
	fs.cache.AddACLsCacheMulti(dataobjectAccesses)

	return accesses, nil
}
