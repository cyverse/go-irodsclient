package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// ListGroupUsers lists all users in a group
func (fs *FileSystem) ListGroupUsers(group string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetGroupUsersCache(group)
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	users, err := irods_fs.ListGroupUsers(conn, group)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddGroupUsersCache(group, users)

	return users, nil
}

// ListGroups lists all groups
func (fs *FileSystem) ListGroups() ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroups := fs.cache.GetGroupsCache()
	if cachedGroups != nil {
		return cachedGroups, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groups, err := irods_fs.ListGroups(conn)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddGroupsCache(groups)

	return groups, nil
}

// ListUserGroups lists all groups that a user belongs to
func (fs *FileSystem) ListUserGroups(user string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroups := fs.cache.GetUserGroupsCache(user)
	if cachedGroups != nil {
		return cachedGroups, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groupNames, err := irods_fs.ListUserGroupNames(conn, user)
	if err != nil {
		return nil, err
	}

	groups := []*types.IRODSUser{}
	for _, groupName := range groupNames {
		group, err := irods_fs.GetGroup(conn, groupName)
		if err != nil {
			return nil, err
		}

		groups = append(groups, group)
	}

	// cache it
	fs.cache.AddUserGroupsCache(user, groups)

	return groups, nil
}

// ListUsers lists all users
func (fs *FileSystem) ListUsers() ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetUsersCache()
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	users, err := irods_fs.ListUsers(conn)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUsersCache(users)

	return users, nil
}
