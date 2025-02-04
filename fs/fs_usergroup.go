package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// ListGroupMembers lists all members in a group
func (fs *FileSystem) ListGroupMembers(group string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetGroupMembersCache(group)
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	users, err := irods_fs.ListGroupMembers(conn, group)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddGroupMembersCache(group, users)

	return users, nil
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
		group, err := irods_fs.GetUser(conn, groupName, types.IRODSUserRodsGroup)
		if err != nil {
			return nil, err
		}

		groups = append(groups, group)
	}

	// cache it
	fs.cache.AddUserGroupsCache(user, groups)

	return groups, nil
}

// GetUser returns a user
func (fs *FileSystem) GetUser(username string, userType types.IRODSUserType) (*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetUsersCache(userType)
	for _, cachedUser := range cachedUsers {
		if cachedUser.Name == username {
			return cachedUser, nil
		}
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	user, err := irods_fs.GetUser(conn, username, userType)
	if err != nil {
		return nil, err
	}

	// cache it
	if cachedUsers == nil {
		cachedUsers = append(cachedUsers, user)
		fs.cache.AddUsersCache(userType, cachedUsers)
	}

	return user, nil
}

// ListUsers lists all users
func (fs *FileSystem) ListUsers(userType types.IRODSUserType) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsers := fs.cache.GetUsersCache(userType)
	if cachedUsers != nil {
		return cachedUsers, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	users, err := irods_fs.ListUsers(conn, userType)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUsersCache(userType, users)

	return users, nil
}

/*
func (fs *FileSystem) CreateUser(username string, zone string, userType string) (*types.IRODSUser, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.CreateUser(conn, username, zone, userType)
	if err != nil {
		return nil, err
	}

	user, err := irods_fs.GetUser(conn, username)
}
*/
