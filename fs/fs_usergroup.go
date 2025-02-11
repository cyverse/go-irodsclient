package fs

import (
	irods_fs "github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// GetUser returns a user or a group
func (fs *FileSystem) GetUser(username string, zoneName string, userType types.IRODSUserType) (*types.IRODSUser, error) {
	// check cache first
	cachedUser := fs.cache.GetUserCache(username, zoneName)
	if cachedUser != nil {
		return cachedUser, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	user, err := irods_fs.GetUser(conn, username, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUserCache(user)

	return user, nil
}

// ListUsers lists all users
func (fs *FileSystem) ListUsers(zoneName string, userType types.IRODSUserType) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsernames := fs.cache.GetUserListCache(zoneName, userType)
	if cachedUsernames != nil {
		users := []*types.IRODSUser{}
		allCached := true
		for _, username := range cachedUsernames {
			user := fs.cache.GetUserCache(username, zoneName)
			if user != nil {
				users = append(users, user)
			} else {
				allCached = false
				break
			}
		}

		if allCached {
			return users, nil
		}

		// fall through to retrieve it
	}

	// retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	users, err := irods_fs.ListUsersByType(conn, userType, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	usernames := []string{}
	for _, user := range users {
		usernames = append(usernames, user.Name)
		fs.cache.AddUserCache(user)
	}

	fs.cache.AddUserListCache(zoneName, userType, usernames)
	return users, nil
}

// ListGroupMemberNames lists all member names in a group
func (fs *FileSystem) ListGroupMemberNames(zoneName string, groupName string) ([]string, error) {
	// check cache first
	cachedUsernames := fs.cache.GetGroupMemberCache(groupName, zoneName)
	if cachedUsernames != nil {
		return cachedUsernames, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groupMembers, err := irods_fs.ListGroupMembers(conn, groupName, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	usernames := []string{}
	for _, member := range groupMembers {
		fs.cache.AddUserCache(member)
		usernames = append(usernames, member.Name)
	}

	fs.cache.AddGroupMemberCache(groupName, zoneName, usernames)

	return usernames, nil
}

// ListGroupMembers lists all members in a group
func (fs *FileSystem) ListGroupMembers(zoneName string, groupName string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedUsernames := fs.cache.GetGroupMemberCache(groupName, zoneName)
	if cachedUsernames != nil {
		users := []*types.IRODSUser{}
		allCached := true
		for _, username := range cachedUsernames {
			user := fs.cache.GetUserCache(username, zoneName)
			if user != nil {
				users = append(users, user)
			} else {
				allCached = false
				break
			}
		}

		if allCached {
			return users, nil
		}

		// fall through to retrieve it
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groupMembers, err := irods_fs.ListGroupMembers(conn, groupName, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	usernames := []string{}
	for _, member := range groupMembers {
		fs.cache.AddUserCache(member)
		usernames = append(usernames, member.Name)
	}

	fs.cache.AddGroupMemberCache(groupName, zoneName, usernames)

	return groupMembers, nil
}

// ListUserGroupNames lists all group names that a user belongs to
func (fs *FileSystem) ListUserGroupNames(zoneName string, username string) ([]string, error) {
	// check cache first
	cachedGroupNames := fs.cache.GetUserGroupCache(zoneName, username)
	if cachedGroupNames != nil {
		return cachedGroupNames, nil
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groupNames, err := irods_fs.ListUserGroupNames(conn, username, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUserGroupCache(zoneName, username, groupNames)

	return groupNames, nil
}

// ListUserGroups lists all groups that a user belongs to
func (fs *FileSystem) ListUserGroups(zoneName string, username string) ([]*types.IRODSUser, error) {
	// check cache first
	cachedGroupNames := fs.cache.GetUserGroupCache(zoneName, username)
	if cachedGroupNames != nil {
		groups := []*types.IRODSUser{}
		allCached := true
		for _, groupName := range cachedGroupNames {
			group := fs.cache.GetUserCache(groupName, zoneName)
			if group != nil {
				groups = append(groups, group)
			} else {
				allCached = false
				break
			}
		}

		if allCached {
			return groups, nil
		}

		// fall through to retrieve it
	}

	// otherwise, retrieve it and add it to cache
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	groupNames, err := irods_fs.ListUserGroupNames(conn, username, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUserGroupCache(zoneName, username, groupNames)

	groups := []*types.IRODSUser{}
	allCached := true
	for _, groupName := range groupNames {
		group := fs.cache.GetUserCache(groupName, zoneName)
		if group != nil {
			groups = append(groups, group)
		} else {
			allCached = false
			break
		}
	}

	if allCached {
		return groups, nil
	}

	// retrieve all groups and cache them
	groupList, err := irods_fs.ListUsersByType(conn, types.IRODSUserRodsGroup, zoneName)
	if err != nil {
		return nil, err
	}

	for _, group := range groupList {
		fs.cache.AddUserCache(group)
	}

	return groups, nil
}

// CreateUser creates a new user
func (fs *FileSystem) CreateUser(username string, zoneName string, userType types.IRODSUserType) (*types.IRODSUser, error) {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return nil, err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.CreateUser(conn, username, zoneName, userType)
	if err != nil {
		return nil, err
	}

	user, err := irods_fs.GetUser(conn, username, zoneName)
	if err != nil {
		return nil, err
	}

	// cache it
	fs.cache.AddUserCache(user)

	return user, nil
}

// ChangeUserPassword changes a user's password
func (fs *FileSystem) ChangeUserPassword(username string, zoneName string, newPassword string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ChangeUserPassword(conn, username, zoneName, newPassword)
	if err != nil {
		return err
	}

	return nil
}

// ChangeUserType changes a user's type
func (fs *FileSystem) ChangeUserType(username string, zoneName string, newType types.IRODSUserType) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.ChangeUserType(conn, username, zoneName, newType)
	if err != nil {
		return err
	}

	return nil
}

// RemoveUser removes a user
func (fs *FileSystem) RemoveUser(username string, zoneName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.RemoveUser(conn, username, zoneName)
	if err != nil {
		return err
	}

	return nil
}

// AddGroupMember adds a user to a group
func (fs *FileSystem) AddGroupMember(groupName string, username string, zoneName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.AddGroupMember(conn, groupName, username, zoneName)
	if err != nil {
		return err
	}

	return nil
}

// RemoveGroupMember removes a user from a group
func (fs *FileSystem) RemoveGroupMember(groupName string, username string, zoneName string) error {
	conn, err := fs.metadataSession.AcquireConnection()
	if err != nil {
		return err
	}
	defer fs.metadataSession.ReturnConnection(conn) //nolint

	err = irods_fs.RemoveGroupMember(conn, groupName, username, zoneName)
	if err != nil {
		return err
	}

	return nil
}
