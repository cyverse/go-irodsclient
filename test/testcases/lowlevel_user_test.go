package testcases

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"
)

func getLowlevelUserTest() Test {
	return Test{
		Name: "Lowlevel_User",
		Func: lowlevelUserTest,
	}
}

func lowlevelUserTest(t *testing.T, test *Test) {
	t.Run("CreateAndRemoveUser", testCreateAndRemoveUser)
	t.Run("ListUsersByType", testListUsersByType)
	t.Run("AddAndRemoveGroupMembers", testAddAndRemoveGroupMembers)
}

func testCreateAndRemoveUser(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection()
	FailError(t, err)
	defer session.ReturnConnection(conn)

	account := server.GetAccountCopy()

	// create
	testUsername := "testuser1"
	testPassword := "testpassword1"

	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, xerrors.Errorf("User %s already exists", testUsername))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	err = fs.CreateUser(conn, testUsername, account.ClientZone, types.IRODSUserRodsUser)
	FailError(t, err)

	err = fs.ChangeUserPassword(conn, testUsername, account.ClientZone, testPassword)
	FailError(t, err)

	myUser, err := fs.GetUser(conn, testUsername, account.ClientZone)
	FailError(t, err)

	assert.Equal(t, testUsername, myUser.Name)
	assert.Equal(t, account.ClientZone, myUser.Zone)
	assert.Equal(t, types.IRODSUserRodsUser, myUser.Type)

	// login test
	userAccount := server.GetAccountCopy()
	userAccount.ClientUser = testUsername
	userAccount.ProxyUser = testUsername
	userAccount.Password = testPassword

	userConn := connection.NewIRODSConnection(userAccount, 300*time.Second, server.GetApplicationName())
	err = userConn.Connect()
	FailError(t, err)
	userConn.Disconnect()

	// delete
	err = fs.RemoveUser(conn, testUsername, account.ClientZone, types.IRODSUserRodsUser)
	FailError(t, err)

	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, xerrors.Errorf("User %s still exists", testUsername))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	userConn = connection.NewIRODSConnection(userAccount, 300*time.Second, server.GetApplicationName())
	err = userConn.Connect()
	assert.Error(t, err)
	userConn.Disconnect()
}

func testAddAndRemoveGroupMembers(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection()
	FailError(t, err)
	defer session.ReturnConnection(conn)

	account := server.GetAccountCopy()

	// create
	testGroupName := "testgroup2"
	testUsername := "testuser2"

	// create user first
	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, xerrors.Errorf("User %s already exists", testUsername))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	err = fs.CreateUser(conn, testUsername, account.ClientZone, types.IRODSUserRodsUser)
	FailError(t, err)

	myUser, err := fs.GetUser(conn, testUsername, account.ClientZone)
	FailError(t, err)

	assert.Equal(t, testUsername, myUser.Name)
	assert.Equal(t, account.ClientZone, myUser.Zone)
	assert.Equal(t, types.IRODSUserRodsUser, myUser.Type)

	// create group next
	_, err = fs.GetUser(conn, testGroupName, account.ClientZone)
	if err == nil {
		FailError(t, xerrors.Errorf("Group %s already exists", testGroupName))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	err = fs.CreateUser(conn, testGroupName, account.ClientZone, types.IRODSUserRodsGroup)
	FailError(t, err)

	myGroup, err := fs.GetUser(conn, testGroupName, account.ClientZone)
	FailError(t, err)

	assert.Equal(t, testGroupName, myGroup.Name)
	assert.Equal(t, account.ClientZone, myGroup.Zone)
	assert.Equal(t, types.IRODSUserRodsGroup, myGroup.Type)

	// add user to group
	err = fs.AddGroupMember(conn, testGroupName, testUsername, account.ClientZone)
	FailError(t, err)

	// list groups
	groupNames, err := fs.ListUserGroupNames(conn, testUsername, account.ClientZone)
	FailError(t, err)

	assert.Contains(t, groupNames, testGroupName)

	// list members
	users, err := fs.ListGroupMembers(conn, testGroupName, account.ClientZone)
	FailError(t, err)

	found := false
	for _, user := range users {
		if user.Name == testUsername {
			found = true
			break
		}
	}

	assert.True(t, found)

	// remove user from group
	err = fs.RemoveGroupMember(conn, testGroupName, testUsername, account.ClientZone)
	FailError(t, err)

	// list groups
	groupNames, err = fs.ListUserGroupNames(conn, testUsername, account.ClientZone)
	FailError(t, err)

	assert.NotContains(t, groupNames, testGroupName)

	// list members
	users, err = fs.ListGroupMembers(conn, testGroupName, account.ClientZone)
	FailError(t, err)

	found = false
	for _, user := range users {
		if user.Name == testUsername {
			found = true
			break
		}
	}

	assert.False(t, found)

	// delete user
	err = fs.RemoveUser(conn, testUsername, account.ClientZone, types.IRODSUserRodsUser)
	FailError(t, err)

	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, xerrors.Errorf("User %s still exists", testUsername))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	// delete group
	err = fs.RemoveUser(conn, testGroupName, account.ClientZone, types.IRODSUserRodsGroup)
	FailError(t, err)

	_, err = fs.GetUser(conn, testGroupName, account.ClientZone)
	if err == nil {
		FailError(t, xerrors.Errorf("Group %s still exists", testGroupName))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}
}

func testListUsersByType(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetServer()
	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection()
	FailError(t, err)
	defer session.ReturnConnection(conn)

	account := server.GetAccountCopy()

	users, err := fs.ListUsersByType(conn, types.IRODSUserRodsUser, account.ClientZone)
	FailError(t, err)

	for _, user := range users {
		if user.Type != types.IRODSUserRodsUser {
			FailError(t, xerrors.Errorf("User %s is not %s", user.Name, types.IRODSUserRodsUser))
		}
	}

	groups, err := fs.ListUsersByType(conn, types.IRODSUserRodsGroup, account.ClientZone)
	FailError(t, err)

	for _, group := range groups {
		if group.Type != types.IRODSUserRodsGroup {
			FailError(t, xerrors.Errorf("Group %s is not %s", group.Name, types.IRODSUserRodsGroup))
		}
	}

	admins, err := fs.ListUsersByType(conn, types.IRODSUserRodsAdmin, account.ClientZone)
	FailError(t, err)

	for _, admin := range admins {
		if admin.Type != types.IRODSUserRodsAdmin {
			FailError(t, xerrors.Errorf("Admin %s is not %s", admin.Name, types.IRODSUserRodsAdmin))
		}
	}

	groupAdmins, err := fs.ListUsersByType(conn, types.IRODSUserGroupAdmin, account.ClientZone)
	FailError(t, err)

	for _, groupAdmin := range groupAdmins {
		if groupAdmin.Type != types.IRODSUserGroupAdmin {
			FailError(t, xerrors.Errorf("Group Admin %s is not %s", groupAdmin.Name, types.IRODSUserGroupAdmin))
		}
	}

	// at least there should be one user
	assert.GreaterOrEqual(t, len(users)+len(admins), 1)
	found := false
	for _, user := range users {
		if user.Name == account.ProxyUser {
			found = true
			break
		}
	}
	for _, admin := range admins {
		if admin.Name == account.ProxyUser {
			found = true
			break
		}
	}
	assert.True(t, found)
}
