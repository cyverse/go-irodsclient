package testcases

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func getLowlevelUserTest() Test {
	return Test{
		Name: "Lowlevel_User",
		Func: lowlevelUserTest,
	}
}

func lowlevelUserTest(t *testing.T, test *Test) {
	t.Run("CreateAndRemoveUser", testCreateAndRemoveUser)
	t.Run("CreateUserWithSpecialCharacterPasswords", testCreateUserWithSpecialCharacterPasswords)
	t.Run("ListUsersByType", testListUsersByType)
	t.Run("AddAndRemoveGroupMembers", testAddAndRemoveGroupMembers)
}

func testCreateAndRemoveUser(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer func() {
		_ = session.ReturnConnection(conn)
	}()

	account, err := server.GetAccount()
	FailError(t, err)

	// create
	testUsername := "testuser1"
	testPassword := "testpassword1"

	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, errors.Errorf("User %s already exists", testUsername))
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
	userAccount, err := server.GetAccount()
	FailError(t, err)

	userAccount.ClientUser = testUsername
	userAccount.ProxyUser = testUsername
	userAccount.Password = testPassword

	conn, err = connection.NewIRODSConnection(account, server.GetConnectionConfig())
	FailError(t, err)

	err = conn.Connect()
	FailError(t, err)
	defer func() {
		_ = conn.Disconnect()
	}()

	// delete
	err = fs.RemoveUser(conn, testUsername, account.ClientZone, types.IRODSUserRodsUser)
	FailError(t, err)

	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, errors.Errorf("User %s still exists", testUsername))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	userConn, err := connection.NewIRODSConnection(userAccount, server.GetConnectionConfig())
	FailError(t, err)

	err = userConn.Connect()
	assert.Error(t, err)
	_ = userConn.Disconnect()
}

func testCreateUserWithSpecialCharacterPasswords(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer func() {
		_ = session.ReturnConnection(conn)
	}()

	account, err := server.GetAccount()
	FailError(t, err)

	// create
	specialCharacters := []string{
		"!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-", "_",
		"=", "+", "{", "}", "[", "]", "|", "\\", ":", ";", "\"", "'",
		"<", ">", ",", "?", "`", "~",
	}

	testUserPattern := "testuser_%d"
	testPasswordPattern := "testpassword_%s"

	for idx, char := range specialCharacters {
		testUsername := fmt.Sprintf(testUserPattern, idx)
		testPassword := fmt.Sprintf(testPasswordPattern, char)

		_, err = fs.GetUser(conn, testUsername, account.ClientZone)
		if err == nil {
			FailError(t, errors.Errorf("User %s already exists", testUsername))
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
		userAccount, err := server.GetAccount()
		FailError(t, err)

		userAccount.ClientUser = testUsername
		userAccount.ProxyUser = testUsername
		userAccount.Password = testPassword

		userConn, err := connection.NewIRODSConnection(userAccount, server.GetConnectionConfig())
		FailError(t, err)

		err = userConn.Connect()
		FailError(t, err)
		err = userConn.Disconnect()
		FailError(t, err)

		// delete
		err = fs.RemoveUser(conn, testUsername, account.ClientZone, types.IRODSUserRodsUser)
		FailError(t, err)

		_, err = fs.GetUser(conn, testUsername, account.ClientZone)
		if err == nil {
			FailError(t, errors.Errorf("User %s still exists", testUsername))
		}
		if err != nil && !types.IsUserNotFoundError(err) {
			FailError(t, err)
		}
	}
}

func testAddAndRemoveGroupMembers(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer func() {
		_ = session.ReturnConnection(conn)
	}()

	account, err := server.GetAccount()
	FailError(t, err)

	// create
	testGroupName := "testgroup2"
	testUsername := "testuser2"

	// create user first
	_, err = fs.GetUser(conn, testUsername, account.ClientZone)
	if err == nil {
		FailError(t, errors.Errorf("User %s already exists", testUsername))
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
		FailError(t, errors.Errorf("Group %s already exists", testGroupName))
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
		FailError(t, errors.Errorf("User %s still exists", testUsername))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}

	// delete group
	err = fs.RemoveUser(conn, testGroupName, account.ClientZone, types.IRODSUserRodsGroup)
	FailError(t, err)

	_, err = fs.GetUser(conn, testGroupName, account.ClientZone)
	if err == nil {
		FailError(t, errors.Errorf("Group %s still exists", testGroupName))
	}
	if err != nil && !types.IsUserNotFoundError(err) {
		FailError(t, err)
	}
}

func testListUsersByType(t *testing.T) {
	test := GetCurrentTest()
	server := test.GetCurrentServer()

	session, err := server.GetSession()
	FailError(t, err)
	defer session.Release()

	conn, err := session.AcquireConnection(true)
	FailError(t, err)
	defer func() {
		_ = session.ReturnConnection(conn)
	}()

	account, err := server.GetAccount()
	FailError(t, err)

	users, err := fs.ListUsersByType(conn, types.IRODSUserRodsUser, account.ClientZone)
	FailError(t, err)

	for _, user := range users {
		if user.Type != types.IRODSUserRodsUser {
			FailError(t, errors.Errorf("User %s is not %s", user.Name, types.IRODSUserRodsUser))
		}
	}

	groups, err := fs.ListUsersByType(conn, types.IRODSUserRodsGroup, account.ClientZone)
	FailError(t, err)

	for _, group := range groups {
		if group.Type != types.IRODSUserRodsGroup {
			FailError(t, errors.Errorf("Group %s is not %s", group.Name, types.IRODSUserRodsGroup))
		}
	}

	admins, err := fs.ListUsersByType(conn, types.IRODSUserRodsAdmin, account.ClientZone)
	FailError(t, err)

	for _, admin := range admins {
		if admin.Type != types.IRODSUserRodsAdmin {
			FailError(t, errors.Errorf("Admin %s is not %s", admin.Name, types.IRODSUserRodsAdmin))
		}
	}

	groupAdmins, err := fs.ListUsersByType(conn, types.IRODSUserGroupAdmin, account.ClientZone)
	FailError(t, err)

	for _, groupAdmin := range groupAdmins {
		if groupAdmin.Type != types.IRODSUserGroupAdmin {
			FailError(t, errors.Errorf("Group Admin %s is not %s", groupAdmin.Name, types.IRODSUserGroupAdmin))
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
