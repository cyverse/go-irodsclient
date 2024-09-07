package fs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

// GetGroup returns the group
func GetGroup(conn *connection.IRODSConnection, group string) (*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	users := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condNameVal := fmt.Sprintf("= '%s'", group)
		query.AddCondition(common.ICAT_COLUMN_USER_NAME, condNameVal)
		condTypeVal := fmt.Sprintf("= '%s'", types.IRODSUserRodsGroup)
		query.AddCondition(common.ICAT_COLUMN_USER_TYPE, condTypeVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a group query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			return nil, xerrors.Errorf("received a group query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive group attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsers := make([]*types.IRODSUser, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive group rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedUsers[row] == nil {
					// create a new
					pagenatedUsers[row] = &types.IRODSUser{
						ID:   -1,
						Zone: "",
						Name: "",
						Type: types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_USER_ID):
					userID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse user id %q: %w", value, err)
					}
					pagenatedUsers[row].ID = userID
				case int(common.ICAT_COLUMN_USER_ZONE):
					pagenatedUsers[row].Zone = value
				case int(common.ICAT_COLUMN_USER_NAME):
					pagenatedUsers[row].Name = value
				case int(common.ICAT_COLUMN_USER_TYPE):
					pagenatedUsers[row].Type = types.IRODSUserType(value)
				default:
					// ignore
				}
			}
		}

		users = append(users, pagenatedUsers...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	if len(users) == 0 {
		return nil, xerrors.Errorf("failed to find the group for name %q: %w", group, types.NewUserNotFoundError(group))
	}

	return users[0], nil
}

// ListGroupUsers returns users in the group
func ListGroupUsers(conn *connection.IRODSConnection, group string) ([]*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	users := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condNameVal := fmt.Sprintf("= '%s'", group)
		query.AddCondition(common.ICAT_COLUMN_COLL_USER_GROUP_NAME, condNameVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a group user query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received a group user query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive group user attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsers := make([]*types.IRODSUser, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive group user rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedUsers[row] == nil {
					// create a new
					pagenatedUsers[row] = &types.IRODSUser{
						ID:   -1,
						Zone: "",
						Name: "",
						Type: types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_USER_ID):
					userID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse user id %q: %w", value, err)
					}
					pagenatedUsers[row].ID = userID
				case int(common.ICAT_COLUMN_USER_ZONE):
					pagenatedUsers[row].Zone = value
				case int(common.ICAT_COLUMN_USER_NAME):
					pagenatedUsers[row].Name = value
				case int(common.ICAT_COLUMN_USER_TYPE):
					pagenatedUsers[row].Type = types.IRODSUserType(value)
				default:
					// ignore
				}
			}
		}

		users = append(users, pagenatedUsers...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return users, nil
}

// ListGroups returns all groups
func ListGroups(conn *connection.IRODSConnection) ([]*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	groups := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condTypeVal := fmt.Sprintf("= '%s'", types.IRODSUserRodsGroup)
		query.AddCondition(common.ICAT_COLUMN_USER_TYPE, condTypeVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a group query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received a group query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive group attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedGroups := make([]*types.IRODSUser, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive group rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedGroups[row] == nil {
					// create a new
					pagenatedGroups[row] = &types.IRODSUser{
						ID:   -1,
						Zone: "",
						Name: "",
						Type: types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_USER_ID):
					userID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse user id %q: %w", value, err)
					}
					pagenatedGroups[row].ID = userID
				case int(common.ICAT_COLUMN_USER_ZONE):
					pagenatedGroups[row].Zone = value
				case int(common.ICAT_COLUMN_USER_NAME):
					pagenatedGroups[row].Name = value
				case int(common.ICAT_COLUMN_USER_TYPE):
					pagenatedGroups[row].Type = types.IRODSUserType(value)
				default:
					// ignore
				}
			}
		}

		groups = append(groups, pagenatedGroups...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return groups, nil
}

// ListUsers lists all users
func ListUsers(conn *connection.IRODSConnection) ([]*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	users := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condTypeVal := fmt.Sprintf("<> '%s'", types.IRODSUserRodsGroup)
		query.AddCondition(common.ICAT_COLUMN_USER_TYPE, condTypeVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a user query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received a user query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive user attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsers := make([]*types.IRODSUser, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive user rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedUsers[row] == nil {
					// create a new
					pagenatedUsers[row] = &types.IRODSUser{
						ID:   -1,
						Zone: "",
						Name: "",
						Type: types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_USER_ID):
					userID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse user id %q: %w", value, err)
					}
					pagenatedUsers[row].ID = userID
				case int(common.ICAT_COLUMN_USER_ZONE):
					pagenatedUsers[row].Zone = value
				case int(common.ICAT_COLUMN_USER_NAME):
					pagenatedUsers[row].Name = value
				case int(common.ICAT_COLUMN_USER_TYPE):
					pagenatedUsers[row].Type = types.IRODSUserType(value)
				default:
					// ignore
				}
			}
		}

		users = append(users, pagenatedUsers...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return users, nil
}

// ListUserGroupNames lists the group names a user is a member of
func ListUserGroupNames(conn *connection.IRODSConnection, user string) ([]string, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	var groups []string

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_USER_GROUP_NAME, 1)

		condTypeVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_USER_NAME, condTypeVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a group query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received a group query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive group attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		var groupNames []string

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive group rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if value != user {
					groupNames = append(groupNames, value)
				}

			}
		}

		groups = append(groups, groupNames...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return groups, nil
}

// CreateUser creates a user.
func CreateUser(conn *connection.IRODSConnection, username string, zone string, userType string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	userZoneName := fmt.Sprintf("%s#%s", username, zone)

	req := message.NewIRODSMessageAdminRequest("add", "user", userZoneName, userType, zone)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received create user error: %w", err)
	}
	return nil
}

// ChangeUserPassword changes the password of a user object
func ChangeUserPassword(conn *connection.IRODSConnection, username string, zone string, newPassword string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	userZoneName := fmt.Sprintf("%s#%s", username, zone)

	account := conn.GetAccount()

	oldPassword := account.Password
	if account.AuthenticationScheme.IsPAM() {
		oldPassword = conn.GetPAMToken()
	}

	scrambledPassword := util.ObfuscateNewPassword(newPassword, oldPassword, conn.GetClientSignature())

	req := message.NewIRODSMessageAdminChangePasswordRequest(userZoneName, scrambledPassword, zone)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received change user password error: %w", err)
	}
	return nil
}

// ChangeUserType changes the type / role of a user object
func ChangeUserType(conn *connection.IRODSConnection, username string, zone string, newType string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	userZoneName := fmt.Sprintf("%s#%s", username, zone)

	req := message.NewIRODSMessageAdminRequest("modify", "user", userZoneName, "type", newType, zone)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received change user type error: %w", err)
	}
	return nil
}

// RemoveUser removes a user or a group.
func RemoveUser(conn *connection.IRODSConnection, username string, zone string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("rm", "user", username, zone)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received remove user error: %w", err)
	}
	return nil
}

// CreateGroup creates a group.
func CreateGroup(conn *connection.IRODSConnection, groupname string, groupType string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("add", "user", groupname, groupType)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received create group error: %w", err)
	}
	return nil
}

// AddGroupMember adds a user to a group.
func AddGroupMember(conn *connection.IRODSConnection, groupname string, username string, zone string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("modify", "group", groupname, "add", username, zone)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received add group member error: %w", err)
	}
	return nil
}

// RemoveGroupMember removes a user from a group.
func RemoveGroupMember(conn *connection.IRODSConnection, groupname string, username string, zone string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("modify", "group", groupname, "remove", username, zone)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received remove group member error: %w", err)
	}
	return nil
}

// ListUserResourceQuota lists all existing resource quota of a user or group
func ListUserResourceQuota(conn *connection.IRODSConnection, user string) ([]*types.IRODSQuota, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	quota := []*types.IRODSQuota{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_QUOTA_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_QUOTA_LIMIT, 1)

		condTypeVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_QUOTA_USER_NAME, condTypeVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a quota query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received a quota query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive quota attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedQuota := make([]*types.IRODSQuota, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive quota rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedQuota[row] == nil {
					// create a new
					pagenatedQuota[row] = &types.IRODSQuota{
						RescName: "",
						Limit:    -1,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_QUOTA_RESC_NAME):
					pagenatedQuota[row].RescName = value
				case int(common.ICAT_COLUMN_QUOTA_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse quota limit %q: %w", value, err)
					}
					pagenatedQuota[row].Limit = limit
				default:
					// ignore
				}
			}
		}

		quota = append(quota, pagenatedQuota...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return quota, nil
}

// GetUserGlobalQuota returns the global quota of a user or group
func GetUserGlobalQuota(conn *connection.IRODSConnection, user string) (*types.IRODSQuota, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	quota := []*types.IRODSQuota{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_QUOTA_LIMIT, 1)

		condTypeVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_QUOTA_USER_NAME, condTypeVal)
		condTypeVal = fmt.Sprintf("= '%s'", "0")
		query.AddCondition(common.ICAT_COLUMN_QUOTA_RESC_ID, condTypeVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a quota query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			return nil, xerrors.Errorf("received a quota query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive quota attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedQuota := make([]*types.IRODSQuota, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive quota rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedQuota[row] == nil {
					// create a new
					pagenatedQuota[row] = &types.IRODSQuota{
						RescName: "global",
						Limit:    -1,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_QUOTA_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse quota limit %q: %w", value, err)
					}
					pagenatedQuota[row].Limit = limit
				default:
					// ignore
				}
			}
		}

		quota = append(quota, pagenatedQuota...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return quota[0], nil
}

// AddUserMeta sets metadata of a user object to given key values.
func AddUserMeta(conn *connection.IRODSConnection, user string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSUserMetaItemType, user, metadata)
	response := message.IRODSMessageModifyMetadataResponse{}
	return conn.RequestAndCheck(request, &response, nil)
}

// DeleteUserMeta removes the metadata of a user object.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteUserMeta(conn *connection.IRODSConnection, user string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	var request *message.IRODSMessageModifyMetadataRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSUserMetaItemType, user, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSUserMetaItemType, user, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSUserMetaItemType, user, metadata)
	}

	response := message.IRODSMessageModifyMetadataResponse{}
	return conn.RequestAndCheck(request, &response, nil)
}

// ListUserMeta returns all metadata for the user
func ListUserMeta(conn *connection.IRODSConnection, user string) ([]*types.IRODSMeta, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_UNITS, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_MODIFY_TIME, 1)

		nameCondVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_USER_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a user metadata query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received a user metadata query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive user metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive user metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedMetas[row] == nil {
					// create a new
					pagenatedMetas[row] = &types.IRODSMeta{
						AVUID:      -1,
						Name:       "",
						Value:      "",
						Units:      "",
						CreateTime: time.Time{},
						ModifyTime: time.Time{},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_META_USER_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse user metadata id %q: %w", value, err)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_USER_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_USER_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_USER_ATTR_UNITS):
					pagenatedMetas[row].Units = value
				case int(common.ICAT_COLUMN_META_USER_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedMetas[row].CreateTime = cT
				case int(common.ICAT_COLUMN_META_USER_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
					}
					pagenatedMetas[row].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		metas = append(metas, pagenatedMetas...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return metas, nil
}

// AddChildToResc adds a child to a parent resource
func AddChildToResc(conn *connection.IRODSConnection, parent string, child string, options string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("add", "childtoresc", parent, child, options)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received add child to resc error: %w", err)
	}
	return nil
}

// SetUserQuota sets quota for a given user and resource ('total' for global)
func SetUserQuota(conn *connection.IRODSConnection, user string, resource string, value string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("set-quota", "user", user, resource, value)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received set user quota error: %w", err)
	}
	return nil
}

// SetGroupQuota sets quota for a given user and resource ('total' for global)
func SetGroupQuota(conn *connection.IRODSConnection, group string, resource string, value string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("set-quota", "group", group, resource, value)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received set group quota error: %w", err)
	}
	return nil
}
