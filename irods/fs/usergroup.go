package fs

import (
	"fmt"
	"strconv"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
)

// GetGroup returns the group
func GetGroup(conn *connection.IRODSConnection, group string) (*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	users := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condNameVal := fmt.Sprintf("= '%s'", group)
		query.AddCondition(common.ICAT_COLUMN_USER_NAME, condNameVal)
		condTypeVal := fmt.Sprintf("= '%s'", types.IRODSUserRodsGroup)
		query.AddCondition(common.ICAT_COLUMN_USER_TYPE, condTypeVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a group query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive group attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsers := make([]*types.IRODSUser, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive group rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("Could not parse user id - %s", value)
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
		return nil, types.NewFileNotFoundErrorf("Could not find a group")
	}

	return users[0], nil
}

// ListGroupUsers returns users in the group
func ListGroupUsers(conn *connection.IRODSConnection, group string) ([]*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	users := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condNameVal := fmt.Sprintf("= '%s'", group)
		query.AddCondition(common.ICAT_COLUMN_COLL_USER_GROUP_NAME, condNameVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a group user query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive group user attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsers := make([]*types.IRODSUser, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive group user rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("Could not parse user id - %s", value)
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

// ListGroups returns the groups
func ListGroups(conn *connection.IRODSConnection) ([]*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	groups := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condTypeVal := fmt.Sprintf("= '%s'", types.IRODSUserRodsGroup)
		query.AddCondition(common.ICAT_COLUMN_USER_TYPE, condTypeVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a group query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive group attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedGroups := make([]*types.IRODSUser, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive group rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("Could not parse user id - %s", value)
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

func ListUsers(conn *connection.IRODSConnection) ([]*types.IRODSUser, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	users := []*types.IRODSUser{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_USER_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)

		condTypeVal := fmt.Sprintf("<> '%s'", types.IRODSUserRodsGroup)
		query.AddCondition(common.ICAT_COLUMN_USER_TYPE, condTypeVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a user query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive user attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsers := make([]*types.IRODSUser, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive user rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("Could not parse user id - %s", value)
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
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	var groups []string

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_USER_GROUP_NAME, 1)

		condTypeVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_USER_NAME, condTypeVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a group query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive group attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		var groupNames []string

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive group rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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

// ListUserResourceQuota lists all existing resource quota of a user or group
func ListUserResourceQuota(conn *connection.IRODSConnection, user string) ([]*types.IRODSQuota, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	quota := []*types.IRODSQuota{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_QUOTA_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_QUOTA_LIMIT, 1)

		condTypeVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_QUOTA_USER_NAME, condTypeVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a quota query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive quota attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedQuota := make([]*types.IRODSQuota, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive quota rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("Could not parse quota limit - %s", value)
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

// ListUserGlobalQuota lists the global quota of a user or group
func ListUserGlobalQuota(conn *connection.IRODSConnection, user string) (*types.IRODSQuota, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	quota := []*types.IRODSQuota{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_QUOTA_LIMIT, 1)

		condTypeVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_QUOTA_USER_NAME, condTypeVal)
		condTypeVal = fmt.Sprintf("= '%s'", "0")
		query.AddCondition(common.ICAT_COLUMN_QUOTA_RESC_ID, condTypeVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a quota query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive quota attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedQuota := make([]*types.IRODSQuota, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive quota rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("Could not parse quota limit - %s", value)
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
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSUserMetaItemType, user, metadata)
	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// DeleteDataObjectMeta removes the metadata of a user object for the path to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteUserMeta(conn *connection.IRODSConnection, user string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	var request *message.IRODSMessageModMetaRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSUserMetaItemType, user, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSUserMetaItemType, user, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSUserMetaItemType, user, metadata)
	}

	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// ListUserMeta returns a data object metadata for the path
func ListUserMeta(conn *connection.IRODSConnection, user string) ([]*types.IRODSMeta, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_USER_ATTR_UNITS, 1)

		nameCondVal := fmt.Sprintf("= '%s'", user)
		query.AddCondition(common.ICAT_COLUMN_USER_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a user object metadata query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive user object metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive user object metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedMetas[row] == nil {
					// create a new
					pagenatedMetas[row] = &types.IRODSMeta{
						AVUID: -1,
						Name:  "",
						Value: "",
						Units: "",
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_META_USER_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse user object metadata id - %s", value)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_USER_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_USER_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_USER_ATTR_UNITS):
					pagenatedMetas[row].Units = value
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
