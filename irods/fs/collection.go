package fs

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

/*
Table "public.r_coll_main"
Column      |          Type           | Collation | Nullable |        Default
------------------+-------------------------+-----------+----------+------------------------
coll_id          | bigint                  |           | not null |
parent_coll_name | character varying(2700) |           | not null |
coll_name        | character varying(2700) |           | not null |
coll_owner_name  | character varying(250)  |           | not null |
coll_owner_zone  | character varying(250)  |           | not null |
coll_map_id      | bigint                  |           |          | 0
coll_inheritance | character varying(1000) |           |          |
coll_type        | character varying(250)  |           |          | '0'::character varying
coll_info1       | character varying(2700) |           |          | '0'::character varying
coll_info2       | character varying(2700) |           |          | '0'::character varying
coll_expiry_ts   | character varying(32)   |           |          |
r_comment        | character varying(1000) |           |          |
create_ts        | character varying(32)   |           |          |
modify_ts        | character varying(32)   |           |          |
Indexes:
"idx_coll_main2" UNIQUE, btree (parent_coll_name, coll_name)
"idx_coll_main3" UNIQUE, btree (coll_name)
"idx_coll_main1" btree (coll_id)
"idx_coll_main_parent_coll_name" btree (parent_coll_name)
*/

// GetCollection returns a collection for the path
func GetCollection(conn *connection.IRODSConnection, path string) (*types.IRODSCollection, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
	query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

	condVal := fmt.Sprintf("= '%s'", path)
	query.AddCondition(common.ICAT_COLUMN_COLL_NAME, condVal)

	queryResult := message.IRODSMessageQueryResult{}
	err := conn.Request(query, &queryResult)
	if err != nil {
		return nil, fmt.Errorf("Could not receive a collection query result message - %v", err)
	}

	if queryResult.RowCount != 1 {
		// file not found
		return nil, types.NewFileNotFoundErrorf("Could not find a collection")
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, fmt.Errorf("Could not receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	var collectionID int64 = -1
	collectionPath := ""
	collectionOwner := ""
	createTime := time.Time{}
	modifyTime := time.Time{}
	for idx := 0; idx < queryResult.AttributeCount; idx++ {
		sqlResult := queryResult.SQLResult[idx]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, fmt.Errorf("Could not receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		value := sqlResult.Values[0]

		switch sqlResult.AttributeIndex {
		case int(common.ICAT_COLUMN_COLL_ID):
			cID, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Could not parse collection id - %s", value)
			}
			collectionID = cID
		case int(common.ICAT_COLUMN_COLL_NAME):
			collectionPath = value
		case int(common.ICAT_COLUMN_COLL_OWNER_NAME):
			collectionOwner = value
		case int(common.ICAT_COLUMN_COLL_CREATE_TIME):
			cT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, fmt.Errorf("Could not parse create time - %s", value)
			}
			createTime = cT
		case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
			mT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, fmt.Errorf("Could not parse modify time - %s", value)
			}
			modifyTime = mT
		default:
			// ignore
		}
	}

	if collectionID == -1 {
		return nil, types.NewFileNotFoundErrorf("Could not find a collection")
	}

	return &types.IRODSCollection{
		ID:         collectionID,
		Path:       collectionPath,
		Name:       util.GetIRODSPathFileName(collectionPath),
		Owner:      collectionOwner,
		CreateTime: createTime,
		ModifyTime: modifyTime,
	}, nil
}

// ListCollectionMeta returns a colleciton metadata for the path
func ListCollectionMeta(conn *connection.IRODSConnection, path string) ([]*types.IRODSMeta, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_UNITS, 1)

		condVal := fmt.Sprintf("= '%s'", path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, condVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection metadata query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive collection metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive collection metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
				case int(common.ICAT_COLUMN_META_COLL_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection metadata id - %s", value)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_COLL_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_COLL_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_COLL_ATTR_UNITS):
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

// ListCollectionAccess returns collection accesses for the path
func ListCollectionAccess(conn *connection.IRODSConnection, path string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	accesses := []*types.IRODSAccess{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		condVal := fmt.Sprintf("= '%s'", path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, condVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection access query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive collection access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive collection access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        path,
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNone,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ACCESS_NAME):
					pagenatedAccesses[row].AccessLevel = types.IRODSAccessLevelType(value)
				case int(common.ICAT_COLUMN_USER_TYPE):
					pagenatedAccesses[row].UserType = types.IRODSUserType(value)
				case int(common.ICAT_COLUMN_USER_NAME):
					pagenatedAccesses[row].UserName = value
				case int(common.ICAT_COLUMN_USER_ZONE):
					pagenatedAccesses[row].UserZone = value
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, pagenatedAccesses...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return accesses, nil
}

// ListSubCollections lists subcollections in the given collection
func ListSubCollections(conn *connection.IRODSConnection, path string) ([]*types.IRODSCollection, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		condVal := fmt.Sprintf("= '%s'", path)
		query.AddCondition(common.ICAT_COLUMN_COLL_PARENT_NAME, condVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedCollections[row] == nil {
					// create a new
					pagenatedCollections[row] = &types.IRODSCollection{
						ID:         -1,
						Path:       "",
						Name:       "",
						Owner:      "",
						CreateTime: time.Time{},
						ModifyTime: time.Time{},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					cID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection id - %s", value)
					}
					pagenatedCollections[row].ID = cID
				case int(common.ICAT_COLUMN_COLL_NAME):
					pagenatedCollections[row].Path = value
					pagenatedCollections[row].Name = util.GetIRODSPathFileName(value)
				case int(common.ICAT_COLUMN_COLL_OWNER_NAME):
					pagenatedCollections[row].Owner = value
				case int(common.ICAT_COLUMN_COLL_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedCollections[row].CreateTime = cT
				case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedCollections[row].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		collections = append(collections, pagenatedCollections...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return collections, nil
}

// CreateCollection creates a collection for the path
func CreateCollection(conn *connection.IRODSConnection, path string, recurse bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageMkcolRequest(path, recurse)
	response := message.IRODSMessageMkcolResponse{}
	return conn.RequestAndCheck(request, &response)
}

// DeleteCollection deletes a collection for the path
func DeleteCollection(conn *connection.IRODSConnection, path string, recurse bool, force bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageRmcolRequest(path, recurse, force)
	response := message.IRODSMessageRmcolResponse{}
	err := conn.RequestAndCheck(request, &response)
	if err != nil {
		return err
	}

	for response.Result == int(common.SYS_SVR_TO_CLI_COLL_STAT) {
		// pack length - Big Endian
		replyBuffer := make([]byte, 4)
		binary.BigEndian.PutUint32(replyBuffer, uint32(common.SYS_CLI_TO_SVR_COLL_STAT_REPLY))

		err = conn.Send(replyBuffer, 4)
		if err != nil {
			return fmt.Errorf("Could not reply to a collection deletion response message - %v", err)
		}

		responseMessageReply, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("Could not receive a collection deletion response message - %v", err)
		}

		response.FromMessage(responseMessageReply)
	}

	return nil
}

// MoveCollection moves a collection for the path to another path
func MoveCollection(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageMvcolRequest(srcPath, destPath)
	response := message.IRODSMessageMvcolResponse{}
	return conn.RequestAndCheck(request, &response)
}

// AddCollectionMeta sets metadata of a data object for the path to the given key values.
// metadata.AVUID is ignored
func AddCollectionMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSCollectionMetaItemType, path, metadata)
	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// DeleteCollectionMeta sets metadata of a data object for the path to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteCollectionMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	var request *message.IRODSMessageModMetaRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSCollectionMetaItemType, path, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSCollectionMetaItemType, path, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSCollectionMetaItemType, path, metadata)
	}

	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// SearchCollectionsByMeta searches collections by metadata
func SearchCollectionsByMeta(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSCollection, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_COLL_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("= '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_COLL_ATTR_VALUE, metaValueCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedCollections[row] == nil {
					// create a new
					pagenatedCollections[row] = &types.IRODSCollection{
						ID:         -1,
						Path:       "",
						Name:       "",
						Owner:      "",
						CreateTime: time.Time{},
						ModifyTime: time.Time{},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					cID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection id - %s", value)
					}
					pagenatedCollections[row].ID = cID
				case int(common.ICAT_COLUMN_COLL_NAME):
					pagenatedCollections[row].Path = value
					pagenatedCollections[row].Name = util.GetIRODSPathFileName(value)
				case int(common.ICAT_COLUMN_COLL_OWNER_NAME):
					pagenatedCollections[row].Owner = value
				case int(common.ICAT_COLUMN_COLL_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedCollections[row].CreateTime = cT
				case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedCollections[row].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		collections = append(collections, pagenatedCollections...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return collections, nil
}

// SearchCollectionsByMetaWildcard searches collections by metadata
// Caution: This is a very slow operation
func SearchCollectionsByMetaWildcard(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSCollection, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_COLL_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("like '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_COLL_ATTR_VALUE, metaValueCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for attr := 0; attr < queryResult.AttributeCount; attr++ {
				sqlResult := queryResult.SQLResult[attr]
				if len(sqlResult.Values) != queryResult.RowCount {
					return nil, fmt.Errorf("Could not receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
				}

				for row := 0; row < queryResult.RowCount; row++ {
					value := sqlResult.Values[row]

					if pagenatedCollections[row] == nil {
						// create a new
						pagenatedCollections[row] = &types.IRODSCollection{
							ID:         -1,
							Path:       "",
							Name:       "",
							Owner:      "",
							CreateTime: time.Time{},
							ModifyTime: time.Time{},
						}
					}

					switch sqlResult.AttributeIndex {
					case int(common.ICAT_COLUMN_COLL_ID):
						cID, err := strconv.ParseInt(value, 10, 64)
						if err != nil {
							return nil, fmt.Errorf("Could not parse collection id - %s", value)
						}
						pagenatedCollections[row].ID = cID
					case int(common.ICAT_COLUMN_COLL_NAME):
						pagenatedCollections[row].Path = value
						pagenatedCollections[row].Name = util.GetIRODSPathFileName(value)
					case int(common.ICAT_COLUMN_COLL_OWNER_NAME):
						pagenatedCollections[row].Owner = value
					case int(common.ICAT_COLUMN_COLL_CREATE_TIME):
						cT, err := util.GetIRODSDateTime(value)
						if err != nil {
							return nil, fmt.Errorf("Could not parse create time - %s", value)
						}
						pagenatedCollections[row].CreateTime = cT
					case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
						mT, err := util.GetIRODSDateTime(value)
						if err != nil {
							return nil, fmt.Errorf("Could not parse modify time - %s", value)
						}
						pagenatedCollections[row].ModifyTime = mT
					default:
						// ignore
					}
				}
			}
		}

		collections = append(collections, pagenatedCollections...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return collections, nil
}

// ChangeAccessControlCollection changes access control on a data object.
func ChangeAccessControlCollection(conn *connection.IRODSConnection, path string, access types.IRODSAccessLevelType, userName, zoneName string, recursive, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageModAccessRequest(access.ChmodString(), userName, zoneName, path, recursive, adminFlag)
	response := message.IRODSMessageModAccessResponse{}
	return conn.RequestAndCheck(request, &response)
}

// SetInheritAccessControl sets the inherit bit on a collection.
func SetInheritAccessControl(conn *connection.IRODSConnection, path string, inherit, recursive, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	inheritStr := "inherit"

	if !inherit {
		inheritStr = "noinherit"
	}

	request := message.NewIRODSMessageModAccessRequest(inheritStr, "", "", path, recursive, adminFlag)
	response := message.IRODSMessageModAccessResponse{}
	return conn.RequestAndCheck(request, &response)
}
