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
)

/*
Table "public.r_data_main"
Column      |          Type           | Collation | Nullable |        Default
-----------------+-------------------------+-----------+----------+------------------------
data_id         | bigint                  |           | not null |
coll_id         | bigint                  |           | not null |
data_name       | character varying(1000) |           | not null |
data_repl_num   | integer                 |           | not null |
data_version    | character varying(250)  |           |          | '0'::character varying
data_type_name  | character varying(250)  |           | not null |
data_size       | bigint                  |           | not null |
resc_group_name | character varying(250)  |           |          |
resc_name       | character varying(250)  |           | not null |
data_path       | character varying(2700) |           | not null |
data_owner_name | character varying(250)  |           | not null |
data_owner_zone | character varying(250)  |           | not null |
data_is_dirty   | integer                 |           |          | 0
data_status     | character varying(250)  |           |          |
data_checksum   | character varying(1000) |           |          |
data_expiry_ts  | character varying(32)   |           |          |
data_map_id     | bigint                  |           |          | 0
data_mode       | character varying(32)   |           |          |
r_comment       | character varying(1000) |           |          |
create_ts       | character varying(32)   |           |          |
modify_ts       | character varying(32)   |           |          |
resc_hier       | character varying(1000) |           |          |
Indexes:
"idx_data_main2" UNIQUE, btree (coll_id, data_name, data_repl_num, data_version)
"idx_data_main1" btree (data_id)
"idx_data_main3" btree (coll_id)
"idx_data_main4" btree (data_name)
"idx_data_main5" btree (data_type_name)
"idx_data_main6" btree (data_path)
*/

// GetDataObject returns a data object for the path
func GetDataObject(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) (*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collidCondVal := fmt.Sprintf("= '%d'", collection.ID)
		query.AddCondition(common.ICAT_COLUMN_D_COLL_ID, collidCondVal)
		pathCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, pathCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:       -1,
						Path:     "",
						Name:     "",
						Size:     0,
						Replicas: []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	// merge data objects per file
	mergedDataObjectsMap := map[int64]*types.IRODSDataObject{}
	for _, object := range dataObjects {
		existingObj, exists := mergedDataObjectsMap[object.ID]
		if exists {
			// merge
			existingObj.Replicas = append(existingObj.Replicas, object.Replicas[0])
		} else {
			// add
			mergedDataObjectsMap[object.ID] = object
		}
	}

	// convert map to array
	mergedDataObjects := []*types.IRODSDataObject{}
	for _, object := range mergedDataObjectsMap {
		mergedDataObjects = append(mergedDataObjects, object)
	}

	if len(mergedDataObjects) == 0 {
		return nil, types.NewFileNotFoundErrorf("Could not find a data object")
	}

	return mergedDataObjects[0], nil
}

// GetDataObjectMasterReplica returns a data object for the path, returns only master replica
func GetDataObjectMasterReplica(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) (*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collidCondVal := fmt.Sprintf("= '%d'", collection.ID)
		query.AddCondition(common.ICAT_COLUMN_D_COLL_ID, collidCondVal)
		pathCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, pathCondVal)
		query.AddCondition(common.ICAT_COLUMN_DATA_REPL_NUM, "= '0'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:       -1,
						Path:     "",
						Name:     "",
						Size:     0,
						Replicas: []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	if len(dataObjects) == 0 {
		return nil, types.NewFileNotFoundErrorf("Could not find a data object")
	}

	return dataObjects[0], nil
}

// ListDataObjects lists data objects in the given collection
func ListDataObjects(conn *connection.IRODSConnection, collection *types.IRODSCollection) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collidCondVal := fmt.Sprintf("= '%d'", collection.ID)
		query.AddCondition(common.ICAT_COLUMN_D_COLL_ID, collidCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:           -1,
						CollectionID: collection.ID,
						Path:         "",
						Name:         "",
						Size:         0,
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	// merge data objects per file
	mergedDataObjectsMap := map[int64]*types.IRODSDataObject{}
	for _, object := range dataObjects {
		existingObj, exists := mergedDataObjectsMap[object.ID]
		if exists {
			// merge
			existingObj.Replicas = append(existingObj.Replicas, object.Replicas[0])
		} else {
			// add
			mergedDataObjectsMap[object.ID] = object
		}
	}

	// convert map to array
	mergedDataObjects := []*types.IRODSDataObject{}
	for _, object := range mergedDataObjectsMap {
		mergedDataObjects = append(mergedDataObjects, object)
	}

	return mergedDataObjects, nil
}

// ListDataObjectsMasterReplica lists data objects in the given collection, returns only master replica
func ListDataObjectsMasterReplica(conn *connection.IRODSConnection, collection *types.IRODSCollection) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collidCondVal := fmt.Sprintf("= '%d'", collection.ID)
		query.AddCondition(common.ICAT_COLUMN_D_COLL_ID, collidCondVal)
		query.AddCondition(common.ICAT_COLUMN_DATA_REPL_NUM, "= '0'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:           -1,
						CollectionID: collection.ID,
						Path:         "",
						Name:         "",
						Size:         0,
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return dataObjects, nil
}

// ListDataObjectMeta returns a data object metadata for the path
func ListDataObjectMeta(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) ([]*types.IRODSMeta, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_UNITS, 1)

		collidCondVal := fmt.Sprintf("= '%d'", collection.ID)
		query.AddCondition(common.ICAT_COLUMN_D_COLL_ID, collidCondVal)
		nameCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object metadata query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
				case int(common.ICAT_COLUMN_META_DATA_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object metadata id - %s", value)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_DATA_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_DATA_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_DATA_ATTR_UNITS):
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

// ListDataObjectAccess returns data object accesses for the path
func ListDataObjectAccess(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	accesses := []*types.IRODSAccess{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_DATA_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		collidCondVal := fmt.Sprintf("= '%d'", collection.ID)
		query.AddCondition(common.ICAT_COLUMN_D_COLL_ID, collidCondVal)
		nameCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object access query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        util.MakeIRODSPath(collection.Path, filename),
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNone,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
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

// DeleteDataObject deletes a data object for the path
func DeleteDataObject(conn *connection.IRODSConnection, path string, force bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageRmobjRequest(path, force)
	response := message.IRODSMessageRmobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// MoveDataObject moves a data object for the path to another path
func MoveDataObject(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageMvobjRequest(srcPath, destPath)
	response := message.IRODSMessageMvobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// CopyDataObject creates a copy of a data object for the path
func CopyDataObject(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageCpobjRequest(srcPath, destPath)
	response := message.IRODSMessageCpobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// TruncateDataObject truncates a data object for the path to the given size
func TruncateDataObject(conn *connection.IRODSConnection, path string, size int64) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageTruncobjRequest(path, size)
	response := message.IRODSMessageTruncobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// ReplicateDataObject replicates a data object for the path to the given reousrce
func ReplicateDataObject(conn *connection.IRODSConnection, path string, resource string, update bool, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageReplobjRequest(path, resource)

	if update {
		request.AddKeyVal(common.UPDATE_REPL_KW, "")
	}

	if adminFlag {
		request.AddKeyVal(common.ADMIN_KW, "")
	}

	response := message.IRODSMessageReplobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// TrimDataObject trims replicas for a data object
func TrimDataObject(conn *connection.IRODSConnection, path string, resource string, minCopies int, minAgeMinutes int, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageTrimobjRequest(path, resource, minCopies, minAgeMinutes)

	if adminFlag {
		request.AddKeyVal(common.ADMIN_KW, "")
	}

	response := message.IRODSMessageTrimobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// CreateDataObject creates a data object for the path, returns a file handle
func CreateDataObject(conn *connection.IRODSConnection, path string, resource string, force bool) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageCreateobjRequest(path, resource, force)
	response := message.IRODSMessageCreateobjResponse{}
	err := conn.RequestAndCheck(request, &response)
	if err != nil {
		return nil, err
	}

	return &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
	}, nil
}

// OpenDataObject opens a data object for the path, returns a file handle
func OpenDataObject(conn *connection.IRODSConnection, path string, resource string, mode string) (*types.IRODSFileHandle, int64, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, -1, fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageOpenobjRequest(path, resource, types.FileOpenMode(mode))
	response := message.IRODSMessageOpenobjResponse{}
	err := conn.RequestAndCheck(request, &response)
	if err != nil {
		return nil, -1, err
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
	}

	// handle seek
	_, seekToEnd := types.GetFileOpenFlagSeekToEnd(types.FileOpenMode(mode))
	var offset int64 = 0
	if seekToEnd {
		offset, err = SeekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, -1, fmt.Errorf("Could not seek a data object - %v", err)
		}
	}

	return handle, offset, nil
}

// OpenDataObjectWithOperation opens a data object for the path, returns a file handle
func OpenDataObjectWithOperation(conn *connection.IRODSConnection, path string, resource string, mode string, oper common.OperationType) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageOpenobjRequestWithOperation(path, resource, types.FileOpenMode(mode), oper)
	response := message.IRODSMessageOpenobjResponse{}
	err := conn.RequestAndCheck(request, &response)
	if err != nil {
		return nil, err
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
	}

	// handle seek
	_, seekToEnd := types.GetFileOpenFlagSeekToEnd(types.FileOpenMode(mode))
	if seekToEnd {
		_, err = SeekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, fmt.Errorf("Could not seek a data object - %v", err)
		}
	}

	return handle, nil
}

// SeekDataObject moves file pointer of a data object, returns offset
func SeekDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, offset int64, whence types.Whence) (int64, error) {
	if conn == nil || !conn.IsConnected() {
		return -1, fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageSeekobjRequest(handle.FileDescriptor, offset, whence)
	response := message.IRODSMessageSeekobjResponse{}
	err := conn.RequestAndCheck(request, &response)
	if err != nil {
		return -1, err
	}

	return response.Offset, nil
}

// ReadDataObject reads data from a data object
func ReadDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, length int) ([]byte, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageReadobjRequest(handle.FileDescriptor, length)
	response := message.IRODSMessageReadobjResponse{}
	err := conn.RequestAndCheck(request, &response)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// WriteDataObject writes data to a data object
func WriteDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, data []byte) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageWriteobjRequest(handle.FileDescriptor, data)
	response := message.IRODSMessageWriteobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// CloseDataObject closes a file handle of a data object
func CloseDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageCloseobjRequest(handle.FileDescriptor)
	response := message.IRODSMessageCloseobjResponse{}
	return conn.RequestAndCheck(request, &response)
}

// AddDataObjectMeta sets metadata of a data object for the path to the given key values.
// metadata.AVUID is ignored
func AddDataObjectMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSDataObjectMetaItemType, path, metadata)
	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// DeleteDataObjectMeta sets metadata of a data object for the path to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteDataObjectMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	var request *message.IRODSMessageModMetaRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSDataObjectMetaItemType, path, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSDataObjectMetaItemType, path, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSDataObjectMetaItemType, path, metadata)
	}

	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// SearchDataObjectsByMeta searches data objects by metadata
func SearchDataObjectsByMeta(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("= '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, metaValueCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:           -1,
						CollectionID: -1,
						Path:         "",
						Name:         "",
						Size:         0,
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection id - %s", value)
					}
					pagenatedDataObjects[row].CollectionID = collID
				case int(common.ICAT_COLUMN_COLL_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(value, pagenatedDataObjects[row].Path)
					} else {
						pagenatedDataObjects[row].Path = value
					}
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(pagenatedDataObjects[row].Path, value)
					} else {
						pagenatedDataObjects[row].Path = value
					}
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	// merge data objects per file
	mergedDataObjectsMap := map[int64]*types.IRODSDataObject{}
	for _, object := range dataObjects {
		existingObj, exists := mergedDataObjectsMap[object.ID]
		if exists {
			// merge
			existingObj.Replicas = append(existingObj.Replicas, object.Replicas[0])
		} else {
			// add
			mergedDataObjectsMap[object.ID] = object
		}
	}

	// convert map to array
	mergedDataObjects := []*types.IRODSDataObject{}
	for _, object := range mergedDataObjectsMap {
		mergedDataObjects = append(mergedDataObjects, object)
	}

	return mergedDataObjects, nil
}

// SearchDataObjectsMasterReplicaByMeta searches data objects by metadata, returns only master replica
func SearchDataObjectsMasterReplicaByMeta(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("= '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, metaValueCondVal)
		query.AddCondition(common.ICAT_COLUMN_DATA_REPL_NUM, "= '0'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:           -1,
						CollectionID: -1,
						Path:         "",
						Name:         "",
						Size:         0,
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection id - %s", value)
					}
					pagenatedDataObjects[row].CollectionID = collID
				case int(common.ICAT_COLUMN_COLL_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(value, pagenatedDataObjects[row].Path)
					} else {
						pagenatedDataObjects[row].Path = value
					}
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(pagenatedDataObjects[row].Path, value)
					} else {
						pagenatedDataObjects[row].Path = value
					}
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return dataObjects, nil
}

// SearchDataObjectsByMetaWildcard searches data objects by metadata
// Caution: This is a very slow operation
func SearchDataObjectsByMetaWildcard(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("like '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, metaValueCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:           -1,
						CollectionID: -1,
						Path:         "",
						Name:         "",
						Size:         0,
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection id - %s", value)
					}
					pagenatedDataObjects[row].CollectionID = collID
				case int(common.ICAT_COLUMN_COLL_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(value, pagenatedDataObjects[row].Path)
					} else {
						pagenatedDataObjects[row].Path = value
					}
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(pagenatedDataObjects[row].Path, value)
					} else {
						pagenatedDataObjects[row].Path = value
					}
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	// merge data objects per file
	mergedDataObjectsMap := map[int64]*types.IRODSDataObject{}
	for _, object := range dataObjects {
		existingObj, exists := mergedDataObjectsMap[object.ID]
		if exists {
			// merge
			existingObj.Replicas = append(existingObj.Replicas, object.Replicas[0])
		} else {
			// add
			mergedDataObjectsMap[object.ID] = object
		}
	}

	// convert map to array
	mergedDataObjects := []*types.IRODSDataObject{}
	for _, object := range mergedDataObjectsMap {
		mergedDataObjects = append(mergedDataObjects, object)
	}

	return mergedDataObjects, nil
}

// SearchDataObjectsMasterReplicaByMetaWildcard searches data objects by metadata, returns only master replica
// Caution: This is a very slow operation
func SearchDataObjectsMasterReplicaByMetaWildcard(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("like '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, metaValueCondVal)
		query.AddCondition(common.ICAT_COLUMN_DATA_REPL_NUM, "= '0'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						CheckSum:          "",
						Status:            "",
						ResourceName:      "",
						Path:              "",
						ResourceHierarchy: "",
						CreateTime:        time.Time{},
						ModifyTime:        time.Time{},
					}

					pagenatedDataObjects[row] = &types.IRODSDataObject{
						ID:           -1,
						CollectionID: -1,
						Path:         "",
						Name:         "",
						Size:         0,
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse collection id - %s", value)
					}
					pagenatedDataObjects[row].CollectionID = collID
				case int(common.ICAT_COLUMN_COLL_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(value, pagenatedDataObjects[row].Path)
					} else {
						pagenatedDataObjects[row].Path = value
					}
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					if len(pagenatedDataObjects[row].Path) > 0 {
						pagenatedDataObjects[row].Path = util.MakeIRODSPath(pagenatedDataObjects[row].Path, value)
					} else {
						pagenatedDataObjects[row].Path = value
					}
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_DATA_STATUS):
					pagenatedDataObjects[row].Replicas[0].Status = value
				case int(common.ICAT_COLUMN_D_RESC_NAME):
					pagenatedDataObjects[row].Replicas[0].ResourceName = value
				case int(common.ICAT_COLUMN_D_DATA_PATH):
					pagenatedDataObjects[row].Replicas[0].Path = value
				case int(common.ICAT_COLUMN_D_RESC_HIER):
					pagenatedDataObjects[row].Replicas[0].ResourceHierarchy = value
				case int(common.ICAT_COLUMN_D_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("Could not parse modify time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		dataObjects = append(dataObjects, pagenatedDataObjects...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return dataObjects, nil
}

// ChangeAccessControlDataObject changes access control on a data object.
func ChangeAccessControlDataObject(conn *connection.IRODSConnection, path string, access types.IRODSAccessLevelType, userName, zoneName string, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageModAccessRequest(access.ChmodString(), userName, zoneName, path, false, adminFlag)
	response := message.IRODSMessageModAccessResponse{}
	return conn.RequestAndCheck(request, &response)
}
