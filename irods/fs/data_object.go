package fs

import (
	"fmt"
	"io"
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

	conn.IncreaseDataObjectMetricsStat(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)

		pathCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, pathCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				return nil, types.NewFileNotFoundErrorf("could not find a data object")
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType: "",
						Replicas: []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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
		return nil, types.NewFileNotFoundErrorf("could not find a data object")
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

	for _, object := range mergedDataObjectsMap {
		// returns only the first object
		return object, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a data object")
}

// GetDataObjectMasterReplica returns a data object for the path, returns only master replica
func GetDataObjectMasterReplica(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) (*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsStat(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)
		pathCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, pathCondVal)
		query.AddCondition(common.ICAT_COLUMN_D_REPL_STATUS, "= '1'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				return nil, types.NewFileNotFoundErrorf("could not find a data object")
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType: "",
						Replicas: []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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
		return nil, types.NewFileNotFoundErrorf("could not find a data object")
	}

	// merge data objects per file
	mergedDataObjectsMap := map[int64]*types.IRODSDataObject{}
	for _, object := range dataObjects {
		existingObj, exists := mergedDataObjectsMap[object.ID]
		if exists {
			// compare and replace
			if len(existingObj.Replicas) == 0 {
				// replace
				mergedDataObjectsMap[object.ID] = object
			} else if len(object.Replicas) > 0 {
				if existingObj.Replicas[0].CreateTime.After(object.Replicas[0].CreateTime) {
					// found old replica (meaning master) - replace
					mergedDataObjectsMap[object.ID] = object
				}
			}
		} else {
			// add
			mergedDataObjectsMap[object.ID] = object
		}
	}

	for _, object := range mergedDataObjectsMap {
		// returns only the first object
		return object, nil
	}

	return nil, types.NewFileNotFoundErrorf("could not find a data object")
}

// ListDataObjects lists data objects in the given collection
func ListDataObjects(conn *connection.IRODSConnection, collection *types.IRODSCollection) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseCollectionMetricsList(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return dataObjects, nil
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType:     "",
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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

	conn.IncreaseCollectionMetricsList(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_SIZE, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)
		query.AddCondition(common.ICAT_COLUMN_D_REPL_STATUS, "= '1'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return dataObjects, nil
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType:     "",
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_D_DATA_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object id - %s", value)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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
			// compare and replace
			if len(existingObj.Replicas) == 0 {
				// replace
				mergedDataObjectsMap[object.ID] = object
			} else if len(object.Replicas) > 0 {
				if existingObj.Replicas[0].CreateTime.After(object.Replicas[0].CreateTime) {
					// found old replica (meaning master) - replace
					mergedDataObjectsMap[object.ID] = object
				}
			}
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

// ListDataObjectMeta returns a data object metadata for the path
func ListDataObjectMeta(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) ([]*types.IRODSMeta, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_UNITS, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)
		nameCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object metadata query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return metas, nil
			}

			return nil, fmt.Errorf("received a data object metadata query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, fmt.Errorf("could not parse data object metadata id - %s", value)
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

// ListDataObjectAccesses returns data object accesses for the path
func ListDataObjectAccesses(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	accesses := []*types.IRODSAccess{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_DATA_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)
		nameCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object access query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return accesses, nil
			}

			return nil, fmt.Errorf("received a data object access query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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

// ListAccessesForDataObjects returns data object accesses for data objects in the given path
func ListAccessesForDataObjects(conn *connection.IRODSConnection, collection *types.IRODSCollection) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	accesses := []*types.IRODSAccess{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQuery(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object access query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return accesses, nil
			}

			return nil, fmt.Errorf("received a data object access query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        "",
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNone,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedAccesses[row].Path = util.MakeIRODSPath(collection.Path, value)
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

	conn.IncreaseDataObjectMetricsDelete(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageRmobjRequest(path, force)
	response := message.IRODSMessageRmobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// MoveDataObject moves a data object for the path to another path
func MoveDataObject(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsRename(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageMvobjRequest(srcPath, destPath)
	response := message.IRODSMessageMvobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// CopyDataObject creates a copy of a data object for the path
func CopyDataObject(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageCpobjRequest(srcPath, destPath)
	response := message.IRODSMessageCpobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// TruncateDataObject truncates a data object for the path to the given size
func TruncateDataObject(conn *connection.IRODSConnection, path string, size int64) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageTruncobjRequest(path, size)
	response := message.IRODSMessageTruncobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// ReplicateDataObject replicates a data object for the path to the given reousrce
func ReplicateDataObject(conn *connection.IRODSConnection, path string, resource string, update bool, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageReplobjRequest(path, resource)

	if update {
		request.AddKeyVal(common.UPDATE_REPL_KW, "")
	}

	if adminFlag {
		request.AddKeyVal(common.ADMIN_KW, "")
	}

	response := message.IRODSMessageReplobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// TrimDataObject trims replicas for a data object
func TrimDataObject(conn *connection.IRODSConnection, path string, resource string, minCopies int, minAgeMinutes int, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageTrimobjRequest(path, resource, minCopies, minAgeMinutes)

	if adminFlag {
		request.AddKeyVal(common.ADMIN_KW, "")
	}

	response := message.IRODSMessageTrimobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// CreateDataObject creates a data object for the path, returns a file handle
func CreateDataObject(conn *connection.IRODSConnection, path string, resource string, mode string, force bool) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	fileOpenMode := types.FileOpenMode(mode)

	conn.IncreaseDataObjectMetricsCreate(1)

	request := message.NewIRODSMessageCreateobjRequest(path, resource, fileOpenMode, force)
	response := message.IRODSMessageCreateobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		return nil, err
	}

	return &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           common.OPER_TYPE_NONE,
	}, nil
}

// OpenDataObject opens a data object for the path, returns a file handle
func OpenDataObject(conn *connection.IRODSConnection, path string, resource string, mode string) (*types.IRODSFileHandle, int64, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, -1, fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	fileOpenMode := types.FileOpenMode(mode)

	conn.IncreaseDataObjectMetricsCreate(1)

	request := message.NewIRODSMessageOpenobjRequest(path, resource, fileOpenMode)
	response := message.IRODSMessageOpenobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, -1, types.NewFileNotFoundErrorf("could not find a data object")
		}

		return nil, -1, err
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           common.OPER_TYPE_NONE,
	}

	// handle seek
	var offset int64 = 0
	if fileOpenMode.SeekToEnd() {
		offset, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, -1, fmt.Errorf("could not seek a data object - %v", err)
		}
	}

	return handle, offset, nil
}

// OpenDataObjectWithReplicaToken opens a data object for the path, returns a file handle
func OpenDataObjectWithReplicaToken(conn *connection.IRODSConnection, path string, resource string, mode string, replicaToken string, resourceHierarchy string) (*types.IRODSFileHandle, int64, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, -1, fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	fileOpenMode := types.FileOpenMode(mode)

	request := message.NewIRODSMessageOpenobjRequestWithReplicaToken(path, fileOpenMode, resourceHierarchy, replicaToken)
	response := message.IRODSMessageOpenobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, -1, types.NewFileNotFoundErrorf("could not find a data object")
		}

		return nil, -1, err
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           common.OPER_TYPE_NONE,
	}

	// handle seek
	var offset int64 = 0
	if fileOpenMode.SeekToEnd() {
		offset, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, -1, fmt.Errorf("could not seek a data object - %v", err)
		}
	}

	return handle, offset, nil
}

// OpenDataObjectWithOperation opens a data object for the path, returns a file handle
func OpenDataObjectWithOperation(conn *connection.IRODSConnection, path string, resource string, mode string, oper common.OperationType) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	fileOpenMode := types.FileOpenMode(mode)

	request := message.NewIRODSMessageOpenobjRequestWithOperation(path, resource, fileOpenMode, oper)
	response := message.IRODSMessageOpenobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, types.NewFileNotFoundErrorf("could not find a data object")
		}

		return nil, err
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           oper,
	}

	// handle seek
	if fileOpenMode.SeekToEnd() {
		_, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, fmt.Errorf("could not seek a data object - %v", err)
		}
	}

	return handle, nil
}

// GetReplicaAccessInfo returns replica token and resource hierarchy
func GetReplicaAccessInfo(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) (string, string, error) {
	if conn == nil || !conn.IsConnected() {
		return "", "", fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageDescriptorInfoRequest(handle.FileDescriptor)
	response := message.IRODSMessageDescriptorInfoResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return "", "", types.NewFileNotFoundErrorf("could not find a data object")
		}

		return "", "", err
	}

	return response.ReplicaToken, response.ResourceHierarchy, nil
}

// SeekDataObject moves file pointer of a data object, returns offset
func SeekDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, offset int64, whence types.Whence) (int64, error) {
	if conn == nil || !conn.IsConnected() {
		return -1, fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	return seekDataObject(conn, handle, offset, whence)
}

func seekDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, offset int64, whence types.Whence) (int64, error) {
	request := message.NewIRODSMessageSeekobjRequest(handle.FileDescriptor, offset, whence)
	response := message.IRODSMessageSeekobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return -1, types.NewFileNotFoundErrorf("could not find a data object")
		}

		return -1, err
	}

	return response.Offset, nil
}

// ReadDataObject reads data from a data object
func ReadDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, buffer []byte) (int, error) {
	if conn == nil || !conn.IsConnected() {
		return 0, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsRead(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageReadobjRequest(handle.FileDescriptor, len(buffer))
	response := message.IRODSMessageReadobjResponse{}
	err := conn.RequestAndCheck(request, &response, buffer)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return 0, types.NewFileNotFoundErrorf("could not find a data object")
		}

		return 0, err
	}

	readLen := len(response.Data)
	if readLen < len(buffer) {
		// EOF
		return readLen, io.EOF
	}

	return readLen, nil
}

// WriteDataObject writes data to a data object
func WriteDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, data []byte) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsWrite(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageWriteobjRequest(handle.FileDescriptor, data)
	response := message.IRODSMessageWriteobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// TruncateDataObjectHandle truncates a data object to the given size
func TruncateDataObjectHandle(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, size int64) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsWrite(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// iRODS does not provide FTruncate operation as far as I know.
	// Implement this by close/truncate/reopen

	// get offset
	offset, err := seekDataObject(conn, handle, 0, types.SeekCur)
	if err != nil {
		return fmt.Errorf("could not seek a data object - %v", err)
	}

	// close
	request1 := message.NewIRODSMessageCloseobjRequest(handle.FileDescriptor)
	response1 := message.IRODSMessageCloseobjResponse{}
	err = conn.RequestAndCheck(request1, &response1, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}

	// truncate
	request2 := message.NewIRODSMessageTruncobjRequest(handle.Path, size)
	response2 := message.IRODSMessageTruncobjResponse{}
	err = conn.RequestAndCheck(request2, &response2, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}

	// reopen
	request3 := message.NewIRODSMessageOpenobjRequestWithOperation(handle.Path, handle.Resource, handle.OpenMode, handle.Oper)
	response3 := message.IRODSMessageOpenobjResponse{}
	err = conn.RequestAndCheck(request3, &response3, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return types.NewFileNotFoundErrorf("could not find a data object")
		}

		return err
	}

	handle.FileDescriptor = response3.GetFileDescriptor()

	// seek
	request4 := message.NewIRODSMessageSeekobjRequest(handle.FileDescriptor, offset, types.SeekSet)
	response4 := message.IRODSMessageSeekobjResponse{}
	err = conn.RequestAndCheck(request4, &response4, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return types.NewFileNotFoundErrorf("could not find a data object")
		}

		return err
	}

	return nil
}

// CloseDataObject closes a file handle of a data object
func CloseDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageCloseobjRequest(handle.FileDescriptor)
	response := message.IRODSMessageCloseobjResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// AddDataObjectMeta sets metadata of a data object for the path to the given key values.
// metadata.AVUID is ignored
func AddDataObjectMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSDataObjectMetaItemType, path, metadata)
	response := message.IRODSMessageModMetaResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// DeleteDataObjectMeta sets metadata of a data object for the path to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteDataObjectMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	var request *message.IRODSMessageModMetaRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSDataObjectMetaItemType, path, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSDataObjectMetaItemType, path, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSDataObjectMetaItemType, path, metadata)
	}

	response := message.IRODSMessageModMetaResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// SearchDataObjectsByMeta searches data objects by metadata
func SearchDataObjectsByMeta(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

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
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
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
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return dataObjects, nil
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType:     "",
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse collection id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

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
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("= '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, metaValueCondVal)
		query.AddCondition(common.ICAT_COLUMN_D_REPL_STATUS, "= '1'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return dataObjects, nil
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType:     "",
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse collection id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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
			// compare and replace
			if len(existingObj.Replicas) == 0 {
				// replace
				mergedDataObjectsMap[object.ID] = object
			} else if len(object.Replicas) > 0 {
				if existingObj.Replicas[0].CreateTime.After(object.Replicas[0].CreateTime) {
					// found old replica (meaning master) - replace
					mergedDataObjectsMap[object.ID] = object
				}
			}
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

// SearchDataObjectsByMetaWildcard searches data objects by metadata
// Caution: This is a very slow operation
func SearchDataObjectsByMetaWildcard(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

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
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
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
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return dataObjects, nil
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType:     "",
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse collection id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

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
		query.AddSelect(common.ICAT_COLUMN_DATA_TYPE_NAME, 1)

		// replica
		query.AddSelect(common.ICAT_COLUMN_DATA_REPL_NUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_CHECKSUM, 1)
		query.AddSelect(common.ICAT_COLUMN_D_REPL_STATUS, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_DATA_PATH, 1)
		query.AddSelect(common.ICAT_COLUMN_D_RESC_HIER, 1)
		query.AddSelect(common.ICAT_COLUMN_D_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_D_MODIFY_TIME, 1)

		metaNameCondVal := fmt.Sprintf("= '%s'", metaName)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_NAME, metaNameCondVal)
		metaValueCondVal := fmt.Sprintf("like '%s'", metaValue)
		query.AddCondition(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, metaValueCondVal)
		query.AddCondition(common.ICAT_COLUMN_D_REPL_STATUS, "= '1'")

		queryResult := message.IRODSMessageQueryResult{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, fmt.Errorf("could not receive a data object query result message - %v", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return dataObjects, nil
			}

			return nil, fmt.Errorf("received a data object query error - %v", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, fmt.Errorf("could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, fmt.Errorf("could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						DataType:     "",
						Replicas:     []*types.IRODSReplica{replica},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_ID):
					collID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse collection id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object id - %s", value)
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
						return nil, fmt.Errorf("could not parse data object size - %s", value)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("could not parse data object replica number - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					pagenatedDataObjects[row].Replicas[0].CheckSum = value
				case int(common.ICAT_COLUMN_D_REPL_STATUS):
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
						return nil, fmt.Errorf("could not parse create time - %s", value)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, fmt.Errorf("could not parse modify time - %s", value)
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
			// compare and replace
			if len(existingObj.Replicas) == 0 {
				// replace
				mergedDataObjectsMap[object.ID] = object
			} else if len(object.Replicas) > 0 {
				if existingObj.Replicas[0].CreateTime.After(object.Replicas[0].CreateTime) {
					// found old replica (meaning master) - replace
					mergedDataObjectsMap[object.ID] = object
				}
			}
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

// ChangeDataObjectAccess changes access control on a data object.
func ChangeDataObjectAccess(conn *connection.IRODSConnection, path string, access types.IRODSAccessLevelType, userName, zoneName string, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	conn.IncreaseDataObjectMetricsMeta(1)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageModAccessRequest(access.ChmodString(), userName, zoneName, path, false, adminFlag)
	response := message.IRODSMessageModAccessResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}
