package query

import (
	"fmt"
	"strconv"
	"time"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	"github.com/iychoi/go-irodsclient/pkg/irods/message"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
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

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a data object query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a data object query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
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
		return nil, fmt.Errorf("Could not find matching data object - %s, %s", collection.Path, filename)
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

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a data object query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a data object query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
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
		return nil, fmt.Errorf("Could not find matching data object - %s, %s", collection.Path, filename)
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

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a data object query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a data object query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
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

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a data object query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a data object query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object query result message - %s", err.Error())
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

	return dataObjects, nil
}

// GetDataObjectMeta returns a data object metadata for the path
func GetDataObjectMeta(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) ([]*types.IRODSMeta, error) {
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

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a data object metadata query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a data object metadata query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object metadata query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a data object metadata query result message - %s", err.Error())
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

			metas = append(metas, pagenatedMetas...)

			continueIndex = queryResult.ContinueIndex
			if continueIndex == 0 {
				continueQuery = false
			}
		}
	}

	return metas, nil
}
