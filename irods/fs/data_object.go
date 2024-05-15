package fs

import (
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForStat(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}
	filepath := path.Join(collection.Path, filename)

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				return nil, xerrors.Errorf("failed to find the data object for path %s: %w", filepath, types.NewFileNotFoundError(filepath))
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("failed to find the data object for path %s: %w", filepath, types.NewFileNotFoundError(filepath))
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

	return nil, xerrors.Errorf("failed to find the data object for path %s: %w", filepath, types.NewFileNotFoundError(filepath))
}

// GetDataObjectMasterReplica returns a data object for the path, returns only master replica
func GetDataObjectMasterReplica(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) (*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForStat(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}
	filepath := path.Join(collection.Path, filename)

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				return nil, xerrors.Errorf("failed to find the data object for path %s: %w", filepath, types.NewFileNotFoundError(filepath))
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("failed to find the data object for path %s: %w", filepath, types.NewFileNotFoundError(filepath))
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

	return nil, xerrors.Errorf("failed to find the data object for path %s: %w", filepath, types.NewFileNotFoundError(filepath))
}

// ListDataObjects lists data objects in the given collection
func ListDataObjects(conn *connection.IRODSConnection, collection *types.IRODSCollection) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForList(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForList(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
					}
					pagenatedDataObjects[row].ID = objID
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedDataObjects[row].Path = util.MakeIRODSPath(collection.Path, value)
					pagenatedDataObjects[row].Name = value
				case int(common.ICAT_COLUMN_DATA_SIZE):
					objSize, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForMetadataList(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_ATTR_UNITS, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_DATA_MODIFY_TIME, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)
		nameCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object metadata query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object metadata query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
				case int(common.ICAT_COLUMN_META_DATA_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object metadata id '%s': %w", value, err)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_DATA_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_DATA_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_DATA_ATTR_UNITS):
					pagenatedMetas[row].Units = value
				case int(common.ICAT_COLUMN_META_DATA_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedMetas[row].CreateTime = cT
				case int(common.ICAT_COLUMN_META_DATA_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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

// ListDataObjectAccesses returns data object accesses for the path
func ListDataObjectAccesses(conn *connection.IRODSConnection, collection *types.IRODSCollection, filename string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForAccessList(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	accesses := []*types.IRODSAccess{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_DATA_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)
		nameCondVal := fmt.Sprintf("= '%s'", filename)
		query.AddCondition(common.ICAT_COLUMN_DATA_NAME, nameCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        util.MakeIRODSPath(collection.Path, filename),
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
					pagenatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForAccessList(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	accesses := []*types.IRODSAccess{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_DATA_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		collCondVal := fmt.Sprintf("= '%s'", collection.Path)
		query.AddCondition(common.ICAT_COLUMN_COLL_NAME, collCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        "",
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_NAME):
					pagenatedAccesses[row].Path = util.MakeIRODSPath(collection.Path, value)
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
					pagenatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
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
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectDelete(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageRemoveDataObjectRequest(path, force)
	response := message.IRODSMessageRemoveDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to delete data object: %w", err)
	}
	return nil
}

// MoveDataObject moves a data object for the path to another path
func MoveDataObject(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectRename(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageMoveDataObjectRequest(srcPath, destPath)
	response := message.IRODSMessageMoveDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", srcPath, types.NewFileNotFoundError(srcPath))
		}
		return xerrors.Errorf("failed to move data object: %w", err)
	}
	return nil
}

// CopyDataObject creates a copy of a data object for the path
func CopyDataObject(conn *connection.IRODSConnection, srcPath string, destPath string, force bool) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectRename(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageCopyDataObjectRequest(srcPath, destPath, force)
	response := message.IRODSMessageCopyDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", srcPath, types.NewFileNotFoundError(srcPath))
		}
		return xerrors.Errorf("failed to copy data object: %w", err)
	}
	return nil
}

// TruncateDataObject truncates a data object for the path to the given size
func TruncateDataObject(conn *connection.IRODSConnection, path string, size int64) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageTruncateDataObjectRequest(path, size)
	response := message.IRODSMessageTruncateDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to truncate data object: %w", err)
	}
	return nil
}

// ReplicateDataObject replicates a data object for the path to the given reousrce
func ReplicateDataObject(conn *connection.IRODSConnection, path string, resource string, update bool, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageReplicateDataObjectRequest(path, resource)

	if update {
		request.AddKeyVal(common.UPDATE_REPL_KW, "")
	}

	if adminFlag {
		request.AddKeyVal(common.ADMIN_KW, "")
	}

	response := message.IRODSMessageReplicateDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to replicate data object: %w", err)
	}
	return nil
}

// TrimDataObject trims replicas for a data object
func TrimDataObject(conn *connection.IRODSConnection, path string, resource string, minCopies int, minAgeMinutes int, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	request := message.NewIRODSMessageTrimDataObjectRequest(path, resource, minCopies, minAgeMinutes)

	if adminFlag {
		request.AddKeyVal(common.ADMIN_KW, "")
	}

	response := message.IRODSMessageTrimDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to trim data object: %w", err)
	}
	return nil
}

// CreateDataObject creates a data object for the path, returns a file handle
func CreateDataObject(conn *connection.IRODSConnection, path string, resource string, mode string, force bool) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectCreate(1)
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

	request := message.NewIRODSMessageCreateDataObjectRequest(path, resource, fileOpenMode, force)
	response := message.IRODSMessageCreateDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to create data object: %w", err)
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
		return nil, -1, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
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

	request := message.NewIRODSMessageOpenDataObjectRequest(path, resource, fileOpenMode)
	response := message.IRODSMessageOpenDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, -1, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return nil, -1, xerrors.Errorf("failed to open data object: %w", err)
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           common.OPER_TYPE_NONE,
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	// handle seek
	var offset int64 = 0
	if fileOpenMode.SeekToEnd() {
		offset, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, -1, err
		}
	}

	return handle, offset, nil
}

// OpenDataObjectWithReplicaToken opens a data object for the path, returns a file handle
func OpenDataObjectWithReplicaToken(conn *connection.IRODSConnection, path string, resource string, mode string, replicaToken string, resourceHierarchy string, threadNum int, dataSize int64) (*types.IRODSFileHandle, int64, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, -1, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
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

	request := message.NewIRODSMessageOpenobjRequestWithReplicaToken(path, fileOpenMode, resourceHierarchy, replicaToken, threadNum, dataSize)

	response := message.IRODSMessageOpenDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, -1, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return nil, -1, xerrors.Errorf("failed to open data object with replica token: %w", err)
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           common.OPER_TYPE_NONE,
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	// handle seek
	var offset int64 = 0
	if fileOpenMode.SeekToEnd() {
		offset, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, -1, err
		}
	}

	return handle, offset, nil
}

// OpenDataObjectWithOperation opens a data object for the path, returns a file handle
func OpenDataObjectWithOperation(conn *connection.IRODSConnection, path string, resource string, mode string, oper common.OperationType) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
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
	response := message.IRODSMessageOpenDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return nil, xerrors.Errorf("failed to open data object: %w", err)
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           oper,
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	// handle seek
	if fileOpenMode.SeekToEnd() {
		_, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, err
		}
	}

	return handle, nil
}

// OpenDataObjectForPutParallel opens a data object for the path, returns a file handle
func OpenDataObjectForPutParallel(conn *connection.IRODSConnection, path string, resource string, mode string, oper common.OperationType, threadNum int, dataSize int64) (*types.IRODSFileHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
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

	request := message.NewIRODSMessageOpenobjRequestForPutParallel(path, resource, fileOpenMode, oper, threadNum, dataSize)
	response := message.IRODSMessageOpenDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return nil, xerrors.Errorf("failed to open data object: %w", err)
	}

	handle := &types.IRODSFileHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       fileOpenMode,
		Resource:       resource,
		Oper:           oper,
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	// handle seek
	if fileOpenMode.SeekToEnd() {
		_, err = seekDataObject(conn, handle, 0, types.SeekEnd)
		if err != nil {
			return handle, err
		}
	}

	return handle, nil
}

// GetReplicaAccessInfo returns replica token and resource hierarchy
func GetReplicaAccessInfo(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) (string, string, error) {
	if conn == nil || !conn.IsConnected() {
		return "", "", xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForStat(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageGetDescriptorInfoRequest(handle.FileDescriptor)
	response := message.IRODSMessageGetDescriptorInfoResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return "", "", xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return "", "", xerrors.Errorf("failed to get replica access info: %w", err)
	}

	// handle fields buried in other structs
	// ResourceHierarchy
	resourceHierarchy := ""
	if response.DataObjectInfo != nil {
		if resourceHierarchyInfo, ok := response.DataObjectInfo["resource_hierarchy"]; ok {
			resourceHierarchy = fmt.Sprintf("%v", resourceHierarchyInfo)
			resourceHierarchy = strings.TrimSpace(resourceHierarchy)
		}
	}

	return response.ReplicaToken, resourceHierarchy, nil
}

// SeekDataObject moves file pointer of a data object, returns offset
func SeekDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, offset int64, whence types.Whence) (int64, error) {
	if conn == nil || !conn.IsConnected() {
		return -1, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	seekLoc, err := seekDataObject(conn, handle, offset, whence)
	if err != nil {
		return seekLoc, xerrors.Errorf("failed to seek data object: %w", err)
	}
	return seekLoc, nil
}

func seekDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, offset int64, whence types.Whence) (int64, error) {
	request := message.NewIRODSMessageSeekDataObjectRequest(handle.FileDescriptor, offset, whence)
	response := message.IRODSMessageSeekDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return -1, xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return -1, xerrors.Errorf("failed to seek data object: %w", err)
	}

	return response.Offset, nil
}

// ReadDataObject reads data from a data object
func ReadDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, buffer []byte) (int, error) {
	return ReadDataObjectWithTrackerCallBack(conn, handle, buffer, nil)
}

// ReadDataObjectWithTrackerCallBack reads data from a data object
func ReadDataObjectWithTrackerCallBack(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, buffer []byte, callback common.TrackerCallBack) (int, error) {
	if conn == nil || !conn.IsConnected() {
		return 0, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectRead(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageReadDataObjectRequest(handle.FileDescriptor, len(buffer))
	response := message.IRODSMessageReadDataObjectResponse{}
	err := conn.RequestAndCheckWithTrackerCallBack(request, &response, buffer, nil, callback)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return 0, xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return 0, xerrors.Errorf("failed to read data object: %w", err)
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
	return WriteDataObjectWithTrackerCallBack(conn, handle, data, nil)
}

// WriteDataObjectWithTrackerCallBack writes data to a data object
func WriteDataObjectWithTrackerCallBack(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, data []byte, callback common.TrackerCallBack) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectWrite(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageWriteDataObjectRequest(handle.FileDescriptor, data)
	response := message.IRODSMessageWriteDataObjectResponse{}
	err := conn.RequestAndCheckWithTrackerCallBack(request, &response, nil, callback, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to write to data object: %w", err)
	}
	return nil
}

// WriteDataObjectAsyncWithTrackerCallBack writes data to a data object asynchronously
func WriteDataObjectAsyncWithTrackerCallBack(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, dataReader io.Reader, totalDataSize int64, callback common.TrackerCallBack) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	requestRRChan := make(chan connection.RequestResponsePair, 100)
	responseRRChan := conn.RequestAsyncWithTrackerCallBack(requestRRChan)

	curProcessed := int64(0)

	var returnErr error

	wg := sync.WaitGroup{}
	wg.Add(1)

	// check response
	go func() {
		defer wg.Done()

		for {
			rrPair, ok := <-responseRRChan
			if !ok {
				// output closed
				// done
				return
			}

			if rrPair.Error != nil {
				// errored
				returnErr = rrPair.Error
				return
			}

			if res, ok := rrPair.Response.(connection.CheckErrorResponse); ok {
				resErr := res.CheckError()
				if resErr != nil {
					if types.GetIRODSErrorCode(resErr) == common.CAT_NO_ROWS_FOUND {
						returnErr = xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
						return
					}

					returnErr = xerrors.Errorf("failed to write to data object: %w", resErr)
					return
				}
			}

			// if no error, drain
		}
	}()

	bufPool := sync.Pool{
		New: func() interface{} {
			b := make([]byte, 640*1024) // 640KB
			return &b
		},
	}

	for {
		if returnErr != nil {
			break
		}

		//buffer := make([]byte, common.ReadWriteBufferSize)
		bufferPtr := bufPool.Get().(*[]byte)
		buffer := *bufferPtr

		bytesRead, readErr := dataReader.Read(buffer)
		if bytesRead > 0 {
			metrics := conn.GetMetrics()
			if metrics != nil {
				metrics.IncreaseCounterForDataObjectWrite(1)
			}

			rrPair := connection.RequestResponsePair{
				Request:  message.NewIRODSMessageWriteDataObjectRequest(handle.FileDescriptor, buffer[:bytesRead]),
				Response: &message.IRODSMessageWriteDataObjectResponse{},
				BsBuffer: nil,
				RequestCallback: func(processed int64, total int64) {
					// callback
					if processed > 0 && processed == total {
						// update
						curProcessed += processed
						if callback != nil {
							callback(curProcessed, totalDataSize)
						}
					}
				},
			}

			// input
			requestRRChan <- rrPair
		}
		// return buffer
		bufPool.Put(bufferPtr)

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				returnErr = xerrors.Errorf("failed to read from Reader: %w", readErr)
				break
			}
		}
	}

	close(requestRRChan)

	// wait until write responses are drained
	wg.Wait()

	return returnErr
}

// TruncateDataObjectHandle truncates a data object to the given size
func TruncateDataObjectHandle(conn *connection.IRODSConnection, handle *types.IRODSFileHandle, size int64) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// iRODS does not provide FTruncate operation as far as I know.
	// Implement this by close/truncate/reopen

	// get offset
	offset, err := seekDataObject(conn, handle, 0, types.SeekCur)
	if err != nil {
		return err
	}

	// close
	request1 := message.NewIRODSMessageCloseDataObjectRequest(handle.FileDescriptor)
	response1 := message.IRODSMessageCloseDataObjectResponse{}
	err = conn.RequestAndCheck(request1, &response1, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to close data object: %w", err)
	}

	// truncate
	request2 := message.NewIRODSMessageTruncateDataObjectRequest(handle.Path, size)
	response2 := message.IRODSMessageTruncateDataObjectResponse{}
	err = conn.RequestAndCheck(request2, &response2, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to truncate data object: %w", err)
	}

	// reopen
	request3 := message.NewIRODSMessageOpenobjRequestWithOperation(handle.Path, handle.Resource, handle.OpenMode, handle.Oper)
	response3 := message.IRODSMessageOpenDataObjectResponse{}
	err = conn.RequestAndCheck(request3, &response3, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to reopen data object: %w", err)
	}

	handle.FileDescriptor = response3.GetFileDescriptor()

	// seek
	request4 := message.NewIRODSMessageSeekDataObjectRequest(handle.FileDescriptor, offset, types.SeekSet)
	response4 := message.IRODSMessageSeekDataObjectResponse{}
	err = conn.RequestAndCheck(request4, &response4, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to seek data object: %w", err)
	}

	return nil
}

// CloseDataObject closes a file handle of a data object
func CloseDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectClose(1)
	}

	if metrics != nil {
		metrics.DecreaseCounterForOpenFileHandles(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageCloseDataObjectRequest(handle.FileDescriptor)
	response := message.IRODSMessageCloseDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to close data object: %w", err)
	}
	return nil
}

// LockDataObject locks a data object for the path, returns a file lock handle
func LockDataObject(conn *connection.IRODSConnection, path string, lockType types.DataObjectLockType, lockCommand types.DataObjectLockCommand) (*types.IRODSFileLockHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	if lockType != types.DataObjectLockTypeRead && lockType != types.DataObjectLockTypeWrite {
		return nil, xerrors.Errorf("lock type is neither read nor write")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageLockDataObjectRequest(path, lockType, lockCommand)
	response := message.IRODSMessageLockDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return nil, xerrors.Errorf("failed to lock data object: %w", err)
	}

	handle := &types.IRODSFileLockHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       lockType.GetFileOpenMode(),
		Type:           lockType,
		Command:        lockCommand,
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	return handle, nil
}

// GetLockDataObject returns a data object lock for the path, returns a file lock handle
func GetLockDataObject(conn *connection.IRODSConnection, path string) (*types.IRODSFileLockHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageLockDataObjectRequest(path, types.DataObjectLockTypeWrite, types.DataObjectLockCommandGetLock)
	response := message.IRODSMessageLockDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return nil, xerrors.Errorf("failed to get data object lock: %w", err)
	}

	handle := &types.IRODSFileLockHandle{
		FileDescriptor: response.GetFileDescriptor(),
		Path:           path,
		OpenMode:       types.DataObjectLockTypeWrite.GetFileOpenMode(),
		Type:           types.DataObjectLockTypeWrite,
		Command:        types.DataObjectLockCommandGetLock,
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	return handle, nil
}

// UnlockDataObject unlocks a file handle of a data object
func UnlockDataObject(conn *connection.IRODSConnection, handle *types.IRODSFileLockHandle) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectClose(1)
	}

	if metrics != nil {
		metrics.DecreaseCounterForOpenFileHandles(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageUnlockDataObjectRequest(handle.Path, handle.FileDescriptor)
	response := message.IRODSMessageUnlockDataObjectResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}
		return xerrors.Errorf("failed to unlock data object: %w", err)
	}
	return nil
}

// AddDataObjectMeta sets metadata of a data object for the path to the given key values.
// metadata.AVUID is ignored
func AddDataObjectMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForMetadataCreate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSDataObjectMetaItemType, path, metadata)
	response := message.IRODSMessageModifyMetadataResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to add data object meta: %w", err)
	}
	return nil
}

// DeleteDataObjectMeta sets metadata of a data object for the path to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteDataObjectMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForMetadataDelete(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	var request *message.IRODSMessageModifyMetadataRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSDataObjectMetaItemType, path, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSDataObjectMetaItemType, path, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSDataObjectMetaItemType, path, metadata)
	}

	response := message.IRODSMessageModifyMetadataResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to delete data object meta: %w", err)
	}
	return nil
}

// SearchDataObjectsByMeta searches data objects by metadata
func SearchDataObjectsByMeta(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSDataObject, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForSearch(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse collection id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForSearch(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse collection id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForSearch(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse collection id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForSearch(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	dataObjects := []*types.IRODSDataObject{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		// data object
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a data object query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received data object query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedDataObjects := make([]*types.IRODSDataObject, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedDataObjects[row] == nil {
					// create a new
					replica := &types.IRODSReplica{
						Number:            -1,
						Owner:             "",
						Checksum:          nil,
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
						return nil, xerrors.Errorf("failed to parse collection id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object id '%s': %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse data object size '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Size = objSize
				case int(common.ICAT_COLUMN_DATA_TYPE_NAME):
					pagenatedDataObjects[row].DataType = value
				case int(common.ICAT_COLUMN_DATA_REPL_NUM):
					repNum, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object replica number '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Number = repNum
				case int(common.ICAT_COLUMN_D_OWNER_NAME):
					pagenatedDataObjects[row].Replicas[0].Owner = value
				case int(common.ICAT_COLUMN_D_DATA_CHECKSUM):
					checksum, err := types.CreateIRODSChecksum(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse data object checksum '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].Checksum = checksum
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
						return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
					}
					pagenatedDataObjects[row].Replicas[0].CreateTime = cT
				case int(common.ICAT_COLUMN_D_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForAccessUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageModifyAccessRequest(access.ChmodString(), userName, zoneName, path, false, adminFlag)
	response := message.IRODSMessageModifyAccessResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the data object for path %s: %w", path, types.NewFileNotFoundError(path))
		}
		return xerrors.Errorf("failed to change data object access: %w", err)
	}
	return nil
}
