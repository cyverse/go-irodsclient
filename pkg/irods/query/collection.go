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

// GetCollection returns a collection for the path
func GetCollection(conn *connection.IRODSConnection, path string) (*types.IRODSCollection, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
	query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

	condVal := fmt.Sprintf("= '%s'", path)
	query.AddCondition(common.ICAT_COLUMN_COLL_NAME, condVal)

	queryMessage, err := query.GetMessage()
	if err != nil {
		return nil, fmt.Errorf("Could not make a collection query message - %s", err.Error())
	}

	err = conn.SendMessage(queryMessage)
	if err != nil {
		return nil, fmt.Errorf("Could not send a collection query message - %s", err.Error())
	}

	// Server responds with results
	queryResultMessage, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("Could not receive a collection query result message - %s", err.Error())
	}

	queryResult := message.IRODSMessageQueryResult{}
	err = queryResult.FromMessage(queryResultMessage)
	if err != nil {
		return nil, fmt.Errorf("Could not receive a collection query result message - %s", err.Error())
	}

	if queryResult.RowCount != 1 {
		return nil, fmt.Errorf("Could not receive a collection - received %d rows", queryResult.RowCount)
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, fmt.Errorf("Could not receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	var collectionID int64 = -1
	collectionPath := ""
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

	return &types.IRODSCollection{
		ID:         collectionID,
		Path:       collectionPath,
		Name:       util.GetIRODSPathFileName(collectionPath),
		CreateTime: createTime,
		ModifyTime: modifyTime,
	}, nil
}

// GetCollectionMeta returns a colleciton metadata for the path
func GetCollectionMeta(conn *connection.IRODSConnection, path string) ([]*types.IRODSMeta, error) {
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

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a collection metadata query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a collection metadata query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection metadata query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection metadata query result message - %s", err.Error())
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
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		condVal := fmt.Sprintf("= '%s'", path)
		query.AddCondition(common.ICAT_COLUMN_COLL_PARENT_NAME, condVal)

		queryMessage, err := query.GetMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not make a collection query message - %s", err.Error())
		}

		err = conn.SendMessage(queryMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not send a collection query message - %s", err.Error())
		}

		// Server responds with results
		queryResultMessage, err := conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection query result message - %s", err.Error())
		}

		queryResult := message.IRODSMessageQueryResult{}
		err = queryResult.FromMessage(queryResultMessage)
		if err != nil {
			return nil, fmt.Errorf("Could not receive a collection query result message - %s", err.Error())
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
