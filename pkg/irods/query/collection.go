package query

import (
	"fmt"
	"strconv"

	"github.com/iychoi/go-irodsclient/pkg/irods/common"
	"github.com/iychoi/go-irodsclient/pkg/irods/connection"
	"github.com/iychoi/go-irodsclient/pkg/irods/message"
	"github.com/iychoi/go-irodsclient/pkg/irods/types"
	"github.com/iychoi/go-irodsclient/pkg/irods/util"
)

// GetCollection returns a collection for the path
func GetCollection(conn *connection.IRODSConnection, path string) (*types.IRODSCollection, error) {
	query := message.NewIRODSMessageQuery(common.MAX_QUERY_ROWS, 0, 0, 0)
	query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)

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

	collectionID := -1
	collectionPath := ""
	for idx := 0; idx < queryResult.AttributeCount; idx++ {
		sqlResult := queryResult.SQLResult[idx]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, fmt.Errorf("Could not receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		value := sqlResult.Values[0]

		switch sqlResult.AttributeIndex {
		case int(common.ICAT_COLUMN_COLL_ID):
			cID, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("Could not parse collection id - %s", value)
			}
			collectionID = cID
		case int(common.ICAT_COLUMN_COLL_NAME):
			collectionPath = value
		default:
			// ignore
		}
	}

	return &types.IRODSCollection{
		ID:   collectionID,
		Path: collectionPath,
		Name: util.GetIRODSPathFileName(collectionPath),
		Meta: nil,
	}, nil
}

// ListSubCollections lists subcollections in the given collection
func ListSubCollections(conn *connection.IRODSConnection, path string) ([]*types.IRODSCollection, error) {
	query := message.NewIRODSMessageQuery(common.MAX_QUERY_ROWS, 0, 0, 0)
	query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)

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

	collections := make([]*types.IRODSCollection, queryResult.RowCount, queryResult.RowCount)
	if queryResult.RowCount == 0 {
		return collections, nil
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, fmt.Errorf("Could not receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	for attr := 0; attr < queryResult.AttributeCount; attr++ {
		sqlResult := queryResult.SQLResult[attr]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, fmt.Errorf("Could not receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		for row := 0; row < queryResult.RowCount; row++ {
			value := sqlResult.Values[row]

			if collections[row] == nil {
				// create a new
				collections[row] = &types.IRODSCollection{
					ID:   -1,
					Path: "",
					Name: "",
					Meta: nil,
				}
			}

			switch sqlResult.AttributeIndex {
			case int(common.ICAT_COLUMN_COLL_ID):
				cID, err := strconv.Atoi(value)
				if err != nil {
					return nil, fmt.Errorf("Could not parse collection id - %s", value)
				}
				collections[row].ID = cID

			case int(common.ICAT_COLUMN_COLL_NAME):
				collections[row].Path = value
				collections[row].Name = util.GetIRODSPathFileName(value)
			default:
				// ignore
			}
		}

		collections = append(collections)
	}
	return collections, nil
}
