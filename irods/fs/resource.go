package fs

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// GetResource returns a data object for the path
func GetResource(conn *connection.IRODSConnection, name string) (*types.IRODSResource, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, fmt.Errorf("connection is nil or disconnected")
	}

	// query with AUTO_CLOSE option
	query := message.NewIRODSMessageQuery(1, 0, 0, 0x100)
	query.AddSelect(common.ICAT_COLUMN_R_RESC_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_R_RESC_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_R_ZONE_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_R_TYPE_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_R_CLASS_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_R_LOC, 1)
	query.AddSelect(common.ICAT_COLUMN_R_VAULT_PATH, 1)
	query.AddSelect(common.ICAT_COLUMN_R_RESC_CONTEXT, 1)
	query.AddSelect(common.ICAT_COLUMN_R_CREATE_TIME, 1)
	query.AddSelect(common.ICAT_COLUMN_R_MODIFY_TIME, 1)

	rescCondVal := fmt.Sprintf("= '%s'", name)
	query.AddCondition(common.ICAT_COLUMN_R_RESC_NAME, rescCondVal)

	queryResult := message.IRODSMessageQueryResult{}
	err := conn.Request(query, &queryResult)
	if err != nil {
		return nil, fmt.Errorf("Could not receive a data object query result message - %v", err)
	}

	if queryResult.ContinueIndex != 0 {
		util.LogDebugf("resource query for %s would have continued, more than one result found\n", name)
	}

	if queryResult.RowCount == 0 {
		return nil, errors.New("No row found")
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, fmt.Errorf("Could not receive data object attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	resource := &types.IRODSResource{}

	for attr := 0; attr < queryResult.AttributeCount; attr++ {
		sqlResult := queryResult.SQLResult[attr]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, fmt.Errorf("Could not receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		value := sqlResult.Values[0]

		switch sqlResult.AttributeIndex {
		case int(common.ICAT_COLUMN_R_RESC_ID):
			objID, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Could not parse resource id - %s", value)
			}
			resource.RescID = objID
		case int(common.ICAT_COLUMN_R_RESC_NAME):
			resource.Name = value
		case int(common.ICAT_COLUMN_R_ZONE_NAME):
			resource.Zone = value
		case int(common.ICAT_COLUMN_R_TYPE_NAME):
			resource.Type = value
		case int(common.ICAT_COLUMN_R_CLASS_NAME):
			resource.Class = value
		case int(common.ICAT_COLUMN_R_LOC):
			resource.Location = value
		case int(common.ICAT_COLUMN_R_VAULT_PATH):
			resource.Path = value
		case int(common.ICAT_COLUMN_R_RESC_CONTEXT):
			resource.Context = value
		case int(common.ICAT_COLUMN_R_CREATE_TIME):
			cT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, fmt.Errorf("Could not parse create time - %s", value)
			}
			resource.CreateTime = cT
		case int(common.ICAT_COLUMN_R_MODIFY_TIME):
			mT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, fmt.Errorf("Could not parse modify time - %s", value)
			}
			resource.ModifyTime = mT
		default:
			// ignore
		}
	}

	return resource, nil
}

// AddResourceMeta sets metadata of a resource to the given key values.
// metadata.AVUID is ignored
func AddResourceMeta(conn *connection.IRODSConnection, name string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSResourceMetaItemType, name, metadata)
	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}

// DeleteResourceMeta sets metadata of a resource to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteResourceMeta(conn *connection.IRODSConnection, name string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	var request *message.IRODSMessageModMetaRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSResourceMetaItemType, name, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSResourceMetaItemType, name, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSResourceMetaItemType, name, metadata)
	}

	response := message.IRODSMessageModMetaResponse{}
	return conn.RequestAndCheck(request, &response)
}
