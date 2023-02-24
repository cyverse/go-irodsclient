package fs

import (
	"fmt"
	"strconv"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

// GetResource returns a resource for the name
func GetResource(conn *connection.IRODSConnection, name string) (*types.IRODSResource, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// query with AUTO_CLOSE option
	query := message.NewIRODSMessageQueryRequest(1, 0, 0, 0x100)
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

	queryResult := message.IRODSMessageQueryResponse{}
	err := conn.Request(query, &queryResult, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to receive a resource query result message: %w", err)
	}

	err = queryResult.CheckError()
	if err != nil {
		return nil, xerrors.Errorf("received a data resource query error: %w", err)
	}

	// TODO: do we need to print out?
	//if queryResult.ContinueIndex != 0 {
	//	logger.Debugf("resource query for %s would have continued, more than one result found\n", name)
	//}

	if queryResult.RowCount == 0 {
		return nil, xerrors.Errorf("no row found")
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, xerrors.Errorf("failed to receive resource attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	resource := &types.IRODSResource{}

	for attr := 0; attr < queryResult.AttributeCount; attr++ {
		sqlResult := queryResult.SQLResult[attr]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, xerrors.Errorf("failed to receive resource rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		value := sqlResult.Values[0]

		switch sqlResult.AttributeIndex {
		case int(common.ICAT_COLUMN_R_RESC_ID):
			objID, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse resource id '%s': %w", value, err)
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
				return nil, xerrors.Errorf("failed to parse create time '%s': %w", value, err)
			}
			resource.CreateTime = cT
		case int(common.ICAT_COLUMN_R_MODIFY_TIME):
			mT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse modify time '%s': %w", value, err)
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
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSResourceMetaItemType, name, metadata)
	response := message.IRODSMessageModifyMetadataResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		return xerrors.Errorf("received an add data resource meta error: %w", err)
	}
	return nil
}

// DeleteResourceMeta sets metadata of a resource to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteResourceMeta(conn *connection.IRODSConnection, name string, metadata *types.IRODSMeta) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	var request *message.IRODSMessageModifyMetadataRequest

	if metadata.AVUID != 0 {
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSResourceMetaItemType, name, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSResourceMetaItemType, name, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSResourceMetaItemType, name, metadata)
	}

	response := message.IRODSMessageModifyMetadataResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		return xerrors.Errorf("received a delete data resource meta error: %w", err)
	}
	return nil
}
