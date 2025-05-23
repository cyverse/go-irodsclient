package fs

import (
	"strconv"
	"time"

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

	query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, 0, 0, 0)
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

	query.AddEqualStringCondition(common.ICAT_COLUMN_R_RESC_NAME, name)

	queryResult := message.IRODSMessageQueryResponse{}
	err := conn.Request(query, &queryResult, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
			return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		}

		return nil, xerrors.Errorf("failed to receive a resource query result message: %w", err)
	}

	err = queryResult.CheckError()
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
			return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		}

		return nil, xerrors.Errorf("received a data resource query error: %w", err)
	}

	if queryResult.RowCount == 0 {
		return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
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
				return nil, xerrors.Errorf("failed to parse resource id %q: %w", value, err)
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
				return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
			}
			resource.CreateTime = cT
		case int(common.ICAT_COLUMN_R_MODIFY_TIME):
			mT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
			}
			resource.ModifyTime = mT
		default:
			// ignore
		}
	}

	return resource, nil
}

// ListResources lists resources
func ListResources(conn *connection.IRODSConnection) ([]*types.IRODSResource, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	resources := []*types.IRODSResource{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
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

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
				return nil, xerrors.Errorf("failed to list the resource: %w", err)
			}

			return nil, xerrors.Errorf("failed to receive a resource query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
				return nil, xerrors.Errorf("failed to list the resource: %w", err)
			}

			return nil, xerrors.Errorf("received a data resource query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive resource attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedResources := make([]*types.IRODSResource, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive resource rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedResources[row] == nil {
					// create a new
					pagenatedResources[row] = &types.IRODSResource{
						RescID:     -1,
						Name:       "",
						Zone:       "",
						Type:       "",
						Class:      "",
						Location:   "",
						Path:       "",
						Context:    "",
						CreateTime: time.Time{},
						ModifyTime: time.Time{},
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_R_RESC_ID):
					objID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse resource id %q: %w", value, err)
					}
					pagenatedResources[row].RescID = objID
				case int(common.ICAT_COLUMN_R_RESC_NAME):
					pagenatedResources[row].Name = value
				case int(common.ICAT_COLUMN_R_ZONE_NAME):
					pagenatedResources[row].Zone = value
				case int(common.ICAT_COLUMN_R_TYPE_NAME):
					pagenatedResources[row].Type = value
				case int(common.ICAT_COLUMN_R_CLASS_NAME):
					pagenatedResources[row].Class = value
				case int(common.ICAT_COLUMN_R_LOC):
					pagenatedResources[row].Location = value
				case int(common.ICAT_COLUMN_R_VAULT_PATH):
					pagenatedResources[row].Path = value
				case int(common.ICAT_COLUMN_R_RESC_CONTEXT):
					pagenatedResources[row].Context = value
				case int(common.ICAT_COLUMN_R_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedResources[row].CreateTime = cT
				case int(common.ICAT_COLUMN_R_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
					}
					pagenatedResources[row].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		resources = append(resources, pagenatedResources...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return resources, nil
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
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
			return xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		}

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
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
			return xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
		}

		return xerrors.Errorf("received a delete data resource meta error: %w", err)
	}
	return nil
}

// ListResourceMeta returns all metadata for the resource
func ListResourceMeta(conn *connection.IRODSConnection, name string) ([]*types.IRODSMeta, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	metas := []*types.IRODSMeta{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_META_RESC_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_RESC_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_RESC_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_RESC_ATTR_UNITS, 1)
		query.AddSelect(common.ICAT_COLUMN_META_RESC_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_RESC_MODIFY_TIME, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_R_RESC_NAME, name)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
				return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
			}

			return nil, xerrors.Errorf("failed to receive a resource metadata query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_RESOURCE {
				return nil, xerrors.Errorf("failed to find the resource for name %q: %w", name, types.NewResourceNotFoundError(name))
			}

			return nil, xerrors.Errorf("received a resource metadata query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive resource metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive resource metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
				case int(common.ICAT_COLUMN_META_RESC_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse resource metadata id %q: %w", value, err)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_RESC_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_RESC_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_RESC_ATTR_UNITS):
					pagenatedMetas[row].Units = value
				case int(common.ICAT_COLUMN_META_RESC_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedMetas[row].CreateTime = cT
				case int(common.ICAT_COLUMN_META_RESC_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
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

// AddChildToResource adds a child to a parent resource
func AddChildToResource(conn *connection.IRODSConnection, parent string, child string, options string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageAdminRequest("add", "childtoresc", parent, child, options)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil, conn.GetOperationTimeout())
	if err != nil {
		return xerrors.Errorf("received add child to resc error: %w", err)
	}
	return nil
}
