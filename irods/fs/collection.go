package fs

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/danwakefield/fnmatch"
	"golang.org/x/xerrors"
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForStat(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, 0, 0, 0)
	query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
	query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
	query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

	query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path)

	queryResult := message.IRODSMessageQueryResponse{}
	err := conn.Request(query, &queryResult, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return nil, xerrors.Errorf("failed to receive collection query result message: %w", err)
	}

	err = queryResult.CheckError()
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return nil, xerrors.Errorf("received collection query error: %w", err)
	}

	if queryResult.RowCount != 1 {
		// file not found
		return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, xerrors.Errorf("failed to receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	var collectionID int64 = -1
	collectionPath := ""
	collectionOwner := ""
	createTime := time.Time{}
	modifyTime := time.Time{}
	for idx := 0; idx < queryResult.AttributeCount; idx++ {
		sqlResult := queryResult.SQLResult[idx]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, xerrors.Errorf("failed to receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		value := sqlResult.Values[0]

		switch sqlResult.AttributeIndex {
		case int(common.ICAT_COLUMN_COLL_ID):
			cID, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse collection id %q: %w", value, err)
			}
			collectionID = cID
		case int(common.ICAT_COLUMN_COLL_NAME):
			collectionPath = value
		case int(common.ICAT_COLUMN_COLL_OWNER_NAME):
			collectionOwner = value
		case int(common.ICAT_COLUMN_COLL_CREATE_TIME):
			cT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
			}
			createTime = cT
		case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
			mT, err := util.GetIRODSDateTime(value)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
			}
			modifyTime = mT
		default:
			// ignore
		}
	}

	if collectionID == -1 {
		return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
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

// ListCollectionMeta returns a collection metadata for the path
func ListCollectionMeta(conn *connection.IRODSConnection, path string) ([]*types.IRODSMeta, error) {
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
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_VALUE, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_ATTR_UNITS, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_META_COLL_MODIFY_TIME, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("failed to receive a collection metadata query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("received collection metadata query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection metadata attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedMetas := make([]*types.IRODSMeta, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection metadata rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
				case int(common.ICAT_COLUMN_META_COLL_ATTR_ID):
					avuID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse collection metadata id %q: %w", value, err)
					}
					pagenatedMetas[row].AVUID = avuID
				case int(common.ICAT_COLUMN_META_COLL_ATTR_NAME):
					pagenatedMetas[row].Name = value
				case int(common.ICAT_COLUMN_META_COLL_ATTR_VALUE):
					pagenatedMetas[row].Value = value
				case int(common.ICAT_COLUMN_META_COLL_ATTR_UNITS):
					pagenatedMetas[row].Units = value
				case int(common.ICAT_COLUMN_META_COLL_CREATE_TIME):
					cT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedMetas[row].CreateTime = cT
				case int(common.ICAT_COLUMN_META_COLL_MODIFY_TIME):
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

// ListSubCollections lists subcollections in the given collection
func ListSubCollections(conn *connection.IRODSConnection, path string) ([]*types.IRODSCollection, error) {
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

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_PARENT_NAME, path)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("failed to receive a collection query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("received collection query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, xerrors.Errorf("failed to parse collection id %q: %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedCollections[row].CreateTime = cT
				case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
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

// SearchCollectionsUnixWildcard searches collections using unix-style wildcard
func SearchCollectionsUnixWildcard(conn *connection.IRODSConnection, pathUnixWildcard string) ([]*types.IRODSCollection, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForList(1)
	}

	pathSqlWildcard := util.UnixWildcardsToSQLWildcards(pathUnixWildcard)

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		query.AddLikeStringCondition(common.ICAT_COLUMN_COLL_NAME, pathSqlWildcard)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				break
			}

			return nil, xerrors.Errorf("failed to receive a collection query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				break
			}

			return nil, xerrors.Errorf("received collection query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, xerrors.Errorf("failed to parse collection id %q: %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedCollections[row].CreateTime = cT
				case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
					}
					pagenatedCollections[row].ModifyTime = mT
				default:
					// ignore
				}
			}
		}

		// Filter results by original unix wildcard, since the SQL wildcards
		// are less strict (e.g. a unix wildcard range is converted to a generic wildcards in SQL).
		for _, pagenatedCollection := range pagenatedCollections {
			if fnmatch.Match(pathUnixWildcard, pagenatedCollection.Path, fnmatch.FNM_PATHNAME) {
				collections = append(collections, pagenatedCollection)
			}
		}

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
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForCollectionCreate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageMakeCollectionRequest(path, recurse)
	response := message.IRODSMessageMakeCollectionResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		return xerrors.Errorf("received create collection error: %w", err)
	}
	return nil
}

// DeleteCollection deletes a collection for the path
func DeleteCollection(conn *connection.IRODSConnection, path string, recurse bool, force bool) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForCollectionDelete(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageRemoveCollectionRequest(path, recurse, force)
	response := message.IRODSMessageRemoveCollectionResponse{}
	timeout := conn.GetOperationTimeout()
	if recurse {
		// recursive collection deletion requires long response operation timeout
		timeout = conn.GetLongResponseOperationTimeout()
	}

	err := conn.RequestAndCheck(request, &response, nil, timeout)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_COLLECTION_NOT_EMPTY {
			return xerrors.Errorf("the collection for path %q is empty: %w", path, types.NewCollectionNotEmptyError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return xerrors.Errorf("received delete collection error: %w", err)
	}

	for response.Result == int(common.SYS_SVR_TO_CLI_COLL_STAT) {
		// pack length - Big Endian
		replyBuffer := make([]byte, 4)
		binary.BigEndian.PutUint32(replyBuffer, uint32(common.SYS_CLI_TO_SVR_COLL_STAT_REPLY))

		err = conn.Send(replyBuffer, 4, nil)
		if err != nil {
			return xerrors.Errorf("failed to reply to a collection deletion response message: %w", err)
		}

		responseMessageReply, err := conn.ReadMessage(nil, timeout.ResponseTimeout)
		if err != nil {
			return xerrors.Errorf("failed to receive a collection deletion response message: %w", err)
		}

		response.FromMessage(responseMessageReply)
	}

	return nil
}

// MoveCollection moves a collection for the path to another path
func MoveCollection(conn *connection.IRODSConnection, srcPath string, destPath string) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForCollectionRename(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageMoveCollectionRequest(srcPath, destPath)
	response := message.IRODSMessageMoveCollectionResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetLongResponseOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the collection for path %q: %w", srcPath, types.NewFileNotFoundError(srcPath))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the collection for path %q: %w", srcPath, types.NewFileNotFoundError(srcPath))
		}

		return xerrors.Errorf("received move collection error: %w", err)
	}
	return nil
}

// AddCollectionMeta sets metadata of a data object for the path to the given key values.
// metadata.AVUID is ignored
func AddCollectionMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
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

	request := message.NewIRODSMessageAddMetadataRequest(types.IRODSCollectionMetaItemType, path, metadata)
	response := message.IRODSMessageModifyMetadataResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return xerrors.Errorf("received add collection meta error: %w", err)
	}
	return nil
}

// DeleteCollectionMeta sets metadata of a data object for the path to the given key values.
// The metadata AVU is selected on basis of AVUID if it is supplied, otherwise on basis of Name, Value and Units.
func DeleteCollectionMeta(conn *connection.IRODSConnection, path string, metadata *types.IRODSMeta) error {
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
		request = message.NewIRODSMessageRemoveMetadataByIDRequest(types.IRODSCollectionMetaItemType, path, metadata.AVUID)
	} else if metadata.Units == "" && metadata.Value == "" {
		request = message.NewIRODSMessageRemoveMetadataWildcardRequest(types.IRODSCollectionMetaItemType, path, metadata.Name)
	} else {
		request = message.NewIRODSMessageRemoveMetadataRequest(types.IRODSCollectionMetaItemType, path, metadata)
	}

	response := message.IRODSMessageModifyMetadataResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return xerrors.Errorf("received delete collection meta error: %w", err)
	}
	return nil
}

// SearchCollectionsByMeta searches collections by metadata
func SearchCollectionsByMeta(conn *connection.IRODSConnection, metaName string, metaValue string) ([]*types.IRODSCollection, error) {
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

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_META_COLL_ATTR_NAME, metaName)
		query.AddEqualStringCondition(common.ICAT_COLUMN_META_COLL_ATTR_VALUE, metaValue)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}

			return nil, xerrors.Errorf("failed to receive a collection query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received collection query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
						return nil, xerrors.Errorf("failed to parse collection id %q: %w", value, err)
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
						return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
					}
					pagenatedCollections[row].CreateTime = cT
				case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
					mT, err := util.GetIRODSDateTime(value)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
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
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForSearch(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	collections := []*types.IRODSCollection{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_COLL_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_CREATE_TIME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_MODIFY_TIME, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_META_COLL_ATTR_NAME, metaName)
		query.AddLikeStringCondition(common.ICAT_COLUMN_META_COLL_ATTR_VALUE, metaValue)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}

			return nil, xerrors.Errorf("failed to receive a collection query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			}
			return nil, xerrors.Errorf("received collection query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedCollections := make([]*types.IRODSCollection, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for attr := 0; attr < queryResult.AttributeCount; attr++ {
				sqlResult := queryResult.SQLResult[attr]
				if len(sqlResult.Values) != queryResult.RowCount {
					return nil, xerrors.Errorf("failed to receive collection rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
							return nil, xerrors.Errorf("failed to parse collection id %q: %w", value, err)
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
							return nil, xerrors.Errorf("failed to parse create time %q: %w", value, err)
						}
						pagenatedCollections[row].CreateTime = cT
					case int(common.ICAT_COLUMN_COLL_MODIFY_TIME):
						mT, err := util.GetIRODSDateTime(value)
						if err != nil {
							return nil, xerrors.Errorf("failed to parse modify time %q: %w", value, err)
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
