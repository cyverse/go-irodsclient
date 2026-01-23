package fs

import (
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// GetCollectionAccessInheritance returns collection access inheritance info for the path
func GetCollectionAccessInheritance(conn *connection.IRODSConnection, path string) (*types.IRODSAccessInheritance, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForAccessList(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	inheritances := []*types.IRODSAccessInheritance{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
		query.AddSelect(common.ICAT_COLUMN_COLL_INHERITANCE, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(path))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", path)
			}

			return nil, errors.Wrapf(err, "failed to receive a collection access inheritance query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(path))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", path)
			}

			return nil, errors.Wrapf(err, "received collection access inheritance query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive collection access inheritance attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccessInheritances := make([]*types.IRODSAccessInheritance, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive collection access inheritance rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccessInheritances[row] == nil {
					// create a new
					pagenatedAccessInheritances[row] = &types.IRODSAccessInheritance{
						Path:        path,
						Inheritance: false,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_INHERITANCE):
					inherit, _ := strconv.ParseBool(value)
					// if error, assume false
					pagenatedAccessInheritances[row].Inheritance = inherit
				default:
					// ignore
				}
			}
		}

		inheritances = append(inheritances, pagenatedAccessInheritances...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return inheritances[0], nil
}

// ListCollectionAccesses returns collection accesses for the path
func ListCollectionAccesses(conn *connection.IRODSConnection, path string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
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
		sql := "select distinct R_USER_MAIN.user_name, R_USER_MAIN.zone_name, R_TOKN_MAIN.token_name, R_USER_MAIN.user_type_name from R_USER_MAIN, R_TOKN_MAIN, R_OBJT_ACCESS, R_COLL_MAIN where R_OBJT_ACCESS.object_id = R_COLL_MAIN.coll_id AND R_COLL_MAIN.coll_name = '" + path + "' AND R_TOKN_MAIN.token_namespace = 'access_type' AND R_USER_MAIN.user_id = R_OBJT_ACCESS.user_id AND R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id"

		query := message.NewIRODSMessageQuerySpecificRequest(sql, []string{}, common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(path))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", path)
			}

			return nil, errors.Wrapf(err, "failed to receive a collection access query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(path))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", path)
			}

			return nil, errors.Wrapf(err, "received collection access query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive collection access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive collection access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        path,
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch attr {
				case 0:
					pagenatedAccesses[row].UserName = value
				case 1:
					pagenatedAccesses[row].UserZone = value
				case 2:
					pagenatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case 3:
					pagenatedAccesses[row].UserType = types.IRODSUserType(value)
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

// ListDataObjectAccesses returns data object accesses for the path
func ListDataObjectAccesses(conn *connection.IRODSConnection, dataObjPath string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path.Dir(dataObjPath))
		query.AddEqualStringCondition(common.ICAT_COLUMN_DATA_NAME, path.Base(dataObjPath))

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(dataObjPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", dataObjPath)
			}

			return nil, errors.Wrapf(err, "failed to receive a data object access query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(dataObjPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", dataObjPath)
			}

			return nil, errors.Wrapf(err, "received data object access query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        dataObjPath,
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

// ListDataObjectAccessesWithoutCollection returns data object accesses for the path
func ListDataObjectAccessesWithoutCollection(conn *connection.IRODSConnection, dataObjPath string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path.Dir(dataObjPath))
		query.AddEqualStringCondition(common.ICAT_COLUMN_DATA_NAME, path.Base(dataObjPath))

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(dataObjPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", dataObjPath)
			}

			return nil, errors.Wrapf(err, "failed to receive a data object access query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(dataObjPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", dataObjPath)
			}

			return nil, errors.Wrapf(err, "received data object access query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedAccesses[row] == nil {
					// create a new
					pagenatedAccesses[row] = &types.IRODSAccess{
						Path:        util.GetCorrectIRODSPath(dataObjPath),
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

// ListAccessesForSubCollections returns collection accesses for subcollections in the given path
func ListAccessesForSubCollections(conn *connection.IRODSConnection, collPath string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
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
		sql := "select disctinct R_COLL_MAIN.coll_name, R_TOKN_MAIN.token_name, R_USER_MAIN.user_name, R_USER_MAIN.zone_name, R_USER_MAIN.user_type_name from R_COLL_MAIN, R_TOKN_MAIN, R_USER_MAIN, R_OBJT_ACCESS where R_COLL_MAIN.parent_coll_name = '" + collPath + "' AND R_USER_MAIN.user_id = R_OBJT_ACCESS.user_id AND R_OBJT_ACCESS.access_type_id = R_TOKN_MAIN.token_id AND R_COLL_MAIN.coll_id = R_OBJT_ACCESS.object_id order by R_COLL_MAIN.coll_name"

		query := message.NewIRODSMessageQuerySpecificRequest(sql, []string{}, common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)

		/*
			// this has a bug - it omits collections without data objects in them
			query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
			query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)
			query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
			query.AddSelect(common.ICAT_COLUMN_COLL_ACCESS_NAME, 1)
			query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
			query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
			query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

			query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_PARENT_NAME, collPath)
		*/

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(collPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection access for path %q", collPath)
			}

			return nil, errors.Wrapf(err, "failed to receive a collection access query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(collPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", collPath)
			}

			return nil, errors.Wrapf(err, "received collection access query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive collection access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive collection access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
				case int(common.ICAT_COLUMN_COLL_NAME):
					pagenatedAccesses[row].Path = value
				case int(common.ICAT_COLUMN_COLL_ACCESS_NAME):
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

// ListAccessesForDataObjectsInCollection returns data object accesses for data objects in the given path
func ListAccessesForDataObjectsInCollection(conn *connection.IRODSConnection, collPath string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, collPath)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(collPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", collPath)
			}

			return nil, errors.Wrapf(err, "failed to receive a data object access query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(collPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", collPath)
			}

			return nil, errors.Wrapf(err, "received data object access query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
					pagenatedAccesses[row].Path = util.MakeIRODSPath(collPath, value)
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
func ListAccessesForDataObjectsWithoutCollection(conn *connection.IRODSConnection, collPath string) ([]*types.IRODSAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.Errorf("connection is nil or disconnected")
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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, collPath)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(collPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", collPath)
			}

			return nil, errors.Wrapf(err, "failed to receive a data object access query result message")
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				newErr := errors.Join(err, types.NewFileNotFoundError(collPath))
				return nil, errors.Wrapf(newErr, "failed to find the collection for path %q", collPath)
			}

			return nil, errors.Wrapf(err, "received data object access query error")
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, errors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, errors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
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
					pagenatedAccesses[row].Path = util.MakeIRODSPath(collPath, value)
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

// ChangeAccessInherit changes the inherit bit on a collection.
func ChangeAccessInherit(conn *connection.IRODSConnection, path string, inherit bool, recurse bool, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return errors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForAccessUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageModifyAccessInheritRequest(inherit, path, recurse, adminFlag)
	response := message.IRODSMessageModifyAccessInheritResponse{}
	timeout := conn.GetOperationTimeout()
	if recurse {
		// recursive collection deletion requires long response operation timeout
		timeout = conn.GetLongResponseOperationTimeout()
	}

	err := conn.RequestAndCheck(request, &response, nil, timeout)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the collection for path %q", path)
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the collection for path %q", path)
		}

		return errors.Wrapf(err, "received set access inherit error")
	}
	return nil
}

// ChangeAccess changes access control on a data object or collection.
func ChangeAccess(conn *connection.IRODSConnection, path string, access types.IRODSAccessLevelType, userName string, zoneName string, recurse bool, adminFlag bool) error {
	if conn == nil || !conn.IsConnected() {
		return errors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForAccessUpdate(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageModifyAccessRequest(access.ChmodString(), userName, zoneName, path, recurse, adminFlag)
	response := message.IRODSMessageModifyAccessResponse{}
	timeout := conn.GetOperationTimeout()
	if recurse {
		// recursive collection deletion requires long response operation timeout
		timeout = conn.GetLongResponseOperationTimeout()
	}

	err := conn.RequestAndCheck(request, &response, nil, timeout)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the data-object/collection for path %q", path)
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			newErr := errors.Join(err, types.NewFileNotFoundError(path))
			return errors.Wrapf(newErr, "failed to find the collection for path %q", path)
		}

		return errors.Wrapf(err, "failed to change data-object/collection access")
	}
	return nil
}
