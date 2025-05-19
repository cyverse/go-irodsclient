package fs

import (
	"path"
	"strconv"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

// GetCollectionAccessInheritance returns collection access inheritance info for the path
func GetCollectionAccessInheritance(conn *connection.IRODSConnection, path string) (*types.IRODSAccessInheritance, error) {
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
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("failed to receive a collection access inheritance query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("received collection access inheritance query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection access inheritance attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccessInheritances := make([]*types.IRODSAccessInheritance, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection access inheritance rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccessInheritances[row] == nil {
					// create a new
					paginatedAccessInheritances[row] = &types.IRODSAccessInheritance{
						Path:        path,
						Inheritance: false,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_INHERITANCE):
					inherit, _ := strconv.ParseBool(value)
					// if error, assume false
					paginatedAccessInheritances[row].Inheritance = inherit
				default:
					// ignore
				}
			}
		}

		inheritances = append(inheritances, paginatedAccessInheritances...)

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
		query := message.NewIRODSMessageQuerySpecificRequest("ShowCollAcls", []string{path}, common.MaxQueryRows, continueIndex, 0, 0)
		query.AddKeyVal(common.ZONE_KW, conn.GetAccount().ClientZone)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("failed to receive a collection access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
			}

			return nil, xerrors.Errorf("received collection access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccesses[row] == nil {
					// create a new
					paginatedAccesses[row] = &types.IRODSAccess{
						Path:        path,
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch attr {
				case 0:
					paginatedAccesses[row].UserName = value
				case 1:
					paginatedAccesses[row].UserZone = value
				case 2:
					paginatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case 3:
					paginatedAccesses[row].UserType = types.IRODSUserType(value)
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, paginatedAccesses...)

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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path.Dir(dataObjPath))
		query.AddEqualStringCondition(common.ICAT_COLUMN_DATA_NAME, path.Base(dataObjPath))

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", dataObjPath, types.NewFileNotFoundError(dataObjPath))
			}

			return nil, xerrors.Errorf("failed to receive a data object access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", dataObjPath, types.NewFileNotFoundError(dataObjPath))
			}

			return nil, xerrors.Errorf("received data object access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccesses[row] == nil {
					// create a new
					paginatedAccesses[row] = &types.IRODSAccess{
						Path:        dataObjPath,
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
					paginatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case int(common.ICAT_COLUMN_USER_TYPE):
					paginatedAccesses[row].UserType = types.IRODSUserType(value)
				case int(common.ICAT_COLUMN_USER_NAME):
					paginatedAccesses[row].UserName = value
				case int(common.ICAT_COLUMN_USER_ZONE):
					paginatedAccesses[row].UserZone = value
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, paginatedAccesses...)

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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, path.Dir(dataObjPath))
		query.AddEqualStringCondition(common.ICAT_COLUMN_DATA_NAME, path.Base(dataObjPath))

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", dataObjPath, types.NewFileNotFoundError(dataObjPath))
			}

			return nil, xerrors.Errorf("failed to receive a data object access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", dataObjPath, types.NewFileNotFoundError(dataObjPath))
			}

			return nil, xerrors.Errorf("received data object access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccesses[row] == nil {
					// create a new
					paginatedAccesses[row] = &types.IRODSAccess{
						Path:        util.GetCorrectIRODSPath(dataObjPath),
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
					paginatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case int(common.ICAT_COLUMN_USER_TYPE):
					paginatedAccesses[row].UserType = types.IRODSUserType(value)
				case int(common.ICAT_COLUMN_USER_NAME):
					paginatedAccesses[row].UserName = value
				case int(common.ICAT_COLUMN_USER_ZONE):
					paginatedAccesses[row].UserZone = value
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, paginatedAccesses...)

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
		query.AddSelect(common.ICAT_COLUMN_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_COLL_ACCESS_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_ZONE, 1)
		query.AddSelect(common.ICAT_COLUMN_USER_TYPE, 1)

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_PARENT_NAME, collPath)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection access for path %q: %w", collPath, types.NewFileNotFoundError(collPath))
			}

			return nil, xerrors.Errorf("failed to receive a collection access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", collPath, types.NewFileNotFoundError(collPath))
			}

			return nil, xerrors.Errorf("received collection access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive collection access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive collection access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccesses[row] == nil {
					// create a new
					paginatedAccesses[row] = &types.IRODSAccess{
						Path:        "",
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_COLL_NAME):
					paginatedAccesses[row].Path = value
				case int(common.ICAT_COLUMN_COLL_ACCESS_NAME):
					paginatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case int(common.ICAT_COLUMN_USER_TYPE):
					paginatedAccesses[row].UserType = types.IRODSUserType(value)
				case int(common.ICAT_COLUMN_USER_NAME):
					paginatedAccesses[row].UserName = value
				case int(common.ICAT_COLUMN_USER_ZONE):
					paginatedAccesses[row].UserZone = value
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, paginatedAccesses...)

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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, collPath)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", collPath, types.NewFileNotFoundError(collPath))
			}

			return nil, xerrors.Errorf("failed to receive a data object access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", collPath, types.NewFileNotFoundError(collPath))
			}

			return nil, xerrors.Errorf("received data object access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccesses[row] == nil {
					// create a new
					paginatedAccesses[row] = &types.IRODSAccess{
						Path:        "",
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_NAME):
					paginatedAccesses[row].Path = util.MakeIRODSPath(collPath, value)
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
					paginatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case int(common.ICAT_COLUMN_USER_TYPE):
					paginatedAccesses[row].UserType = types.IRODSUserType(value)
				case int(common.ICAT_COLUMN_USER_NAME):
					paginatedAccesses[row].UserName = value
				case int(common.ICAT_COLUMN_USER_ZONE):
					paginatedAccesses[row].UserZone = value
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, paginatedAccesses...)

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

		query.AddEqualStringCondition(common.ICAT_COLUMN_COLL_NAME, collPath)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil, conn.GetLongResponseOperationTimeout())
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", collPath, types.NewFileNotFoundError(collPath))
			}

			return nil, xerrors.Errorf("failed to receive a data object access query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				break
			} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
				return nil, xerrors.Errorf("failed to find the collection for path %q: %w", collPath, types.NewFileNotFoundError(collPath))
			}

			return nil, xerrors.Errorf("received data object access query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive data object access attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		paginatedAccesses := make([]*types.IRODSAccess, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive data object access rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if paginatedAccesses[row] == nil {
					// create a new
					paginatedAccesses[row] = &types.IRODSAccess{
						Path:        "",
						UserName:    "",
						UserZone:    "",
						AccessLevel: types.IRODSAccessLevelNull,
						UserType:    types.IRODSUserRodsUser,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_DATA_NAME):
					paginatedAccesses[row].Path = util.MakeIRODSPath(collPath, value)
				case int(common.ICAT_COLUMN_DATA_ACCESS_NAME):
					paginatedAccesses[row].AccessLevel = types.GetIRODSAccessLevelType(value)
				case int(common.ICAT_COLUMN_USER_TYPE):
					paginatedAccesses[row].UserType = types.IRODSUserType(value)
				case int(common.ICAT_COLUMN_USER_NAME):
					paginatedAccesses[row].UserName = value
				case int(common.ICAT_COLUMN_USER_ZONE):
					paginatedAccesses[row].UserZone = value
				default:
					// ignore
				}
			}
		}

		accesses = append(accesses, paginatedAccesses...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return accesses, nil
}

// ChangeAccessInherit changes the inherit bit on a collection.
func ChangeAccessInherit(conn *connection.IRODSConnection, path string, inherit bool, recursive bool, adminFlag bool) error {
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

	request := message.NewIRODSMessageModifyAccessInheritRequest(inherit, path, recursive, adminFlag)
	response := message.IRODSMessageModifyAccessInheritResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return xerrors.Errorf("received set access inherit error: %w", err)
	}
	return nil
}

// ChangeAccess changes access control on a data object or collection.
func ChangeAccess(conn *connection.IRODSConnection, path string, access types.IRODSAccessLevelType, userName string, zoneName string, recursive bool, adminFlag bool) error {
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

	request := message.NewIRODSMessageModifyAccessRequest(access.ChmodString(), userName, zoneName, path, recursive, adminFlag)
	response := message.IRODSMessageModifyAccessResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the data-object/collection for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return xerrors.Errorf("failed to change data-object/collection access: %w", err)
	}
	return nil
}
