package fs

import (
	"strconv"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"golang.org/x/xerrors"
)

// StatProcess stats processes.
func StatProcess(conn *connection.IRODSConnection, address string, zone string) ([]*types.IRODSProcess, error) {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	processes := []*types.IRODSProcess{}
	req := message.NewIRODSMessageGetProcessstatRequest(address, zone)

	queryResult := message.IRODSMessageQueryResponse{}
	err := conn.Request(req, &queryResult, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to receive a process stat result message: %w", err)
	}

	err = queryResult.CheckError()
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			// empty
			return processes, nil
		}
		return nil, xerrors.Errorf("received a process stat query error: %w", err)
	}

	if queryResult.RowCount == 0 {
		return processes, nil
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, xerrors.Errorf("failed to receive process stat attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	pagenatedProcesses := make([]*types.IRODSProcess, queryResult.RowCount)

	for attr := 0; attr < queryResult.AttributeCount; attr++ {
		sqlResult := queryResult.SQLResult[attr]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, xerrors.Errorf("failed to receive process stat rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		for row := 0; row < queryResult.RowCount; row++ {
			value := sqlResult.Values[row]

			if pagenatedProcesses[row] == nil {
				// create a new
				pagenatedProcesses[row] = &types.IRODSProcess{
					ID:            -1,
					ProxyUser:     "",
					ProxyZone:     "",
					ClientUser:    "",
					ClientZone:    "",
					ClientAddress: "",
					ServerAddress: "",
					ClientProgram: "",
				}
			}

			switch sqlResult.AttributeIndex {
			case int(common.ICAT_COLUMN_PROCESS_ID):
				processID, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse process id %q: %w", value, err)
				}
				pagenatedProcesses[row].ID = processID
			case int(common.ICAT_COLUMN_STARTTIME):
				sT, err := util.GetIRODSDateTime(value)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse start time %q: %w", value, err)
				}
				pagenatedProcesses[row].StartTime = sT
			case int(common.ICAT_COLUMN_CLIENT_NAME):
				pagenatedProcesses[row].ClientUser = value
			case int(common.ICAT_COLUMN_CLIENT_ZONE):
				pagenatedProcesses[row].ClientZone = value
			case int(common.ICAT_COLUMN_PROXY_NAME):
				pagenatedProcesses[row].ProxyUser = value
			case int(common.ICAT_COLUMN_PROXY_ZONE):
				pagenatedProcesses[row].ProxyZone = value
			case int(common.ICAT_COLUMN_REMOTE_ADDR):
				pagenatedProcesses[row].ClientAddress = value
			case int(common.ICAT_COLUMN_PROG_NAME):
				pagenatedProcesses[row].ClientProgram = value
			case int(common.ICAT_COLUMN_SERVER_ADDR):
				pagenatedProcesses[row].ServerAddress = value
			default:
				// ignore
			}
		}
	}

	processes = append(processes, pagenatedProcesses...)
	return processes, nil
}
