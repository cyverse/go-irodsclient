package fs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
	"github.com/rs/xid"
	"golang.org/x/xerrors"
)

// https://github.com/irods/irods_client_s3_ticketbooth/blob/b92e8aaa3127cb56fcb8fef09caa00244bd29ca6/ticket_booth/main.py
// GetTicketForAnonymousAccess returns minimal ticket information for the ticket name string
func GetTicketForAnonymousAccess(conn *connection.IRODSConnection, ticketName string) (*types.IRODSTicketForAnonymousAccess, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, 0, 0, 0)
	query.AddSelect(common.ICAT_COLUMN_TICKET_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_TYPE, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_COLL_NAME, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_EXPIRY_TS, 1)
	// We can't get common.ICAT_COLUMN_TICKET_STRING using query since it's not available for anonymous access

	condVal := fmt.Sprintf("= '%s'", ticketName)
	query.AddCondition(common.ICAT_COLUMN_TICKET_STRING, condVal)

	queryResult := message.IRODSMessageQueryResponse{}
	err := conn.Request(query, &queryResult, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to receive a ticket query result message: %w", err)
	}

	err = queryResult.CheckError()
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return nil, types.NewFileNotFoundErrorf("failed to find a ticket")
		}

		return nil, xerrors.Errorf("received a ticket query error: %w", err)
	}

	if queryResult.RowCount != 1 {
		// file not found
		return nil, types.NewFileNotFoundErrorf("failed to find a ticket")
	}

	if queryResult.AttributeCount > len(queryResult.SQLResult) {
		return nil, xerrors.Errorf("failed to receive ticket attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
	}

	var ticketID int64 = -1
	ticketType := types.TicketTypeRead
	ticketPath := ""
	expireTime := time.Time{}

	for idx := 0; idx < queryResult.AttributeCount; idx++ {
		sqlResult := queryResult.SQLResult[idx]
		if len(sqlResult.Values) != queryResult.RowCount {
			return nil, xerrors.Errorf("failed to receive ticket rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
		}

		value := sqlResult.Values[0]

		switch sqlResult.AttributeIndex {
		case int(common.ICAT_COLUMN_TICKET_ID):
			cID, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse ticket id '%s': %w", value, err)
			}
			ticketID = cID
		case int(common.ICAT_COLUMN_TICKET_TYPE):
			ticketType = types.TicketType(value)
		case int(common.ICAT_COLUMN_TICKET_COLL_NAME):
			ticketPath = value
		case int(common.ICAT_COLUMN_TICKET_EXPIRY_TS):
			if len(strings.TrimSpace(value)) > 0 {
				mT, err := util.GetIRODSDateTime(value)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse expiry time '%s': %w", value, err)
				}
				expireTime = mT
			}
		default:
			// ignore
		}
	}

	if ticketID == -1 {
		return nil, types.NewFileNotFoundErrorf("failed to find a ticket")
	}

	return &types.IRODSTicketForAnonymousAccess{
		ID:             ticketID,
		Name:           ticketName,
		Type:           ticketType,
		Path:           ticketPath,
		ExpirationTime: expireTime,
	}, nil
}

// ListTickets returns tickets
func ListTickets(conn *connection.IRODSConnection) ([]*types.IRODSTicket, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	tickets := []*types.IRODSTicket{}

	ticketsColl, err := ListTicketsForCollections(conn)
	if err != nil {
		return nil, err
	}

	tickets = append(tickets, ticketsColl...)

	ticketsDataObj, err := ListTicketsForDataObjects(conn)
	if err != nil {
		return nil, err
	}

	tickets = append(tickets, ticketsDataObj...)

	return tickets, nil
}

// ListTicketsForDataObjects returns tickets for data objects
func ListTicketsForDataObjects(conn *connection.IRODSConnection) ([]*types.IRODSTicket, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	tickets := []*types.IRODSTicket{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_TICKET_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_STRING, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OBJECT_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_USES_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_USES_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_EXPIRY_TS, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_DATA_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_DATA_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OWNER_ZONE, 1)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a ticket query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return tickets, nil
			}

			return nil, xerrors.Errorf("received a ticket query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive ticket attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedTickets := make([]*types.IRODSTicket, queryResult.RowCount)
		tempValues := make([]map[string]string, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive ticket rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedTickets[row] == nil {
					// create a new
					pagenatedTickets[row] = &types.IRODSTicket{
						ID:             -1,
						Name:           "",
						Type:           types.TicketTypeRead,
						Owner:          "",
						OwnerZone:      "",
						ObjectType:     types.ObjectTypeCollection,
						Path:           "",
						ExpirationTime: time.Time{},
						UsesLimit:      0,
						UsesCount:      0,
						WriteFileLimit: 0,
						WriteFileCount: 0,
						WriteByteLimit: 0,
						WriteByteCount: 0,
					}
				}

				if tempValues[row] == nil {
					// create a new
					tempValues[row] = map[string]string{}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_TICKET_ID):
					tID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse ticket id '%s': %w", value, err)
					}
					pagenatedTickets[row].ID = tID
				case int(common.ICAT_COLUMN_TICKET_STRING):
					pagenatedTickets[row].Name = value
				case int(common.ICAT_COLUMN_TICKET_TYPE):
					pagenatedTickets[row].Type = types.TicketType(value)
				case int(common.ICAT_COLUMN_TICKET_OBJECT_TYPE):
					pagenatedTickets[row].ObjectType = types.ObjectType(value)
				case int(common.ICAT_COLUMN_TICKET_USES_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse uses limit '%s': %w", value, err)
					}
					pagenatedTickets[row].UsesLimit = limit
				case int(common.ICAT_COLUMN_TICKET_USES_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse uses count '%s': %w", value, err)
					}
					pagenatedTickets[row].UsesCount = count
				case int(common.ICAT_COLUMN_TICKET_EXPIRY_TS):
					if len(strings.TrimSpace(value)) > 0 {
						mT, err := util.GetIRODSDateTime(value)
						if err != nil {
							return nil, xerrors.Errorf("failed to parse expiry time '%s': %w", value, err)
						}
						pagenatedTickets[row].ExpirationTime = mT
					}
				case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write file limit '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteFileLimit = limit
				case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write file count '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteFileCount = count
				case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write byte limit '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteByteLimit = limit
				case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write byte count '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteByteCount = count
				case int(common.ICAT_COLUMN_TICKET_DATA_NAME):
					tempValues[row]["data_name"] = value
					if dataCollName, ok := tempValues[row]["data_coll_name"]; ok {
						pagenatedTickets[row].Path = util.MakeIRODSPath(dataCollName, value)
					}
				case int(common.ICAT_COLUMN_TICKET_DATA_COLL_NAME):
					tempValues[row]["data_coll_name"] = value
					if dataName, ok := tempValues[row]["data_name"]; ok {
						pagenatedTickets[row].Path = util.MakeIRODSPath(value, dataName)
					}
				case int(common.ICAT_COLUMN_TICKET_OWNER_NAME):
					pagenatedTickets[row].Owner = value
				case int(common.ICAT_COLUMN_TICKET_OWNER_ZONE):
					pagenatedTickets[row].OwnerZone = value
				default:
					// ignore
				}
			}
		}

		tickets = append(tickets, pagenatedTickets...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return tickets, nil
}

// ListTicketsForCollections returns tickets for collections
func ListTicketsForCollections(conn *connection.IRODSConnection) ([]*types.IRODSTicket, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	tickets := []*types.IRODSTicket{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_TICKET_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_STRING, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OBJECT_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_USES_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_USES_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_EXPIRY_TS, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_COLL_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OWNER_ZONE, 1)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a ticket query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return tickets, nil
			}

			return nil, xerrors.Errorf("received a ticket query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive ticket attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedTickets := make([]*types.IRODSTicket, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive ticket rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedTickets[row] == nil {
					// create a new
					pagenatedTickets[row] = &types.IRODSTicket{
						ID:             -1,
						Name:           "",
						Type:           types.TicketTypeRead,
						Owner:          "",
						OwnerZone:      "",
						ObjectType:     types.ObjectTypeCollection,
						Path:           "",
						ExpirationTime: time.Time{},
						UsesLimit:      0,
						UsesCount:      0,
						WriteFileLimit: 0,
						WriteFileCount: 0,
						WriteByteLimit: 0,
						WriteByteCount: 0,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_TICKET_ID):
					tID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse ticket id '%s': %w", value, err)
					}
					pagenatedTickets[row].ID = tID
				case int(common.ICAT_COLUMN_TICKET_STRING):
					pagenatedTickets[row].Name = value
				case int(common.ICAT_COLUMN_TICKET_TYPE):
					pagenatedTickets[row].Type = types.TicketType(value)
				case int(common.ICAT_COLUMN_TICKET_OBJECT_TYPE):
					pagenatedTickets[row].ObjectType = types.ObjectType(value)
				case int(common.ICAT_COLUMN_TICKET_USES_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse uses limit '%s': %w", value, err)
					}
					pagenatedTickets[row].UsesLimit = limit
				case int(common.ICAT_COLUMN_TICKET_USES_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse uses count '%s': %w", value, err)
					}
					pagenatedTickets[row].UsesCount = count
				case int(common.ICAT_COLUMN_TICKET_EXPIRY_TS):
					if len(strings.TrimSpace(value)) > 0 {
						mT, err := util.GetIRODSDateTime(value)
						if err != nil {
							return nil, xerrors.Errorf("failed to parse expiry time '%s': %w", value, err)
						}
						pagenatedTickets[row].ExpirationTime = mT
					}
				case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write file limit '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteFileLimit = limit
				case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write file count '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteFileCount = count
				case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write byte limit '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteByteLimit = limit
				case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write byte count '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteByteCount = count
				case int(common.ICAT_COLUMN_TICKET_COLL_NAME):
					pagenatedTickets[row].Path = value
				case int(common.ICAT_COLUMN_TICKET_OWNER_NAME):
					pagenatedTickets[row].Owner = value
				case int(common.ICAT_COLUMN_TICKET_OWNER_ZONE):
					pagenatedTickets[row].OwnerZone = value
				default:
					// ignore
				}
			}
		}

		tickets = append(tickets, pagenatedTickets...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return tickets, nil
}

// ListTicketsBasic returns tickets with basic info
func ListTicketsBasic(conn *connection.IRODSConnection) ([]*types.IRODSTicket, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	tickets := []*types.IRODSTicket{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_TICKET_ID, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_STRING, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OBJECT_TYPE, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_USES_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_USES_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_EXPIRY_TS, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OWNER_NAME, 1)
		query.AddSelect(common.ICAT_COLUMN_TICKET_OWNER_ZONE, 1)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a ticket query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return tickets, nil
			}

			return nil, xerrors.Errorf("received a ticket query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive ticket attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedTickets := make([]*types.IRODSTicket, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive ticket rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				if pagenatedTickets[row] == nil {
					// create a new
					pagenatedTickets[row] = &types.IRODSTicket{
						ID:             -1,
						Name:           "",
						Type:           types.TicketTypeRead,
						Owner:          "",
						OwnerZone:      "",
						ObjectType:     types.ObjectTypeCollection,
						Path:           "",
						ExpirationTime: time.Time{},
						UsesLimit:      0,
						UsesCount:      0,
						WriteFileLimit: 0,
						WriteFileCount: 0,
						WriteByteLimit: 0,
						WriteByteCount: 0,
					}
				}

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_TICKET_ID):
					tID, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse ticket id '%s': %w", value, err)
					}
					pagenatedTickets[row].ID = tID
				case int(common.ICAT_COLUMN_TICKET_STRING):
					pagenatedTickets[row].Name = value
				case int(common.ICAT_COLUMN_TICKET_TYPE):
					pagenatedTickets[row].Type = types.TicketType(value)
				case int(common.ICAT_COLUMN_TICKET_OBJECT_TYPE):
					pagenatedTickets[row].ObjectType = types.ObjectType(value)
				case int(common.ICAT_COLUMN_TICKET_USES_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse uses limit '%s': %w", value, err)
					}
					pagenatedTickets[row].UsesLimit = limit
				case int(common.ICAT_COLUMN_TICKET_USES_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse uses count '%s': %w", value, err)
					}
					pagenatedTickets[row].UsesCount = count
				case int(common.ICAT_COLUMN_TICKET_EXPIRY_TS):
					if len(strings.TrimSpace(value)) > 0 {
						mT, err := util.GetIRODSDateTime(value)
						if err != nil {
							return nil, xerrors.Errorf("failed to parse expiry time '%s': %w", value, err)
						}
						pagenatedTickets[row].ExpirationTime = mT
					}
				case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write file limit '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteFileLimit = limit
				case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write file count '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteFileCount = count
				case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT):
					limit, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write byte limit '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteByteLimit = limit
				case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT):
					count, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, xerrors.Errorf("failed to parse write byte count '%s': %w", value, err)
					}
					pagenatedTickets[row].WriteByteCount = count
				case int(common.ICAT_COLUMN_TICKET_OWNER_NAME):
					pagenatedTickets[row].Owner = value
				case int(common.ICAT_COLUMN_TICKET_OWNER_ZONE):
					pagenatedTickets[row].OwnerZone = value
				default:
					// ignore
				}
			}
		}

		tickets = append(tickets, pagenatedTickets...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return tickets, nil
}

// ListTicketAllowedHosts returns allowed hosts for the given ticket
func ListTicketAllowedHosts(conn *connection.IRODSConnection, ticketID int64) ([]string, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	hosts := []string{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_TICKET_ALLOWED_HOST, 1)

		collCondVal := fmt.Sprintf("= '%d'", ticketID)
		query.AddCondition(common.ICAT_COLUMN_TICKET_ALLOWED_HOST_TICKET_ID, collCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a ticket restriction query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return hosts, nil
			}

			return nil, xerrors.Errorf("received a ticket restriction query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive ticket restriction attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedHosts := make([]string, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive ticket restriction rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_TICKET_ALLOWED_HOST):
					pagenatedHosts[row] = value
				default:
					// ignore
				}
			}
		}

		hosts = append(hosts, pagenatedHosts...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return hosts, nil
}

// ListTicketAllowedUserNames returns allowed user names for the given ticket
func ListTicketAllowedUserNames(conn *connection.IRODSConnection, ticketID int64) ([]string, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	usernames := []string{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_TICKET_ALLOWED_USER_NAME, 1)

		collCondVal := fmt.Sprintf("= '%d'", ticketID)
		query.AddCondition(common.ICAT_COLUMN_TICKET_ALLOWED_USER_TICKET_ID, collCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a ticket restriction query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return usernames, nil
			}

			return nil, xerrors.Errorf("received a ticket restriction query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive ticket restriction attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedUsernames := make([]string, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive ticket restriction rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_TICKET_ALLOWED_USER_NAME):
					pagenatedUsernames[row] = value
				default:
					// ignore
				}
			}
		}

		usernames = append(usernames, pagenatedUsernames...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return usernames, nil
}

// ListTicketAllowedGroupNames returns allowed group names for the given ticket
func ListTicketAllowedGroupNames(conn *connection.IRODSConnection, ticketID int64) ([]string, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	groupnames := []string{}

	continueQuery := true
	continueIndex := 0
	for continueQuery {
		query := message.NewIRODSMessageQueryRequest(common.MaxQueryRows, continueIndex, 0, 0)
		query.AddSelect(common.ICAT_COLUMN_TICKET_ALLOWED_GROUP_NAME, 1)

		collCondVal := fmt.Sprintf("= '%d'", ticketID)
		query.AddCondition(common.ICAT_COLUMN_TICKET_ALLOWED_GROUP_TICKET_ID, collCondVal)

		queryResult := message.IRODSMessageQueryResponse{}
		err := conn.Request(query, &queryResult, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive a ticket restriction query result message: %w", err)
		}

		err = queryResult.CheckError()
		if err != nil {
			if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
				// empty
				return groupnames, nil
			}

			return nil, xerrors.Errorf("received a ticket restriction query error: %w", err)
		}

		if queryResult.RowCount == 0 {
			break
		}

		if queryResult.AttributeCount > len(queryResult.SQLResult) {
			return nil, xerrors.Errorf("failed to receive ticket restriction attributes - requires %d, but received %d attributes", queryResult.AttributeCount, len(queryResult.SQLResult))
		}

		pagenatedGroupnames := make([]string, queryResult.RowCount)

		for attr := 0; attr < queryResult.AttributeCount; attr++ {
			sqlResult := queryResult.SQLResult[attr]
			if len(sqlResult.Values) != queryResult.RowCount {
				return nil, xerrors.Errorf("failed to receive ticket restriction rows - requires %d, but received %d attributes", queryResult.RowCount, len(sqlResult.Values))
			}

			for row := 0; row < queryResult.RowCount; row++ {
				value := sqlResult.Values[row]

				switch sqlResult.AttributeIndex {
				case int(common.ICAT_COLUMN_TICKET_ALLOWED_GROUP_NAME):
					pagenatedGroupnames[row] = value
				default:
					// ignore
				}
			}
		}

		groupnames = append(groupnames, pagenatedGroupnames...)

		continueIndex = queryResult.ContinueIndex
		if continueIndex == 0 {
			continueQuery = false
		}
	}

	return groupnames, nil
}

// CreateTicket creates a ticket
func CreateTicket(conn *connection.IRODSConnection, ticketName string, ticketType types.TicketType, path string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	ticketName = strings.TrimSpace(ticketName)
	if len(ticketName) == 0 {
		ticketName = xid.New().String()
	}

	req := message.NewIRODSMessageTicketAdminRequest("create", ticketName, string(ticketType), path)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received create ticket error: %w", err)
	}
	return nil
}

// DeleteTicket deletes the ticket
func DeleteTicket(conn *connection.IRODSConnection, ticketName string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageTicketAdminRequest("delete", ticketName)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received delete ticket error: %w", err)
	}
	return nil
}

// ModifyTicket modifies the given ticket
func ModifyTicket(conn *connection.IRODSConnection, ticketName string, args ...string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageTicketAdminRequest("mod", ticketName, args...)

	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received mod ticket error: %w", err)
	}
	return nil
}

// ModifyTicketUseLimit modifies the use limit of the given ticket
func ModifyTicketUseLimit(conn *connection.IRODSConnection, ticketName string, uses int) error {
	return ModifyTicket(conn, ticketName, "uses", fmt.Sprintf("%d", uses))
}

// ClearTicketUseLimit clears the use limit of the given ticket
func ClearTicketUseLimit(conn *connection.IRODSConnection, ticketName string) error {
	return ModifyTicketUseLimit(conn, ticketName, 0)
}

// ModifyTicketWriteFileLimit modifies the write file limit of the given ticket
func ModifyTicketWriteFileLimit(conn *connection.IRODSConnection, ticketName string, count int) error {
	return ModifyTicket(conn, ticketName, "write-file", fmt.Sprintf("%d", count))
}

// ClearTicketWriteFileLimit clears the write file limit of the given ticket
func ClearTicketWriteFileLimit(conn *connection.IRODSConnection, ticketName string) error {
	return ModifyTicketWriteFileLimit(conn, ticketName, 0)
}

// ModifyTicketWriteByteLimit modifies the write byte limit of the given ticket
func ModifyTicketWriteByteLimit(conn *connection.IRODSConnection, ticketName string, bytes int) error {
	return ModifyTicket(conn, ticketName, "write-byte", fmt.Sprintf("%d", bytes))
}

// ClearTicketWriteByteLimit clears the write byte limit of the given ticket
func ClearTicketWriteByteLimit(conn *connection.IRODSConnection, ticketName string) error {
	return ModifyTicketWriteByteLimit(conn, ticketName, 0)
}

// AddTicketAllowedUser adds a user to the allowed user names list of the given ticket
func AddTicketAllowedUser(conn *connection.IRODSConnection, ticketName string, userName string) error {
	return ModifyTicket(conn, ticketName, "add", "user", userName)
}

// RemoveTicketAllowedUser removes the user from the allowed user names list of the given ticket
func RemoveTicketAllowedUser(conn *connection.IRODSConnection, ticketName string, userName string) error {
	return ModifyTicket(conn, ticketName, "remove", "user", userName)
}

// AddTicketAllowedGroup adds a group to the allowed group names list of the given ticket
func AddTicketAllowedGroup(conn *connection.IRODSConnection, ticketName string, groupName string) error {
	return ModifyTicket(conn, ticketName, "add", "group", groupName)
}

// RemoveTicketAllowedGroup removes the group from the allowed group names list of the given ticket
func RemoveTicketAllowedGroup(conn *connection.IRODSConnection, ticketName string, groupName string) error {
	return ModifyTicket(conn, ticketName, "remove", "group", groupName)
}

// AddTicketAllowedHost adds a host to the allowed hosts list of the given ticket
func AddTicketAllowedHost(conn *connection.IRODSConnection, ticketName string, host string) error {
	return ModifyTicket(conn, ticketName, "add", "host", host)
}

// RemoveTicketAllowedHost removes the host from the allowed hosts list of the given ticket
func RemoveTicketAllowedHost(conn *connection.IRODSConnection, ticketName string, host string) error {
	return ModifyTicket(conn, ticketName, "remove", "host", host)
}

// ModifyTicketExpirationTime modifies the expiration time of the given ticket
func ModifyTicketExpirationTime(conn *connection.IRODSConnection, ticketName string, expirationTime time.Time) error {
	expirationTimeString := util.GetIRODSDateTimeStringForTicket(expirationTime)

	return ModifyTicket(conn, ticketName, "expiry", expirationTimeString)
}

// ClearTicketExpirationTime clears the expiration time of the given ticket
func ClearTicketExpirationTime(conn *connection.IRODSConnection, ticketName string) error {
	return ModifyTicketExpirationTime(conn, ticketName, time.Time{})
}

// SupplyTicket supplies a ticket to obtain access
func SupplyTicket(conn *connection.IRODSConnection, ticketName string) error {
	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	req := message.NewIRODSMessageTicketAdminRequest("session", ticketName)
	err := conn.RequestAndCheck(req, &message.IRODSMessageAdminResponse{}, nil)
	if err != nil {
		return xerrors.Errorf("received supply ticket error: %w", err)
	}
	return nil
}
