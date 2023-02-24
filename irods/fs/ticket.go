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
	"golang.org/x/xerrors"
)

// https://github.com/irods/irods_client_s3_ticketbooth/blob/b92e8aaa3127cb56fcb8fef09caa00244bd29ca6/ticket_booth/main.py
// GetTicketForAnonymousAccess returns minimal ticket information for the ticket string
func GetTicketForAnonymousAccess(conn *connection.IRODSConnection, ticket string) (*types.IRODSTicketForAnonymousAccess, error) {
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

	condVal := fmt.Sprintf("= '%s'", ticket)
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
		ID:         ticketID,
		Name:       ticket,
		Type:       ticketType,
		Path:       ticketPath,
		ExpireTime: expireTime,
	}, nil
}

/*
// Need to resolve USER_ID and OBJECT_ID since they are not what we want
// GetTicket returns a ticket for the ticket string
func GetTicket(conn *connection.IRODSConnection, ticket string) (*types.IRODSTicket, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	query := message.NewIRODSMessageQuery(common.MaxQueryRows, 0, 0, 0)
	query.AddSelect(common.ICAT_COLUMN_TICKET_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_STRING, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_TYPE, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_USER_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_OBJECT_ID, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_OBJECT_TYPE, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_USES_LIMIT, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_USES_COUNT, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_EXPIRY_TS, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT, 1)
	query.AddSelect(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT, 1)

	condVal := fmt.Sprintf("= '%s'", ticket)
	query.AddCondition(common.ICAT_COLUMN_TICKET_STRING, condVal)

	queryResult := message.IRODSMessageQueryResult{}
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
	ticketName := ""
	ticketType := types.TicketTypeRead
	ticketOwner := ""
	ticketObjectType := types.ObjectTypeCollection
	ticketPath := ""
	var usesLimit int64 = 0
	var usesCount int64 = 0
	var writeFileLimit int64 = 0
	var writeFileCount int64 = 0
	var writeByteLimit int64 = 0
	var writeByteCount int64 = 0
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
		case int(common.ICAT_COLUMN_TICKET_STRING):
			ticketName = value
		case int(common.ICAT_COLUMN_TICKET_TYPE):
			ticketType = types.TicketType(value)
		case int(common.ICAT_COLUMN_TICKET_USER_ID):
			// TODO:
			//ticketOwner = value
		case int(common.ICAT_COLUMN_USER_NAME):
			ticketOwner = value
		case int(common.ICAT_COLUMN_TICKET_OBJECT_ID):
			ticketPath = value
		case int(common.ICAT_COLUMN_TICKET_OBJECT_TYPE):
			ticketObjectType = types.ObjectType(value)
		case int(common.ICAT_COLUMN_TICKET_EXPIRY_TS):
			if len(strings.TrimSpace(value)) > 0 {
				mT, err := util.GetIRODSDateTime(value)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse expiry time '%s': %w", value, err)
				}
				expireTime = mT
			}
		case int(common.ICAT_COLUMN_TICKET_USES_LIMIT):
			limit, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse uses limit '%s': %w", value, err)
			}
			usesLimit = limit
		case int(common.ICAT_COLUMN_TICKET_USES_COUNT):
			count, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse uses count '%s': %w", value, err)
			}
			usesCount = count
		case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_LIMIT):
			limit, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse write file limit '%s': %w", value, err)
			}
			writeFileLimit = limit
		case int(common.ICAT_COLUMN_TICKET_WRITE_FILE_COUNT):
			count, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse write file count '%s': %w", value, err)
			}
			writeFileCount = count
		case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_LIMIT):
			limit, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse write byte limit '%s': %w", value, err)
			}
			writeByteLimit = limit
		case int(common.ICAT_COLUMN_TICKET_WRITE_BYTE_COUNT):
			count, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse write byte count '%s': %w", value, err)
			}
			writeByteCount = count
		default:
			// ignore
		}
	}

	if ticketID == -1 {
		return nil, types.NewFileNotFoundErrorf("failed to find a ticket")
	}

	return &types.IRODSTicket{
		ID:             ticketID,
		Name:           ticketName,
		Type:           ticketType,
		Owner:          ticketOwner,
		ObjectType:     ticketObjectType,
		Path:           ticketPath,
		ExpireTime:     expireTime,
		UsesLimit:      usesLimit,
		UsesCount:      usesCount,
		WriteFileLimit: writeFileLimit,
		WriteFileCount: writeFileCount,
		WriteByteLimit: writeByteLimit,
		WriteByteCount: writeByteCount,
	}, nil
}
*/
