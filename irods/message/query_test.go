package message

import (
	"testing"

	"github.com/iychoi/go-irodsclient/irods/util"
)

func init() {
	util.SetLogLevel(9)
}

func TestIRODSQuery(t *testing.T) {
	query := NewIRODSMessageQuery(500, 0, 0, 0)

	queryBytes, err := query.GetBytes()
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Query : %s", queryBytes)
}

func TestIRODSQueryKeyVal(t *testing.T) {
	query := NewIRODSMessageQuery(500, 0, 0, 0)
	query.Selects.Add(500, 1)
	query.Selects.Add(501, 1)
	query.Selects.Add(502, 1)
	query.Selects.Add(503, 1)
	query.Selects.Add(504, 1)

	query.Conditions.Add(501, "= '/iplant/home/iychoi'")
	queryBytes, err := query.GetBytes()
	if err != nil {
		util.LogErrorf("err - %v", err)
		panic(err)
	}

	util.LogDebugf("Query : %s", queryBytes)
}
