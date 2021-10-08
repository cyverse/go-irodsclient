package message

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestIRODSQuery(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "message",
		"function": "TestIRODSQuery",
	})

	query := NewIRODSMessageQuery(500, 0, 0, 0)

	queryBytes, err := query.GetBytes()
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}

	logger.Debugf("Query : %s", queryBytes)
}

func TestIRODSQueryKeyVal(t *testing.T) {
	logger := log.WithFields(log.Fields{
		"package":  "message",
		"function": "TestIRODSQueryKeyVal",
	})

	query := NewIRODSMessageQuery(500, 0, 0, 0)
	query.Selects.Add(500, 1)
	query.Selects.Add(501, 1)
	query.Selects.Add(502, 1)
	query.Selects.Add(503, 1)
	query.Selects.Add(504, 1)

	query.Conditions.Add(501, "= '/iplant/home/iychoi'")
	queryBytes, err := query.GetBytes()
	if err != nil {
		logger.Errorf("err - %v", err)
		panic(err)
	}

	logger.Debugf("Query : %s", queryBytes)
}
