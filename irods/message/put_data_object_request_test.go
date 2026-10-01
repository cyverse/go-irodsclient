package message

import (
	"strings"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/common"
)

func TestPutDataObjectRequestCarriesDataSize(t *testing.T) {
	const fileLength int64 = 1234567

	request := NewIRODSMessagePutDataObjectRequest("/zone/home/user/f", "demoResc", fileLength, 4)

	if request.Size != fileLength {
		t.Fatalf("request size %d, want %d", request.Size, fileLength)
	}

	// the reference client (rcDataObjPut) sends the size in the condInput as well, so that
	// resource plugins reading the keyword see it
	found := ""
	for i, key := range request.KeyVals.Keys {
		if key == string(common.DATA_SIZE_KW) {
			found = request.KeyVals.Values[i].Value
			break
		}
	}

	if found == "" {
		t.Fatalf("%s keyword is missing from the put request", common.DATA_SIZE_KW)
	}

	if found != "1234567" {
		t.Fatalf("%s keyword is %q, want %q", common.DATA_SIZE_KW, found, "1234567")
	}

	xmlBytes, err := request.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	xmlText := string(xmlBytes)
	if !strings.Contains(xmlText, "<dataSize>1234567</dataSize>") {
		t.Fatalf("marshalled request is missing the dataSize field:\n%s", xmlText)
	}
}
