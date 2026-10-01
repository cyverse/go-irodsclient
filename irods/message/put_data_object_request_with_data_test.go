package message

import (
	"bytes"
	"testing"

	"github.com/cyverse/go-irodsclient/irods/common"
)

func putRequestKeyVal(request *IRODSMessagePutDataObjectRequest, key common.KeyWord) (string, bool) {
	for i, k := range request.KeyVals.Keys {
		if k == string(key) {
			return request.KeyVals.Values[i].Value, true
		}
	}

	return "", false
}

func TestPutDataObjectRequestWithDataWireFormat(t *testing.T) {
	data := []byte("hello irods, this content travels with the request")

	request := NewIRODSMessagePutDataObjectRequestWithData("/zone/home/user/f", "demoResc", true, data)

	if request.Size != int64(len(data)) {
		t.Fatalf("request size %d, want %d", request.Size, len(data))
	}

	// PUT_OPR, as the reference client sets before sending
	if request.OperationType != int(common.OPER_TYPE_PUT_DATA_OBJ) {
		t.Fatalf("operation type %d, want %d", request.OperationType, common.OPER_TYPE_PUT_DATA_OBJ)
	}

	for _, key := range []common.KeyWord{
		common.DATA_INCLUDED_KW,
		common.DATA_SIZE_KW,
		common.DATA_TYPE_KW,
		common.FORCE_FLAG_KW,
		common.DEST_RESC_NAME_KW,
	} {
		if _, ok := putRequestKeyVal(request, key); !ok {
			t.Fatalf("%s keyword is missing", key)
		}
	}

	if size, _ := putRequestKeyVal(request, common.DATA_SIZE_KW); size != "50" {
		t.Fatalf("%s keyword is %q, want %q", common.DATA_SIZE_KW, size, "50")
	}

	// the content must travel in the bs section, never in the marshalled xml
	xmlBytes, err := request.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(xmlBytes, data) {
		t.Fatalf("the data leaked into the marshalled xml:\n%s", xmlBytes)
	}

	msg, err := request.GetMessage()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(msg.Body.Bs, data) {
		t.Fatalf("message bs holds %d bytes, want the %d bytes of content", len(msg.Body.Bs), len(data))
	}

	if msg.Body.IntInfo != int32(common.DATA_OBJ_PUT_AN) {
		t.Fatalf("api number %d, want %d", msg.Body.IntInfo, common.DATA_OBJ_PUT_AN)
	}

	if msg.Header.BsLen != uint32(len(data)) {
		t.Fatalf("header bsLen %d, want %d", msg.Header.BsLen, len(data))
	}

	if msg.Header.MessageLen != uint32(len(xmlBytes)) {
		t.Fatalf("header msgLen %d, want %d", msg.Header.MessageLen, len(xmlBytes))
	}
}

func TestPutDataObjectRequestWithoutForceOmitsTheFlag(t *testing.T) {
	request := NewIRODSMessagePutDataObjectRequestWithData("/zone/home/user/f", "demoResc", false, []byte("x"))

	if _, ok := putRequestKeyVal(request, common.FORCE_FLAG_KW); ok {
		t.Fatal("the force flag must only be set when asked for")
	}
}

// TestPutDataObjectRequestXMLIsUnchanged pins the marshalled request, so that carrying the data
// on the request type never changes what the plain put request puts on the wire
func TestPutDataObjectRequestXMLIsUnchanged(t *testing.T) {
	const want = `<DataObjInp_PI><objPath>/zone/home/user/f</objPath><createMode>0</createMode>` +
		`<openFlags>0</openFlags><offset>0</offset><dataSize>1234</dataSize><numThreads>4</numThreads>` +
		`<oprType>1</oprType><KeyValPair_PI><ssLen>3</ssLen><keyWord>destRescName</keyWord>` +
		`<keyWord>dataSize</keyWord><keyWord>extra</keyWord><svalue>demoResc</svalue>` +
		`<svalue>1234</svalue><svalue>val</svalue></KeyValPair_PI></DataObjInp_PI>`

	request := NewIRODSMessagePutDataObjectRequest("/zone/home/user/f", "demoResc", 1234, 4)
	request.AddKeyVal("extra", "val")

	xmlBytes, err := request.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	if string(xmlBytes) != want {
		t.Fatalf("marshalled request changed\ngot:  %s\nwant: %s", xmlBytes, want)
	}

	msg, err := request.GetMessage()
	if err != nil {
		t.Fatal(err)
	}

	if msg.Body.Bs != nil {
		t.Fatalf("the plain put request must not carry a bs section, got %d bytes", len(msg.Body.Bs))
	}

	if msg.Header.BsLen != 0 {
		t.Fatalf("header bsLen %d, want 0", msg.Header.BsLen)
	}
}

func TestPutDataObjectRequestDataIsNotMarshalled(t *testing.T) {
	withoutData := NewIRODSMessagePutDataObjectRequest("/zone/home/user/f", "demoResc", 3, 0)

	withData := NewIRODSMessagePutDataObjectRequest("/zone/home/user/f", "demoResc", 3, 0)
	withData.Data = []byte{1, 2, 3}

	a, err := withoutData.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	b, err := withData.GetBytes()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(a, b) {
		t.Fatalf("setting Data changed the marshalled xml:\n%s\nvs\n%s", a, b)
	}

	if bytes.Contains(b, []byte{1, 2, 3}) {
		t.Fatalf("the Data content reached the marshalled xml:\n%s", b)
	}
}
