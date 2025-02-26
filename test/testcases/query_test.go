package testcases

/*
func TestQuery(t *testing.T) {
	StartIRODSTestServer()
	defer shutdownIRODSTestServer()

	t.Run("test Query Struct", testQueryStruct)
	t.Run("test Query Struct with Key and Value", testQueryKeyVal)
}

func testQueryStruct(t *testing.T) {
	query := message.NewIRODSMessageQueryRequest(500, 0, 0, 0)

	queryBytes, err := query.GetBytes()
	FailError(t, err)

	assert.Equal(
		t,
		"<GenQueryInp_PI><maxRows>500</maxRows><continueInx>0</continueInx><partialStartIndex>0</partialStartIndex><options>0</options><KeyValPair_PI><ssLen>0</ssLen></KeyValPair_PI><InxIvalPair_PI><iiLen>0</iiLen></InxIvalPair_PI><InxValPair_PI><isLen>0</isLen></InxValPair_PI></GenQueryInp_PI>",
		string(queryBytes),
	)
}

func testQueryKeyVal(t *testing.T) {
	query := message.NewIRODSMessageQueryRequest(500, 0, 0, 0)
	query.Selects.Add(500, 1)
	query.Selects.Add(501, 1)
	query.Selects.Add(502, 1)
	query.Selects.Add(503, 1)
	query.Selects.Add(504, 1)

	query.Conditions.Add(501, "= '/zone/home/user'")
	queryBytes, err := query.GetBytes()
	FailError(t, err)
	assert.Equal(
		t,
		"<GenQueryInp_PI><maxRows>500</maxRows><continueInx>0</continueInx><partialStartIndex>0</partialStartIndex><options>0</options><KeyValPair_PI><ssLen>0</ssLen></KeyValPair_PI><InxIvalPair_PI><iiLen>5</iiLen><inx>500</inx><inx>501</inx><inx>502</inx><inx>503</inx><inx>504</inx><ivalue>1</ivalue><ivalue>1</ivalue><ivalue>1</ivalue><ivalue>1</ivalue><ivalue>1</ivalue></InxIvalPair_PI><InxValPair_PI><isLen>1</isLen><inx>501</inx><svalue>= '/zone/home/user'</svalue></InxValPair_PI></GenQueryInp_PI>",
		string(queryBytes),
	)
}
*/
