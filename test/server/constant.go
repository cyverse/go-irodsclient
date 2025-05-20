package server

type IRODSTestServerVersion string

const (
	IRODS_4_2_8  IRODSTestServerVersion = "4.2.8"
	IRODS_4_2_11 IRODSTestServerVersion = "4.2.11"
	IRODS_4_3_3  IRODSTestServerVersion = "4.3.3"
)

var (
	Test_IRODS_Versions []IRODSTestServerVersion = []IRODSTestServerVersion{
		IRODS_4_2_8,
		IRODS_4_2_11,
		IRODS_4_3_3,
	}

	Production_IRODS_Versions []IRODSTestServerVersion = []IRODSTestServerVersion{
		IRODS_4_2_8,
	}
)

const (
	testServerHost          string = "localhost"
	testServerPort          int    = 1247
	testServerAdminUser     string = "rods"
	testServerAdminPassword string = "rods"
	testServerZone          string = "tempZone"
	testServerResource      string = "demoResc"

	productionServerHost          string = "data.cyverse.org"
	productionServerPort          int    = 1247
	productionServerAdminUser     string = ""
	productionServerAdminPassword string = ""
	productionServerZone          string = "iplant"
	productionServerResource      string = ""
)
