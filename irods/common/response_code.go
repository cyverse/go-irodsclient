package common

// ResponseCode ...
type ResponseCode int

// response codes
const (
	SYS_SVR_TO_CLI_COLL_STAT       ResponseCode = 99999996
	SYS_CLI_TO_SVR_COLL_STAT_REPLY ResponseCode = 99999997
)
