package common

type ObjType int

const (
	UNKNOWN_OBJ_T ObjType = iota
	DATA_OBJ_T
	COLL_OBJ_T
	UNKNOWN_FILE_T
	LOCAL_FILE_T
	LOCAL_DIR_T
	NO_INPUT_T
)
