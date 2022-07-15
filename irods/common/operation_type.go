package common

// OperationType ...
type OperationType int

// operation types
const (
	OPER_TYPE_NONE               OperationType = 0
	OPER_TYPE_PUT_DATA_OBJ       OperationType = 1
	OPER_TYPE_REPLICATE_DATA_OBJ OperationType = 6
	OPER_TYPE_COPY_DATA_OBJ_DEST OperationType = 9
	OPER_TYPE_COPY_DATA_OBJ_SRC  OperationType = 10
	OPER_TYPE_RENAME_DATA_OBJ    OperationType = 11
	OPER_TYPE_RENAME_COLL        OperationType = 12

	// flag for oprType of dataObjInp_t and structFileOprInp_t
	OPER_TYPE_PURGE_STRUCT_FILE_CACHE OperationType = 1
	OPER_TYPE_DELETE_STRUCT_FILE      OperationType = 2
	OPER_TYPE_NO_REG_COLL_INFO        OperationType = 4
	OPER_TYPE_LOGICAL_BUNDLE          OperationType = 8
	OPER_TYPE_CREATE_TAR              OperationType = 0
	OPER_TYPE_ADD_TO_TAR              OperationType = 16
	OPER_TYPE_PRESERVE_COLL_PATH      OperationType = 32
	OPER_TYPE_PRESERVE_DIR_CONT       OperationType = 64
)
