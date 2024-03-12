package common

// OperationType ...
type OperationType int

// operation types
const (
	OPER_TYPE_NONE                    OperationType = 0
	OPER_TYPE_PUT_DATA_OBJ            OperationType = 1
	OPER_TYPE_GET_DATA_OBJ            OperationType = 2
	OPER_TYPE_SAME_HOST_COPY_OBJ      OperationType = 3
	OPER_TYPE_COPY_TO_LOCAL_OBJ       OperationType = 4
	OPER_TYPE_COPY_TO_REMOTE_OBJ      OperationType = 5
	OPER_TYPE_REPLICATE_DATA_OBJ      OperationType = 6
	OPER_TYPE_REPLICATE_DATA_OBJ_DEST OperationType = 7
	OPER_TYPE_REPLICATE_DATA_OBJ_SRC  OperationType = 8
	OPER_TYPE_COPY_DATA_OBJ_DEST      OperationType = 9
	OPER_TYPE_COPY_DATA_OBJ_SRC       OperationType = 10
	OPER_TYPE_RENAME_DATA_OBJ         OperationType = 11
	OPER_TYPE_RENAME_COLL             OperationType = 12
	OPER_TYPE_MOVE                    OperationType = 13
	OPER_TYPE_RSYNC                   OperationType = 14
	OPER_TYPE_PHYMV                   OperationType = 15
	OPER_TYPE_PHYMV_SRC               OperationType = 16
	OPER_TYPE_PHYMV_DEST              OperationType = 17
	OPER_TYPE_QUERY_DATA_OBJ          OperationType = 18
	OPER_TYPE_QUERY_DATA_OBJ_RECUR    OperationType = 19
	OPER_TYPE_QUERY_COLL_OBJ          OperationType = 20
	OPER_TYPE_QUERY_COLL_OBJ_RECUR    OperationType = 21
	OPER_TYPE_RENAME_UNKNOWN_TYPE     OperationType = 22
	OPER_TYPE_REMOTE_ZONE             OperationType = 24
	OPER_TYPE_UNREG                   OperationType = 26
	OPER_TYPE_DONE                    OperationType = 9999

	// flag for oprType of dataObjInp_t and structFileOprInp_t
	OPER_TYPE_PURGE_STRUCT_FILE_CACHE OperationType = 1
	OPER_TYPE_DELETE_STRUCT_FILE      OperationType = 2
	OPER_TYPE_NO_REG_COLL_INFO        OperationType = 4
	OPER_TYPE_LOGICAL_BUNDLE          OperationType = 8
	OPER_TYPE_CREATE_TAR              OperationType = 0
	OPER_TYPE_ADD_TO_TAR              OperationType = 16
	OPER_TYPE_PRESERVE_COLL_PATH      OperationType = 32
	OPER_TYPE_PRESERVE_DIR_CONT       OperationType = 64

	/* definition for openType in l1desc_t */
	OPER_TYPE_CREATE_TYPE    OperationType = 1
	OPER_TYPE_OPEN_FOR_READ  OperationType = 2
	OPER_TYPE_OPEN_FOR_WRITE OperationType = 3
)
