package common

// OperationType ...
type OperationType int

// operation types
const (
	OPER_TYPE_REPLICATE_DATA_OBJ OperationType = 6
	OPER_TYPE_COPY_DATA_OBJ_DEST OperationType = 9
	OPER_TYPE_COPY_DATA_OBJ_SRC  OperationType = 10
	OPER_TYPE_RENAME_DATA_OBJ    OperationType = 11
	OPER_TYPE_RENAME_COLL        OperationType = 12
)
