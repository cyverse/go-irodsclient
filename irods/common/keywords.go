package common

// KeyWord is a type for some reserved keywords
type KeyWord string

// reserved keywords
const (
	ZONE_KW          KeyWord = "zone"
	RECURSIVE_OPR_KW KeyWord = "recursiveOpr"
	FORCE_FLAG_KW    KeyWord = "forceFlag"
	BULK_OPR_KW      KeyWord = "bulkOpr"
	//ALL_KW           KeyWord = "all"
	DEST_RESC_NAME_KW  KeyWord = "destRescName"
	DATA_TYPE_KW       KeyWord = "dataType"
	DATA_SIZE_KW       KeyWord = "dataSize"
	NUM_THREADS_KW     KeyWord = "numThreads"
	OPR_TYPE_KW        KeyWord = "oprType"
	UPDATE_REPL_KW     KeyWord = "updateRepl"
	RESC_NAME_KW       KeyWord = "rescName"
	COPIES_KW          KeyWord = "copies"
	AGE_KW             KeyWord = "age"
	ADMIN_KW           KeyWord = "irodsAdmin"
	COLLECTION_TYPE_KW KeyWord = "collectionType"

	RESC_HIER_STR_KW      KeyWord = "resc_hier"
	REPLICA_TOKEN_KW      KeyWord = "replicaToken"
	DEST_RESC_HIER_STR_KW KeyWord = "dest_resc_hier"
	IN_PDMO_KW            KeyWord = "in_pdmo"
	STAGE_OBJ_KW          KeyWord = "stage_object"
	SYNC_OBJ_KW           KeyWord = "sync_object"
	IN_REPL_KW            KeyWord = "in_repl"
)
