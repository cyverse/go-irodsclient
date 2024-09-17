package common

// KeyWord is a type for some reserved keywords
type KeyWord string

// reserved keywords
const (
	ALL_KW                 KeyWord = "all"              // operation done on all replicas
	COPIES_KW              KeyWord = "copies"           // the number of copies
	EXEC_LOCALLY_KW        KeyWord = "execLocally"      // execute locally
	FORCE_FLAG_KW          KeyWord = "forceFlag"        // force update
	CLI_IN_SVR_FIREWALL_KW KeyWord = "cliInSvrFirewall" // cli behind same firewall
	REG_CHKSUM_KW          KeyWord = "regChksum"        // register checksum
	VERIFY_CHKSUM_KW       KeyWord = "verifyChksum"     // verify checksum
	NO_COMPUTE_KW          KeyWord = "no_compute"
	VERIFY_BY_SIZE_KW      KeyWord = "verifyBySize" // verify by size - used by irsync
	OBJ_PATH_KW            KeyWord = "objPath"      // logical path of the object
	RECURSIVE_OPR_KW       KeyWord = "recursiveOpr"

	RESC_NAME_KW         KeyWord = "rescName"       // resource name
	DEST_RESC_NAME_KW    KeyWord = "destRescName"   // destination resource name
	DEF_RESC_NAME_KW     KeyWord = "defRescName"    // default resource name
	BACKUP_RESC_NAME_KW  KeyWord = "backupRescName" // backup resource name
	DATA_TYPE_KW         KeyWord = "dataType"       // data type
	DATA_SIZE_KW         KeyWord = "dataSize"
	CHKSUM_KW            KeyWord = "chksum"
	ORIG_CHKSUM_KW       KeyWord = "orig_chksum"
	VERSION_KW           KeyWord = "version"
	FILE_PATH_KW         KeyWord = "filePath"         // the physical file path
	BUN_FILE_PATH_KW     KeyWord = "bunFilePath"      // the physical bun file path
	REPL_NUM_KW          KeyWord = "replNum"          // replica number
	WRITE_FLAG_KW        KeyWord = "writeFlag"        // whether it is opened for write
	REPL_STATUS_KW       KeyWord = "replStatus"       // status of the replica
	ALL_REPL_STATUS_KW   KeyWord = "allReplStatus"    // update all replStatus
	METADATA_INCLUDED_KW KeyWord = "metadataIncluded" // for atomic puts of data / metadata
	ACL_INCLUDED_KW      KeyWord = "aclIncluded"      // for atomic puts of data / access controls
	DATA_INCLUDED_KW     KeyWord = "dataIncluded"     // data included in the input buffer
	DATA_OWNER_KW        KeyWord = "dataOwner"
	DATA_OWNER_ZONE_KW   KeyWord = "dataOwnerZone"
	DATA_EXPIRY_KW       KeyWord = "dataExpiry"
	DATA_COMMENTS_KW     KeyWord = "dataComments"
	DATA_CREATE_KW       KeyWord = "dataCreate"
	DATA_MODIFY_KW       KeyWord = "dataModify"
	DATA_ACCESS_KW       KeyWord = "dataAccess"
	DATA_ACCESS_INX_KW   KeyWord = "dataAccessInx"
	NO_OPEN_FLAG_KW      KeyWord = "noOpenFlag"
	PHYOPEN_BY_SIZE_KW   KeyWord = "phyOpenBySize"
	STREAMING_KW         KeyWord = "streaming"
	DATA_ID_KW           KeyWord = "dataId"
	COLL_ID_KW           KeyWord = "collId"
	DATA_MODE_KW         KeyWord = "dataMode"
	STATUS_STRING_KW     KeyWord = "statusString"
	DATA_MAP_ID_KW       KeyWord = "dataMapId"
	NO_PARA_OP_KW        KeyWord = "noParaOpr"
	LOCAL_PATH_KW        KeyWord = "localPath"
	RSYNC_MODE_KW        KeyWord = "rsyncMode"
	RSYNC_DEST_PATH_KW   KeyWord = "rsyncDestPath"
	RSYNC_CHKSUM_KW      KeyWord = "rsyncChksum"
	CHKSUM_ALL_KW        KeyWord = "ChksumAll"
	FORCE_CHKSUM_KW      KeyWord = "forceChksum"
	COLLECTION_KW        KeyWord = "collection"
	ADMIN_KW             KeyWord = "irodsAdmin"
	ADMIN_RMTRASH_KW     KeyWord = "irodsAdminRmTrash"
	UNREG_KW             KeyWord = "unreg"
	RMTRASH_KW           KeyWord = "irodsRmTrash"
	RECURSIVE_OPR__KW    KeyWord = "recursiveOpr"
	COLLECTION_TYPE_KW   KeyWord = "collectionType"
	COLLECTION_INFO1_KW  KeyWord = "collectionInfo1"
	COLLECTION_INFO2_KW  KeyWord = "collectionInfo2"
	SEL_OBJ_TYPE_KW      KeyWord = "selObjType"
	STRUCT_FILE_OPR_KW   KeyWord = "structFileOpr"
	ALL_MS_PARAM_KW      KeyWord = "allMsParam"
	UNREG_COLL_KW        KeyWord = "unregColl"
	UPDATE_REPL_KW       KeyWord = "updateRepl"
	RBUDP_TRANSFER_KW    KeyWord = "rbudpTransfer"
	VERY_VERBOSE_KW      KeyWord = "veryVerbose"
	RBUDP_SEND_RATE_KW   KeyWord = "rbudpSendRate"
	RBUDP_PACK_SIZE_KW   KeyWord = "rbudpPackSize"
	ZONE_KW              KeyWord = "zone"
	REMOTE_ZONE_OPR_KW   KeyWord = "remoteZoneOpr"
	REPL_DATA_OBJ_INP_KW KeyWord = "replDataObjInp"
	CROSS_ZONE_CREATE_KW KeyWord = "replDataObjInp" // use the same for backward compatibility
	QUERY_BY_DATA_ID_KW  KeyWord = "queryByDataID"
	SU_CLIENT_USER_KW    KeyWord = "suClientUser"
	RM_BUN_COPY_KW       KeyWord = "rmBunCopy"
	KEY_WORD_KW          KeyWord = "keyWord"    // the msKeyValStr is a keyword
	CREATE_MODE_KW       KeyWord = "createMode" // a msKeyValStr keyword
	OPEN_FLAGS_KW        KeyWord = "openFlags"  // a msKeyValStr keyword
	OFFSET_KW            KeyWord = "offset"     // a msKeyValStr keyword

	// DATA_SIZE_KW already defined
	NUM_THREADS_KW               KeyWord = "numThreads"       // a msKeyValStr keyword
	OPR_TYPE_KW                  KeyWord = "oprType"          // a msKeyValStr keyword
	COLL_FLAGS_KW                KeyWord = "collFlags"        // a msKeyValStr keyword
	TRANSLATED_PATH_KW           KeyWord = "translatedPath"   // the path translated
	NO_TRANSLATE_LINKPT_KW       KeyWord = "noTranslateMntpt" // don't translate mntpt
	BULK_OPR_KW                  KeyWord = "bulkOpr"          // the bulk operation
	NON_BULK_OPR_KW              KeyWord = "nonBulkOpr"       // non bulk operation
	EXEC_CMD_RULE_KW             KeyWord = "execCmdRule"      // the rule that invoke execCmd
	EXEC_MY_RULE_KW              KeyWord = "execMyRule"       // the rule is invoked by rsExecMyRule
	STREAM_STDOUT_KW             KeyWord = "streamStdout"     // the stream stdout for execCmd
	REG_REPL_KW                  KeyWord = "regRepl"          // register replica
	AGE_KW                       KeyWord = "age"              // age of the file for itrim
	DRYRUN_KW                    KeyWord = "dryrun"           // do a dry run
	ACL_COLLECTION_KW            KeyWord = "aclCollection"    // the collection from which the ACL should be used
	NO_CHK_COPY_LEN_KW           KeyWord = "noChkCopyLen"     // Don't check the len when transferring
	TICKET_KW                    KeyWord = "ticket"           // for ticket-based-access
	PURGE_CACHE_KW               KeyWord = "purgeCache"       // purge the cache copy right after the operation JMC - backport 4537
	EMPTY_BUNDLE_ONLY_KW         KeyWord = "emptyBundleOnly"  // delete emptyBundleOnly // // JMC - backport 4552
	GET_RESOURCE_INFO_OP_TYPE_KW KeyWord = "getResourceInfoOpType"

	// =-=-=-=-=-=-=-
	// JMC - backport 4599
	LOCK_TYPE_KW       KeyWord = "lockType"   // valid values are READ_LOCK_TYPE, WRITE_LOCK_TYPE and UNLOCK_TYPE
	LOCK_CMD_KW        KeyWord = "lockCmd"    // valid values are SET_LOCK_WAIT_CMD, SET_LOCK_CMD and GET_LOCK_CMD
	LOCK_FD_KW         KeyWord = "lockFd"     // Lock file desc for unlock
	MAX_SUB_FILE_KW    KeyWord = "maxSubFile" // max number of files for tar file bundles
	MAX_BUNDLE_SIZE_KW KeyWord = "maxBunSize" // max size of a tar bundle in Gbs
	NO_STAGING_KW      KeyWord = "noStaging"

	// OBJ_PATH_KW already defined
	// COLL_NAME_KW already defined
	FILE_UID_KW         KeyWord = "fileUid"
	FILE_OWNER_KW       KeyWord = "fileOwner"
	FILE_GID_KW         KeyWord = "fileGid"
	FILE_GROUP_KW       KeyWord = "fileGroup"
	FILE_MODE_KW        KeyWord = "fileMode"
	FILE_CTIME_KW       KeyWord = "fileCtime"
	FILE_MTIME_KW       KeyWord = "fileMtime"
	FILE_SOURCE_PATH_KW KeyWord = "fileSourcePath"
	EXCLUDE_FILE_KW     KeyWord = "excludeFile"

	// The following are the keyWord definition for the rescCond key/value pair
	// RESC_NAME_KW is defined above
	RESC_ZONE_KW            KeyWord = "zoneName"
	RESC_LOC_KW             KeyWord = "rescLoc" // resc_net in DB
	RESC_TYPE_KW            KeyWord = "rescType"
	RESC_CLASS_KW           KeyWord = "rescClass"
	RESC_VAULT_PATH_KW      KeyWord = "rescVaultPath" // resc_def_path in DB
	RESC_STATUS_KW          KeyWord = "rescStatus"
	GATEWAY_ADDR_KW         KeyWord = "gateWayAddr"
	RESC_MAX_OBJ_SIZE_KW    KeyWord = "rescMaxObjSize"
	FREE_SPACE_KW           KeyWord = "freeSpace"
	FREE_SPACE_TIME_KW      KeyWord = "freeSpaceTime"
	FREE_SPACE_TIMESTAMP_KW KeyWord = "freeSpaceTimeStamp"
	RESC_TYPE_INX_KW        KeyWord = "rescTypeInx"
	RESC_CLASS_INX_KW       KeyWord = "rescClassInx"
	RESC_ID_KW              KeyWord = "rescId"
	RESC_COMMENTS_KW        KeyWord = "rescComments"
	RESC_CREATE_KW          KeyWord = "rescCreate"
	RESC_MODIFY_KW          KeyWord = "rescModify"

	// The following are the keyWord definition for the userCond key/value pair
	USER_NAME_CLIENT_KW        KeyWord = "userNameClient"
	RODS_ZONE_CLIENT_KW        KeyWord = "rodsZoneClient"
	HOST_CLIENT_KW             KeyWord = "hostClient"
	CLIENT_ADDR_KW             KeyWord = "clientAddr"
	USER_TYPE_CLIENT_KW        KeyWord = "userTypeClient"
	AUTH_STR_CLIENT_KW         KeyWord = "authStrClient" // user distin name
	USER_AUTH_SCHEME_CLIENT_KW KeyWord = "userAuthSchemeClient"
	USER_INFO_CLIENT_KW        KeyWord = "userInfoClient"
	USER_COMMENT_CLIENT_KW     KeyWord = "userCommentClient"
	USER_CREATE_CLIENT_KW      KeyWord = "userCreateClient"
	USER_MODIFY_CLIENT_KW      KeyWord = "userModifyClient"
	USER_NAME_PROXY_KW         KeyWord = "userNameProxy"
	RODS_ZONE_PROXY_KW         KeyWord = "rodsZoneProxy"
	HOST_PROXY_KW              KeyWord = "hostProxy"
	USER_TYPE_PROXY_KW         KeyWord = "userTypeProxy"
	AUTH_STR_PROXY_KW          KeyWord = "authStrProxy" // dn
	USER_AUTH_SCHEME_PROXY_KW  KeyWord = "userAuthSchemeProxy"
	USER_INFO_PROXY_KW         KeyWord = "userInfoProxy"
	USER_COMMENT_PROXY_KW      KeyWord = "userCommentProxy"
	USER_CREATE_PROXY_KW       KeyWord = "userCreateProxy"
	USER_MODIFY_PROXY_KW       KeyWord = "userModifyProxy"
	ACCESS_PERMISSION_KW       KeyWord = "accessPermission"
	NO_CHK_FILE_PERM_KW        KeyWord = "noChkFilePerm"

	// The following are the keyWord definition for the collCond key/value pair
	COLL_NAME_KW        KeyWord = "collName"
	COLL_PARENT_NAME_KW KeyWord = "collParentName" // parent_coll_name in DB
	COLL_OWNER_NAME_KW  KeyWord = "collOwnername"
	COLL_OWNER_ZONE_KW  KeyWord = "collOwnerZone"
	COLL_MAP_ID_KW      KeyWord = "collMapId"
	COLL_INHERITANCE_KW KeyWord = "collInheritance"
	COLL_COMMENTS_KW    KeyWord = "collComments"
	COLL_EXPIRY_KW      KeyWord = "collExpiry"
	COLL_CREATE_KW      KeyWord = "collCreate"
	COLL_MODIFY_KW      KeyWord = "collModify"
	COLL_ACCESS_KW      KeyWord = "collAccess"
	COLL_ACCESS_INX_KW  KeyWord = "collAccessInx"

	// The following are the keyWord definitions for the keyValPair_t input to chlModRuleExec.
	RULE_NAME_KW              KeyWord = "ruleName"
	RULE_REI_FILE_PATH_KW     KeyWord = "reiFilePath"
	RULE_USER_NAME_KW         KeyWord = "userName"
	RULE_EXE_ADDRESS_KW       KeyWord = "exeAddress"
	RULE_EXE_TIME_KW          KeyWord = "exeTime"
	RULE_EXE_FREQUENCY_KW     KeyWord = "exeFrequency"
	RULE_PRIORITY_KW          KeyWord = "priority"
	RULE_ESTIMATE_EXE_TIME_KW KeyWord = "estimateExeTime"
	RULE_NOTIFICATION_ADDR_KW KeyWord = "notificationAddr"
	RULE_LAST_EXE_TIME_KW     KeyWord = "lastExeTime"
	RULE_EXE_STATUS_KW        KeyWord = "exeStatus"

	// =-=-=-=-=-=-=-
	// irods general keywords definitions
	RESC_HIER_STR_KW      KeyWord = "resc_hier"
	REPLICA_TOKEN_KW      KeyWord = "replicaToken"
	DEST_RESC_HIER_STR_KW KeyWord = "dest_resc_hier"
	IN_PDMO_KW            KeyWord = "in_pdmo"
	STAGE_OBJ_KW          KeyWord = "stage_object"
	SYNC_OBJ_KW           KeyWord = "sync_object"
	IN_REPL_KW            KeyWord = "in_repl"

	// =-=-=-=-=-=-=-
	// irods tcp keyword definitions
	SOCKET_HANDLE_KW KeyWord = "tcp_socket_handle"

	// =-=-=-=-=-=-=-
	// irods ssl keyword definitions
	SSL_HOST_KW            KeyWord = "ssl_host"
	SSL_SHARED_SECRET_KW   KeyWord = "ssl_shared_secret"
	SSL_KEY_SIZE_KW        KeyWord = "ssl_key_size"
	SSL_SALT_SIZE_KW       KeyWord = "ssl_salt_size"
	SSL_NUM_HASH_ROUNDS_KW KeyWord = "ssl_num_hash_rounds"
	SSL_ALGORITHM_KW       KeyWord = "ssl_algorithm"

	// =-=-=-=-=-=-=-
	// irods data_object keyword definitions
	PHYSICAL_PATH_KW KeyWord = "physical_path"
	MODE_KW          KeyWord = "mode_kw"
	FLAGS_KW         KeyWord = "flags_kw"
	// borrowed RESC_HIER_STR_KW

	// =-=-=-=-=-=-=-
	// irods file_object keyword definitions
	LOGICAL_PATH_KW    KeyWord = "logical_path"
	FILE_DESCRIPTOR_KW KeyWord = "file_descriptor"
	L1_DESC_IDX_KW     KeyWord = "l1_desc_idx"
	SIZE_KW            KeyWord = "file_size"
	REPL_REQUESTED_KW  KeyWord = "repl_requested"
	// borrowed IN_PDMO_KW

	// =-=-=-=-=-=-=-
	// irods structured_object keyword definitions
	HOST_ADDR_KW     KeyWord = "host_addr"
	ZONE_NAME_KW     KeyWord = "zone_name"
	PORT_NUM_KW      KeyWord = "port_num"
	SUB_FILE_PATH_KW KeyWord = "sub_file_path"
	// borrowed OFFSET_KW
	// borrowed DATA_TYPE_KW
	// borrowed OPR_TYPE_KW

	// =-=-=-=-=-=-=-
	// irods spec coll keyword definitions
	SPEC_COLL_CLASS_KW     KeyWord = "spec_coll_class"
	SPEC_COLL_TYPE_KW      KeyWord = "spec_coll_type"
	SPEC_COLL_OBJ_PATH_KW  KeyWord = "spec_coll_obj_path"
	SPEC_COLL_RESOURCE_KW  KeyWord = "spec_coll_resource"
	SPEC_COLL_RESC_HIER_KW KeyWord = "spec_coll_resc_hier"
	SPEC_COLL_PHY_PATH_KW  KeyWord = "spec_coll_phy_path"
	SPEC_COLL_CACHE_DIR_KW KeyWord = "spec_coll_cache_dir"
	SPEC_COLL_CACHE_DIRTY  KeyWord = "spec_coll_cache_dirty"
	SPEC_COLL_REPL_NUM     KeyWord = "spec_coll_repl_num"

	DISABLE_STRICT_ACL_KW KeyWord = "disable_strict_acls"
)
