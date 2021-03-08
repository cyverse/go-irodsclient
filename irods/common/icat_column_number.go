package common

// ICATColumnNumber is an ICAT Column number type
type ICATColumnNumber int

// column numbers
const (
	// User
	ICAT_COLUMN_USER_ID          ICATColumnNumber = 201
	ICAT_COLUMN_USER_NAME        ICATColumnNumber = 202
	ICAT_COLUMN_USER_TYPE        ICATColumnNumber = 203
	ICAT_COLUMN_USER_ZONE        ICATColumnNumber = 204
	ICAT_COLUMN_USER_INFO        ICATColumnNumber = 206
	ICAT_COLUMN_USER_COMMENT     ICATColumnNumber = 207
	ICAT_COLUMN_USER_CREATE_TIME ICATColumnNumber = 208
	ICAT_COLUMN_USER_MODIFY_TIME ICATColumnNumber = 209

	// Data Object
	ICAT_COLUMN_D_DATA_ID       ICATColumnNumber = 401
	ICAT_COLUMN_D_COLL_ID       ICATColumnNumber = 402
	ICAT_COLUMN_DATA_NAME       ICATColumnNumber = 403 // basename
	ICAT_COLUMN_DATA_REPL_NUM   ICATColumnNumber = 404
	ICAT_COLUMN_DATA_VERSION    ICATColumnNumber = 405
	ICAT_COLUMN_DATA_TYPE_NAME  ICATColumnNumber = 406
	ICAT_COLUMN_DATA_SIZE       ICATColumnNumber = 407
	ICAT_COLUMN_D_RESC_NAME     ICATColumnNumber = 409
	ICAT_COLUMN_D_DATA_PATH     ICATColumnNumber = 410 // physical path on resource
	ICAT_COLUMN_D_OWNER_NAME    ICATColumnNumber = 411
	ICAT_COLUMN_D_OWNER_ZONE    ICATColumnNumber = 412
	ICAT_COLUMN_D_REPL_STATUS   ICATColumnNumber = 413
	ICAT_COLUMN_D_DATA_STATUS   ICATColumnNumber = 414
	ICAT_COLUMN_D_DATA_CHECKSUM ICATColumnNumber = 415
	ICAT_COLUMN_D_EXPIRY        ICATColumnNumber = 416
	ICAT_COLUMN_D_MAP_ID        ICATColumnNumber = 417
	ICAT_COLUMN_D_COMMENTS      ICATColumnNumber = 418
	ICAT_COLUMN_D_CREATE_TIME   ICATColumnNumber = 419
	ICAT_COLUMN_D_MODIFY_TIME   ICATColumnNumber = 420
	ICAT_COLUMN_D_RESC_HIER     ICATColumnNumber = 422
	ICAT_COLUMN_D_RESC_ID       ICATColumnNumber = 423

	// Collection
	ICAT_COLUMN_COLL_ID          ICATColumnNumber = 500
	ICAT_COLUMN_COLL_NAME        ICATColumnNumber = 501
	ICAT_COLUMN_COLL_PARENT_NAME ICATColumnNumber = 502
	ICAT_COLUMN_COLL_OWNER_NAME  ICATColumnNumber = 503
	ICAT_COLUMN_COLL_OWNER_ZONE  ICATColumnNumber = 504
	ICAT_COLUMN_COLL_MAP_ID      ICATColumnNumber = 505
	ICAT_COLUMN_COLL_INHERITANCE ICATColumnNumber = 506
	ICAT_COLUMN_COLL_COMMENTS    ICATColumnNumber = 507
	ICAT_COLUMN_COLL_CREATE_TIME ICATColumnNumber = 508
	ICAT_COLUMN_COLL_MODIFY_TIME ICATColumnNumber = 509

	// Data Object Meta
	ICAT_COLUMN_META_DATA_ATTR_NAME   ICATColumnNumber = 600
	ICAT_COLUMN_META_DATA_ATTR_VALUE  ICATColumnNumber = 601
	ICAT_COLUMN_META_DATA_ATTR_UNITS  ICATColumnNumber = 602
	ICAT_COLUMN_META_DATA_ATTR_ID     ICATColumnNumber = 603
	ICAT_COLUMN_META_DATA_CREATE_TIME ICATColumnNumber = 604
	ICAT_COLUMN_META_DATA_MODIFY_TIME ICATColumnNumber = 605

	// Collection Meta
	ICAT_COLUMN_META_COLL_ATTR_NAME   ICATColumnNumber = 610
	ICAT_COLUMN_META_COLL_ATTR_VALUE  ICATColumnNumber = 611
	ICAT_COLUMN_META_COLL_ATTR_UNITS  ICATColumnNumber = 612
	ICAT_COLUMN_META_COLL_ATTR_ID     ICATColumnNumber = 613
	ICAT_COLUMN_META_COLL_CREATE_TIME ICATColumnNumber = 614
	ICAT_COLUMN_META_COLL_MODIFY_TIME ICATColumnNumber = 615

	// Data Object Access
	ICAT_COLUMN_DATA_ACCESS_TYPE     ICATColumnNumber = 700
	ICAT_COLUMN_DATA_ACCESS_NAME     ICATColumnNumber = 701
	ICAT_COLUMN_DATA_TOKEN_NAMESPACE ICATColumnNumber = 702
	ICAT_COLUMN_DATA_ACCESS_USER_ID  ICATColumnNumber = 703
	ICAT_COLUMN_DATA_ACCESS_DATA_ID  ICATColumnNumber = 704

	// Collection Access
	ICAT_COLUMN_COLL_ACCESS_TYPE     ICATColumnNumber = 710
	ICAT_COLUMN_COLL_ACCESS_NAME     ICATColumnNumber = 711
	ICAT_COLUMN_COLL_TOKEN_NAMESPACE ICATColumnNumber = 712
	ICAT_COLUMN_COLL_ACCESS_USER_ID  ICATColumnNumber = 713
	ICAT_COLUMN_COLL_ACCESS_COLL_ID  ICATColumnNumber = 714

	// Group
	ICAT_COLUMN_COLL_USER_GROUP_ID   ICATColumnNumber = 900
	ICAT_COLUMN_COLL_USER_GROUP_NAME ICATColumnNumber = 901
)