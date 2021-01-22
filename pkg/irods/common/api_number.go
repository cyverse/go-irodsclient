package common

// APINumber is a api number type
type APINumber int

// api numbers
const (
	// 500 - 599 - Internal File I/O API calls
	FILE_CREATE_AN            APINumber = 500
	FILE_OPEN_AN              APINumber = 501
	FILE_WRITE_AN             APINumber = 502
	FILE_CLOSE_AN             APINumber = 503
	FILE_LSEEK_AN             APINumber = 504
	FILE_READ_AN              APINumber = 505
	FILE_UNLINK_AN            APINumber = 506
	FILE_MKDIR_AN             APINumber = 507
	FILE_CHMOD_AN             APINumber = 508
	FILE_RMDIR_AN             APINumber = 509
	FILE_STAT_AN              APINumber = 510
	FILE_FSTAT_AN             APINumber = 511
	FILE_FSYNC_AN             APINumber = 512
	FILE_STAGE_AN             APINumber = 513
	FILE_GET_FS_FREE_SPACE_AN APINumber = 514
	FILE_OPENDIR_AN           APINumber = 515
	FILE_CLOSEDIR_AN          APINumber = 516
	FILE_READDIR_AN           APINumber = 517
	FILE_PUT_AN               APINumber = 518
	FILE_GET_AN               APINumber = 519
	FILE_CHKSUM_AN            APINumber = 520
	CHK_N_V_PATH_PERM_AN      APINumber = 521
	FILE_RENAME_AN            APINumber = 522
	FILE_TRUNCATE_AN          APINumber = 523
	FILE_STAGE_TO_CACHE_AN    APINumber = 524
	FILE_SYNC_TO_ARCH_AN      APINumber = 525

	// 600 - 699 - Object File I/O API calls
	DATA_OBJ_CREATE_AN           APINumber = 601
	DATA_OBJ_OPEN_AN             APINumber = 602
	DATA_OBJ_PUT_AN              APINumber = 606
	DATA_PUT_AN                  APINumber = 607
	DATA_OBJ_GET_AN              APINumber = 608
	DATA_GET_AN                  APINumber = 609
	DATA_OBJ_REPL250_AN          APINumber = 610
	DATA_COPY_AN                 APINumber = 611
	DATA_OBJ_COPY250_AN          APINumber = 613
	SIMPLE_QUERY_AN              APINumber = 614
	DATA_OBJ_UNLINK_AN           APINumber = 615
	REG_DATA_OBJ_AN              APINumber = 619
	UNREG_DATA_OBJ_AN            APINumber = 620
	REG_REPLICA_AN               APINumber = 621
	MOD_DATA_OBJ_META_AN         APINumber = 622
	RULE_EXEC_SUBMIT_AN          APINumber = 623
	RULE_EXEC_DEL_AN             APINumber = 624
	EXEC_MY_RULE_AN              APINumber = 625
	OPR_COMPLETE_AN              APINumber = 626
	DATA_OBJ_RENAME_AN           APINumber = 627
	DATA_OBJ_RSYNC_AN            APINumber = 628
	DATA_OBJ_CHKSUM_AN           APINumber = 629
	PHY_PATH_REG_AN              APINumber = 630
	DATA_OBJ_PHYMV250_AN         APINumber = 631
	DATA_OBJ_TRIM_AN             APINumber = 632
	OBJ_STAT_AN                  APINumber = 633
	SUB_STRUCT_FILE_CREATE_AN    APINumber = 635
	SUB_STRUCT_FILE_OPEN_AN      APINumber = 636
	SUB_STRUCT_FILE_READ_AN      APINumber = 637
	SUB_STRUCT_FILE_WRITE_AN     APINumber = 638
	SUB_STRUCT_FILE_CLOSE_AN     APINumber = 639
	SUB_STRUCT_FILE_UNLINK_AN    APINumber = 640
	SUB_STRUCT_FILE_STAT_AN      APINumber = 641
	SUB_STRUCT_FILE_FSTAT_AN     APINumber = 642
	SUB_STRUCT_FILE_LSEEK_AN     APINumber = 643
	SUB_STRUCT_FILE_RENAME_AN    APINumber = 644
	QUERY_SPEC_COLL_AN           APINumber = 645
	SUB_STRUCT_FILE_MKDIR_AN     APINumber = 647
	SUB_STRUCT_FILE_RMDIR_AN     APINumber = 648
	SUB_STRUCT_FILE_OPENDIR_AN   APINumber = 649
	SUB_STRUCT_FILE_READDIR_AN   APINumber = 650
	SUB_STRUCT_FILE_CLOSEDIR_AN  APINumber = 651
	DATA_OBJ_TRUNCATE_AN         APINumber = 652
	SUB_STRUCT_FILE_TRUNCATE_AN  APINumber = 653
	GET_XMSG_TICKET_AN           APINumber = 654
	SEND_XMSG_AN                 APINumber = 655
	RCV_XMSG_AN                  APINumber = 656
	SUB_STRUCT_FILE_GET_AN       APINumber = 657
	SUB_STRUCT_FILE_PUT_AN       APINumber = 658
	SYNC_MOUNTED_COLL_AN         APINumber = 659
	STRUCT_FILE_SYNC_AN          APINumber = 660
	CLOSE_COLLECTION_AN          APINumber = 661
	STRUCT_FILE_EXTRACT_AN       APINumber = 664
	STRUCT_FILE_EXT_AND_REG_AN   APINumber = 665
	STRUCT_FILE_BUNDLE_AN        APINumber = 666
	CHK_OBJ_PERM_AND_STAT_AN     APINumber = 667
	GET_REMOTE_ZONE_RESC_AN      APINumber = 668
	DATA_OBJ_OPEN_AND_STAT_AN    APINumber = 669
	L3_FILE_GET_SINGLE_BUF_AN    APINumber = 670
	L3_FILE_PUT_SINGLE_BUF_AN    APINumber = 671
	DATA_OBJ_CREATE_AND_STAT_AN  APINumber = 672
	DATA_OBJ_CLOSE_AN            APINumber = 673
	DATA_OBJ_LSEEK_AN            APINumber = 674
	DATA_OBJ_READ_AN             APINumber = 675
	DATA_OBJ_WRITE_AN            APINumber = 676
	COLL_REPL_AN                 APINumber = 677
	OPEN_COLLECTION_AN           APINumber = 678
	RM_COLL_AN                   APINumber = 679
	MOD_COLL_AN                  APINumber = 680
	COLL_CREATE_AN               APINumber = 681
	RM_COLL_OLD_AN               APINumber = 682
	REG_COLL_AN                  APINumber = 683
	PHY_BUNDLE_COLL_AN           APINumber = 684
	UNBUN_AND_REG_PHY_BUNFILE_AN APINumber = 685
	GET_HOST_FOR_PUT_AN          APINumber = 686
	GET_RESC_QUOTA_AN            APINumber = 687
	BULK_DATA_OBJ_REG_AN         APINumber = 688
	BULK_DATA_OBJ_PUT_AN         APINumber = 689
	PROC_STAT_AN                 APINumber = 690
	STREAM_READ_AN               APINumber = 691
	EXEC_CMD_AN                  APINumber = 692
	STREAM_CLOSE_AN              APINumber = 693
	GET_HOST_FOR_GET_AN          APINumber = 694
	DATA_OBJ_REPL_AN             APINumber = 695
	DATA_OBJ_COPY_AN             APINumber = 696
	DATA_OBJ_PHYMV_AN            APINumber = 697
	DATA_OBJ_FSYNC_AN            APINumber = 698
	DATA_OBJ_LOCK_AN             APINumber = 699

	// 700 - 799 - Metadata API calls
	GET_MISC_SVR_INFO_AN           APINumber = 700
	GENERAL_ADMIN_AN               APINumber = 701
	GEN_QUERY_AN                   APINumber = 702
	AUTH_REQUEST_AN                APINumber = 703
	AUTH_RESPONSE_AN               APINumber = 704
	AUTH_CHECK_AN                  APINumber = 705
	MOD_AVU_METADATA_AN            APINumber = 706
	MOD_ACCESS_CONTROL_AN          APINumber = 707
	RULE_EXEC_MOD_AN               APINumber = 708
	GET_TEMP_PASSWORD_AN           APINumber = 709
	GENERAL_UPDATE_AN              APINumber = 710
	GSI_AUTH_REQUEST_AN            APINumber = 711
	READ_COLLECTION_AN             APINumber = 713
	USER_ADMIN_AN                  APINumber = 714
	GENERAL_ROW_INSERT_AN          APINumber = 715
	GENERAL_ROW_PURGE_AN           APINumber = 716
	KRB_AUTH_REQUEST_AN            APINumber = 717
	END_TRANSACTION_AN             APINumber = 718
	DATABASE_RESC_OPEN_AN          APINumber = 719
	DATABASE_OBJ_CONTROL_AN        APINumber = 720
	DATABASE_RESC_CLOSE_AN         APINumber = 721
	SPECIFIC_QUERY_AN              APINumber = 722
	TICKET_ADMIN_AN                APINumber = 723
	GET_TEMP_PASSWORD_FOR_OTHER_AN APINumber = 724
	PAM_AUTH_REQUEST_AN            APINumber = 725

	EXEC_CMD241_AN APINumber = 634

	DATA_OBJ_READ201_AN   APINumber = 603
	DATA_OBJ_WRITE201_AN  APINumber = 604
	DATA_OBJ_CLOSE201_AN  APINumber = 605
	DATA_OBJ_LSEEK201_AN  APINumber = 612
	RM_COLL_OLD201_AN     APINumber = 617
	REG_COLL201_AN        APINumber = 618
	MOD_COLL201_AN        APINumber = 646
	COLL_REPL201_AN       APINumber = 662
	RM_COLL201_AN         APINumber = 663
	OPEN_COLLECTION201_AN APINumber = 712

	// 1000 - 1059 - NETCDF API calls
	NC_OPEN_AN             APINumber = 1000
	NC_CREATE_AN           APINumber = 1001
	NC_CLOSE_AN            APINumber = 1002
	NC_INQ_ID_AN           APINumber = 1003
	NC_INQ_WITH_ID_AN      APINumber = 1004
	NC_GET_VARS_BY_TYPE_AN APINumber = 1005
	NCCF_GET_VARA_AN       APINumber = 1006
	NC_INQ_AN              APINumber = 1007
	NC_OPEN_GROUP_AN       APINumber = 1008
	NC_INQ_GRPS_AN         APINumber = 1009
	NC_REG_GLOBAL_ATTR_AN  APINumber = 1010

	// 1060 - 1099 - OOI API calls
	OOI_GEN_SERV_REQ_AN APINumber = 1060

	// 1100 - 1200 - SSL API calls
	SSL_START_AN APINumber = 1100
	SSL_END_AN   APINumber = 1101
)
