CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.COLLECTION_GROUP_CV`(
  CLCTN_GROUP_ID OPTIONS(description="Collection Group ID,"),
  CLCTN_STRTGY OPTIONS(description="Collection Strategy,"),
  CLCTN_GROUP_DESC_TXT OPTIONS(description="Collection Group Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 COLL_GROUP_UDM_COLL_GRP     AS     CLCTN_GROUP_ID
,COLL_STRATEGY_UDM_COLL_GRP     AS     CLCTN_STRTGY
,COLL_GROUP_TEXT_UDM_COLL_GRPT     AS     CLCTN_GROUP_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.COLLECTION_GROUP_CV`;