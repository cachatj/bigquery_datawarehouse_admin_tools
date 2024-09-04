CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.COLLECTION_GROUP_CV`
(
  COLL_GROUP_UDM_COLL_GRP STRING OPTIONS(description="Collection Group ID"),
  COLL_STRATEGY_UDM_COLL_GRP STRING OPTIONS(description="Collection Strategy"),
  COLL_GROUP_TEXT_UDM_COLL_GRPT STRING OPTIONS(description="Collection Group Description Text;UDM_COLL_GRPT.COLL_GROUP_TEXT where UDM_COLL_GRPT.LANG = E and UDM_COLL_GRPT.COLL_GROUP = UDM_COLL_GRP.COLL_GROUP"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY COLL_GROUP_UDM_COLL_GRP;