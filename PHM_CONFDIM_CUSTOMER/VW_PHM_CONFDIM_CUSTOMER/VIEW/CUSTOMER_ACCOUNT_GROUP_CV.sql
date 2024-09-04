CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CUSTOMER_ACCOUNT_GROUP_CV`(
  CUSTOMER_ACCT_GROUP_ID OPTIONS(description="Customer Account Group ID"),
  CUSTOMER_ACCT_GROUP_DESC_TXT OPTIONS(description="Customer Account Group Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KTOKD_T077X     AS     CUSTOMER_ACCT_GROUP_ID
,TXT30_T077X     AS     CUSTOMER_ACCT_GROUP_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_ACCOUNT_GROUP_CV`;