CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.VARIABLE_PO_UNIT_ACTIVITY_CV`
(
  VABME_MARA STRING OPTIONS(description="Variable Purchase Order Unit Activity ID"),
  VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT STRING OPTIONS(description="Variable Purchase Order Unit Activity Description Text; Case TRIM(MARA.VABME), When Blank then NOT ACTIVE, When 1 then ACTIVE, When 2 then ACTIVE WITH OWN PRICE, Else N/A"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY VABME_MARA;