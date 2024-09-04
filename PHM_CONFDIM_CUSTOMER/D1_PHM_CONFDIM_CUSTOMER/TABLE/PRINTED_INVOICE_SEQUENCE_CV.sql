CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRINTED_INVOICE_SEQUENCE_CV`
(
  YYINVSEQ_YTSD_INVSEQ_TEXT STRING OPTIONS(description="Printed Invoice Sequence ID;YTSD_INVSEQ_TEXT.YYINVSEQ where YTSD_INVSEQ_TEXT.SPRAS = E"),
  YYINVSEQ_DES_YTSD_INVSEQ_TEXT STRING OPTIONS(description="Printed Invoice Sequence Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYINVSEQ_YTSD_INVSEQ_TEXT;