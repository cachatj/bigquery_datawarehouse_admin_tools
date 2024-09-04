CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.PRINTED_INVOICE_SEQUENCE_CV`(
  PRINTED_INVOICE_SEQ_ID OPTIONS(description="Printed Invoice Sequence ID"),
  PRINTED_INVOICE_SEQ_DESC_TXT OPTIONS(description="Printed Invoice Sequence Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYINVSEQ_YTSD_INVSEQ_TEXT     AS     PRINTED_INVOICE_SEQ_ID
,YYINVSEQ_DES_YTSD_INVSEQ_TEXT     AS     PRINTED_INVOICE_SEQ_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRINTED_INVOICE_SEQUENCE_CV`;