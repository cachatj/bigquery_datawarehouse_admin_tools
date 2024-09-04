CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CUSTOMER_COMPANY_CODE_CV`(
  CUST_NUM OPTIONS(description="Customer Number"),
  COMPANY_CDE OPTIONS(description="Company Code"),
  PRSNL_NUM OPTIONS(description="Personnel Number"),
  CREATED_DTE OPTIONS(description="Date on which the Record Was Created"),
  PERSON_NAME OPTIONS(description="Name of Person who Created the Object"),
  COMPANY_PSTNG_BLCK_CDE OPTIONS(description="Posting block for company code"),
  MSTR_REC_DEL_FLG OPTIONS(description="Deletion Flag for Master Record (Company Code Level)"),
  ASGNMT_KEY_NUM OPTIONS(description="Key for sorting according to assignment numbers"),
  ACCT_CLERK_NUM OPTIONS(description="Accounting clerk"),
  RCNCL_ACC_NUM OPTIONS(description="Reconciliation Account in General Ledger"),
  AUTH_GROUP_ID OPTIONS(description="Authorization Group"),
  HEAD_OFFICE_ACC_NUM OPTIONS(description="Head office account number"),
  ALT_PAYER_ACC_NUM OPTIONS(description="Account number of an alternative payer"),
  CUSTOMER_PYMT_NTCE_FLG OPTIONS(description="Customer payment notice Indicator"),
  SALE_DEPT_PYMT_NTCE_FLG OPTIONS(description="sales department payment notice Indicator"),
  LGL_DEPT_PYMT_NTCE_FLG OPTIONS(description="legal department payment notice Indicator"),
  ACCT_DEPT_PYMT_NTCE_FLG OPTIONS(description="accounting department Payment notice Indicator"),
  CUSTOMER_PYMT_NTCE_WO_CLR_ITEMS_FLG OPTIONS(description="customer payment notice Indicator without cleared items"),
  PYMT_MTHD_ID OPTIONS(description="List of the Payment Methods to be Considered"),
  VENDOR_CUSTOMER_CLR_FLG OPTIONS(description="Indicator: Clearing between customer and vendor ?"),
  PYMT_BLCK_KEY_NUM OPTIONS(description="Block Key for Payment"),
  PYMT_TERM_ID OPTIONS(description="Terms of Payment Key"),
  BILL_PYMT_TERM_ID OPTIONS(description="Terms of payment key for bill of exchange charges"),
  INTRST_CALC_FLG OPTIONS(description="Interest calculation indicator"),
  LAST_INTRST_CALC_DTE OPTIONS(description="Key date of the last interest calculation"),
  INTRST_CALC_MNTH_COUNT OPTIONS(description="Interest calculation frequency in months"),
  CUSTOMER_ACCT_NUM OPTIONS(description="Our account number at customer"),
  CUSTOMER_USER_NAME OPTIONS(description="User at customer"),
  MEMO_ID OPTIONS(description="Memo"),
  PLAN_GROUP_ID OPTIONS(description="Planning group"),
  EXP_CREDIT_INSR_INST_NUM OPTIONS(description="Export credit insurance institution number"),
  INSR_AMOUNT OPTIONS(description="Amount Insured"),
  INSR_LEAD_MTH_QTY OPTIONS(description="Insurance lead months"),
  DEDUCT_PCT OPTIONS(description="Deductible percentage rate"),
  INSR_NUM OPTIONS(description="Insurance number"),
  INSR_VALID_DTE OPTIONS(description="Insurance validity date"),
  CLCTV_INVOICE_VRNT_ID OPTIONS(description="Collective invoice variant"),
  LOCAL_PROCESS_FLG OPTIONS(description="Local processing Indicator"),
  PRDC_ACCT_STMNT_FLG OPTIONS(description="periodic account statements Indicator"),
  EXCHG_BILL_AMT OPTIONS(description="Bill of exchange limit (in local currency)"),
  PYE_ID OPTIONS(description="Next payee"),
  LAST_INTRST_CALC_RUN_DTE OPTIONS(description="Date of the last interest calculation run"),
  REC_PYMT_HIST_FLG OPTIONS(description="Record Payment History Indicator"),
  BSNS_PTNR_TLRNC_GRP_ID OPTIONS(description="Tolerance group for the business partner/G/L account"),
  PRBL_TIM_QTY OPTIONS(description="Probable time until check is paid"),
  HOUSE_BNK_SHRT_KEY_NUM OPTIONS(description="Short Key for a House Bank"),
  PAY_ALL_ITEMS_FLG OPTIONS(description="Pay all items separately Indicator"),
  REDUCTION_RATE_FLG OPTIONS(description="Subsidy indicator for determining the reduction rates"),
  PREV_MSTR_REC_NUM OPTIONS(description="Previous Master Record Number"),
  PYMT_GRP_KEY_NUM OPTIONS(description="Key for Payment Grouping"),
  NEG_LEAVE_SHRT_KEY_NUM OPTIONS(description="Short Key for Known/Negotiated Leave"),
  DNG_GRP_KEY_NUM OPTIONS(description="Key for dunning notice grouping"),
  LOCKBOX_KEY_NUM OPTIONS(description="Key of the Lockbox to Which the Customer Is To Pay"),
  PYMT_MTHD_SPLMNT_ID OPTIONS(description="Payment Method Supplement"),
  BUY_GRP_ACCT_NUM OPTIONS(description="Account Number of Buying Group"),
  PYMT_ADVS_ID OPTIONS(description="Selection Rule for Payment Advices"),
  EDI_PYMT_ADVS_FLG OPTIONS(description="EDI Send Payment Advices Indicator"),
  APRV_GRP_NUM OPTIONS(description="Release Approval Group"),
  REASON_CDE OPTIONS(description="Reason Code Conversion Version"),
  ACCT_CLERK_FAX_NUM OPTIONS(description="Accounting clerk's fax number at the customer/vendor"),
  PRTNR_COMP_CLERK_ADDRESS_TEXT OPTIONS(description="Internet address of partner company clerk"),
  ALT_PAYER_ACC_NUM_FLG OPTIONS(description="Alternative payer using account number Indicator"),
  PYMT_TERM_CREDIT_ID OPTIONS(description="Payment Terms Key for Credit Memos"),
  GROSS_INCOME_TAX_CDE OPTIONS(description="Activity Code for Gross Income Tax"),
  EMPLOYMENT_TAX_DIST_CDE OPTIONS(description="Distribution Type for Employment Tax"),
  VALUE_ADJ_KEY_NUM OPTIONS(description="Value Adjustment Key"),
  CHG_ATHRZ_STATUS_CDE OPTIONS(description="Status of Change Authorization (Company Code Level)"),
  CHG_CONFIRM_DTE OPTIONS(description="Date on Which the Changes Were Confirmed"),
  LAST_CHG_CONFIRM_DTE OPTIONS(description="Time of Last Change Confirmation"),
  DEL_BLCK_ID OPTIONS(description="Deletion bock for master record (company code level)"),
  ACCT_CLERK_PHONE_NUM OPTIONS(description="Accounting clerk's telephone number at business partner"),
  ACCT_RCVBL_FLG OPTIONS(description="Accounts Receivable Pledging Indicator"),
  XML_PYMT_ADV_FLG OPTIONS(description="XML Send Payment Advice Indicator"),
  EMAIL_ADDR_ID OPTIONS(description="E-Mail Address for Avis: Hash Value"),
  WTHLD_TAX_CNTRY_KEY_NUM OPTIONS(description="Withholding Tax Country Key"),
  BSNS_PRP_CMPLT_FLG OPTIONS(description="Business Purpose Completed Flag"),
  EXEC_CUSTOMER_ID OPTIONS(description="Customer is in execution"),
  STMNT_CDE OPTIONS(description="Statement code"),
  ROW_ADD_STP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Added User Id"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User Id")
)
AS SELECT
--MANDT_KNB1    AS    CLIENT_ID
KUNNR_KNB1    AS    CUST_NUM
,BUKRS_KNB1    AS    COMPANY_CDE
,PERNR_KNB1    AS    PRSNL_NUM
,ERDAT_KNB1    AS    CREATED_DTE
,ERNAM_KNB1    AS    PERSON_NAME
,SPERR_KNB1    AS    COMPANY_PSTNG_BLCK_CDE
,LOEVM_KNB1    AS    MSTR_REC_DEL_FLG
,ZUAWA_KNB1    AS    ASGNMT_KEY_NUM
,BUSAB_KNB1    AS    ACCT_CLERK_NUM
,AKONT_KNB1    AS    RCNCL_ACC_NUM
,BEGRU_KNB1    AS    AUTH_GROUP_ID
,KNRZE_KNB1    AS    HEAD_OFFICE_ACC_NUM
,KNRZB_KNB1    AS    ALT_PAYER_ACC_NUM
,ZAMIM_KNB1    AS    CUSTOMER_PYMT_NTCE_FLG
,ZAMIV_KNB1    AS    SALE_DEPT_PYMT_NTCE_FLG
,ZAMIR_KNB1    AS    LGL_DEPT_PYMT_NTCE_FLG
,ZAMIB_KNB1    AS    ACCT_DEPT_PYMT_NTCE_FLG
,ZAMIO_KNB1    AS    CUSTOMER_PYMT_NTCE_WO_CLR_ITEMS_FLG
,ZWELS_KNB1    AS    PYMT_MTHD_ID
,XVERR_KNB1    AS    VENDOR_CUSTOMER_CLR_FLG
,ZAHLS_KNB1    AS    PYMT_BLCK_KEY_NUM
,ZTERM_KNB1    AS    PYMT_TERM_ID
,WAKON_KNB1    AS    BILL_PYMT_TERM_ID
,VZSKZ_KNB1    AS    INTRST_CALC_FLG
,ZINDT_KNB1    AS    LAST_INTRST_CALC_DTE
,ZINRT_KNB1    AS    INTRST_CALC_MNTH_COUNT
,EIKTO_KNB1    AS    CUSTOMER_ACCT_NUM
,ZSABE_KNB1    AS    CUSTOMER_USER_NAME
,KVERM_KNB1    AS    MEMO_ID
,FDGRV_KNB1    AS    PLAN_GROUP_ID
,VRBKZ_KNB1    AS    EXP_CREDIT_INSR_INST_NUM
,VLIBB_KNB1    AS    INSR_AMOUNT
,VRSZL_KNB1    AS    INSR_LEAD_MTH_QTY
,VRSPR_KNB1    AS    DEDUCT_PCT
,VRSNR_KNB1    AS    INSR_NUM
,VERDT_KNB1    AS    INSR_VALID_DTE
,PERKZ_KNB1    AS    CLCTV_INVOICE_VRNT_ID
,XDEZV_KNB1    AS    LOCAL_PROCESS_FLG
,XAUSZ_KNB1    AS    PRDC_ACCT_STMNT_FLG
,WEBTR_KNB1    AS    EXCHG_BILL_AMT
,REMIT_KNB1    AS    PYE_ID
,DATLZ_KNB1    AS    LAST_INTRST_CALC_RUN_DTE
,XZVER_KNB1    AS    REC_PYMT_HIST_FLG
,TOGRU_KNB1    AS    BSNS_PTNR_TLRNC_GRP_ID
,KULTG_KNB1    AS    PRBL_TIM_QTY
,HBKID_KNB1    AS    HOUSE_BNK_SHRT_KEY_NUM
,XPORE_KNB1    AS    PAY_ALL_ITEMS_FLG
,BLNKZ_KNB1    AS    REDUCTION_RATE_FLG
,ALTKN_KNB1    AS    PREV_MSTR_REC_NUM
,ZGRUP_KNB1    AS    PYMT_GRP_KEY_NUM
,URLID_KNB1    AS    NEG_LEAVE_SHRT_KEY_NUM
,MGRUP_KNB1    AS    DNG_GRP_KEY_NUM
,LOCKB_KNB1    AS    LOCKBOX_KEY_NUM
,UZAWE_KNB1    AS    PYMT_MTHD_SPLMNT_ID
,EKVBD_KNB1    AS    BUY_GRP_ACCT_NUM
,SREGL_KNB1    AS    PYMT_ADVS_ID
,XEDIP_KNB1    AS    EDI_PYMT_ADVS_FLG
,FRGRP_KNB1    AS    APRV_GRP_NUM
,VRSDG_KNB1    AS    REASON_CDE
,TLFXS_KNB1    AS     ACCT_CLERK_FAX_NUM
,INTAD_KNB1    AS    PRTNR_COMP_CLERK_ADDRESS_TEXT
,XKNZB_KNB1    AS    ALT_PAYER_ACC_NUM_FLG
,GUZTE_KNB1    AS    PYMT_TERM_CREDIT_ID
,GRICD_KNB1    AS    GROSS_INCOME_TAX_CDE
,GRIDT_KNB1    AS    EMPLOYMENT_TAX_DIST_CDE
,WBRSL_KNB1    AS    VALUE_ADJ_KEY_NUM
,CONFS_KNB1    AS    CHG_ATHRZ_STATUS_CDE
,UPDAT_KNB1    AS    CHG_CONFIRM_DTE
,UPTIM_KNB1    AS    LAST_CHG_CONFIRM_DTE
,NODEL_KNB1    AS    DEL_BLCK_ID
,TLFNS_KNB1    AS    ACCT_CLERK_PHONE_NUM
,CESSION_KZ_KNB1    	AS 		ACCT_RCVBL_FLG
,AVSND_KNB1          AS 		XML_PYMT_ADV_FLG
,AD_HASH_KNB1        AS 		EMAIL_ADDR_ID
,QLAND_KNB1          AS 		WTHLD_TAX_CNTRY_KEY_NUM
,CVP_XBLCK_B_KNB1    AS 		BSNS_PRP_CMPLT_FLG
,GMVKZD_KNB1         AS 		EXEC_CUSTOMER_ID
,YY_STMD_IND_KNB1    AS 		STMNT_CDE
,ROW_ADD_STP 		AS 		ROW_ADD_STP
,ROW_ADD_USER_ID 	AS 		ROW_ADD_USER_ID
,ROW_UPDATE_STP  	AS 		ROW_UPDATE_STP
,ROW_UPDATE_USER_ID  AS 		ROW_UPDATE_USER_ID 	FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_COMPANY_CODE_CV`;