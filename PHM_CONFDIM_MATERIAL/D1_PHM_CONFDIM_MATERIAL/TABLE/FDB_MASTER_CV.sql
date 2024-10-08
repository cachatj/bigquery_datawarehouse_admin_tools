CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_MASTER_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  LBLRID STRING OPTIONS(description="Labeler Identifier"),
  GCN_SEQNO STRING OPTIONS(description="Clinical Formulation ID (Stable ID)"),
  PS STRING OPTIONS(description="Package Size"),
  DF STRING OPTIONS(description="Drug Form Code"),
  AD STRING OPTIONS(description="Additional Descriptor"),
  LN STRING OPTIONS(description="Label Name"),
  BN STRING OPTIONS(description="Brand Name"),
  PNDC STRING OPTIONS(description="Previous National Drug Code"),
  REPNDC STRING OPTIONS(description="Replacement National Drug Code"),
  NDCFI STRING OPTIONS(description="NDC Format Indicator"),
  DADDNC STRING OPTIONS(description="Date of Add—NDC"),
  DUPDC STRING OPTIONS(description="Date of Update—NDC"),
  DESI STRING OPTIONS(description="DESI Drug Indicator"),
  DESDTEC STRING OPTIONS(description="DESI Status Change Effective Date"),
  DESI2 STRING OPTIONS(description="DESI2 Drug Indicator"),
  DES2DTEC STRING OPTIONS(description="DESI2 Status Change Effective Date"),
  DEA STRING OPTIONS(description="Drug Enforcement Administration Code"),
  CL STRING OPTIONS(description="Drug Class"),
  GPI STRING OPTIONS(description="This column is not currently being used."),
  HOSP STRING OPTIONS(description="Hospital Selection Indicator"),
  INNOV STRING OPTIONS(description="Innovator Indicator"),
  IPI STRING OPTIONS(description="Institutional Product Indicator"),
  MINI STRING OPTIONS(description="Mini Selection Indicator"),
  MAINT STRING OPTIONS(description="Maintenance Drug Indicator"),
  OBC STRING OPTIONS(description="Orange Book Code"),
  OBSDTEC STRING OPTIONS(description="Obsolete Date"),
  PPI STRING OPTIONS(description="Patient Package Insert Indicator"),
  STPK STRING OPTIONS(description="Standard Package Indicator"),
  REPACK STRING OPTIONS(description="Repackaged Indicator"),
  TOP200 STRING OPTIONS(description="Top 200 Drugs Indicator"),
  UD STRING OPTIONS(description="Unit Dose Indicator"),
  CSP STRING OPTIONS(description="Case Pack"),
  NDL_GDGE STRING OPTIONS(description="Needle Gauge"),
  NDL_LNGTH STRING OPTIONS(description="Needle Length"),
  SYR_CPCTY STRING OPTIONS(description="Syringe Capacity"),
  SHLF_PCK STRING OPTIONS(description="Shelf Pack"),
  SHIPPER STRING OPTIONS(description="Shipper Quantity"),
  HCFA_FDA STRING OPTIONS(description="HCFA FDA Therapeutic Equivalency Code"),
  HCFA_UNIT STRING OPTIONS(description="HCFA Unit Indicator"),
  HCFA_PS STRING OPTIONS(description="HCFA Units Per Package"),
  HCFA_APPC STRING OPTIONS(description="HCFA FDA Approval Date"),
  HCFA_MRKC STRING OPTIONS(description="HCFA Market Entry Date"),
  HCFA_TRMC STRING OPTIONS(description="HCFA Termination Date"),
  HCFA_TYP STRING OPTIONS(description="HCFA Drug Type Code"),
  HCFA_DESC1 STRING OPTIONS(description="HCFA DESI Effective Date"),
  HCFA_DESI1 STRING OPTIONS(description="HCFA DESI Code"),
  UU STRING OPTIONS(description="Unit of Use Indicator"),
  PD STRING OPTIONS(description="Package Description"),
  LN25 STRING OPTIONS(description="This column is not currently being used."),
  LN25I STRING OPTIONS(description="Label Name - 25/Generic Name Use Indicator"),
  GPIDC STRING OPTIONS(description="This column is not currently being used."),
  BBDC STRING OPTIONS(description="This column is not currently being used."),
  HOME STRING OPTIONS(description="Home Health Selection Indicator"),
  INPCKI STRING OPTIONS(description="Inner Package Indicator"),
  OUTPCKI STRING OPTIONS(description="Outer Package Indicator"),
  OBC_EXP STRING OPTIONS(description="Expanded Orange Book Code"),
  PS_EQUIV STRING OPTIONS(description="Package Size Equivalent Value"),
  PLBLR STRING OPTIONS(description="Private Labeler Indicator"),
  TOP50GEN STRING OPTIONS(description="Top 50 Generics"),
  GMI STRING OPTIONS(description="Generic Manufacturer Indicator"),
  GNI STRING OPTIONS(description="Generic Name Indicator"),
  GSI STRING OPTIONS(description="This column is not currently being used."),
  GTI STRING OPTIONS(description="Therapeutic Equivalence Indicator"),
  NDCGI1 STRING OPTIONS(description="Multi-Source/Single Source Indicator (NDC-Level)"),
  HCFA_DC STRING OPTIONS(description="HCFA Drug Category"),
  LN60 STRING OPTIONS(description="Label Name - 60"),
  HICL_SEQNO STRING OPTIONS(description="Ingredient List Identifier (formerly the Hierarchical Ingredient Code List Sequence Number) (Stable ID)"),
  GCDF STRING OPTIONS(description="Dosage Form Code (2-character)"),
  GCRT STRING OPTIONS(description="Route of Administration Code (1-character)"),
  STR STRING OPTIONS(description="Drug Strength Description"),
  GTC STRING OPTIONS(description="Therapeutic Class Code, Generic"),
  TC STRING OPTIONS(description="Therapeutic Class Code, Standard"),
  DCC STRING OPTIONS(description="Drug Category Code"),
  GCNSEQ_GI STRING OPTIONS(description="GCN_SEQNO-Level Multi-Source/Single Source Indicator"),
  GENDER STRING OPTIONS(description="Gender-Specific Drug Indicator"),
  HIC3_SEQN STRING OPTIONS(description="Hierarchical Specific Therapeutic Class Code Sequence Number (Stable ID)"),
  STR60 STRING OPTIONS(description="Drug Strength Description - 60"),
  RT STRING OPTIONS(description="Route Description"),
  GCRT2 STRING OPTIONS(description="Route of Administration Code (2-character)"),
  GCRT_DESC STRING OPTIONS(description="Route Code Interpretation"),
  SYSTEMIC STRING OPTIONS(description="Systemic Route Indicator"),
  MFG STRING OPTIONS(description="Manufacturer Name"),
  LBLRIND STRING OPTIONS(description="Labeler Indicator Code"),
  COLOR_TXT STRING OPTIONS(description="FDB Color"),
  SHAPE_TXT STRING OPTIONS(description="FDB Shape"),
  FLAVOR_TXT STRING OPTIONS(description="FDB Flavor"),
  CHG_FLAG STRING OPTIONS(description="Change Flag"),
  NDA_IND STRING OPTIONS(description="NDA Status Indicator"),
  ANDA_IND STRING OPTIONS(description="ANDA Status Indicator"),
  LISTING_SEQ_NO STRING OPTIONS(description="FDA Listing Sequence Number"),
  TRADENAME STRING OPTIONS(description="FDA Trade Name"),
  DOSE STRING OPTIONS(description="Dosage Form Description"),
  GCDF_DESC STRING OPTIONS(description="Dosage Form Code Description"),
  STRNUM STRING OPTIONS(description="Drug Strength Number"),
  VOLNUM STRING OPTIONS(description="Drug Strength Volume Number"),
  STRUN50 STRING OPTIONS(description="Drug Strength Units - 50"),
  VOLUN50 STRING OPTIONS(description="Drug Strength Volume Units - 50"),
  OBC_GCN STRING OPTIONS(description="Formulation ID"),
  GNN STRING OPTIONS(description="Generic Name - Short Version"),
  GNN60 STRING OPTIONS(description="Generic Name - Long Version"),
  AHFS8 STRING OPTIONS(description="Therapeutic Class, AHFS"),
  AHFS_DESC STRING OPTIONS(description="American Hospital Formulary Service - Code Description"),
  GCN STRING OPTIONS(description="Formulation ID"),
  DCC_DESC STRING OPTIONS(description="Drug Category Code Description"),
  GTC_DESC STRING OPTIONS(description="Generic Therapeutic Class Description"),
  HIC3 STRING OPTIONS(description="Hierarchical Specific Therapeutic Class Code"),
  HIC3_DESC STRING OPTIONS(description="Hierarchical Specific Therapeutic Class Code Description"),
  HIC3_GRPN STRING OPTIONS(description="Hierarchical Specific Therapeutic Class Code Group ID"),
  HIC3_ROOT STRING OPTIONS(description="Hierarchical Specific Therapeutic Class Parent HIC2 Sequence Number"),
  OBC_SN STRING OPTIONS(description="Orange Book Text Sequence Number"),
  OBC_DESC STRING OPTIONS(description="Orange Book Code Description"),
  MEDID STRING OPTIONS(description="MED Medication ID (Stable ID)"),
  TM_SOURCE_ID STRING OPTIONS(description="TM Editorial Source ID"),
  TM_IND STRING OPTIONS(description="TM Indicator"),
  TM_ALT_MEDID_DESC STRING OPTIONS(description="TM Altered Medication Description"),
  TC_DESC STRING OPTIONS(description="Standard Therapeutic Class Description"),
  EFFECT_DTE STRING OPTIONS(description="AMP Price Effective Dae"),
  AMP_PRICE STRING OPTIONS(description="AMP Price"),
  HCPC STRING OPTIONS(description="HCFA Common Procedure Code"),
  HCPC_PBC1 STRING OPTIONS(description="HCPC Part B/NOC Price Effective Date"),
  HCPC_PBP1 STRING OPTIONS(description="HCPC Part B/NOC Price"),
  HCPC_PBC2 STRING OPTIONS(description="HCPC Part B/NOC Previous Price Effective Date"),
  HCPC_PBP2 STRING OPTIONS(description="HCPC Part B/NOC Previous Price"),
  HCPC_PBC3 STRING OPTIONS(description="HCPC Part B/NOC 2nd Previous Price Effective Date"),
  HCPC_PBP3 STRING OPTIONS(description="HCPC Part B/NOC 2nd Previous Price"),
  HCPC_PBC4 STRING OPTIONS(description="HCPC Part B/NOC 3rd Previous Price Effective Date"),
  HCPC_PBP4 STRING OPTIONS(description="HCPC Part B/NOC 3rd Previous Price"),
  HCPC_PBC5 STRING OPTIONS(description="HCPC Part B/NOC 4th Previous Price Effective Date"),
  HCPC_PBP5 STRING OPTIONS(description="HCPC Part B/NOC 4th Previous Price"),
  HCPC_PBC6 STRING OPTIONS(description="HCPC Part B/NOC 5th Previous Price Effective Date"),
  HCPC_PBP6 STRING OPTIONS(description="HCPC Part B/NOC 5th Previous Price"),
  HCPC_PBC7 STRING OPTIONS(description="HCPC Part B/NOC 6th Previous Price Effective Date"),
  HCPC_PBP7 STRING OPTIONS(description="HCPC Part B/NOC 6th Previous Price"),
  HCPC_PBC8 STRING OPTIONS(description="HCPC Part B/NOC 7th Previous Price Effective Date"),
  HCPC_PBP8 STRING OPTIONS(description="HCPC Part B/NOC 7th Previous Price"),
  MCR_BCDESC STRING OPTIONS(description="Medicare Billing Code Description"),
  OBC3 STRING OPTIONS(description="Orange Book Code; Three-byte Version"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC;