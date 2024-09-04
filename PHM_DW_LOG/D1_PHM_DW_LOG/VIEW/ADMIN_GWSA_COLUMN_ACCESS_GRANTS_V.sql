CREATE VIEW `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_GWSA_COLUMN_ACCESS_GRANTS_V`
AS SELECT
  ADMIN_ENTITY_INVENTORY_ENTITY_SCHEMA,
  ADMIN_ENTITY_INVENTORY_ENTITY_NAME,
  D1_COLUMN_NAME,
  VW_COLUMN_NAME,
  REGEXP_REPLACE(
    CONCAT(
      'required_access_grants: [',
      IF(GwsaPubRedOak = 'Y', 'gwsapubredoak,', ''),
      IF(GwsaPubOosSrc = 'Y', 'gwsapuboossrc,', ''),
      IF(GwsaPubPricing = 'Y', 'gwsapubpricing,', ''),
      IF(GwsaPublished = 'Y', 'gwsapublished,', ''),
      IF(GwsaOperational = 'Y', 'gwsaoperational,', ''),
      ']'
    ),
    r',\]',
    ']'
  ) AS required_access_grants,

FROM
  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_GWSA_COLUMN_SECURITY_CV`;