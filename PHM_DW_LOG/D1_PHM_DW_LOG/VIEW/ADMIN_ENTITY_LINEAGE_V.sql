CREATE VIEW `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_LINEAGE_V`
AS WITH RawData AS (
    SELECT
        dataset_id AS ReferencedDatasetID,
        table_id AS ReferencedTableID,
        admin_jobs_t.destination_table.dataset_id AS DestinationDatasetID,
        admin_jobs_t.destination_table.table_id AS DestinationTableID,
        COUNT(DISTINCT admin_jobs_t.job_id) AS JobCount
    FROM
        `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_T` AS admin_jobs_t
        LEFT JOIN UNNEST(admin_jobs_t.referenced_tables) AS ReferencedTables
    WHERE 1=1
        AND table_id IS NOT NULL
    GROUP BY
        1, 2, 3, 4
    ORDER BY
        3 DESC
),
D0Data AS (
    SELECT *
    FROM RawData
    WHERE ReferencedDatasetID LIKE '%D0%'
),
D1Data AS (
    SELECT *
    FROM RawData
    WHERE ReferencedDatasetID LIKE '%D1%'
),
VWData AS (
    SELECT *
    FROM RawData
    WHERE DestinationDatasetID LIKE '%VW%'
)
SELECT DISTINCT
    D0Data.ReferencedDatasetID AS D0_Dataset,
    D0Data.ReferencedTableID AS D0_Entity,
    REPLACE(D0Data.DestinationDatasetID, "D1_", "VI2_") AS VI2_Dataset,
    CONCAT(REPLACE(REPLACE(D0Data.ReferencedDatasetID,"D0_",""),"_NP",""), "__", D0Data.ReferencedTableID) AS VI2_Entity,
    D0Data.DestinationDatasetID AS D1_Dataset,
    D0Data.DestinationTableID AS D1_Entity,
    VWData.DestinationDatasetID AS VW_Dataset,
    VWData.DestinationTableID AS VW_Entity
FROM
    D0Data
    LEFT JOIN D1Data ON D0Data.DestinationTableID = D1Data.DestinationTableID
    LEFT JOIN VWData ON D0Data.DestinationTableID = VWData.ReferencedTableID;