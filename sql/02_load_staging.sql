-- =============================================================================
-- LOAD STAGING (Bronze Layer)
-- Loads CMS DE-SynPUF Sample 1 CSVs into staging tables
-- Run from psql: psql -d healthcare_dw -f sql/02_load_staging.sql
-- =============================================================================

\echo 'Loading beneficiary_2008...'
TRUNCATE TABLE staging.beneficiary_2008;
COPY staging.beneficiary_2008
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading beneficiary_2009...'
TRUNCATE TABLE staging.beneficiary_2009;
COPY staging.beneficiary_2009
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2009_Beneficiary_Summary_File_Sample_1.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading beneficiary_2010...'
TRUNCATE TABLE staging.beneficiary_2010;
COPY staging.beneficiary_2010
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2010_Beneficiary_Summary_File_Sample_1.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading inpatient_claims...'
TRUNCATE TABLE staging.inpatient_claims;
COPY staging.inpatient_claims
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading outpatient_claims...'
TRUNCATE TABLE staging.outpatient_claims;
COPY staging.outpatient_claims
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading carrier_claims (segment A)...'
TRUNCATE TABLE staging.carrier_claims;
COPY staging.carrier_claims
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading carrier_claims (segment B)...'
COPY staging.carrier_claims
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading prescription_drug_events...'
TRUNCATE TABLE staging.prescription_drug_events;
COPY staging.prescription_drug_events
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/synpuf/sample_01/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv'
WITH (FORMAT csv, HEADER true);

\echo 'Loading kaggle_encounters...'
TRUNCATE TABLE staging.kaggle_encounters;
COPY staging.kaggle_encounters
FROM '/Users/zepyoorkhechadoorian/Documents/projects/healthcare-data-warehouse/data/kaggle/healthcare_dataset.csv'
WITH (FORMAT csv, HEADER true);

-- =============================================================================
-- Row count sanity check
-- =============================================================================
\echo ''
\echo 'Row counts:'
SELECT 'beneficiary_2008'        AS table_name, COUNT(*) AS rows FROM staging.beneficiary_2008
UNION ALL -- duplicates are not removed and/or hidden
SELECT 'beneficiary_2009',                       COUNT(*) FROM staging.beneficiary_2009
UNION ALL
SELECT 'beneficiary_2010',                       COUNT(*) FROM staging.beneficiary_2010
UNION ALL
SELECT 'inpatient_claims',                       COUNT(*) FROM staging.inpatient_claims
UNION ALL
SELECT 'outpatient_claims',                      COUNT(*) FROM staging.outpatient_claims
UNION ALL
SELECT 'carrier_claims',                         COUNT(*) FROM staging.carrier_claims
UNION ALL
SELECT 'prescription_drug_events',               COUNT(*) FROM staging.prescription_drug_events
UNION ALL
SELECT 'kaggle_encounters',                      COUNT(*) FROM staging.kaggle_encounters
ORDER BY table_name;