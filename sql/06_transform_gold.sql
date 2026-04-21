-- =============================================================================
-- TRANSFORM GOLD LAYER
-- Run from psql: psql -d healthcare_dw -f sql/06_transform_gold.sql
--
-- Populates gold dimensional model from silver layer.
-- Prerequisites: 05_create_gold.sql must be run first.
--
-- This script is idempotent: TRUNCATE before each insert.
-- =============================================================================

-- =============================================================================
-- 1. Populate dim_time (2008-2010 calendar)
-- =============================================================================

TRUNCATE TABLE gold.dim_time CASCADE;

INSERT INTO gold.dim_time (full_date, year, month, quarter, day_of_week, week_of_year)
SELECT
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    CEIL(EXTRACT(MONTH FROM d) / 3.0)::SMALLINT AS quarter,
    EXTRACT(DOW FROM d)::SMALLINT AS day_of_week,
    EXTRACT(WEEK FROM d)::SMALLINT AS week_of_year
FROM (
    SELECT generate_series('2008-01-01'::DATE, '2010-12-31'::DATE, '1 day'::INTERVAL) AS d
) dates;

-- =============================================================================
-- 2. Populate dim_beneficiary
-- =============================================================================

TRUNCATE TABLE gold.dim_beneficiary CASCADE;

INSERT INTO gold.dim_beneficiary (
    beneficiary_id, date_of_birth, age_2008, gender, race, state_code, county_code,
    has_diabetes, has_heart_failure, has_copd, has_ami, has_stroke, has_cancer,
    has_hypertension, has_hyperlipidemia, has_depression, has_ckd, chronic_condition_count
)
SELECT
    b.desynpuf_id AS beneficiary_id,
    b.bene_birth_dt AS date_of_birth,
    EXTRACT(YEAR FROM AGE('2008-01-01'::DATE, b.bene_birth_dt))::SMALLINT AS age_2008,
    b.bene_sex AS gender,
    b.bene_race AS race,
    b.sp_state_code::CHAR(2) AS state_code,
    b.bene_county_cd::CHAR(3) AS county_code,
    
    -- chronic condition flags (TRUE if present in any year)
    (MAX(COALESCE(b.sp_diabetes::INTEGER, 0)) = 1) AS has_diabetes,
    (MAX(COALESCE(b.sp_chf::INTEGER, 0)) = 1) AS has_heart_failure,
    (MAX(COALESCE(b.sp_copd::INTEGER, 0)) = 1) AS has_copd,
    (MAX(COALESCE(b.sp_ischmcht::INTEGER, 0)) = 1) AS has_ami,
    (MAX(COALESCE(b.sp_strketia::INTEGER, 0)) = 1) AS has_stroke,
    (MAX(COALESCE(b.sp_cncr::INTEGER, 0)) = 1) AS has_cancer,
    NULL AS has_hypertension,  -- Not in beneficiary table
    NULL AS has_hyperlipidemia,  -- Not in beneficiary table
    (MAX(COALESCE(b.sp_depressn::INTEGER, 0)) = 1) AS has_depression,
    (MAX(COALESCE(b.sp_chrnkidn::INTEGER, 0)) = 1) AS has_ckd,
    
    -- count of chronic conditions
    SUM(CASE
        WHEN COALESCE(b.sp_diabetes::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_chf::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_copd::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_ischmcht::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_strketia::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_cncr::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_depressn::INTEGER, 0) = 1 THEN 1
        WHEN COALESCE(b.sp_chrnkidn::INTEGER, 0) = 1 THEN 1
        ELSE 0
    END)::SMALLINT AS chronic_condition_count
    
FROM silver.beneficiary b
GROUP BY b.desynpuf_id, b.bene_birth_dt, b.bene_sex, b.bene_race, b.sp_state_code, b.bene_county_cd;

-- =============================================================================
-- 3. Populate dim_provider
-- =============================================================================

TRUNCATE TABLE gold.dim_provider CASCADE;

INSERT INTO gold.dim_provider (provider_id, provider_type)
SELECT DISTINCT ON (provider_id) provider_id, provider_type
FROM (
    SELECT DISTINCT prvdr_num AS provider_id, 'inpatient' AS provider_type
    FROM silver.inpatient_claims WHERE prvdr_num IS NOT NULL
    UNION ALL
    SELECT DISTINCT prvdr_num, 'outpatient'
    FROM silver.outpatient_claims WHERE prvdr_num IS NOT NULL
    UNION ALL
    SELECT DISTINCT prf_physn_npi_1, 'carrier'
    FROM silver.carrier_claims WHERE prf_physn_npi_1 IS NOT NULL
) all_providers
ORDER BY provider_id, provider_type;

-- =============================================================================
-- 4. Populate dim_diagnosis (sparse; only codes that appear in claims)
-- =============================================================================

TRUNCATE TABLE gold.dim_diagnosis CASCADE;

WITH all_diagnosis_codes AS (
    SELECT icd9_dgns_cd_1 AS icd9_code FROM silver.inpatient_claims WHERE icd9_dgns_cd_1 IS NOT NULL
    UNION SELECT icd9_dgns_cd_2 FROM silver.inpatient_claims WHERE icd9_dgns_cd_2 IS NOT NULL
    UNION SELECT icd9_dgns_cd_1 FROM silver.outpatient_claims WHERE icd9_dgns_cd_1 IS NOT NULL
    UNION SELECT icd9_dgns_cd_2 FROM silver.outpatient_claims WHERE icd9_dgns_cd_2 IS NOT NULL
    UNION SELECT icd9_dgns_cd_1 FROM silver.carrier_claims WHERE icd9_dgns_cd_1 IS NOT NULL
    UNION SELECT icd9_dgns_cd_2 FROM silver.carrier_claims WHERE icd9_dgns_cd_2 IS NOT NULL
)
INSERT INTO gold.dim_diagnosis (icd9_code, description, category)
SELECT
    icd9_code,
    NULL AS description,
    NULL AS category
FROM all_diagnosis_codes
WHERE icd9_code IS NOT NULL;

-- =============================================================================
-- 5. Populate fct_claims (union of inpatient, outpatient, carrier)
-- =============================================================================

TRUNCATE TABLE gold.fct_claims CASCADE;

-- Inpatient claims
INSERT INTO gold.fct_claims (
    beneficiary_key, provider_key, service_date_key,
    claim_id, claim_type, service_date, claim_from_date, claim_thru_date,
    primary_diagnosis_code, secondary_diagnosis_code, procedure_code_1, procedure_code_2,
    days_of_stay, total_charge_amount, total_allowed_amount, total_payment_amount
)
SELECT
    db.beneficiary_key,
    dp.provider_key,
    dt.time_key,
    ic.clm_id,
    'inpatient' AS claim_type,
    ic.clm_from_dt,
    ic.clm_from_dt,
    ic.clm_thru_dt,
    ic.icd9_dgns_cd_1,
    ic.icd9_dgns_cd_2,
    ic.icd9_prcdr_cd_1,
    ic.icd9_prcdr_cd_2,
    ic.clm_utlztn_day_cnt,
    ic.clm_pmt_amt,
    ic.clm_pmt_amt,
    ic.clm_pmt_amt
FROM silver.inpatient_claims ic
JOIN gold.dim_beneficiary db ON ic.desynpuf_id = db.beneficiary_id
LEFT JOIN gold.dim_provider dp ON ic.prvdr_num = dp.provider_id AND dp.provider_type = 'inpatient'
LEFT JOIN gold.dim_time dt ON ic.clm_from_dt = dt.full_date;

-- Outpatient claims
INSERT INTO gold.fct_claims (
    beneficiary_key, provider_key, service_date_key,
    claim_id, claim_type, service_date, claim_from_date, claim_thru_date,
    primary_diagnosis_code, secondary_diagnosis_code, procedure_code_1, procedure_code_2,
    total_charge_amount, total_allowed_amount, total_payment_amount
)
SELECT
    db.beneficiary_key,
    dp.provider_key,
    dt.time_key,
    oc.clm_id,
    'outpatient' AS claim_type,
    oc.clm_from_dt,
    oc.clm_from_dt,
    oc.clm_thru_dt,
    oc.icd9_dgns_cd_1,
    oc.icd9_dgns_cd_2,
    oc.icd9_prcdr_cd_1,
    oc.icd9_prcdr_cd_2,
    oc.clm_pmt_amt,
    oc.clm_pmt_amt,
    oc.clm_pmt_amt
FROM silver.outpatient_claims oc
JOIN gold.dim_beneficiary db ON oc.desynpuf_id = db.beneficiary_id
LEFT JOIN gold.dim_provider dp ON oc.prvdr_num = dp.provider_id AND dp.provider_type = 'outpatient'
LEFT JOIN gold.dim_time dt ON oc.clm_from_dt = dt.full_date;

-- Carrier claims
INSERT INTO gold.fct_claims (
    beneficiary_key, provider_key, service_date_key,
    claim_id, claim_type, service_date, claim_from_date, claim_thru_date,
    primary_diagnosis_code, secondary_diagnosis_code, procedure_code_1,
    total_charge_amount, total_allowed_amount, total_payment_amount, beneficiary_responsibility
)
SELECT
    db.beneficiary_key,
    dp.provider_key,
    dt.time_key,
    cc.clm_id,
    'carrier' AS claim_type,
    cc.clm_from_dt,
    cc.clm_from_dt,
    cc.clm_thru_dt,
    cc.icd9_dgns_cd_1,
    cc.icd9_dgns_cd_2,
    cc.hcpcs_cd_1,
    SUM(COALESCE(cc.line_alowd_chrg_amt_1, 0)),
    SUM(COALESCE(cc.line_alowd_chrg_amt_1, 0)),
    SUM(COALESCE(cc.line_nch_pmt_amt_1, 0)),
    SUM(COALESCE(cc.line_bene_ptb_ddctbl_amt_1, 0)) + SUM(COALESCE(cc.line_coinsrnc_amt_1, 0))
FROM silver.carrier_claims cc
JOIN gold.dim_beneficiary db ON cc.desynpuf_id = db.beneficiary_id
LEFT JOIN gold.dim_provider dp ON cc.prf_physn_npi_1 = dp.provider_id AND dp.provider_type = 'carrier'
LEFT JOIN gold.dim_time dt ON cc.clm_from_dt = dt.full_date
GROUP BY db.beneficiary_key, dp.provider_key, dt.time_key, cc.clm_id, cc.clm_from_dt, cc.clm_thru_dt,
         cc.icd9_dgns_cd_1, cc.icd9_dgns_cd_2, cc.hcpcs_cd_1;

-- =============================================================================
-- 6. Populate fct_prescription_events
-- =============================================================================

TRUNCATE TABLE gold.fct_prescription_events CASCADE;

INSERT INTO gold.fct_prescription_events (
    beneficiary_key, service_date_key,
    pde_id, ndc, drug_name, service_date, fill_date,
    quantity_dispensed, days_supply, total_cost_amount, patient_cost_amount
)
SELECT
    db.beneficiary_key,
    dt.time_key,
    pde.pde_id,
    pde.ndc,
    NULL AS drug_name,
    pde.srvc_dt,
    pde.srvc_dt,
    pde.qty_dispensed,
    pde.days_supply,
    pde.tot_rx_cst_amt,
    pde.ptnt_pay_amt
FROM silver.prescription_drug_events pde
JOIN gold.dim_beneficiary db ON pde.desynpuf_id = db.beneficiary_id
LEFT JOIN gold.dim_time dt ON pde.srvc_dt = dt.full_date;

-- =============================================================================
-- 7. Populate agg_beneficiary_year
-- =============================================================================

TRUNCATE TABLE gold.agg_beneficiary_year CASCADE;

INSERT INTO gold.agg_beneficiary_year (
    beneficiary_key, year,
    inpatient_claim_count, inpatient_total_cost, inpatient_total_paid, inpatient_days_of_stay,
    outpatient_claim_count, outpatient_total_cost, outpatient_total_paid,
    carrier_claim_count, carrier_total_cost, carrier_total_paid,
    total_claim_count, total_cost, total_paid,
    pde_count, pde_total_cost
)
SELECT
    COALESCE(fc.beneficiary_key, fpe.beneficiary_key) AS beneficiary_key,
    COALESCE(EXTRACT(YEAR FROM fc.service_date), EXTRACT(YEAR FROM fpe.service_date))::SMALLINT AS year,
    
    SUM(CASE WHEN fc.claim_type = 'inpatient' THEN 1 ELSE 0 END) AS inpatient_claim_count,
    SUM(CASE WHEN fc.claim_type = 'inpatient' THEN fc.total_charge_amount ELSE 0 END) AS inpatient_total_cost,
    SUM(CASE WHEN fc.claim_type = 'inpatient' THEN fc.total_payment_amount ELSE 0 END) AS inpatient_total_paid,
    SUM(CASE WHEN fc.claim_type = 'inpatient' THEN COALESCE(fc.days_of_stay, 0) ELSE 0 END) AS inpatient_days_of_stay,
    
    SUM(CASE WHEN fc.claim_type = 'outpatient' THEN 1 ELSE 0 END) AS outpatient_claim_count,
    SUM(CASE WHEN fc.claim_type = 'outpatient' THEN fc.total_charge_amount ELSE 0 END) AS outpatient_total_cost,
    SUM(CASE WHEN fc.claim_type = 'outpatient' THEN fc.total_payment_amount ELSE 0 END) AS outpatient_total_paid,
    
    SUM(CASE WHEN fc.claim_type = 'carrier' THEN 1 ELSE 0 END) AS carrier_claim_count,
    SUM(CASE WHEN fc.claim_type = 'carrier' THEN fc.total_charge_amount ELSE 0 END) AS carrier_total_cost,
    SUM(CASE WHEN fc.claim_type = 'carrier' THEN fc.total_payment_amount ELSE 0 END) AS carrier_total_paid,
    
    COUNT(DISTINCT fc.claim_key) AS total_claim_count,
    SUM(fc.total_charge_amount) AS total_cost,
    SUM(fc.total_payment_amount) AS total_paid,
    
    COUNT(DISTINCT fpe.pde_key) AS pde_count,
    SUM(fpe.total_cost_amount) AS pde_total_cost
    
FROM gold.dim_beneficiary db
LEFT JOIN gold.fct_claims fc ON db.beneficiary_key = fc.beneficiary_key
LEFT JOIN gold.fct_prescription_events fpe ON db.beneficiary_key = fpe.beneficiary_key
WHERE fc.service_date IS NOT NULL OR fpe.service_date IS NOT NULL
GROUP BY COALESCE(fc.beneficiary_key, fpe.beneficiary_key), year;

-- =============================================================================
-- 8. Populate agg_provider_year
-- =============================================================================

TRUNCATE TABLE gold.agg_provider_year CASCADE;

INSERT INTO gold.agg_provider_year (
    provider_key, year,
    claim_count, total_charge_amount, total_allowed_amount, total_paid_amount, unique_beneficiary_count
)
SELECT
    fc.provider_key,
    EXTRACT(YEAR FROM fc.service_date)::SMALLINT AS year,
    COUNT(DISTINCT fc.claim_key) AS claim_count,
    SUM(fc.total_charge_amount) AS total_charge_amount,
    SUM(fc.total_allowed_amount) AS total_allowed_amount,
    SUM(fc.total_payment_amount) AS total_paid_amount,
    COUNT(DISTINCT fc.beneficiary_key) AS unique_beneficiary_count
FROM gold.fct_claims fc
WHERE fc.provider_key IS NOT NULL AND fc.service_date IS NOT NULL
GROUP BY fc.provider_key, year;

-- =============================================================================
-- 9. Populate agg_medication_adherence
-- =============================================================================

TRUNCATE TABLE gold.agg_medication_adherence CASCADE;

WITH pde_by_year AS (
    SELECT
        fpe.beneficiary_key,
        fpe.ndc,
        EXTRACT(YEAR FROM fpe.service_date)::SMALLINT AS year,
        COUNT(*) AS pde_count,
        SUM(COALESCE(fpe.days_supply, 0)) AS total_days_supply,
        SUM(fpe.quantity_dispensed) AS total_quantity,
        SUM(fpe.total_cost_amount) AS total_cost
    FROM gold.fct_prescription_events fpe
    GROUP BY fpe.beneficiary_key, fpe.ndc, year
)
INSERT INTO gold.agg_medication_adherence (
    beneficiary_key, ndc, drug_name, year,
    pde_count, total_days_supply, total_quantity,
    pdc, is_adherent,
    total_cost
)
SELECT
    py.beneficiary_key,
    py.ndc,
    NULL AS drug_name,
    py.year,
    py.pde_count,
    py.total_days_supply,
    py.total_quantity,
    
    -- PDC: days_supply / days_in_year (capped at 1.0)
    LEAST(py.total_days_supply::NUMERIC / 365.0, 1.0)::NUMERIC(5,4) AS pdc,
    (LEAST(py.total_days_supply::NUMERIC / 365.0, 1.0) >= 0.80) AS is_adherent,
    
    py.total_cost
FROM pde_by_year py;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

\echo '=== GOLD LAYER LOAD COMPLETE ==='
\echo ''
\echo 'Dimension Tables:'
SELECT 'dim_beneficiary' AS tbl, COUNT(*) FROM gold.dim_beneficiary
UNION ALL SELECT 'dim_provider', COUNT(*) FROM gold.dim_provider
UNION ALL SELECT 'dim_diagnosis', COUNT(*) FROM gold.dim_diagnosis
UNION ALL SELECT 'dim_time', COUNT(*) FROM gold.dim_time;

\echo ''
\echo 'Fact Tables:'
SELECT 'fct_claims' AS tbl, COUNT(*) FROM gold.fct_claims
UNION ALL SELECT 'fct_prescription_events', COUNT(*) FROM gold.fct_prescription_events;

\echo ''
\echo 'Aggregate Tables:'
SELECT 'agg_beneficiary_year' AS tbl, COUNT(*) FROM gold.agg_beneficiary_year
UNION ALL SELECT 'agg_provider_year', COUNT(*) FROM gold.agg_provider_year
UNION ALL SELECT 'agg_medication_adherence', COUNT(*) FROM gold.agg_medication_adherence;

\echo ''
\echo 'Claims by Type:'
SELECT claim_type, COUNT(*) FROM gold.fct_claims GROUP BY claim_type ORDER BY claim_type;

\echo ''
\echo 'Top 10 Providers by Claim Volume:'
SELECT dp.provider_id, COUNT(*) as claim_count
FROM gold.fct_claims fc
JOIN gold.dim_provider dp ON fc.provider_key = dp.provider_key
GROUP BY dp.provider_id
ORDER BY claim_count DESC
LIMIT 10;