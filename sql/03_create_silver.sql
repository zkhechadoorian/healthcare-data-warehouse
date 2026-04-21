-- =============================================================================
-- CREATE SILVER LAYER
-- Run from psql: psql -d healthcare_dw -f sql/03_create_silver.sql
--
-- All tables are derived strictly from what exists in staging (01_create_staging.sql).
-- No columns are included that don't have a source in staging.
--
-- Naming convention: silver columns use lowercase snake_case regardless of
-- the UPPERCASE names used in staging.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;


-- =============================================================================
-- silver.beneficiary
-- Source: staging.beneficiary_2008 + staging.beneficiary_2009 + staging.beneficiary_2010
-- All three yearly tables have identical schemas and are unioned into one table,
-- tagged with a `year` column.
-- Natural key: desynpuf_id + year
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.beneficiary (

    -- identity
    desynpuf_id                    TEXT        NOT NULL,
    year                           SMALLINT    NOT NULL,

    -- demographics
    bene_birth_dt                  DATE,
    bene_death_dt                  DATE,
    bene_sex                       TEXT,           -- decoded: 'Male' | 'Female'
    bene_race                      TEXT,           -- decoded: 'White' | 'Black' | etc.
    bene_esrd_ind                  BOOLEAN,

    -- geography
    sp_state_code                  SMALLINT,
    bene_county_cd                 SMALLINT,

    -- coverage months
    bene_hi_cvrage_tot_mons        SMALLINT,
    bene_smi_cvrage_tot_mons       SMALLINT,
    bene_hmo_cvrage_tot_mons       SMALLINT,
    plan_cvrg_mos_num              SMALLINT,

    -- chronic conditions (decoded: 1=TRUE, 2=FALSE)
    sp_alzhdmta                    BOOLEAN,
    sp_chf                         BOOLEAN,
    sp_chrnkidn                    BOOLEAN,
    sp_cncr                        BOOLEAN,
    sp_copd                        BOOLEAN,
    sp_depressn                    BOOLEAN,
    sp_diabetes                    BOOLEAN,
    sp_ischmcht                    BOOLEAN,
    sp_osteoprs                    BOOLEAN,
    sp_ra_oa                       BOOLEAN,
    sp_strketia                    BOOLEAN,

    -- reimbursement amounts
    medreimb_ip                    NUMERIC(12,2),
    benres_ip                      NUMERIC(12,2),
    pppymt_ip                      NUMERIC(12,2),
    medreimb_op                    NUMERIC(12,2),
    benres_op                      NUMERIC(12,2),
    pppymt_op                      NUMERIC(12,2),
    medreimb_car                   NUMERIC(12,2),
    benres_car                     NUMERIC(12,2),
    pppymt_car                     NUMERIC(12,2),

    -- metadata
    _loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _row_hash                      TEXT        NOT NULL,

    PRIMARY KEY (desynpuf_id, year)
);


-- =============================================================================
-- silver.inpatient_claims
-- Source: staging.inpatient_claims
-- Natural key: clm_id + segment
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.inpatient_claims (

    -- identity
    clm_id                         TEXT        NOT NULL,
    desynpuf_id                    TEXT        NOT NULL,
    segment                        SMALLINT    NOT NULL,

    -- dates
    clm_from_dt                    DATE,
    clm_thru_dt                    DATE,
    clm_admsn_dt                   DATE,
    nch_bene_dschrg_dt             DATE,

    -- provider
    prvdr_num                      TEXT,
    at_physn_npi                   TEXT,
    op_physn_npi                   TEXT,
    ot_physn_npi                   TEXT,

    -- claim amounts
    clm_pmt_amt                    NUMERIC(12,2),
    nch_prmry_pyr_clm_pd_amt       NUMERIC(12,2),
    clm_pass_thru_per_diem_amt     NUMERIC(12,2),
    nch_bene_ip_ddctbl_amt         NUMERIC(12,2),
    nch_bene_pta_coinsrnc_lblty_am NUMERIC(12,2),
    nch_bene_blood_ddctbl_lblty_am NUMERIC(12,2),

    -- utilization
    clm_utlztn_day_cnt             SMALLINT,
    clm_drg_cd                     TEXT,

    -- admitting diagnosis
    admtng_icd9_dgns_cd            TEXT,

    -- diagnosis codes (10)
    icd9_dgns_cd_1                 TEXT,
    icd9_dgns_cd_2                 TEXT,
    icd9_dgns_cd_3                 TEXT,
    icd9_dgns_cd_4                 TEXT,
    icd9_dgns_cd_5                 TEXT,
    icd9_dgns_cd_6                 TEXT,
    icd9_dgns_cd_7                 TEXT,
    icd9_dgns_cd_8                 TEXT,
    icd9_dgns_cd_9                 TEXT,
    icd9_dgns_cd_10                TEXT,

    -- procedure codes (6)
    icd9_prcdr_cd_1                TEXT,
    icd9_prcdr_cd_2                TEXT,
    icd9_prcdr_cd_3                TEXT,
    icd9_prcdr_cd_4                TEXT,
    icd9_prcdr_cd_5                TEXT,
    icd9_prcdr_cd_6                TEXT,

    -- HCPCS codes (45)
    hcpcs_cd_1                     TEXT,
    hcpcs_cd_2                     TEXT,
    hcpcs_cd_3                     TEXT,
    hcpcs_cd_4                     TEXT,
    hcpcs_cd_5                     TEXT,
    hcpcs_cd_6                     TEXT,
    hcpcs_cd_7                     TEXT,
    hcpcs_cd_8                     TEXT,
    hcpcs_cd_9                     TEXT,
    hcpcs_cd_10                    TEXT,
    hcpcs_cd_11                    TEXT,
    hcpcs_cd_12                    TEXT,
    hcpcs_cd_13                    TEXT,
    hcpcs_cd_14                    TEXT,
    hcpcs_cd_15                    TEXT,
    hcpcs_cd_16                    TEXT,
    hcpcs_cd_17                    TEXT,
    hcpcs_cd_18                    TEXT,
    hcpcs_cd_19                    TEXT,
    hcpcs_cd_20                    TEXT,
    hcpcs_cd_21                    TEXT,
    hcpcs_cd_22                    TEXT,
    hcpcs_cd_23                    TEXT,
    hcpcs_cd_24                    TEXT,
    hcpcs_cd_25                    TEXT,
    hcpcs_cd_26                    TEXT,
    hcpcs_cd_27                    TEXT,
    hcpcs_cd_28                    TEXT,
    hcpcs_cd_29                    TEXT,
    hcpcs_cd_30                    TEXT,
    hcpcs_cd_31                    TEXT,
    hcpcs_cd_32                    TEXT,
    hcpcs_cd_33                    TEXT,
    hcpcs_cd_34                    TEXT,
    hcpcs_cd_35                    TEXT,
    hcpcs_cd_36                    TEXT,
    hcpcs_cd_37                    TEXT,
    hcpcs_cd_38                    TEXT,
    hcpcs_cd_39                    TEXT,
    hcpcs_cd_40                    TEXT,
    hcpcs_cd_41                    TEXT,
    hcpcs_cd_42                    TEXT,
    hcpcs_cd_43                    TEXT,
    hcpcs_cd_44                    TEXT,
    hcpcs_cd_45                    TEXT,

    -- metadata
    _loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _row_hash                      TEXT        NOT NULL,

    PRIMARY KEY (clm_id, segment),
    CONSTRAINT chk_inpatient_clm_dates CHECK (clm_thru_dt >= clm_from_dt OR clm_thru_dt IS NULL OR clm_from_dt IS NULL)
);


-- =============================================================================
-- silver.outpatient_claims
-- Source: staging.outpatient_claims
-- Natural key: clm_id + segment
-- Note: staging does not include ADMTNG_ICD9_DGNS_CD for outpatient,
--       so it is excluded here.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.outpatient_claims (

    -- identity
    clm_id                         TEXT        NOT NULL,
    desynpuf_id                    TEXT        NOT NULL,
    segment                        SMALLINT    NOT NULL,

    -- dates
    clm_from_dt                    DATE,
    clm_thru_dt                    DATE,

    -- provider
    prvdr_num                      TEXT,
    at_physn_npi                   TEXT,
    op_physn_npi                   TEXT,
    ot_physn_npi                   TEXT,

    -- claim amounts
    clm_pmt_amt                    NUMERIC(12,2),
    nch_prmry_pyr_clm_pd_amt       NUMERIC(12,2),
    nch_bene_blood_ddctbl_lblty_am NUMERIC(12,2),
    nch_bene_ptb_ddctbl_amt        NUMERIC(12,2),
    nch_bene_ptb_coinsrnc_amt      NUMERIC(12,2),

    -- diagnosis codes (10)
    icd9_dgns_cd_1                 TEXT,
    icd9_dgns_cd_2                 TEXT,
    icd9_dgns_cd_3                 TEXT,
    icd9_dgns_cd_4                 TEXT,
    icd9_dgns_cd_5                 TEXT,
    icd9_dgns_cd_6                 TEXT,
    icd9_dgns_cd_7                 TEXT,
    icd9_dgns_cd_8                 TEXT,
    icd9_dgns_cd_9                 TEXT,
    icd9_dgns_cd_10                TEXT,

    -- procedure codes (6)
    icd9_prcdr_cd_1                TEXT,
    icd9_prcdr_cd_2                TEXT,
    icd9_prcdr_cd_3                TEXT,
    icd9_prcdr_cd_4                TEXT,
    icd9_prcdr_cd_5                TEXT,
    icd9_prcdr_cd_6                TEXT,

    -- HCPCS codes (45)
    hcpcs_cd_1                     TEXT,
    hcpcs_cd_2                     TEXT,
    hcpcs_cd_3                     TEXT,
    hcpcs_cd_4                     TEXT,
    hcpcs_cd_5                     TEXT,
    hcpcs_cd_6                     TEXT,
    hcpcs_cd_7                     TEXT,
    hcpcs_cd_8                     TEXT,
    hcpcs_cd_9                     TEXT,
    hcpcs_cd_10                    TEXT,
    hcpcs_cd_11                    TEXT,
    hcpcs_cd_12                    TEXT,
    hcpcs_cd_13                    TEXT,
    hcpcs_cd_14                    TEXT,
    hcpcs_cd_15                    TEXT,
    hcpcs_cd_16                    TEXT,
    hcpcs_cd_17                    TEXT,
    hcpcs_cd_18                    TEXT,
    hcpcs_cd_19                    TEXT,
    hcpcs_cd_20                    TEXT,
    hcpcs_cd_21                    TEXT,
    hcpcs_cd_22                    TEXT,
    hcpcs_cd_23                    TEXT,
    hcpcs_cd_24                    TEXT,
    hcpcs_cd_25                    TEXT,
    hcpcs_cd_26                    TEXT,
    hcpcs_cd_27                    TEXT,
    hcpcs_cd_28                    TEXT,
    hcpcs_cd_29                    TEXT,
    hcpcs_cd_30                    TEXT,
    hcpcs_cd_31                    TEXT,
    hcpcs_cd_32                    TEXT,
    hcpcs_cd_33                    TEXT,
    hcpcs_cd_34                    TEXT,
    hcpcs_cd_35                    TEXT,
    hcpcs_cd_36                    TEXT,
    hcpcs_cd_37                    TEXT,
    hcpcs_cd_38                    TEXT,
    hcpcs_cd_39                    TEXT,
    hcpcs_cd_40                    TEXT,
    hcpcs_cd_41                    TEXT,
    hcpcs_cd_42                    TEXT,
    hcpcs_cd_43                    TEXT,
    hcpcs_cd_44                    TEXT,
    hcpcs_cd_45                    TEXT,

    -- metadata
    _loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _row_hash                      TEXT        NOT NULL,

    PRIMARY KEY (clm_id, segment),
    CONSTRAINT chk_outpatient_clm_dates CHECK (clm_thru_dt >= clm_from_dt OR clm_thru_dt IS NULL OR clm_from_dt IS NULL)
);


-- =============================================================================
-- silver.carrier_claims
-- Source: staging.carrier_claims
-- The SynPUF carrier table is wide: one row per claim with up to 13 line items
-- stored as repeated column groups (_1 through _13). Silver keeps this wide
-- shape and just casts types.
-- Natural key: clm_id (carrier claims have one row per claim in staging)
-- Dedup: on clm_id — duplicate rows from loading segment A + B CSVs
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.carrier_claims (

    -- identity
    clm_id                         TEXT        NOT NULL PRIMARY KEY,
    desynpuf_id                    TEXT        NOT NULL,

    -- dates
    clm_from_dt                    DATE,
    clm_thru_dt                    DATE,

    -- claim-level diagnosis codes (8)
    icd9_dgns_cd_1                 TEXT,
    icd9_dgns_cd_2                 TEXT,
    icd9_dgns_cd_3                 TEXT,
    icd9_dgns_cd_4                 TEXT,
    icd9_dgns_cd_5                 TEXT,
    icd9_dgns_cd_6                 TEXT,
    icd9_dgns_cd_7                 TEXT,
    icd9_dgns_cd_8                 TEXT,

    -- performing physician NPIs per line (13)
    prf_physn_npi_1                TEXT,
    prf_physn_npi_2                TEXT,
    prf_physn_npi_3                TEXT,
    prf_physn_npi_4                TEXT,
    prf_physn_npi_5                TEXT,
    prf_physn_npi_6                TEXT,
    prf_physn_npi_7                TEXT,
    prf_physn_npi_8                TEXT,
    prf_physn_npi_9                TEXT,
    prf_physn_npi_10               TEXT,
    prf_physn_npi_11               TEXT,
    prf_physn_npi_12               TEXT,
    prf_physn_npi_13               TEXT,

    -- tax numbers per line (13)
    tax_num_1                      TEXT,
    tax_num_2                      TEXT,
    tax_num_3                      TEXT,
    tax_num_4                      TEXT,
    tax_num_5                      TEXT,
    tax_num_6                      TEXT,
    tax_num_7                      TEXT,
    tax_num_8                      TEXT,
    tax_num_9                      TEXT,
    tax_num_10                     TEXT,
    tax_num_11                     TEXT,
    tax_num_12                     TEXT,
    tax_num_13                     TEXT,

    -- HCPCS codes per line (13)
    hcpcs_cd_1                     TEXT,
    hcpcs_cd_2                     TEXT,
    hcpcs_cd_3                     TEXT,
    hcpcs_cd_4                     TEXT,
    hcpcs_cd_5                     TEXT,
    hcpcs_cd_6                     TEXT,
    hcpcs_cd_7                     TEXT,
    hcpcs_cd_8                     TEXT,
    hcpcs_cd_9                     TEXT,
    hcpcs_cd_10                    TEXT,
    hcpcs_cd_11                    TEXT,
    hcpcs_cd_12                    TEXT,
    hcpcs_cd_13                    TEXT,

    -- line payment amounts (13)
    line_nch_pmt_amt_1             NUMERIC(12,2),
    line_nch_pmt_amt_2             NUMERIC(12,2),
    line_nch_pmt_amt_3             NUMERIC(12,2),
    line_nch_pmt_amt_4             NUMERIC(12,2),
    line_nch_pmt_amt_5             NUMERIC(12,2),
    line_nch_pmt_amt_6             NUMERIC(12,2),
    line_nch_pmt_amt_7             NUMERIC(12,2),
    line_nch_pmt_amt_8             NUMERIC(12,2),
    line_nch_pmt_amt_9             NUMERIC(12,2),
    line_nch_pmt_amt_10            NUMERIC(12,2),
    line_nch_pmt_amt_11            NUMERIC(12,2),
    line_nch_pmt_amt_12            NUMERIC(12,2),
    line_nch_pmt_amt_13            NUMERIC(12,2),

    -- beneficiary Part B deductible per line (13)
    line_bene_ptb_ddctbl_amt_1     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_2     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_3     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_4     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_5     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_6     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_7     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_8     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_9     NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_10    NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_11    NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_12    NUMERIC(12,2),
    line_bene_ptb_ddctbl_amt_13    NUMERIC(12,2),

    -- primary payer paid amount per line (13)
    line_bene_prmry_pyr_pd_amt_1   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_2   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_3   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_4   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_5   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_6   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_7   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_8   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_9   NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_10  NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_11  NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_12  NUMERIC(12,2),
    line_bene_prmry_pyr_pd_amt_13  NUMERIC(12,2),

    -- coinsurance amount per line (13)
    line_coinsrnc_amt_1            NUMERIC(12,2),
    line_coinsrnc_amt_2            NUMERIC(12,2),
    line_coinsrnc_amt_3            NUMERIC(12,2),
    line_coinsrnc_amt_4            NUMERIC(12,2),
    line_coinsrnc_amt_5            NUMERIC(12,2),
    line_coinsrnc_amt_6            NUMERIC(12,2),
    line_coinsrnc_amt_7            NUMERIC(12,2),
    line_coinsrnc_amt_8            NUMERIC(12,2),
    line_coinsrnc_amt_9            NUMERIC(12,2),
    line_coinsrnc_amt_10           NUMERIC(12,2),
    line_coinsrnc_amt_11           NUMERIC(12,2),
    line_coinsrnc_amt_12           NUMERIC(12,2),
    line_coinsrnc_amt_13           NUMERIC(12,2),

    -- allowed charge per line (13)
    line_alowd_chrg_amt_1          NUMERIC(12,2),
    line_alowd_chrg_amt_2          NUMERIC(12,2),
    line_alowd_chrg_amt_3          NUMERIC(12,2),
    line_alowd_chrg_amt_4          NUMERIC(12,2),
    line_alowd_chrg_amt_5          NUMERIC(12,2),
    line_alowd_chrg_amt_6          NUMERIC(12,2),
    line_alowd_chrg_amt_7          NUMERIC(12,2),
    line_alowd_chrg_amt_8          NUMERIC(12,2),
    line_alowd_chrg_amt_9          NUMERIC(12,2),
    line_alowd_chrg_amt_10         NUMERIC(12,2),
    line_alowd_chrg_amt_11         NUMERIC(12,2),
    line_alowd_chrg_amt_12         NUMERIC(12,2),
    line_alowd_chrg_amt_13         NUMERIC(12,2),

    -- processing indicator code per line (13)
    line_prcsg_ind_cd_1            TEXT,
    line_prcsg_ind_cd_2            TEXT,
    line_prcsg_ind_cd_3            TEXT,
    line_prcsg_ind_cd_4            TEXT,
    line_prcsg_ind_cd_5            TEXT,
    line_prcsg_ind_cd_6            TEXT,
    line_prcsg_ind_cd_7            TEXT,
    line_prcsg_ind_cd_8            TEXT,
    line_prcsg_ind_cd_9            TEXT,
    line_prcsg_ind_cd_10           TEXT,
    line_prcsg_ind_cd_11           TEXT,
    line_prcsg_ind_cd_12           TEXT,
    line_prcsg_ind_cd_13           TEXT,

    -- line-level diagnosis codes (13)
    line_icd9_dgns_cd_1            TEXT,
    line_icd9_dgns_cd_2            TEXT,
    line_icd9_dgns_cd_3            TEXT,
    line_icd9_dgns_cd_4            TEXT,
    line_icd9_dgns_cd_5            TEXT,
    line_icd9_dgns_cd_6            TEXT,
    line_icd9_dgns_cd_7            TEXT,
    line_icd9_dgns_cd_8            TEXT,
    line_icd9_dgns_cd_9            TEXT,
    line_icd9_dgns_cd_10           TEXT,
    line_icd9_dgns_cd_11           TEXT,
    line_icd9_dgns_cd_12           TEXT,
    line_icd9_dgns_cd_13           TEXT,

    -- metadata
    _loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _row_hash                      TEXT        NOT NULL,

    CONSTRAINT chk_carrier_clm_dates CHECK (clm_thru_dt >= clm_from_dt OR clm_thru_dt IS NULL OR clm_from_dt IS NULL)
);


-- =============================================================================
-- silver.prescription_drug_events
-- Source: staging.prescription_drug_events (8 columns only)
-- Natural key: pde_id
-- Column mapping:
--   PROD_SRVC_ID  → ndc  (National Drug Code)
--   QTY_DSPNSD_NUM → qty_dispensed
--   PTNT_PAY_AMT  → ptnt_pay_amt
--   TOT_RX_CST_AMT → tot_rx_cst_amt
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.prescription_drug_events (

    -- identity
    pde_id                         TEXT        NOT NULL PRIMARY KEY,
    desynpuf_id                    TEXT        NOT NULL,

    -- date
    srvc_dt                        DATE,

    -- drug details
    ndc                            TEXT,           -- PROD_SRVC_ID in staging
    qty_dispensed                  NUMERIC(8,2),   -- QTY_DSPNSD_NUM
    days_supply                    SMALLINT,       -- DAYS_SUPLY_NUM

    -- costs
    ptnt_pay_amt                   NUMERIC(12,2),  -- PTNT_PAY_AMT
    tot_rx_cst_amt                 NUMERIC(12,2),  -- TOT_RX_CST_AMT

    -- metadata
    _loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _row_hash                      TEXT        NOT NULL
);


-- =============================================================================
-- silver.kaggle_encounters
-- Source: staging.kaggle_encounters
-- No natural key exists in source — surrogate key generated with ROW_NUMBER()
--   during the transform step.
-- Column mapping (quoted mixed-case staging → clean silver):
--   "Name"               → patient_name
--   "Age"                → patient_age
--   "Gender"             → patient_gender
--   "Blood Type"         → blood_type
--   "Medical Condition"  → medical_condition
--   "Date of Admission"  → admission_dt
--   "Doctor"             → doctor
--   "Hospital"           → hospital
--   "Insurance Provider" → insurance_provider
--   "Billing Amount"     → billing_amt
--   "Room Number"        → room_number
--   "Admission Type"     → admission_type
--   "Discharge Date"     → discharge_dt
--   "Medication"         → medication
--   "Test Results"       → test_results
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.kaggle_encounters (

    -- surrogate key (generated at load time)
    encounter_id                   BIGINT      NOT NULL PRIMARY KEY,

    -- patient info
    patient_name                   TEXT,
    patient_age                    SMALLINT,
    patient_gender                 TEXT,
    blood_type                     TEXT,

    -- encounter details
    medical_condition              TEXT,
    admission_dt                   DATE,
    discharge_dt                   DATE,
    admission_type                 TEXT,
    room_number                    SMALLINT,

    -- providers & billing
    doctor                         TEXT,
    hospital                       TEXT,
    insurance_provider             TEXT,
    billing_amt                    NUMERIC(12,2),

    -- treatment
    medication                     TEXT,
    test_results                   TEXT,

    -- metadata
    _loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _row_hash                      TEXT        NOT NULL,

    CONSTRAINT chk_kaggle_dates CHECK (discharge_dt >= admission_dt OR discharge_dt IS NULL OR admission_dt IS NULL)
);


-- =============================================================================
-- INDEXES
-- =============================================================================

-- beneficiary
CREATE INDEX IF NOT EXISTS idx_bene_desynpuf   ON silver.beneficiary(desynpuf_id);
CREATE INDEX IF NOT EXISTS idx_bene_year        ON silver.beneficiary(year);
CREATE INDEX IF NOT EXISTS idx_bene_death_dt    ON silver.beneficiary(bene_death_dt);

-- inpatient_claims
CREATE INDEX IF NOT EXISTS idx_ip_desynpuf      ON silver.inpatient_claims(desynpuf_id);
CREATE INDEX IF NOT EXISTS idx_ip_clm_from_dt   ON silver.inpatient_claims(clm_from_dt);
CREATE INDEX IF NOT EXISTS idx_ip_drg_cd        ON silver.inpatient_claims(clm_drg_cd);

-- outpatient_claims
CREATE INDEX IF NOT EXISTS idx_op_desynpuf      ON silver.outpatient_claims(desynpuf_id);
CREATE INDEX IF NOT EXISTS idx_op_clm_from_dt   ON silver.outpatient_claims(clm_from_dt);

-- carrier_claims
CREATE INDEX IF NOT EXISTS idx_car_desynpuf     ON silver.carrier_claims(desynpuf_id);
CREATE INDEX IF NOT EXISTS idx_car_clm_from_dt  ON silver.carrier_claims(clm_from_dt);

-- prescription_drug_events
CREATE INDEX IF NOT EXISTS idx_pde_desynpuf     ON silver.prescription_drug_events(desynpuf_id);
CREATE INDEX IF NOT EXISTS idx_pde_srvc_dt      ON silver.prescription_drug_events(srvc_dt);
CREATE INDEX IF NOT EXISTS idx_pde_ndc          ON silver.prescription_drug_events(ndc);

-- kaggle_encounters
CREATE INDEX IF NOT EXISTS idx_kag_admission_dt ON silver.kaggle_encounters(admission_dt);
CREATE INDEX IF NOT EXISTS idx_kag_condition    ON silver.kaggle_encounters(medical_condition);


-- =============================================================================
-- QUARANTINE TABLES
-- One companion table per silver table. Rows that fail validation rules
-- (e.g. discharge_date < admission_date) are routed here instead of being
-- silently dropped. Shares the same columns as the silver table, plus three
-- metadata columns describing the rejection.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.beneficiary_quarantine (
    LIKE silver.beneficiary INCLUDING ALL,
    _rejection_reason   TEXT        NOT NULL,
    _rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.inpatient_claims_quarantine (
    LIKE silver.inpatient_claims INCLUDING ALL,
    _rejection_reason   TEXT        NOT NULL,
    _rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.outpatient_claims_quarantine (
    LIKE silver.outpatient_claims INCLUDING ALL,
    _rejection_reason   TEXT        NOT NULL,
    _rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.carrier_claims_quarantine (
    LIKE silver.carrier_claims INCLUDING ALL,
    _rejection_reason   TEXT        NOT NULL,
    _rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.prescription_drug_events_quarantine (
    LIKE silver.prescription_drug_events INCLUDING ALL,
    _rejection_reason   TEXT        NOT NULL,
    _rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.kaggle_encounters_quarantine (
    LIKE silver.kaggle_encounters INCLUDING ALL,
    _rejection_reason   TEXT        NOT NULL,
    _rejected_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT        NOT NULL
);