-- =============================================================================
-- CREATE GOLD LAYER
-- Run from psql: psql -d healthcare_dw -f sql/05_create_gold.sql
--
-- Gold layer implements dimensional modeling (facts + dimensions) optimized
-- for analytics. Supports cost, utilization, and medication adherence analysis
-- across 2008-2010.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- ============================================================================
-- dim_beneficiary
--   One row per beneficiary. Flattens beneficiary data across 3 years.
--   Captures chronic conditions and demographics.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_beneficiary (
    beneficiary_key             SERIAL PRIMARY KEY,
    beneficiary_id              INTEGER NOT NULL UNIQUE,
    
    -- demographics
    date_of_birth               DATE,
    age_2008                    SMALLINT,
    gender                      CHAR(1),
    race                        VARCHAR(50),
    state_code                  CHAR(2),
    county_code                 CHAR(3),
    
    -- chronic condition flags (if present in any year, marked TRUE)
    has_diabetes                BOOLEAN DEFAULT FALSE,
    has_heart_failure           BOOLEAN DEFAULT FALSE,
    has_copd                    BOOLEAN DEFAULT FALSE,
    has_ami                     BOOLEAN DEFAULT FALSE,  -- Acute MI
    has_stroke                  BOOLEAN DEFAULT FALSE,
    has_cancer                  BOOLEAN DEFAULT FALSE,
    has_hypertension            BOOLEAN DEFAULT FALSE,
    has_hyperlipidemia          BOOLEAN DEFAULT FALSE,
    has_depression              BOOLEAN DEFAULT FALSE,
    has_ckd                     BOOLEAN DEFAULT FALSE,  -- Chronic kidney disease
    
    -- counts
    chronic_condition_count     SMALLINT DEFAULT 0,
    
    -- metadata
    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_beneficiary_id ON gold.dim_beneficiary(beneficiary_id);

-- ============================================================================
-- dim_provider
--   One row per provider. Aggregates provider attributes.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_provider (
    provider_key                SERIAL PRIMARY KEY,
    provider_id                 VARCHAR(50) NOT NULL UNIQUE,
    provider_type               VARCHAR(50),  -- inpatient, outpatient, carrier
    state_code                  CHAR(2),
    
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_provider_id ON gold.dim_provider(provider_id);

-- ============================================================================
-- dim_diagnosis
--   ICD-9 diagnosis codes with descriptions (supplementary).
--   Note: We'll populate this sparsely during transform; full ICD-9 lookup
--   would require external reference table.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_diagnosis (
    diagnosis_key               SERIAL PRIMARY KEY,
    icd9_code                   VARCHAR(10) NOT NULL UNIQUE,
    description                 VARCHAR(500),
    category                    VARCHAR(100),  -- chronic, acute, etc.
    
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_diagnosis_code ON gold.dim_diagnosis(icd9_code);

-- ============================================================================
-- dim_time
--   Calendar dimension for year/month/quarter analysis.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_time (
    time_key                    SERIAL PRIMARY KEY,
    full_date                   DATE NOT NULL UNIQUE,
    year                        SMALLINT NOT NULL,
    month                       SMALLINT NOT NULL,
    quarter                     SMALLINT NOT NULL,
    day_of_week                 SMALLINT,
    week_of_year                SMALLINT,
    
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_time_date ON gold.dim_time(full_date);
CREATE INDEX idx_dim_time_year_month ON gold.dim_time(year, month);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ============================================================================
-- fct_claims
--   One row per claim (union of inpatient, outpatient, carrier).
--   Denormalizes key metrics for fast querying.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fct_claims (
    claim_key                   BIGSERIAL PRIMARY KEY,
    
    -- foreign keys
    beneficiary_key             INTEGER NOT NULL REFERENCES gold.dim_beneficiary(beneficiary_key),
    provider_key                INTEGER REFERENCES gold.dim_provider(provider_key),
    service_date_key            INTEGER REFERENCES gold.dim_time(time_key),
    
    -- claim identifiers
    claim_id                    TEXT NOT NULL,
    claim_type                  VARCHAR(50) NOT NULL,  -- inpatient, outpatient, carrier
    
    -- dates
    service_date                DATE NOT NULL,
    claim_from_date             DATE,
    claim_thru_date             DATE,
    
    -- diagnosis & procedure
    primary_diagnosis_code      VARCHAR(10),
    secondary_diagnosis_code    VARCHAR(10),
    procedure_code_1            VARCHAR(10),
    procedure_code_2            VARCHAR(10),
    
    -- utilization
    days_of_stay                SMALLINT,  -- inpatient only
    quantity_dispensed          NUMERIC(8,2),  -- carrier/outpatient
    
    -- costs
    total_charge_amount         NUMERIC(12,2),
    total_allowed_amount        NUMERIC(12,2),
    total_payment_amount        NUMERIC(12,2),
    beneficiary_responsibility  NUMERIC(12,2),
    
    -- metadata
    loaded_at                   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_fct_claims_beneficiary ON gold.fct_claims(beneficiary_key);
CREATE INDEX idx_fct_claims_provider ON gold.fct_claims(provider_key);
CREATE INDEX idx_fct_claims_service_date ON gold.fct_claims(service_date);
CREATE INDEX idx_fct_claims_type ON gold.fct_claims(claim_type);

-- ============================================================================
-- fct_prescription_events
--   One row per prescription filled. Supports medication adherence analysis.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fct_prescription_events (
    pde_key                     BIGSERIAL PRIMARY KEY,
    
    -- foreign keys
    beneficiary_key             INTEGER NOT NULL REFERENCES gold.dim_beneficiary(beneficiary_key),
    service_date_key            INTEGER REFERENCES gold.dim_time(time_key),
    
    -- identifiers
    pde_id                      TEXT NOT NULL UNIQUE,
    ndc                         TEXT NOT NULL,
    drug_name                   VARCHAR(255),
    
    -- dates
    service_date                DATE NOT NULL,
    fill_date                   DATE,
    
    -- utilization
    quantity_dispensed          NUMERIC(8,2),
    days_supply                 SMALLINT,
    
    -- costs
    total_cost_amount           NUMERIC(12,2),
    patient_cost_amount         NUMERIC(12,2),
    
    -- metadata
    loaded_at                   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_fct_pde_beneficiary ON gold.fct_prescription_events(beneficiary_key);
CREATE INDEX idx_fct_pde_service_date ON gold.fct_prescription_events(service_date);
CREATE INDEX idx_fct_pde_ndc ON gold.fct_prescription_events(ndc);

-- =============================================================================
-- AGGREGATE TABLES (for fast reporting)
-- =============================================================================

-- ============================================================================
-- agg_beneficiary_year
--   One row per beneficiary per year. Pre-aggregated utilization + costs.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_beneficiary_year (
    agg_key                     BIGSERIAL PRIMARY KEY,
    
    beneficiary_key             INTEGER NOT NULL REFERENCES gold.dim_beneficiary(beneficiary_key),
    year                        SMALLINT NOT NULL,
    
    -- inpatient
    inpatient_claim_count       INTEGER DEFAULT 0,
    inpatient_total_cost        NUMERIC(15,2) DEFAULT 0,
    inpatient_total_paid        NUMERIC(15,2) DEFAULT 0,
    inpatient_days_of_stay      INTEGER DEFAULT 0,
    
    -- outpatient
    outpatient_claim_count      INTEGER DEFAULT 0,
    outpatient_total_cost       NUMERIC(15,2) DEFAULT 0,
    outpatient_total_paid       NUMERIC(15,2) DEFAULT 0,
    
    -- carrier (physician)
    carrier_claim_count         INTEGER DEFAULT 0,
    carrier_total_cost          NUMERIC(15,2) DEFAULT 0,
    carrier_total_paid          NUMERIC(15,2) DEFAULT 0,
    
    -- total (all claim types)
    total_claim_count           INTEGER DEFAULT 0,
    total_cost                  NUMERIC(15,2) DEFAULT 0,
    total_paid                  NUMERIC(15,2) DEFAULT 0,
    
    -- prescription
    pde_count                   INTEGER DEFAULT 0,
    pde_total_cost              NUMERIC(15,2) DEFAULT 0,
    
    -- metadata
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_agg_bene_year ON gold.agg_beneficiary_year(beneficiary_key, year);

-- ============================================================================
-- agg_provider_year
--   One row per provider per year. Volume + cost metrics.
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_provider_year (
    agg_key                     BIGSERIAL PRIMARY KEY,
    
    provider_key                INTEGER NOT NULL REFERENCES gold.dim_provider(provider_key),
    year                        SMALLINT NOT NULL,
    
    claim_count                 INTEGER DEFAULT 0,
    total_charge_amount         NUMERIC(15,2) DEFAULT 0,
    total_allowed_amount        NUMERIC(15,2) DEFAULT 0,
    total_paid_amount           NUMERIC(15,2) DEFAULT 0,
    unique_beneficiary_count    INTEGER DEFAULT 0,
    
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_agg_provider_year ON gold.agg_provider_year(provider_key, year);

-- ============================================================================
-- agg_medication_adherence
--   One row per beneficiary per drug per year. PDC (Proportion of Days Covered).
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_medication_adherence (
    agg_key                     BIGSERIAL PRIMARY KEY,
    
    beneficiary_key             INTEGER NOT NULL REFERENCES gold.dim_beneficiary(beneficiary_key),
    ndc                         TEXT NOT NULL,
    drug_name                   VARCHAR(255),
    year                        SMALLINT NOT NULL,
    
    -- utilization
    pde_count                   INTEGER DEFAULT 0,
    total_days_supply           INTEGER DEFAULT 0,
    total_quantity              NUMERIC(10,2) DEFAULT 0,
    
    -- adherence metric
    pdc                         NUMERIC(5,4),  -- Proportion of Days Covered (0.0-1.0)
    is_adherent                 BOOLEAN,  -- PDC >= 0.80
    
    -- costs
    total_cost                  NUMERIC(15,2) DEFAULT 0,
    
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_agg_med_adh ON gold.agg_medication_adherence(beneficiary_key, ndc, year);
