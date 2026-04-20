-- =============================================================================
-- CREATE SILVER LAYER
-- Run from psql: psql -d healthcare_dw -f sql/03_create_silver.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.beneficiary (
    -- identity
    desynpuf_id                    TEXT NOT NULL,
    year                           SMALLINT NOT NULL,

    -- demographics
    bene_birth_dt                  DATE,
    bene_death_dt                  DATE,
    bene_sex                       TEXT,
    bene_race                      TEXT,
    bene_esrd_ind                  BOOLEAN,
    sp_state_code                  SMALLINT,
    bene_county_cd                 SMALLINT,

    -- coverage months
    bene_hi_cvrage_tot_mons        SMALLINT,
    bene_smi_cvrage_tot_mons       SMALLINT,
    bene_hmo_cvrage_tot_mons       SMALLINT,
    plan_cvrg_mos_num              SMALLINT,

    -- chronic conditions
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
    _row_hash                      TEXT NOT NULL,

    -- constraints
    PRIMARY KEY (desynpuf_id, year)
);

CREATE TABLE IF NOT EXISTS silver.inpatient_claims (
    -- identity
    clm_id                         TEXT NOT NULL,
    desynpuf_id                    TEXT NOT NULL,
    segment                        SMALLINT NOT NULL,

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

    -- diagnosis codes
    admtng_icd9_dgns_cd            TEXT,
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

    -- procedure codes
    icd9_prcdr_cd_1                TEXT,
    icd9_prcdr_cd_2                TEXT,
    icd9_prcdr_cd_3                TEXT,
    icd9_prcdr_cd_4                TEXT,
    icd9_prcdr_cd_5                TEXT,
    icd9_prcdr_cd_6                TEXT,

    -- HCPCS codes
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
    _row_hash                      TEXT NOT NULL,

    -- constraints
    PRIMARY KEY (clm_id, segment),
    CONSTRAINT chk_clm_dates CHECK (clm_thru_dt >= clm_from_dt)
);

CREATE TABLE IF NOT EXISTS silver.outpatient_claims (
    -- identity
    clm_id                         TEXT NOT NULL,
    desynpuf_id                    TEXT NOT NULL,
    segment                        SMALLINT NOT NULL,

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

    -- diagnosis codes
    admtng_icd9_dgns_cd            TEXT,
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

    -- procedure codes
    icd9_prcdr_cd_1                TEXT,
    icd9_prcdr_cd_2                TEXT,
    icd9_prcdr_cd_3                TEXT,
    icd9_prcdr_cd_4                TEXT,
    icd9_prcdr_cd_5                TEXT,
    icd9_prcdr_cd_6                TEXT,

    -- HCPCS codes
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
    _row_hash                      TEXT NOT NULL,

    -- constraints
    PRIMARY KEY (clm_id, segment),
    CONSTRAINT chk_clm_dates CHECK (clm_thru_dt >= clm_from_dt)
);