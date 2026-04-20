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