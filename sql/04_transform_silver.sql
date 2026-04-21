-- =============================================================================
-- TRANSFORM SILVER LAYER
-- Reads from staging.*, cleans and transforms, inserts into silver.*
--
-- Run from psql: psql -d healthcare_dw -f sql/04_transform_silver.sql
--
-- Prerequisites:
--   01_create_staging.sql  — staging schema + tables exist
--   02_load_staging.sql    — staging tables are populated
--   03_create_silver.sql   — silver schema + tables exist
--
-- This script is idempotent: safe to re-run. Already-loaded rows are
-- skipped via ON CONFLICT DO NOTHING.
--
-- Transformations applied per table:
--   1. Type casting   — TEXT → DATE / NUMERIC / SMALLINT / BOOLEAN
--   2. Null handling  — blank strings → NULL; missing amounts → 0
--   3. Deduplication  — DISTINCT ON natural key inside CTE, cast outside
--   4. Code decoding  — sex, race, chronic flags, ESRD (beneficiary only)
--   5. Metadata       — _loaded_at, _row_hash via MD5
-- =============================================================================


-- =============================================================================
-- HELPER FUNCTIONS
-- Schema-qualified so they don't pollute public.
-- =============================================================================

-- safe_date: converts YYYYMMDD text → DATE, returns NULL on blank/'00000000'/error
CREATE OR REPLACE FUNCTION silver.safe_date(p_raw TEXT)
RETURNS DATE
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF p_raw IS NULL OR TRIM(p_raw) = '' OR p_raw = '00000000' THEN
        RETURN NULL;
    END IF;
    RETURN TO_DATE(TRIM(p_raw), 'YYYYMMDD');
EXCEPTION
    WHEN OTHERS THEN RETURN NULL;
END;
$$;

-- safe_numeric: converts TEXT → NUMERIC, blank → NULL or 0 depending on flag
CREATE OR REPLACE FUNCTION silver.safe_numeric(p_raw TEXT, p_default_zero BOOLEAN DEFAULT FALSE)
RETURNS NUMERIC
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF p_raw IS NULL OR TRIM(p_raw) = '' THEN
        RETURN CASE WHEN p_default_zero THEN 0 ELSE NULL END;
    END IF;
    RETURN TRIM(p_raw)::NUMERIC;
EXCEPTION
    WHEN OTHERS THEN
        RETURN CASE WHEN p_default_zero THEN 0 ELSE NULL END;
END;
$$;

-- safe_smallint: converts TEXT → SMALLINT, blank/error → NULL
CREATE OR REPLACE FUNCTION silver.safe_smallint(p_raw TEXT)
RETURNS SMALLINT
LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF p_raw IS NULL OR TRIM(p_raw) = '' THEN RETURN NULL; END IF;
    RETURN TRIM(p_raw)::SMALLINT;
EXCEPTION
    WHEN OTHERS THEN RETURN NULL;
END;
$$;


-- =============================================================================
-- 1. silver.beneficiary
--    Sources : staging.beneficiary_2008 / 2009 / 2010 (identical schemas)
--    Strategy: UNION ALL the three tables with a src_year tag, dedup on
--              (desynpuf_id, src_year) inside a CTE, then cast in outer query.
--    Decoding:
--      BENE_SEX_IDENT_CD  1→'Male'   2→'Female'
--      BENE_RACE_CD       1→'White'  2→'Black'  3→'Other'
--                         4→'Asian'  5→'Hispanic' 6→'North American Native'
--      SP_* flags         '1'→TRUE   '2'→FALSE
--      BENE_ESRD_IND      'Y'→TRUE   else→FALSE
-- =============================================================================

WITH deduped AS (
    SELECT DISTINCT ON (desynpuf_id, src_year) *
    FROM (
        SELECT *, 2008 AS src_year FROM staging.beneficiary_2008
        UNION ALL
        SELECT *, 2009 AS src_year FROM staging.beneficiary_2009
        UNION ALL
        SELECT *, 2010 AS src_year FROM staging.beneficiary_2010
    ) combined
    ORDER BY desynpuf_id, src_year
)
INSERT INTO silver.beneficiary (
    desynpuf_id, year,
    bene_birth_dt, bene_death_dt, bene_sex, bene_race, bene_esrd_ind,
    sp_state_code, bene_county_cd,
    bene_hi_cvrage_tot_mons, bene_smi_cvrage_tot_mons,
    bene_hmo_cvrage_tot_mons, plan_cvrg_mos_num,
    sp_alzhdmta, sp_chf, sp_chrnkidn, sp_cncr, sp_copd,
    sp_depressn, sp_diabetes, sp_ischmcht, sp_osteoprs, sp_ra_oa, sp_strketia,
    medreimb_ip, benres_ip, pppymt_ip,
    medreimb_op, benres_op, pppymt_op,
    medreimb_car, benres_car, pppymt_car,
    _loaded_at, _row_hash
)
SELECT
    desynpuf_id,
    src_year::SMALLINT                                        AS year,

    silver.safe_date(bene_birth_dt)                           AS bene_birth_dt,
    silver.safe_date(bene_death_dt)                           AS bene_death_dt,

    CASE bene_sex_ident_cd
        WHEN '1' THEN 'Male'
        WHEN '2' THEN 'Female'
        ELSE NULL
    END                                                       AS bene_sex,

    CASE bene_race_cd
        WHEN '1' THEN 'White'
        WHEN '2' THEN 'Black'
        WHEN '3' THEN 'Other'
        WHEN '4' THEN 'Asian'
        WHEN '5' THEN 'Hispanic'
        WHEN '6' THEN 'North American Native'
        ELSE NULL
    END                                                       AS bene_race,

    UPPER(TRIM(bene_esrd_ind)) = 'Y'                          AS bene_esrd_ind,

    silver.safe_smallint(sp_state_code)                       AS sp_state_code,
    silver.safe_smallint(bene_county_cd)                      AS bene_county_cd,

    silver.safe_smallint(bene_hi_cvrage_tot_mons)             AS bene_hi_cvrage_tot_mons,
    silver.safe_smallint(bene_smi_cvrage_tot_mons)            AS bene_smi_cvrage_tot_mons,
    silver.safe_smallint(bene_hmo_cvrage_tot_mons)            AS bene_hmo_cvrage_tot_mons,
    silver.safe_smallint(plan_cvrg_mos_num)                   AS plan_cvrg_mos_num,

    (sp_alzhdmta = '1')                                       AS sp_alzhdmta,
    (sp_chf      = '1')                                       AS sp_chf,
    (sp_chrnkidn = '1')                                       AS sp_chrnkidn,
    (sp_cncr     = '1')                                       AS sp_cncr,
    (sp_copd     = '1')                                       AS sp_copd,
    (sp_depressn = '1')                                       AS sp_depressn,
    (sp_diabetes = '1')                                       AS sp_diabetes,
    (sp_ischmcht = '1')                                       AS sp_ischmcht,
    (sp_osteoprs = '1')                                       AS sp_osteoprs,
    (sp_ra_oa    = '1')                                       AS sp_ra_oa,
    (sp_strketia = '1')                                       AS sp_strketia,

    silver.safe_numeric(medreimb_ip,  TRUE)                   AS medreimb_ip,
    silver.safe_numeric(benres_ip,    TRUE)                   AS benres_ip,
    silver.safe_numeric(pppymt_ip,    TRUE)                   AS pppymt_ip,
    silver.safe_numeric(medreimb_op,  TRUE)                   AS medreimb_op,
    silver.safe_numeric(benres_op,    TRUE)                   AS benres_op,
    silver.safe_numeric(pppymt_op,    TRUE)                   AS pppymt_op,
    silver.safe_numeric(medreimb_car, TRUE)                   AS medreimb_car,
    silver.safe_numeric(benres_car,   TRUE)                   AS benres_car,
    silver.safe_numeric(pppymt_car,   TRUE)                   AS pppymt_car,

    NOW()                                                     AS _loaded_at,
    MD5(ROW(
        desynpuf_id, src_year,
        bene_birth_dt, bene_death_dt, bene_sex_ident_cd, bene_race_cd,
        bene_esrd_ind, sp_state_code, bene_county_cd,
        sp_alzhdmta, sp_chf, sp_chrnkidn, sp_cncr, sp_copd,
        sp_depressn, sp_diabetes, sp_ischmcht, sp_osteoprs, sp_ra_oa, sp_strketia,
        medreimb_ip, benres_ip, pppymt_ip,
        medreimb_op, benres_op, pppymt_op,
        medreimb_car, benres_car, pppymt_car
    )::TEXT)                                                  AS _row_hash

FROM deduped
ON CONFLICT (desynpuf_id, year) DO NOTHING;


-- =============================================================================
-- 2. silver.inpatient_claims
--    Source : staging.inpatient_claims  (UPPERCASE column names)
--    Key    : clm_id + segment
-- =============================================================================

WITH deduped AS (
    SELECT DISTINCT ON (clm_id, segment) *
    FROM staging.inpatient_claims
    ORDER BY clm_id, segment
)
INSERT INTO silver.inpatient_claims (
    clm_id, desynpuf_id, segment,
    clm_from_dt, clm_thru_dt, clm_admsn_dt, nch_bene_dschrg_dt,
    prvdr_num, at_physn_npi, op_physn_npi, ot_physn_npi,
    clm_pmt_amt, nch_prmry_pyr_clm_pd_amt, clm_pass_thru_per_diem_amt,
    nch_bene_ip_ddctbl_amt, nch_bene_pta_coinsrnc_lblty_am,
    nch_bene_blood_ddctbl_lblty_am,
    clm_utlztn_day_cnt, clm_drg_cd,
    admtng_icd9_dgns_cd,
    icd9_dgns_cd_1,  icd9_dgns_cd_2,  icd9_dgns_cd_3,  icd9_dgns_cd_4,
    icd9_dgns_cd_5,  icd9_dgns_cd_6,  icd9_dgns_cd_7,  icd9_dgns_cd_8,
    icd9_dgns_cd_9,  icd9_dgns_cd_10,
    icd9_prcdr_cd_1, icd9_prcdr_cd_2, icd9_prcdr_cd_3,
    icd9_prcdr_cd_4, icd9_prcdr_cd_5, icd9_prcdr_cd_6,
    hcpcs_cd_1,  hcpcs_cd_2,  hcpcs_cd_3,  hcpcs_cd_4,  hcpcs_cd_5,
    hcpcs_cd_6,  hcpcs_cd_7,  hcpcs_cd_8,  hcpcs_cd_9,  hcpcs_cd_10,
    hcpcs_cd_11, hcpcs_cd_12, hcpcs_cd_13, hcpcs_cd_14, hcpcs_cd_15,
    hcpcs_cd_16, hcpcs_cd_17, hcpcs_cd_18, hcpcs_cd_19, hcpcs_cd_20,
    hcpcs_cd_21, hcpcs_cd_22, hcpcs_cd_23, hcpcs_cd_24, hcpcs_cd_25,
    hcpcs_cd_26, hcpcs_cd_27, hcpcs_cd_28, hcpcs_cd_29, hcpcs_cd_30,
    hcpcs_cd_31, hcpcs_cd_32, hcpcs_cd_33, hcpcs_cd_34, hcpcs_cd_35,
    hcpcs_cd_36, hcpcs_cd_37, hcpcs_cd_38, hcpcs_cd_39, hcpcs_cd_40,
    hcpcs_cd_41, hcpcs_cd_42, hcpcs_cd_43, hcpcs_cd_44, hcpcs_cd_45,
    _loaded_at, _row_hash
)
SELECT
    clm_id,
    desynpuf_id,
    silver.safe_smallint(segment)                             AS segment,

    silver.safe_date(clm_from_dt)                             AS clm_from_dt,
    silver.safe_date(clm_thru_dt)                             AS clm_thru_dt,
    silver.safe_date(clm_admsn_dt)                            AS clm_admsn_dt,
    silver.safe_date(nch_bene_dschrg_dt)                      AS nch_bene_dschrg_dt,

    NULLIF(TRIM(prvdr_num),    '')                            AS prvdr_num,
    NULLIF(TRIM(at_physn_npi), '')                            AS at_physn_npi,
    NULLIF(TRIM(op_physn_npi), '')                            AS op_physn_npi,
    NULLIF(TRIM(ot_physn_npi), '')                            AS ot_physn_npi,

    silver.safe_numeric(clm_pmt_amt,                    TRUE) AS clm_pmt_amt,
    silver.safe_numeric(nch_prmry_pyr_clm_pd_amt,       TRUE) AS nch_prmry_pyr_clm_pd_amt,
    silver.safe_numeric(clm_pass_thru_per_diem_amt,     TRUE) AS clm_pass_thru_per_diem_amt,
    silver.safe_numeric(nch_bene_ip_ddctbl_amt,         TRUE) AS nch_bene_ip_ddctbl_amt,
    silver.safe_numeric(nch_bene_pta_coinsrnc_lblty_am, TRUE) AS nch_bene_pta_coinsrnc_lblty_am,
    silver.safe_numeric(nch_bene_blood_ddctbl_lblty_am, TRUE) AS nch_bene_blood_ddctbl_lblty_am,

    silver.safe_smallint(clm_utlztn_day_cnt)                  AS clm_utlztn_day_cnt,
    NULLIF(TRIM(clm_drg_cd), '')                              AS clm_drg_cd,

    NULLIF(TRIM(admtng_icd9_dgns_cd), ''),
    NULLIF(TRIM(icd9_dgns_cd_1),  ''), NULLIF(TRIM(icd9_dgns_cd_2),  ''),
    NULLIF(TRIM(icd9_dgns_cd_3),  ''), NULLIF(TRIM(icd9_dgns_cd_4),  ''),
    NULLIF(TRIM(icd9_dgns_cd_5),  ''), NULLIF(TRIM(icd9_dgns_cd_6),  ''),
    NULLIF(TRIM(icd9_dgns_cd_7),  ''), NULLIF(TRIM(icd9_dgns_cd_8),  ''),
    NULLIF(TRIM(icd9_dgns_cd_9),  ''), NULLIF(TRIM(icd9_dgns_cd_10), ''),

    NULLIF(TRIM(icd9_prcdr_cd_1), ''), NULLIF(TRIM(icd9_prcdr_cd_2), ''),
    NULLIF(TRIM(icd9_prcdr_cd_3), ''), NULLIF(TRIM(icd9_prcdr_cd_4), ''),
    NULLIF(TRIM(icd9_prcdr_cd_5), ''), NULLIF(TRIM(icd9_prcdr_cd_6), ''),

    NULLIF(TRIM(hcpcs_cd_1),  ''), NULLIF(TRIM(hcpcs_cd_2),  ''),
    NULLIF(TRIM(hcpcs_cd_3),  ''), NULLIF(TRIM(hcpcs_cd_4),  ''),
    NULLIF(TRIM(hcpcs_cd_5),  ''), NULLIF(TRIM(hcpcs_cd_6),  ''),
    NULLIF(TRIM(hcpcs_cd_7),  ''), NULLIF(TRIM(hcpcs_cd_8),  ''),
    NULLIF(TRIM(hcpcs_cd_9),  ''), NULLIF(TRIM(hcpcs_cd_10), ''),
    NULLIF(TRIM(hcpcs_cd_11), ''), NULLIF(TRIM(hcpcs_cd_12), ''),
    NULLIF(TRIM(hcpcs_cd_13), ''), NULLIF(TRIM(hcpcs_cd_14), ''),
    NULLIF(TRIM(hcpcs_cd_15), ''), NULLIF(TRIM(hcpcs_cd_16), ''),
    NULLIF(TRIM(hcpcs_cd_17), ''), NULLIF(TRIM(hcpcs_cd_18), ''),
    NULLIF(TRIM(hcpcs_cd_19), ''), NULLIF(TRIM(hcpcs_cd_20), ''),
    NULLIF(TRIM(hcpcs_cd_21), ''), NULLIF(TRIM(hcpcs_cd_22), ''),
    NULLIF(TRIM(hcpcs_cd_23), ''), NULLIF(TRIM(hcpcs_cd_24), ''),
    NULLIF(TRIM(hcpcs_cd_25), ''), NULLIF(TRIM(hcpcs_cd_26), ''),
    NULLIF(TRIM(hcpcs_cd_27), ''), NULLIF(TRIM(hcpcs_cd_28), ''),
    NULLIF(TRIM(hcpcs_cd_29), ''), NULLIF(TRIM(hcpcs_cd_30), ''),
    NULLIF(TRIM(hcpcs_cd_31), ''), NULLIF(TRIM(hcpcs_cd_32), ''),
    NULLIF(TRIM(hcpcs_cd_33), ''), NULLIF(TRIM(hcpcs_cd_34), ''),
    NULLIF(TRIM(hcpcs_cd_35), ''), NULLIF(TRIM(hcpcs_cd_36), ''),
    NULLIF(TRIM(hcpcs_cd_37), ''), NULLIF(TRIM(hcpcs_cd_38), ''),
    NULLIF(TRIM(hcpcs_cd_39), ''), NULLIF(TRIM(hcpcs_cd_40), ''),
    NULLIF(TRIM(hcpcs_cd_41), ''), NULLIF(TRIM(hcpcs_cd_42), ''),
    NULLIF(TRIM(hcpcs_cd_43), ''), NULLIF(TRIM(hcpcs_cd_44), ''),
    NULLIF(TRIM(hcpcs_cd_45), ''),

    NOW()                                                     AS _loaded_at,
    MD5(ROW(
        clm_id, desynpuf_id, segment,
        clm_from_dt, clm_thru_dt, clm_admsn_dt,
        prvdr_num, clm_pmt_amt, clm_utlztn_day_cnt, clm_drg_cd,
        admtng_icd9_dgns_cd, icd9_dgns_cd_1
    )::TEXT)                                                  AS _row_hash

FROM deduped
ON CONFLICT (clm_id, segment) DO NOTHING;


-- =============================================================================
-- 3. silver.outpatient_claims
--    Source : staging.outpatient_claims
--    Key    : clm_id + segment
--    Note   : no admtng_icd9_dgns_cd in staging, excluded per design decision
-- =============================================================================

WITH deduped AS (
    SELECT DISTINCT ON (clm_id, segment) *
    FROM staging.outpatient_claims
    ORDER BY clm_id, segment
)
INSERT INTO silver.outpatient_claims (
    clm_id, desynpuf_id, segment,
    clm_from_dt, clm_thru_dt,
    prvdr_num, at_physn_npi, op_physn_npi, ot_physn_npi,
    clm_pmt_amt, nch_prmry_pyr_clm_pd_amt,
    nch_bene_blood_ddctbl_lblty_am,
    nch_bene_ptb_ddctbl_amt, nch_bene_ptb_coinsrnc_amt,
    icd9_dgns_cd_1,  icd9_dgns_cd_2,  icd9_dgns_cd_3,  icd9_dgns_cd_4,
    icd9_dgns_cd_5,  icd9_dgns_cd_6,  icd9_dgns_cd_7,  icd9_dgns_cd_8,
    icd9_dgns_cd_9,  icd9_dgns_cd_10,
    icd9_prcdr_cd_1, icd9_prcdr_cd_2, icd9_prcdr_cd_3,
    icd9_prcdr_cd_4, icd9_prcdr_cd_5, icd9_prcdr_cd_6,
    hcpcs_cd_1,  hcpcs_cd_2,  hcpcs_cd_3,  hcpcs_cd_4,  hcpcs_cd_5,
    hcpcs_cd_6,  hcpcs_cd_7,  hcpcs_cd_8,  hcpcs_cd_9,  hcpcs_cd_10,
    hcpcs_cd_11, hcpcs_cd_12, hcpcs_cd_13, hcpcs_cd_14, hcpcs_cd_15,
    hcpcs_cd_16, hcpcs_cd_17, hcpcs_cd_18, hcpcs_cd_19, hcpcs_cd_20,
    hcpcs_cd_21, hcpcs_cd_22, hcpcs_cd_23, hcpcs_cd_24, hcpcs_cd_25,
    hcpcs_cd_26, hcpcs_cd_27, hcpcs_cd_28, hcpcs_cd_29, hcpcs_cd_30,
    hcpcs_cd_31, hcpcs_cd_32, hcpcs_cd_33, hcpcs_cd_34, hcpcs_cd_35,
    hcpcs_cd_36, hcpcs_cd_37, hcpcs_cd_38, hcpcs_cd_39, hcpcs_cd_40,
    hcpcs_cd_41, hcpcs_cd_42, hcpcs_cd_43, hcpcs_cd_44, hcpcs_cd_45,
    _loaded_at, _row_hash
)
SELECT
    clm_id,
    desynpuf_id,
    silver.safe_smallint(segment)                             AS segment,

    silver.safe_date(clm_from_dt)                             AS clm_from_dt,
    silver.safe_date(clm_thru_dt)                             AS clm_thru_dt,

    NULLIF(TRIM(prvdr_num),    '')                            AS prvdr_num,
    NULLIF(TRIM(at_physn_npi), '')                            AS at_physn_npi,
    NULLIF(TRIM(op_physn_npi), '')                            AS op_physn_npi,
    NULLIF(TRIM(ot_physn_npi), '')                            AS ot_physn_npi,

    silver.safe_numeric(clm_pmt_amt,                    TRUE) AS clm_pmt_amt,
    silver.safe_numeric(nch_prmry_pyr_clm_pd_amt,       TRUE) AS nch_prmry_pyr_clm_pd_amt,
    silver.safe_numeric(nch_bene_blood_ddctbl_lblty_am, TRUE) AS nch_bene_blood_ddctbl_lblty_am,
    silver.safe_numeric(nch_bene_ptb_ddctbl_amt,        TRUE) AS nch_bene_ptb_ddctbl_amt,
    silver.safe_numeric(nch_bene_ptb_coinsrnc_amt,      TRUE) AS nch_bene_ptb_coinsrnc_amt,

    NULLIF(TRIM(icd9_dgns_cd_1),  ''), NULLIF(TRIM(icd9_dgns_cd_2),  ''),
    NULLIF(TRIM(icd9_dgns_cd_3),  ''), NULLIF(TRIM(icd9_dgns_cd_4),  ''),
    NULLIF(TRIM(icd9_dgns_cd_5),  ''), NULLIF(TRIM(icd9_dgns_cd_6),  ''),
    NULLIF(TRIM(icd9_dgns_cd_7),  ''), NULLIF(TRIM(icd9_dgns_cd_8),  ''),
    NULLIF(TRIM(icd9_dgns_cd_9),  ''), NULLIF(TRIM(icd9_dgns_cd_10), ''),

    NULLIF(TRIM(icd9_prcdr_cd_1), ''), NULLIF(TRIM(icd9_prcdr_cd_2), ''),
    NULLIF(TRIM(icd9_prcdr_cd_3), ''), NULLIF(TRIM(icd9_prcdr_cd_4), ''),
    NULLIF(TRIM(icd9_prcdr_cd_5), ''), NULLIF(TRIM(icd9_prcdr_cd_6), ''),

    NULLIF(TRIM(hcpcs_cd_1),  ''), NULLIF(TRIM(hcpcs_cd_2),  ''),
    NULLIF(TRIM(hcpcs_cd_3),  ''), NULLIF(TRIM(hcpcs_cd_4),  ''),
    NULLIF(TRIM(hcpcs_cd_5),  ''), NULLIF(TRIM(hcpcs_cd_6),  ''),
    NULLIF(TRIM(hcpcs_cd_7),  ''), NULLIF(TRIM(hcpcs_cd_8),  ''),
    NULLIF(TRIM(hcpcs_cd_9),  ''), NULLIF(TRIM(hcpcs_cd_10), ''),
    NULLIF(TRIM(hcpcs_cd_11), ''), NULLIF(TRIM(hcpcs_cd_12), ''),
    NULLIF(TRIM(hcpcs_cd_13), ''), NULLIF(TRIM(hcpcs_cd_14), ''),
    NULLIF(TRIM(hcpcs_cd_15), ''), NULLIF(TRIM(hcpcs_cd_16), ''),
    NULLIF(TRIM(hcpcs_cd_17), ''), NULLIF(TRIM(hcpcs_cd_18), ''),
    NULLIF(TRIM(hcpcs_cd_19), ''), NULLIF(TRIM(hcpcs_cd_20), ''),
    NULLIF(TRIM(hcpcs_cd_21), ''), NULLIF(TRIM(hcpcs_cd_22), ''),
    NULLIF(TRIM(hcpcs_cd_23), ''), NULLIF(TRIM(hcpcs_cd_24), ''),
    NULLIF(TRIM(hcpcs_cd_25), ''), NULLIF(TRIM(hcpcs_cd_26), ''),
    NULLIF(TRIM(hcpcs_cd_27), ''), NULLIF(TRIM(hcpcs_cd_28), ''),
    NULLIF(TRIM(hcpcs_cd_29), ''), NULLIF(TRIM(hcpcs_cd_30), ''),
    NULLIF(TRIM(hcpcs_cd_31), ''), NULLIF(TRIM(hcpcs_cd_32), ''),
    NULLIF(TRIM(hcpcs_cd_33), ''), NULLIF(TRIM(hcpcs_cd_34), ''),
    NULLIF(TRIM(hcpcs_cd_35), ''), NULLIF(TRIM(hcpcs_cd_36), ''),
    NULLIF(TRIM(hcpcs_cd_37), ''), NULLIF(TRIM(hcpcs_cd_38), ''),
    NULLIF(TRIM(hcpcs_cd_39), ''), NULLIF(TRIM(hcpcs_cd_40), ''),
    NULLIF(TRIM(hcpcs_cd_41), ''), NULLIF(TRIM(hcpcs_cd_42), ''),
    NULLIF(TRIM(hcpcs_cd_43), ''), NULLIF(TRIM(hcpcs_cd_44), ''),
    NULLIF(TRIM(hcpcs_cd_45), ''),

    NOW()                                                     AS _loaded_at,
    MD5(ROW(
        clm_id, desynpuf_id, segment,
        clm_from_dt, clm_thru_dt,
        prvdr_num, clm_pmt_amt, icd9_dgns_cd_1
    )::TEXT)                                                  AS _row_hash

FROM deduped
ON CONFLICT (clm_id, segment) DO NOTHING;


-- =============================================================================
-- 4. silver.carrier_claims
--    Source : staging.carrier_claims
--    Key    : clm_id (no line-level key in staging — wide format)
--    Note   : Carrier claims arrive as two CSVs per sample (A + B), both
--             loaded into the same staging table → dedup on clm_id is important.
--             Amount columns default to 0 when blank (financial fields).
-- =============================================================================

WITH deduped AS (
    SELECT DISTINCT ON (clm_id) *
    FROM staging.carrier_claims
    ORDER BY clm_id
)
INSERT INTO silver.carrier_claims (
    clm_id, desynpuf_id,
    clm_from_dt, clm_thru_dt,
    icd9_dgns_cd_1, icd9_dgns_cd_2, icd9_dgns_cd_3, icd9_dgns_cd_4,
    icd9_dgns_cd_5, icd9_dgns_cd_6, icd9_dgns_cd_7, icd9_dgns_cd_8,
    prf_physn_npi_1,  prf_physn_npi_2,  prf_physn_npi_3,  prf_physn_npi_4,
    prf_physn_npi_5,  prf_physn_npi_6,  prf_physn_npi_7,  prf_physn_npi_8,
    prf_physn_npi_9,  prf_physn_npi_10, prf_physn_npi_11, prf_physn_npi_12,
    prf_physn_npi_13,
    tax_num_1,  tax_num_2,  tax_num_3,  tax_num_4,  tax_num_5,
    tax_num_6,  tax_num_7,  tax_num_8,  tax_num_9,  tax_num_10,
    tax_num_11, tax_num_12, tax_num_13,
    hcpcs_cd_1,  hcpcs_cd_2,  hcpcs_cd_3,  hcpcs_cd_4,  hcpcs_cd_5,
    hcpcs_cd_6,  hcpcs_cd_7,  hcpcs_cd_8,  hcpcs_cd_9,  hcpcs_cd_10,
    hcpcs_cd_11, hcpcs_cd_12, hcpcs_cd_13,
    line_nch_pmt_amt_1,  line_nch_pmt_amt_2,  line_nch_pmt_amt_3,
    line_nch_pmt_amt_4,  line_nch_pmt_amt_5,  line_nch_pmt_amt_6,
    line_nch_pmt_amt_7,  line_nch_pmt_amt_8,  line_nch_pmt_amt_9,
    line_nch_pmt_amt_10, line_nch_pmt_amt_11, line_nch_pmt_amt_12,
    line_nch_pmt_amt_13,
    line_bene_ptb_ddctbl_amt_1,  line_bene_ptb_ddctbl_amt_2,
    line_bene_ptb_ddctbl_amt_3,  line_bene_ptb_ddctbl_amt_4,
    line_bene_ptb_ddctbl_amt_5,  line_bene_ptb_ddctbl_amt_6,
    line_bene_ptb_ddctbl_amt_7,  line_bene_ptb_ddctbl_amt_8,
    line_bene_ptb_ddctbl_amt_9,  line_bene_ptb_ddctbl_amt_10,
    line_bene_ptb_ddctbl_amt_11, line_bene_ptb_ddctbl_amt_12,
    line_bene_ptb_ddctbl_amt_13,
    line_bene_prmry_pyr_pd_amt_1,  line_bene_prmry_pyr_pd_amt_2,
    line_bene_prmry_pyr_pd_amt_3,  line_bene_prmry_pyr_pd_amt_4,
    line_bene_prmry_pyr_pd_amt_5,  line_bene_prmry_pyr_pd_amt_6,
    line_bene_prmry_pyr_pd_amt_7,  line_bene_prmry_pyr_pd_amt_8,
    line_bene_prmry_pyr_pd_amt_9,  line_bene_prmry_pyr_pd_amt_10,
    line_bene_prmry_pyr_pd_amt_11, line_bene_prmry_pyr_pd_amt_12,
    line_bene_prmry_pyr_pd_amt_13,
    line_coinsrnc_amt_1,  line_coinsrnc_amt_2,  line_coinsrnc_amt_3,
    line_coinsrnc_amt_4,  line_coinsrnc_amt_5,  line_coinsrnc_amt_6,
    line_coinsrnc_amt_7,  line_coinsrnc_amt_8,  line_coinsrnc_amt_9,
    line_coinsrnc_amt_10, line_coinsrnc_amt_11, line_coinsrnc_amt_12,
    line_coinsrnc_amt_13,
    line_alowd_chrg_amt_1,  line_alowd_chrg_amt_2,  line_alowd_chrg_amt_3,
    line_alowd_chrg_amt_4,  line_alowd_chrg_amt_5,  line_alowd_chrg_amt_6,
    line_alowd_chrg_amt_7,  line_alowd_chrg_amt_8,  line_alowd_chrg_amt_9,
    line_alowd_chrg_amt_10, line_alowd_chrg_amt_11, line_alowd_chrg_amt_12,
    line_alowd_chrg_amt_13,
    line_prcsg_ind_cd_1,  line_prcsg_ind_cd_2,  line_prcsg_ind_cd_3,
    line_prcsg_ind_cd_4,  line_prcsg_ind_cd_5,  line_prcsg_ind_cd_6,
    line_prcsg_ind_cd_7,  line_prcsg_ind_cd_8,  line_prcsg_ind_cd_9,
    line_prcsg_ind_cd_10, line_prcsg_ind_cd_11, line_prcsg_ind_cd_12,
    line_prcsg_ind_cd_13,
    line_icd9_dgns_cd_1,  line_icd9_dgns_cd_2,  line_icd9_dgns_cd_3,
    line_icd9_dgns_cd_4,  line_icd9_dgns_cd_5,  line_icd9_dgns_cd_6,
    line_icd9_dgns_cd_7,  line_icd9_dgns_cd_8,  line_icd9_dgns_cd_9,
    line_icd9_dgns_cd_10, line_icd9_dgns_cd_11, line_icd9_dgns_cd_12,
    line_icd9_dgns_cd_13,
    _loaded_at, _row_hash
)
SELECT
    clm_id,
    desynpuf_id,

    silver.safe_date(clm_from_dt)                             AS clm_from_dt,
    silver.safe_date(clm_thru_dt)                             AS clm_thru_dt,

    NULLIF(TRIM(icd9_dgns_cd_1), ''), NULLIF(TRIM(icd9_dgns_cd_2), ''),
    NULLIF(TRIM(icd9_dgns_cd_3), ''), NULLIF(TRIM(icd9_dgns_cd_4), ''),
    NULLIF(TRIM(icd9_dgns_cd_5), ''), NULLIF(TRIM(icd9_dgns_cd_6), ''),
    NULLIF(TRIM(icd9_dgns_cd_7), ''), NULLIF(TRIM(icd9_dgns_cd_8), ''),

    NULLIF(TRIM(prf_physn_npi_1),  ''), NULLIF(TRIM(prf_physn_npi_2),  ''),
    NULLIF(TRIM(prf_physn_npi_3),  ''), NULLIF(TRIM(prf_physn_npi_4),  ''),
    NULLIF(TRIM(prf_physn_npi_5),  ''), NULLIF(TRIM(prf_physn_npi_6),  ''),
    NULLIF(TRIM(prf_physn_npi_7),  ''), NULLIF(TRIM(prf_physn_npi_8),  ''),
    NULLIF(TRIM(prf_physn_npi_9),  ''), NULLIF(TRIM(prf_physn_npi_10), ''),
    NULLIF(TRIM(prf_physn_npi_11), ''), NULLIF(TRIM(prf_physn_npi_12), ''),
    NULLIF(TRIM(prf_physn_npi_13), ''),

    NULLIF(TRIM(tax_num_1),  ''), NULLIF(TRIM(tax_num_2),  ''),
    NULLIF(TRIM(tax_num_3),  ''), NULLIF(TRIM(tax_num_4),  ''),
    NULLIF(TRIM(tax_num_5),  ''), NULLIF(TRIM(tax_num_6),  ''),
    NULLIF(TRIM(tax_num_7),  ''), NULLIF(TRIM(tax_num_8),  ''),
    NULLIF(TRIM(tax_num_9),  ''), NULLIF(TRIM(tax_num_10), ''),
    NULLIF(TRIM(tax_num_11), ''), NULLIF(TRIM(tax_num_12), ''),
    NULLIF(TRIM(tax_num_13), ''),

    NULLIF(TRIM(hcpcs_cd_1),  ''), NULLIF(TRIM(hcpcs_cd_2),  ''),
    NULLIF(TRIM(hcpcs_cd_3),  ''), NULLIF(TRIM(hcpcs_cd_4),  ''),
    NULLIF(TRIM(hcpcs_cd_5),  ''), NULLIF(TRIM(hcpcs_cd_6),  ''),
    NULLIF(TRIM(hcpcs_cd_7),  ''), NULLIF(TRIM(hcpcs_cd_8),  ''),
    NULLIF(TRIM(hcpcs_cd_9),  ''), NULLIF(TRIM(hcpcs_cd_10), ''),
    NULLIF(TRIM(hcpcs_cd_11), ''), NULLIF(TRIM(hcpcs_cd_12), ''),
    NULLIF(TRIM(hcpcs_cd_13), ''),

    silver.safe_numeric(line_nch_pmt_amt_1,  TRUE), silver.safe_numeric(line_nch_pmt_amt_2,  TRUE),
    silver.safe_numeric(line_nch_pmt_amt_3,  TRUE), silver.safe_numeric(line_nch_pmt_amt_4,  TRUE),
    silver.safe_numeric(line_nch_pmt_amt_5,  TRUE), silver.safe_numeric(line_nch_pmt_amt_6,  TRUE),
    silver.safe_numeric(line_nch_pmt_amt_7,  TRUE), silver.safe_numeric(line_nch_pmt_amt_8,  TRUE),
    silver.safe_numeric(line_nch_pmt_amt_9,  TRUE), silver.safe_numeric(line_nch_pmt_amt_10, TRUE),
    silver.safe_numeric(line_nch_pmt_amt_11, TRUE), silver.safe_numeric(line_nch_pmt_amt_12, TRUE),
    silver.safe_numeric(line_nch_pmt_amt_13, TRUE),

    silver.safe_numeric(line_bene_ptb_ddctbl_amt_1,  TRUE), silver.safe_numeric(line_bene_ptb_ddctbl_amt_2,  TRUE),
    silver.safe_numeric(line_bene_ptb_ddctbl_amt_3,  TRUE), silver.safe_numeric(line_bene_ptb_ddctbl_amt_4,  TRUE),
    silver.safe_numeric(line_bene_ptb_ddctbl_amt_5,  TRUE), silver.safe_numeric(line_bene_ptb_ddctbl_amt_6,  TRUE),
    silver.safe_numeric(line_bene_ptb_ddctbl_amt_7,  TRUE), silver.safe_numeric(line_bene_ptb_ddctbl_amt_8,  TRUE),
    silver.safe_numeric(line_bene_ptb_ddctbl_amt_9,  TRUE), silver.safe_numeric(line_bene_ptb_ddctbl_amt_10, TRUE),
    silver.safe_numeric(line_bene_ptb_ddctbl_amt_11, TRUE), silver.safe_numeric(line_bene_ptb_ddctbl_amt_12, TRUE),
    silver.safe_numeric(line_bene_ptb_ddctbl_amt_13, TRUE),

    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_1,  TRUE), silver.safe_numeric(line_bene_prmry_pyr_pd_amt_2,  TRUE),
    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_3,  TRUE), silver.safe_numeric(line_bene_prmry_pyr_pd_amt_4,  TRUE),
    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_5,  TRUE), silver.safe_numeric(line_bene_prmry_pyr_pd_amt_6,  TRUE),
    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_7,  TRUE), silver.safe_numeric(line_bene_prmry_pyr_pd_amt_8,  TRUE),
    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_9,  TRUE), silver.safe_numeric(line_bene_prmry_pyr_pd_amt_10, TRUE),
    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_11, TRUE), silver.safe_numeric(line_bene_prmry_pyr_pd_amt_12, TRUE),
    silver.safe_numeric(line_bene_prmry_pyr_pd_amt_13, TRUE),

    silver.safe_numeric(line_coinsrnc_amt_1,  TRUE), silver.safe_numeric(line_coinsrnc_amt_2,  TRUE),
    silver.safe_numeric(line_coinsrnc_amt_3,  TRUE), silver.safe_numeric(line_coinsrnc_amt_4,  TRUE),
    silver.safe_numeric(line_coinsrnc_amt_5,  TRUE), silver.safe_numeric(line_coinsrnc_amt_6,  TRUE),
    silver.safe_numeric(line_coinsrnc_amt_7,  TRUE), silver.safe_numeric(line_coinsrnc_amt_8,  TRUE),
    silver.safe_numeric(line_coinsrnc_amt_9,  TRUE), silver.safe_numeric(line_coinsrnc_amt_10, TRUE),
    silver.safe_numeric(line_coinsrnc_amt_11, TRUE), silver.safe_numeric(line_coinsrnc_amt_12, TRUE),
    silver.safe_numeric(line_coinsrnc_amt_13, TRUE),

    silver.safe_numeric(line_alowd_chrg_amt_1,  TRUE), silver.safe_numeric(line_alowd_chrg_amt_2,  TRUE),
    silver.safe_numeric(line_alowd_chrg_amt_3,  TRUE), silver.safe_numeric(line_alowd_chrg_amt_4,  TRUE),
    silver.safe_numeric(line_alowd_chrg_amt_5,  TRUE), silver.safe_numeric(line_alowd_chrg_amt_6,  TRUE),
    silver.safe_numeric(line_alowd_chrg_amt_7,  TRUE), silver.safe_numeric(line_alowd_chrg_amt_8,  TRUE),
    silver.safe_numeric(line_alowd_chrg_amt_9,  TRUE), silver.safe_numeric(line_alowd_chrg_amt_10, TRUE),
    silver.safe_numeric(line_alowd_chrg_amt_11, TRUE), silver.safe_numeric(line_alowd_chrg_amt_12, TRUE),
    silver.safe_numeric(line_alowd_chrg_amt_13, TRUE),

    NULLIF(TRIM(line_prcsg_ind_cd_1),  ''), NULLIF(TRIM(line_prcsg_ind_cd_2),  ''),
    NULLIF(TRIM(line_prcsg_ind_cd_3),  ''), NULLIF(TRIM(line_prcsg_ind_cd_4),  ''),
    NULLIF(TRIM(line_prcsg_ind_cd_5),  ''), NULLIF(TRIM(line_prcsg_ind_cd_6),  ''),
    NULLIF(TRIM(line_prcsg_ind_cd_7),  ''), NULLIF(TRIM(line_prcsg_ind_cd_8),  ''),
    NULLIF(TRIM(line_prcsg_ind_cd_9),  ''), NULLIF(TRIM(line_prcsg_ind_cd_10), ''),
    NULLIF(TRIM(line_prcsg_ind_cd_11), ''), NULLIF(TRIM(line_prcsg_ind_cd_12), ''),
    NULLIF(TRIM(line_prcsg_ind_cd_13), ''),

    NULLIF(TRIM(line_icd9_dgns_cd_1),  ''), NULLIF(TRIM(line_icd9_dgns_cd_2),  ''),
    NULLIF(TRIM(line_icd9_dgns_cd_3),  ''), NULLIF(TRIM(line_icd9_dgns_cd_4),  ''),
    NULLIF(TRIM(line_icd9_dgns_cd_5),  ''), NULLIF(TRIM(line_icd9_dgns_cd_6),  ''),
    NULLIF(TRIM(line_icd9_dgns_cd_7),  ''), NULLIF(TRIM(line_icd9_dgns_cd_8),  ''),
    NULLIF(TRIM(line_icd9_dgns_cd_9),  ''), NULLIF(TRIM(line_icd9_dgns_cd_10), ''),
    NULLIF(TRIM(line_icd9_dgns_cd_11), ''), NULLIF(TRIM(line_icd9_dgns_cd_12), ''),
    NULLIF(TRIM(line_icd9_dgns_cd_13), ''),

    NOW()                                                     AS _loaded_at,
    MD5(ROW(
        clm_id, desynpuf_id,
        clm_from_dt, clm_thru_dt,
        icd9_dgns_cd_1, hcpcs_cd_1,
        line_nch_pmt_amt_1, line_nch_pmt_amt_2
    )::TEXT)                                                  AS _row_hash

FROM deduped
ON CONFLICT (clm_id) DO NOTHING;


-- =============================================================================
-- 5. silver.prescription_drug_events
--    Source : staging.prescription_drug_events
--    Key    : pde_id
--    Column mapping:
--      PROD_SRVC_ID   → ndc
--      QTY_DSPNSD_NUM → qty_dispensed
--      DAYS_SUPLY_NUM → days_supply
--      PTNT_PAY_AMT   → ptnt_pay_amt
--      TOT_RX_CST_AMT → tot_rx_cst_amt
-- =============================================================================

WITH deduped AS (
    SELECT DISTINCT ON (pde_id) *
    FROM staging.prescription_drug_events
    ORDER BY pde_id
)
INSERT INTO silver.prescription_drug_events (
    pde_id, desynpuf_id,
    srvc_dt, ndc,
    qty_dispensed, days_supply,
    ptnt_pay_amt, tot_rx_cst_amt,
    _loaded_at, _row_hash
)
SELECT
    pde_id,
    desynpuf_id,

    silver.safe_date(srvc_dt)                                 AS srvc_dt,
    NULLIF(TRIM(prod_srvc_id), '')                            AS ndc,

    silver.safe_numeric(qty_dspnsd_num, FALSE)                AS qty_dispensed,
    silver.safe_smallint(days_suply_num)                      AS days_supply,

    silver.safe_numeric(ptnt_pay_amt,   TRUE)                 AS ptnt_pay_amt,
    silver.safe_numeric(tot_rx_cst_amt, TRUE)                 AS tot_rx_cst_amt,

    NOW()                                                     AS _loaded_at,
    MD5(ROW(
        pde_id, desynpuf_id,
        srvc_dt, prod_srvc_id,
        qty_dspnsd_num, days_suply_num,
        ptnt_pay_amt, tot_rx_cst_amt
    )::TEXT)                                                  AS _row_hash

FROM deduped
ON CONFLICT (pde_id) DO NOTHING;


-- =============================================================================
-- 6. silver.kaggle_encounters
--    Source : staging.kaggle_encounters
--    Key    : surrogate encounter_id generated via ROW_NUMBER()
--    Note   : Staging columns use quoted mixed-case names with spaces
--             e.g. "Date of Admission", "Blood Type" — must be quoted in SQL.
--             Dates in this dataset are ISO format (YYYY-MM-DD), not YYYYMMDD.
-- =============================================================================

WITH transformed AS (
    SELECT
        ROW_NUMBER() OVER () AS encounter_id,
        NULLIF(TRIM(name), '') AS patient_name,
        silver.safe_smallint(age) AS patient_age,
        NULLIF(TRIM(gender), '') AS patient_gender,
        NULLIF(TRIM(blood_type), '') AS blood_type,
        NULLIF(TRIM(medical_condition), '') AS medical_condition,
        CASE
            WHEN date_of_admission IS NULL OR TRIM(date_of_admission) = '' THEN NULL
            ELSE TRIM(date_of_admission)::DATE
        END AS admission_dt,
        CASE
            WHEN discharge_date IS NULL OR TRIM(discharge_date) = '' THEN NULL
            ELSE TRIM(discharge_date)::DATE
        END AS discharge_dt,
        NULLIF(TRIM(admission_type), '') AS admission_type,
        silver.safe_smallint(room_number) AS room_number,
        NULLIF(TRIM(doctor), '') AS doctor,
        NULLIF(TRIM(hospital), '') AS hospital,
        NULLIF(TRIM(insurance_provider), '') AS insurance_provider,
        silver.safe_numeric(billing_amount, TRUE) AS billing_amt,
        NULLIF(TRIM(medication), '') AS medication,
        NULLIF(TRIM(test_results), '') AS test_results,
        NOW() AS _loaded_at,
        MD5(ROW(name, age, gender, date_of_admission, discharge_date, medical_condition, hospital, billing_amount)::TEXT) AS _row_hash
    FROM staging.kaggle_encounters
)
INSERT INTO silver.kaggle_encounters (
    encounter_id,
    patient_name, patient_age, patient_gender, blood_type,
    medical_condition,
    admission_dt, discharge_dt, admission_type, room_number,
    doctor, hospital, insurance_provider, billing_amt,
    medication, test_results,
    _loaded_at, _row_hash
)
SELECT * FROM transformed
ON CONFLICT (encounter_id) DO NOTHING;


-- =============================================================================
-- VERIFICATION (uncomment to run after load)
-- =============================================================================

/*
SELECT 'beneficiary'              AS tbl, COUNT(*) FROM silver.beneficiary
UNION ALL
SELECT 'inpatient_claims',                COUNT(*) FROM silver.inpatient_claims
UNION ALL
SELECT 'outpatient_claims',               COUNT(*) FROM silver.outpatient_claims
UNION ALL
SELECT 'carrier_claims',                  COUNT(*) FROM silver.carrier_claims
UNION ALL
SELECT 'prescription_drug_events',        COUNT(*) FROM silver.prescription_drug_events
UNION ALL
SELECT 'kaggle_encounters',               COUNT(*) FROM silver.kaggle_encounters;

-- Check sex/race decoding
SELECT bene_sex, bene_race, COUNT(*)
FROM silver.beneficiary
GROUP BY 1, 2 ORDER BY 1, 2;

-- Spot-check date constraint violations (should all return 0)
SELECT COUNT(*) FROM silver.inpatient_claims  WHERE clm_thru_dt < clm_from_dt;
SELECT COUNT(*) FROM silver.outpatient_claims WHERE clm_thru_dt < clm_from_dt;
SELECT COUNT(*) FROM silver.carrier_claims    WHERE clm_thru_dt < clm_from_dt;
SELECT COUNT(*) FROM silver.kaggle_encounters WHERE discharge_dt < admission_dt;
*/