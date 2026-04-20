-- =============================================================================
-- TRANSFORM SILVER LAYER
-- Reads from staging.*, cleans and transforms, inserts into silver.*
--
-- Run from psql: psql -d healthcare_dw -f sql/04_transform_silver.sql
--
-- Steps performed per table:
--   1. Type conversion  (TEXT → DATE / NUMERIC / SMALLINT / BOOLEAN)
--   2. Null / missing-value handling
--   3. Deduplication    (keep latest by natural key)
--   4. Code decoding    (sex, race, chronic-condition flags, ESRD indicator)
--   5. Metadata columns (_loaded_at, _row_hash)
-- =============================================================================


-- =============================================================================
-- HELPER: safe date cast
--   SynPUF dates are stored as YYYYMMDD strings.
--   Returns NULL for blank / '00000000' / unparseable values.
-- =============================================================================
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

-- =============================================================================
-- HELPER: safe numeric cast
--   Returns NULL for blank / non-numeric values; defaults to 0 when
--   the caller passes p_default_zero => TRUE (claim amounts).
-- =============================================================================
CREATE OR REPLACE FUNCTION silver.safe_numeric(p_raw TEXT, p_default_zero BOOLEAN DEFAULT FALSE)
RETURNS NUMERIC
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_num NUMERIC;
BEGIN
    IF p_raw IS NULL OR TRIM(p_raw) = '' THEN
        RETURN CASE WHEN p_default_zero THEN 0 ELSE NULL END;
    END IF;
    v_num := TRIM(p_raw)::NUMERIC;
    RETURN v_num;
EXCEPTION
    WHEN OTHERS THEN
        RETURN CASE WHEN p_default_zero THEN 0 ELSE NULL END;
END;
$$;

-- =============================================================================
-- HELPER: safe smallint cast
-- =============================================================================
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
--    Natural key  : desynpuf_id + year
--    Dedup        : across three yearly staging tables; keep all three years
--                   but deduplicate within each (identical rows → keep one)
--    Code decoding:
--      bene_sex_ident_cd  1 → 'Male'   | 2 → 'Female'
--      bene_race_cd       1 → 'White'  | 2 → 'Black' | 3 → 'Other'
--                         4 → 'Asian'  | 5 → 'Hispanic' | 6 → 'North American Native'
--      chronic flags      1 → TRUE     | 2 → FALSE
--      bene_esrd_ind      'Y' → TRUE   | else FALSE
-- =============================================================================

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
WITH combined AS (
    -- Union all three years, tagging each row with its year
    SELECT *, 2008 AS src_year FROM staging.beneficiary_2008
    UNION ALL
    SELECT *, 2009 AS src_year FROM staging.beneficiary_2009
    UNION ALL
    SELECT *, 2010 AS src_year FROM staging.beneficiary_2010
),
deduped AS (
    -- Within each (desynpuf_id, year) keep one row; all columns identical for
    -- true duplicates so MIN() across text cols is fine.
    SELECT DISTINCT ON (desynpuf_id, src_year)
        *
    FROM combined
    ORDER BY desynpuf_id, src_year
)
SELECT
    desynpuf_id,
    src_year                                                  AS year,

    -- dates (YYYYMMDD → DATE)
    silver.safe_date(bene_birth_dt)                           AS bene_birth_dt,
    silver.safe_date(bene_death_dt)                           AS bene_death_dt,

    -- sex decode
    CASE bene_sex_ident_cd
        WHEN '1' THEN 'Male'
        WHEN '2' THEN 'Female'
        ELSE NULL
    END                                                       AS bene_sex,

    -- race decode
    CASE bene_race_cd
        WHEN '1' THEN 'White'
        WHEN '2' THEN 'Black'
        WHEN '3' THEN 'Other'
        WHEN '4' THEN 'Asian'
        WHEN '5' THEN 'Hispanic'
        WHEN '6' THEN 'North American Native'
        ELSE NULL
    END                                                       AS bene_race,

    -- ESRD indicator: 'Y' = TRUE
    UPPER(TRIM(bene_esrd_ind)) = 'Y'                          AS bene_esrd_ind,

    silver.safe_smallint(sp_state_code)                       AS sp_state_code,
    silver.safe_smallint(bene_county_cd)                      AS bene_county_cd,

    -- coverage months
    silver.safe_smallint(bene_hi_cvrage_tot_mons)             AS bene_hi_cvrage_tot_mons,
    silver.safe_smallint(bene_smi_cvrage_tot_mons)            AS bene_smi_cvrage_tot_mons,
    silver.safe_smallint(bene_hmo_cvrage_tot_mons)            AS bene_hmo_cvrage_tot_mons,
    silver.safe_smallint(plan_cvrg_mos_num)                   AS plan_cvrg_mos_num,

    -- chronic conditions: SynPUF codes 1 = YES, 2 = NO
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

    -- reimbursement amounts: missing → 0 (business rule from README)
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
        bene_hi_cvrage_tot_mons, bene_smi_cvrage_tot_mons,
        bene_hmo_cvrage_tot_mons, plan_cvrg_mos_num,
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
--    Natural key  : clm_id + segment
--    Dedup        : exact-duplicate rows → keep one
--    Missing amounts → 0
-- =============================================================================

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
SELECT DISTINCT ON (clm_id, segment)
    clm_id,
    desynpuf_id,
    silver.safe_smallint(segment)                           AS segment,

    silver.safe_date(clm_from_dt)                           AS clm_from_dt,
    silver.safe_date(clm_thru_dt)                           AS clm_thru_dt,
    silver.safe_date(clm_admsn_dt)                          AS clm_admsn_dt,
    silver.safe_date(nch_bene_dschrg_dt)                    AS nch_bene_dschrg_dt,

    NULLIF(TRIM(prvdr_num),   '')                           AS prvdr_num,
    NULLIF(TRIM(at_physn_npi),'')                           AS at_physn_npi,
    NULLIF(TRIM(op_physn_npi),'')                           AS op_physn_npi,
    NULLIF(TRIM(ot_physn_npi),'')                           AS ot_physn_npi,

    silver.safe_numeric(clm_pmt_amt,                  TRUE) AS clm_pmt_amt,
    silver.safe_numeric(nch_prmry_pyr_clm_pd_amt,     TRUE) AS nch_prmry_pyr_clm_pd_amt,
    silver.safe_numeric(clm_pass_thru_per_diem_amt,   TRUE) AS clm_pass_thru_per_diem_amt,
    silver.safe_numeric(nch_bene_ip_ddctbl_amt,       TRUE) AS nch_bene_ip_ddctbl_amt,
    silver.safe_numeric(nch_bene_pta_coinsrnc_lblty_am,TRUE) AS nch_bene_pta_coinsrnc_lblty_am,
    silver.safe_numeric(nch_bene_blood_ddctbl_lblty_am,TRUE) AS nch_bene_blood_ddctbl_lblty_am,

    silver.safe_smallint(clm_utlztn_day_cnt)                AS clm_utlztn_day_cnt,
    NULLIF(TRIM(clm_drg_cd), '')                            AS clm_drg_cd,

    -- diagnosis codes: trim and nullify blanks
    NULLIF(TRIM(admtng_icd9_dgns_cd), ''),
    NULLIF(TRIM(icd9_dgns_cd_1),  ''), NULLIF(TRIM(icd9_dgns_cd_2),  ''),
    NULLIF(TRIM(icd9_dgns_cd_3),  ''), NULLIF(TRIM(icd9_dgns_cd_4),  ''),
    NULLIF(TRIM(icd9_dgns_cd_5),  ''), NULLIF(TRIM(icd9_dgns_cd_6),  ''),
    NULLIF(TRIM(icd9_dgns_cd_7),  ''), NULLIF(TRIM(icd9_dgns_cd_8),  ''),
    NULLIF(TRIM(icd9_dgns_cd_9),  ''), NULLIF(TRIM(icd9_dgns_cd_10), ''),

    -- procedure codes
    NULLIF(TRIM(icd9_prcdr_cd_1), ''), NULLIF(TRIM(icd9_prcdr_cd_2), ''),
    NULLIF(TRIM(icd9_prcdr_cd_3), ''), NULLIF(TRIM(icd9_prcdr_cd_4), ''),
    NULLIF(TRIM(icd9_prcdr_cd_5), ''), NULLIF(TRIM(icd9_prcdr_cd_6), ''),

    -- HCPCS codes
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

    NOW()                                                   AS _loaded_at,
    MD5(ROW(
        clm_id, desynpuf_id, segment,
        clm_from_dt, clm_thru_dt, clm_admsn_dt, nch_bene_dschrg_dt,
        prvdr_num, clm_pmt_amt, clm_utlztn_day_cnt, clm_drg_cd
    )::TEXT)                                                AS _row_hash

FROM staging.inpatient_claims
ORDER BY clm_id, segment
ON CONFLICT (clm_id, segment) DO NOTHING;


-- =============================================================================
-- 3. silver.outpatient_claims
--    Natural key  : clm_id + segment
-- =============================================================================

INSERT INTO silver.outpatient_claims (
    clm_id, desynpuf_id, segment,
    clm_from_dt, clm_thru_dt,
    prvdr_num, at_physn_npi, op_physn_npi, ot_physn_npi,
    clm_pmt_amt, nch_prmry_pyr_clm_pd_amt,
    nch_bene_blood_ddctbl_lblty_am, nch_bene_ptb_ddctbl_amt,
    nch_bene_ptb_coinsrnc_amt,
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
SELECT DISTINCT ON (clm_id, segment)
    clm_id,
    desynpuf_id,
    silver.safe_smallint(segment)                            AS segment,

    silver.safe_date(clm_from_dt)                            AS clm_from_dt,
    silver.safe_date(clm_thru_dt)                            AS clm_thru_dt,

    NULLIF(TRIM(prvdr_num),   '')                            AS prvdr_num,
    NULLIF(TRIM(at_physn_npi),'')                            AS at_physn_npi,
    NULLIF(TRIM(op_physn_npi),'')                            AS op_physn_npi,
    NULLIF(TRIM(ot_physn_npi),'')                            AS ot_physn_npi,

    silver.safe_numeric(clm_pmt_amt,                   TRUE) AS clm_pmt_amt,
    silver.safe_numeric(nch_prmry_pyr_clm_pd_amt,      TRUE) AS nch_prmry_pyr_clm_pd_amt,
    silver.safe_numeric(nch_bene_blood_ddctbl_lblty_am, TRUE) AS nch_bene_blood_ddctbl_lblty_am,
    silver.safe_numeric(nch_bene_ptb_ddctbl_amt,        TRUE) AS nch_bene_ptb_ddctbl_amt,
    silver.safe_numeric(nch_bene_ptb_coinsrnc_amt,      TRUE) AS nch_bene_ptb_coinsrnc_amt,

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

    NOW()                                                    AS _loaded_at,
    MD5(ROW(
        clm_id, desynpuf_id, segment,
        clm_from_dt, clm_thru_dt,
        prvdr_num, clm_pmt_amt
    )::TEXT)                                                 AS _row_hash

FROM staging.outpatient_claims
ORDER BY clm_id, segment
ON CONFLICT (clm_id, segment) DO NOTHING;


-- =============================================================================
-- 4. silver.carrier_claims
--    Natural key  : clm_id + clm_line_num
--    Note: carrier claims are released as two CSV files per sample (A + B)
--          loaded into a single staging table, so dedup is important here.
-- =============================================================================

INSERT INTO silver.carrier_claims (
    clm_id, clm_line_num, desynpuf_id,
    clm_from_dt, clm_thru_dt,
    prvdr_num, at_physn_npi, op_physn_npi, ot_physn_npi,
    clm_pmt_amt, nch_prmry_pyr_clm_pd_amt,
    nch_bene_ptb_ddctbl_amt, nch_bene_ptb_coinsrnc_amt,
    nch_clm_carr_deductible_amt, nch_carr_line_mtus_cnt,
    line_ndc_cd, line_hcpcs_cd, line_icd9_dgns_cd,
    line_place_of_srvc_cd, line_clm_rsn_cd,
    _loaded_at, _row_hash
)
SELECT DISTINCT ON (clm_id, clm_line_num)
    clm_id,
    silver.safe_smallint(clm_line_num)                       AS clm_line_num,
    desynpuf_id,

    silver.safe_date(clm_from_dt)                            AS clm_from_dt,
    silver.safe_date(clm_thru_dt)                            AS clm_thru_dt,

    NULLIF(TRIM(prvdr_num),   '')                            AS prvdr_num,
    NULLIF(TRIM(at_physn_npi),'')                            AS at_physn_npi,
    NULLIF(TRIM(op_physn_npi),'')                            AS op_physn_npi,
    NULLIF(TRIM(ot_physn_npi),'')                            AS ot_physn_npi,

    silver.safe_numeric(clm_pmt_amt,                   TRUE) AS clm_pmt_amt,
    silver.safe_numeric(nch_prmry_pyr_clm_pd_amt,      TRUE) AS nch_prmry_pyr_clm_pd_amt,
    silver.safe_numeric(nch_bene_ptb_ddctbl_amt,        TRUE) AS nch_bene_ptb_ddctbl_amt,
    silver.safe_numeric(nch_bene_ptb_coinsrnc_amt,      TRUE) AS nch_bene_ptb_coinsrnc_amt,
    silver.safe_numeric(nch_clm_carr_deductible_amt,    TRUE) AS nch_clm_carr_deductible_amt,
    silver.safe_numeric(nch_carr_line_mtus_cnt,        FALSE) AS nch_carr_line_mtus_cnt,

    NULLIF(TRIM(line_ndc_cd),           '')                  AS line_ndc_cd,
    NULLIF(TRIM(line_hcpcs_cd),         '')                  AS line_hcpcs_cd,
    NULLIF(TRIM(line_icd9_dgns_cd),     '')                  AS line_icd9_dgns_cd,
    NULLIF(TRIM(line_place_of_srvc_cd), '')                  AS line_place_of_srvc_cd,
    NULLIF(TRIM(line_clm_rsn_cd),       '')                  AS line_clm_rsn_cd,

    NOW()                                                    AS _loaded_at,
    MD5(ROW(
        clm_id, clm_line_num, desynpuf_id,
        clm_from_dt, clm_thru_dt,
        clm_pmt_amt, line_hcpcs_cd, line_icd9_dgns_cd
    )::TEXT)                                                 AS _row_hash

FROM staging.carrier_claims
ORDER BY clm_id, clm_line_num
ON CONFLICT (clm_id, clm_line_num) DO NOTHING;


-- =============================================================================
-- 5. silver.prescription_drug_events
--    Natural key  : pde_id
--    Note: srvc_dt is the dispensing date; fill_dt is the fill date.
--          The CHECK constraint allows fill_dt < srvc_dt or either being NULL.
-- =============================================================================

INSERT INTO silver.prescription_drug_events (
    pde_id, desynpuf_id,
    srvc_dt, fill_dt,
    ndc,
    qty_dispnsed, days_suply_num, phrmcy_srvc_type_cd,
    total_cost_amt, gcdf_dispnsing_fee_amt,
    nch_pde_ip_drug_cvrg_amt, nch_pde_op_drug_cvrg_amt,
    nch_pde_covered_drug_amt, nch_pde_ncvrd_labr_amt,
    nch_pde_bene_resp_amt,
    _loaded_at, _row_hash
)
SELECT DISTINCT ON (pde_id)
    pde_id,
    desynpuf_id,

    silver.safe_date(srvc_dt)                                AS srvc_dt,
    silver.safe_date(fill_dt)                                AS fill_dt,

    NULLIF(TRIM(ndc), '')                                    AS ndc,

    silver.safe_numeric(qty_dispnsed,        FALSE)          AS qty_dispnsed,
    silver.safe_smallint(days_suply_num)                     AS days_suply_num,
    NULLIF(TRIM(phrmcy_srvc_type_cd), '')                    AS phrmcy_srvc_type_cd,

    silver.safe_numeric(total_cost_amt,              TRUE)   AS total_cost_amt,
    silver.safe_numeric(gcdf_dispnsing_fee_amt,      TRUE)   AS gcdf_dispnsing_fee_amt,
    silver.safe_numeric(nch_pde_ip_drug_cvrg_amt,    TRUE)   AS nch_pde_ip_drug_cvrg_amt,
    silver.safe_numeric(nch_pde_op_drug_cvrg_amt,    TRUE)   AS nch_pde_op_drug_cvrg_amt,
    silver.safe_numeric(nch_pde_covered_drug_amt,    TRUE)   AS nch_pde_covered_drug_amt,
    silver.safe_numeric(nch_pde_ncvrd_labr_amt,      TRUE)   AS nch_pde_ncvrd_labr_amt,
    silver.safe_numeric(nch_pde_bene_resp_amt,        TRUE)   AS nch_pde_bene_resp_amt,

    NOW()                                                    AS _loaded_at,
    MD5(ROW(
        pde_id, desynpuf_id, srvc_dt, fill_dt,
        ndc, qty_dispnsed, days_suply_num, total_cost_amt
    )::TEXT)                                                 AS _row_hash

FROM staging.prescription_drug_events
ORDER BY pde_id
ON CONFLICT (pde_id) DO NOTHING;


-- =============================================================================
-- 6. silver.kaggle_encounters
--    Natural key  : encounter_id
--    The Kaggle dataset uses ISO date strings (YYYY-MM-DD), not YYYYMMDD,
--    so we cast directly rather than using safe_date().
-- =============================================================================

INSERT INTO silver.kaggle_encounters (
    encounter_id, desynpuf_id,
    start_date, end_date,
    patient_age, patient_gender,
    description, code,
    _loaded_at, _row_hash
)
SELECT DISTINCT ON (encounter_id)
    encounter_id,
    NULLIF(TRIM(desynpuf_id), '')                            AS desynpuf_id,

    -- ISO date strings: cast directly, NULL on failure
    CASE
        WHEN start_date IS NULL OR TRIM(start_date) = '' THEN NULL
        ELSE TRIM(start_date)::DATE
    END                                                      AS start_date,
    CASE
        WHEN end_date IS NULL OR TRIM(end_date) = '' THEN NULL
        ELSE TRIM(end_date)::DATE
    END                                                      AS end_date,

    silver.safe_smallint(patient_age)                        AS patient_age,

    -- gender: keep only M/F, null everything else
    CASE
        WHEN UPPER(TRIM(patient_gender)) IN ('M','F') THEN UPPER(TRIM(patient_gender))
        ELSE NULL
    END                                                      AS patient_gender,

    NULLIF(TRIM(description), '')                            AS description,
    NULLIF(TRIM(code),        '')                            AS code,

    NOW()                                                    AS _loaded_at,
    MD5(ROW(
        encounter_id, desynpuf_id,
        start_date, end_date,
        patient_age, patient_gender, code
    )::TEXT)                                                 AS _row_hash

FROM staging.kaggle_encounters
ORDER BY encounter_id
ON CONFLICT (encounter_id) DO NOTHING;


-- =============================================================================
-- VERIFICATION QUERIES
-- Run these after the script to spot-check row counts and null rates.
-- =============================================================================

/*
-- Row count comparison: staging vs silver
SELECT 'beneficiary'     AS tbl,
       (SELECT COUNT(*) FROM staging.beneficiary_2008)
       + (SELECT COUNT(*) FROM staging.beneficiary_2009)
       + (SELECT COUNT(*) FROM staging.beneficiary_2010)   AS staging_rows,
       (SELECT COUNT(*) FROM silver.beneficiary)            AS silver_rows
UNION ALL
SELECT 'inpatient_claims',
       (SELECT COUNT(*) FROM staging.inpatient_claims),
       (SELECT COUNT(*) FROM silver.inpatient_claims)
UNION ALL
SELECT 'outpatient_claims',
       (SELECT COUNT(*) FROM staging.outpatient_claims),
       (SELECT COUNT(*) FROM silver.outpatient_claims)
UNION ALL
SELECT 'carrier_claims',
       (SELECT COUNT(*) FROM staging.carrier_claims),
       (SELECT COUNT(*) FROM silver.carrier_claims)
UNION ALL
SELECT 'prescription_drug_events',
       (SELECT COUNT(*) FROM staging.prescription_drug_events),
       (SELECT COUNT(*) FROM silver.prescription_drug_events)
UNION ALL
SELECT 'kaggle_encounters',
       (SELECT COUNT(*) FROM staging.kaggle_encounters),
       (SELECT COUNT(*) FROM silver.kaggle_encounters);

-- Null rate on key date columns
SELECT
    COUNT(*)                                          AS total,
    COUNT(*) FILTER (WHERE bene_birth_dt IS NULL)     AS null_birth_dt,
    COUNT(*) FILTER (WHERE bene_death_dt IS NULL)     AS null_death_dt,
    ROUND(100.0 * COUNT(*) FILTER (WHERE bene_death_dt IS NULL) / COUNT(*), 1)
                                                      AS pct_null_death_dt
FROM silver.beneficiary;

-- Verify sex / race decodes landed correctly
SELECT bene_sex, bene_race, COUNT(*)
FROM silver.beneficiary
GROUP BY 1, 2
ORDER BY 1, 2;

-- Check for date constraint violations (should return 0 rows)
SELECT COUNT(*) FROM silver.inpatient_claims  WHERE clm_thru_dt < clm_from_dt;
SELECT COUNT(*) FROM silver.outpatient_claims WHERE clm_thru_dt < clm_from_dt;
SELECT COUNT(*) FROM silver.carrier_claims    WHERE clm_thru_dt < clm_from_dt;
*/