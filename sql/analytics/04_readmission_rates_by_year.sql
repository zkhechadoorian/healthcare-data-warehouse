WITH indexed_stays AS (
    -- number each inpatient stay per patient, ordered by admission date
    SELECT
        ic.desynpuf_id,
        ic.clm_id,
        ic.clm_admsn_dt,
        ic.nch_bene_dschrg_dt,
        ic.admtng_icd9_dgns_cd                              AS dx_code,
        EXTRACT(YEAR FROM ic.clm_admsn_dt)::SMALLINT        AS stay_year,
        LAG(ic.nch_bene_dschrg_dt) OVER (
            PARTITION BY ic.desynpuf_id
            ORDER BY ic.clm_admsn_dt
        )                                                   AS prior_discharge_dt
    FROM silver.inpatient_claims ic
    WHERE ic.clm_admsn_dt IS NOT NULL
      AND ic.nch_bene_dschrg_dt IS NOT NULL
      AND ic.admtng_icd9_dgns_cd IS NOT NULL
),
flagged AS (
    -- flag as readmission if admitted within 30 days of prior discharge
    SELECT
        *,
        CASE
            WHEN prior_discharge_dt IS NOT NULL
             AND clm_admsn_dt - prior_discharge_dt <= 30
            THEN 1 ELSE 0
        END AS is_readmission
    FROM indexed_stays
),
top_dx AS (
    -- find the 5 most common admitting diagnoses overall
    SELECT admtng_icd9_dgns_cd AS dx_code
    FROM silver.inpatient_claims
    WHERE admtng_icd9_dgns_cd IS NOT NULL
    GROUP BY admtng_icd9_dgns_cd
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT
    f.dx_code,
    f.stay_year,
    COUNT(*)                                                AS total_stays,
    SUM(f.is_readmission)                                   AS readmissions,
    ROUND(100.0 * SUM(f.is_readmission) / COUNT(*), 2)      AS readmission_rate_pct
FROM flagged f
JOIN top_dx t ON f.dx_code = t.dx_code
GROUP BY f.dx_code, f.stay_year
ORDER BY f.dx_code, f.stay_year;