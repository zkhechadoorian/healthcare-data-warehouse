SELECT
    gold.dim_beneficiary.gender,
    COUNT(*) AS beneficiary_count,
    ROUND(100.0 * SUM(gold.dim_beneficiary.has_diabetes::int)        / COUNT(*), 1) AS pct_diabetes,
    ROUND(100.0 * SUM(gold.dim_beneficiary.has_heart_failure::int)    / COUNT(*), 1) AS pct_heart_failure,
    ROUND(100.0 * SUM(gold.dim_beneficiary.has_copd::int)             / COUNT(*), 1) AS pct_copd,
    ROUND(100.0 * SUM(gold.dim_beneficiary.has_ami::int)              / COUNT(*), 1) AS pct_ami,
    ROUND(100.0 * SUM(has_stroke::int)           / COUNT(*), 1) AS pct_stroke,
    ROUND(100.0 * SUM(has_cancer::int)           / COUNT(*), 1) AS pct_cancer,
    ROUND(100.0 * SUM(has_depression::int)       / COUNT(*), 1) AS pct_depression,
    ROUND(100.0 * SUM(has_ckd::int)              / COUNT(*), 1) AS pct_ckd
FROM gold.dim_beneficiary
WHERE gender IN ('Male', 'Female')
GROUP BY gender
ORDER BY gender;