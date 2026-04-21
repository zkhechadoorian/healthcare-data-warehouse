SELECT
    CASE
        WHEN age_2008 BETWEEN 0  AND 64 THEN 'Under 65'
        WHEN age_2008 BETWEEN 65 AND 69 THEN '65–69'
        WHEN age_2008 BETWEEN 70 AND 74 THEN '70–74'
        WHEN age_2008 BETWEEN 75 AND 79 THEN '75–79'
        WHEN age_2008 BETWEEN 80 AND 84 THEN '80–84'
        WHEN age_2008 >= 85             THEN '85+'
        ELSE 'Unknown'
    END AS age_group,
    COUNT(*) AS beneficiary_count,
    ROUND(100.0 * SUM(has_diabetes::int)      / COUNT(*), 1) AS pct_diabetes,
    ROUND(100.0 * SUM(has_heart_failure::int) / COUNT(*), 1) AS pct_heart_failure,
    ROUND(100.0 * SUM(has_copd::int)          / COUNT(*), 1) AS pct_copd,
    ROUND(100.0 * SUM(has_ami::int)           / COUNT(*), 1) AS pct_ami,
    ROUND(100.0 * SUM(has_stroke::int)        / COUNT(*), 1) AS pct_stroke,
    ROUND(100.0 * SUM(has_depression::int)    / COUNT(*), 1) AS pct_depression,
    ROUND(100.0 * SUM(has_ckd::int)           / COUNT(*), 1) AS pct_ckd
FROM gold.dim_beneficiary
WHERE age_2008 IS NOT NULL
GROUP BY age_group
ORDER BY MIN(age_2008);