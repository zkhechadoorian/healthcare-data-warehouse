SELECT
    dp.provider_id,
    dp.provider_type,
    SUM(apy.unique_beneficiary_count)   AS unique_patients,
    SUM(apy.claim_count)                AS total_claims,
    ROUND(SUM(apy.total_paid_amount)::NUMERIC, 2) AS total_reimbursement
FROM gold.agg_provider_year apy
JOIN gold.dim_provider dp
    ON apy.provider_key = dp.provider_key
GROUP BY dp.provider_id, dp.provider_type
ORDER BY unique_patients DESC
LIMIT 10;