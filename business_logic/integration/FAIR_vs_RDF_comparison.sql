WITH fair_data AS (
    SELECT 
        RatingID,
        AnnualTurnover,
        TotalAssets,
        -- Convert string values to numeric for comparison
        CAST(AnnualTurnover AS DECIMAL(38,2)) AS AnnualTurnover_Numeric,
        CAST(TotalAssets AS DECIMAL(38,2)) AS TotalAssets_Numeric
    FROM bsrc_d.int_{{ RUN_MONTH }}.fair_turnover
    WHERE AnnualTurnover IS NOT NULL 
      AND TotalAssets IS NOT NULL
      AND AnnualTurnover != 'null'
      AND TotalAssets != 'null'
),
rdf_data AS (
    SELECT 
        XPK_COUNTERPARTY,
        TOTAL_TURNOVER,
        TOTAL_ASSETS
    FROM bsrc_d.enter_{{ RUN_MONTH }}.rdf_counterparty
    WHERE TOTAL_TURNOVER IS NOT NULL 
      AND TOTAL_ASSETS IS NOT NULL
)
SELECT 
    f.RatingID,
    r.XPK_COUNTERPARTY,
    
    -- Turnover comparison
    f.AnnualTurnover_Numeric AS FAIR_AnnualTurnover,
    r.TOTAL_TURNOVER AS RDF_TotalTurnover,
    ABS(f.AnnualTurnover_Numeric - r.TOTAL_TURNOVER) AS Turnover_Difference,
    CASE 
        WHEN ABS(f.AnnualTurnover_Numeric - r.TOTAL_TURNOVER) < 0.01 THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS Turnover_Match_Status,
    
    -- Assets comparison
    f.TotalAssets_Numeric AS FAIR_TotalAssets,
    r.TOTAL_ASSETS AS RDF_TotalAssets,
    ABS(f.TotalAssets_Numeric - r.TOTAL_ASSETS) AS Assets_Difference,
    CASE 
        WHEN ABS(f.TotalAssets_Numeric - r.TOTAL_ASSETS) < 0.01 THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS Assets_Match_Status,
    
    -- Overall match
    CASE 
        WHEN ABS(f.AnnualTurnover_Numeric - r.TOTAL_TURNOVER) < 0.01 
         AND ABS(f.TotalAssets_Numeric - r.TOTAL_ASSETS) < 0.01 THEN 'FULL_MATCH'
        ELSE 'MISMATCH'
    END AS Overall_Match_Status
FROM fair_data f
INNER JOIN rdf_data r ON f.RatingID = r.XPK_COUNTERPARTY
ORDER BY 
    Overall_Match_Status DESC,
    f.RatingID;
