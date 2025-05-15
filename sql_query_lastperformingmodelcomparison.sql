-- SQL query to compare LastPerformingModel between the two tables
SELECT 
  orig.FAIR_ID,
  orig.COMPANY_ID,
  orig.PORTFOLIO,
  orig.BUSINESSLINE,
  orig.APPROVED_UCR,
  orig.PD_MODEL_VERSION AS ORIGINAL_MODEL_VERSION,
  new.LastPerformingModel AS NEW_LAST_PERFORMING_MODEL,
  CASE 
    WHEN orig.APPROVED_UCR NOT IN ('6', '7', '8') THEN 'PERFORMING'
    ELSE 'DEFAULT'
  END AS RATING_STATUS_TYPE,
  CASE
    WHEN orig.APPROVED_UCR NOT IN ('6', '7', '8') AND orig.PD_MODEL_VERSION = new.LastPerformingModel THEN 'MATCH_PERFORMING'
    WHEN orig.APPROVED_UCR IN ('6', '7', '8') AND orig.PD_MODEL_VERSION != new.LastPerformingModel THEN 'CORRECTLY_MODIFIED'
    WHEN orig.APPROVED_UCR IN ('6', '7', '8') AND orig.PD_MODEL_VERSION = new.LastPerformingModel THEN 'DEFAULT_UNCHANGED'
    ELSE 'MISMATCH'
  END AS VALIDATION_RESULT,
  months_between(current_date(), orig.PD_APPROVAL_DATE) AS MONTHS_SINCE_APPROVAL
FROM 
  bsrc_d.stg_202412.dial_fair_pd_ucr orig
JOIN 
  bsrc_d.int_202412.pd_ucr_actual new
ON 
  orig.FAIR_ID = new.FAIR_ID
WHERE
  orig.RATING_STATUS = 'ACTUAL'
ORDER BY 
  CASE 
    WHEN orig.APPROVED_UCR IN ('6', '7', '8') THEN 1
    ELSE 2
  END,
  orig.COMPANY_ID,
  orig.PORTFOLIO,
  orig.BUSINESSLINE;
