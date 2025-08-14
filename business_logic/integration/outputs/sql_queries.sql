-- =====================================================
-- COMPARISON OF EC vs RC SOURCE TABLES
-- =====================================================

-- 1. CHECK COLUMN DIFFERENCES
-- Get columns from EC table (bsl_ctrl)
SELECT 
    'bsl_ctrl (EC)' as table_name,
    column_name,
    ordinal_position,
    data_type
FROM information_schema.columns
WHERE table_schema = 'stg_202505'
  AND table_name = 'bsl_ctrl_1286_b2_exmp_cond'
ORDER BY ordinal_position;

-- Get columns from RC table (bsl_andes)
SELECT 
    'bsl_andes (RC)' as table_name,
    column_name,
    ordinal_position,
    data_type
FROM information_schema.columns
WHERE table_schema = 'stg_202505'
  AND table_name = 'bsl_andes_1286_b2_exmp_cond'
ORDER BY ordinal_position;

-- 2. COMPARE SPECIFIC EXEMPTIONS (1552, 1553, 1640)
-- Check data from EC source
SELECT 
    'EC_source' as source,
    exemption_condition_id,
    exemption_id,
    exemption_name,
    basel_approach,
    ec_model_input_indicator,
    exemption_end_date,
    reporting_entity_id,
    b2_pd_type_id,
    specialised_lending_type
FROM stg_202505.bsl_ctrl_1286_b2_exmp_cond
WHERE exemption_condition_id IN (1552, 1553, 1640)
ORDER BY exemption_condition_id
LIMIT 10;

-- Check data from RC source
SELECT 
    'RC_source' as source,
    exemption_condition_id,
    exemption_id,
    exemption_name,
    basel_approach,
    ec_model_input_indicator,
    exemption_end_date,
    reporting_entity_id,
    b2_pd_type_id,
    specialised_lending_type
FROM stg_202505.bsl_andes_1286_b2_exmp_cond
WHERE exemption_condition_id IN (1552, 1553, 1640)
ORDER BY exemption_condition_id
LIMIT 10;

-- 3. CHECK IF EXEMPTIONS EXIST IN BOTH TABLES
WITH ec_exemptions AS (
    SELECT DISTINCT 
        exemption_condition_id,
        exemption_name,
        basel_approach,
        ec_model_input_indicator
    FROM stg_202505.bsl_ctrl_1286_b2_exmp_cond
    WHERE exemption_condition_id IN (100, 359, 1552, 1553, 1640, 1169, 1851)
),
rc_exemptions AS (
    SELECT DISTINCT 
        exemption_condition_id,
        exemption_name,
        basel_approach,
        ec_model_input_indicator
    FROM stg_202505.bsl_andes_1286_b2_exmp_cond
    WHERE exemption_condition_id IN (100, 359, 1552, 1553, 1640, 1169, 1851)
)
SELECT 
    COALESCE(ec.exemption_condition_id, rc.exemption_condition_id) as exemption_condition_id,
    ec.exemption_name as ec_name,
    rc.exemption_name as rc_name,
    ec.basel_approach as ec_approach,
    rc.basel_approach as rc_approach,
    ec.ec_model_input_indicator as ec_indicator,
    rc.ec_model_input_indicator as rc_indicator,
    CASE 
        WHEN ec.exemption_condition_id IS NULL THEN 'RC_ONLY'
        WHEN rc.exemption_condition_id IS NULL THEN 'EC_ONLY'
        ELSE 'BOTH'
    END as presence
FROM ec_exemptions ec
FULL OUTER JOIN rc_exemptions rc
    ON ec.exemption_condition_id = rc.exemption_condition_id
ORDER BY exemption_condition_id;

-- 4. COUNT EXEMPTIONS BY EC_MODEL_INPUT_INDICATOR
-- EC source
SELECT 
    'EC_source' as source,
    ec_model_input_indicator,
    COUNT(DISTINCT exemption_condition_id) as unique_exemptions,
    COUNT(*) as total_rows
FROM stg_202505.bsl_ctrl_1286_b2_exmp_cond
GROUP BY ec_model_input_indicator
ORDER BY ec_model_input_indicator;

-- RC source
SELECT 
    'RC_source' as source,
    ec_model_input_indicator,
    COUNT(DISTINCT exemption_condition_id) as unique_exemptions,
    COUNT(*) as total_rows
FROM stg_202505.bsl_andes_1286_b2_exmp_cond
GROUP BY ec_model_input_indicator
ORDER BY ec_model_input_indicator;

-- 5. CHECK FOR DUPLICATE CONDITION IDs WITH DIFFERENT DATA
-- This might explain the bi-directional swapping
WITH ec_data AS (
    SELECT 
        exemption_condition_id,
        exemption_name,
        basel_approach,
        specialised_lending_type,
        COUNT(*) as row_count
    FROM stg_202505.bsl_ctrl_1286_b2_exmp_cond
    WHERE exemption_condition_id IN (1552, 1553, 1640)
    GROUP BY exemption_condition_id, exemption_name, basel_approach, specialised_lending_type
),
rc_data AS (
    SELECT 
        exemption_condition_id,
        exemption_name,
        basel_approach,
        specialised_lending_type,
        COUNT(*) as row_count
    FROM stg_202505.bsl_andes_1286_b2_exmp_cond
    WHERE exemption_condition_id IN (1552, 1553, 1640)
    GROUP BY exemption_condition_id, exemption_name, basel_approach, specialised_lending_type
)
SELECT 
    'EC' as source,
    exemption_condition_id,
    exemption_name,
    basel_approach,
    specialised_lending_type,
    row_count
FROM ec_data
UNION ALL
SELECT 
    'RC' as source,
    exemption_condition_id,
    exemption_name,
    basel_approach,
    specialised_lending_type,
    row_count
FROM rc_data
ORDER BY exemption_condition_id, source;

-- 6. FIND WHICH COLUMNS ARE MISSING IN RC TABLE
-- This will help understand the 29 vs 26 column difference
SELECT 
    column_name,
    'Missing in RC' as status
FROM (
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'stg_202505'
      AND table_name = 'bsl_ctrl_1286_b2_exmp_cond'
    EXCEPT
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'stg_202505'
      AND table_name = 'bsl_andes_1286_b2_exmp_cond'
) missing_columns
UNION ALL
SELECT 
    column_name,
    'Missing in EC' as status
FROM (
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'stg_202505'
      AND table_name = 'bsl_andes_1286_b2_exmp_cond'
    EXCEPT
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'stg_202505'
      AND table_name = 'bsl_ctrl_1286_b2_exmp_cond'
) missing_columns
ORDER BY status, column_name;
