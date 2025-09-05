from pathlib import Path

import pytest
import yaml
from chispa import assert_df_equality

from abnamro_bsrc_etl.config.constants import MAPPING_ROOT_DIR
from abnamro_bsrc_etl.extract.master_data_sql import GetIntegratedData
from abnamro_bsrc_etl.transform.transform_business_logic_sql import (
    transform_business_logic_sql,
)
from abnamro_bsrc_etl.utils.alias_util import (
    create_dataframes_with_table_aliases,
    get_aliases_on_dataframe,
)
from test.data.epr_transform import TransformTestData

# Load expressions from the YAML file
yaml_path = MAPPING_ROOT_DIR / "business_logic/BCR_CPTY.yml"
if yaml_path.exists():
    with Path.open(yaml_path) as stream:
        EXPRESSIONS = yaml.safe_load(stream)["expressions"]
else:
    # Fallback expressions that match the TransformTestData structure
    EXPRESSIONS = {
        # EPR Party expressions
        "PartyType": "EPR_ENT_PARTY.o_PRTY_TYP",
        "CounterpartyName": "EPR_ENT_PARTY.o_PRTY_NM",
        "GlobalCounterpartyIdentifier": "EPR_ENT_PARTY.o_GLOBAL_ID",
        "SnapshotDate": "EPR_ENT_PARTY.o_SNAPSHOTDATE",
        
        # EPR Organisation expressions  
        "NationalIdentifier": "CASE WHEN EPR_ENT_PARTY.o_PRTY_TYP = 'ORG' THEN EPR_ORG.o_MAIN_COC END",
        "CountryOfIncorporation": "EPR_ORG.o_CNTRY_OF_INCORP",
        
        # EPR Relation expressions
        "UltimateParentOrganisation": "EPR_ENT_PARTY_UP.o_GLOBAL_ID",
        "HeadOfficeOrganisation": "CASE WHEN EPR_RLTN_HO.o_RLTNSHP_TYP = 103 THEN EPR_ENT_PARTY_HO.o_GLOBAL_ID END",
        "ImmediateParentOrganisation": "CASE WHEN EPR_RLTN_DP.o_RLTNSHP_TYP = 101 THEN EPR_ENT_PARTY_DP.o_GLOBAL_ID END",
        
        # EPR Address expressions
        "CountryOfResidence": "CASE WHEN EPR_ADDRESS_PREF.o_ADRES_TYPE = 'PREF' THEN EPR_ADDRESS_PREF.o_CNTRY END",
        
        # EPR Local ID expressions
        "InsighterId": "CASE WHEN EPR_LOC_ID_INSIGHT.o_LOCL_SRC_SYSTM = 'INSIGHTER' THEN EPR_LOC_ID_INSIGHT.o_LOCL_ID END",
        
        # EPR External ID expressions
        "LeiCode": "CASE WHEN EPR_EXT_ID_LEI.o_EXTRN_ID_TYP = 'LEI' THEN EPR_EXT_ID_LEI.o_EXTRN_ID_VAL END",
        
        # EPR Financial expressions
        "Group_Annual_Turnover": "CASE WHEN EPR_RLTN_UP.o_RLTNSHP_TYP = 102 AND EPR_FINANCIAL_DET.o_accnt_data_level = 3 AND EPR_RLTN_UP.o_RLTNSHP_STTS = 1 THEN EPR_FINANCIAL_DET.o_annl_trnover END",
        "Group_Annual_Turnover_Currency": "CASE WHEN EPR_RLTN_UP.o_RLTNSHP_TYP = 102 AND EPR_FINANCIAL_DET.o_accnt_data_level = 3 AND EPR_RLTN_UP.o_RLTNSHP_STTS = 1 THEN EPR_FINANCIAL_DET.o_accnt_crrncy END",
        
        # EPR Classification expressions (not in the test data shown, but may be needed)
        "OFFICIAL_SBI_CODE": "CASE WHEN EPR_CLASS_SBI_01.o_CLSFCTN_TYP = 1 THEN EPR_CLASS_SBI_01.o_CLSFCTN_VAL END",
        "OFFICIAL_NACE": "CASE WHEN EPR_CLASS_02_NACE.o_CLSFCTN_TYP = 2 THEN EPR_CLASS_02_NACE.o_CLSFCTN_VAL END",
        "REAL_SBI_CODE": "CASE WHEN EPR_CLASS_SBI_03.o_CLSFCTN_TYP = 3 THEN EPR_CLASS_SBI_03.o_CLSFCTN_VAL END",
        "SME_INDICATOR": "CASE WHEN EPR_CLASS_VAL.o_CLSFCTN_VAL = 1 THEN 'Y' WHEN EPR_CLASS_VAL.o_CLSFCTN_VAL = 2 THEN 'N' END",
    }


@pytest.mark.parametrize(
    ("data_group", "filter_target"),
    [
        ("epr_party", None),
        # Applying 1 filter condition -> `test.data.epr_transform.TransformTestData`
        ("epr_org", ["CountryOfIncorporation = 'NL'"]),
        ("epr_org", None),
        ("epr_relation", None),
        # Applying 2 filter conditions -> `test.data.epr_transform.TransformTestData`
        (
            "epr_relation",
            ["UltimateParentOrganisation = 1234", "HeadOfficeOrganisation = 4321"],
        ),
        ("epr_address", None),
        ("epr_loc_id", None),
        ("epr_ext_id", None),
        ("epr_finan", None),
    ],
)
def test_transform_business_logic_expressions(spark_session, data_group, filter_target):
    """Test Transform Business Logic for different data domains.

        - EPR Counterparty
        - EPR Organisation
        - EPR Relation
        - EPR Address
        - EPR Local Identifier
        - EPR External Identifier
        - EPR Financial details.

    Note:
        Check documentation of `TransformTestData` in `test/data/epr_transform.py`
        for more details on the different scenarios that are tested per data
        domain.
    """
    data_set = TransformTestData(filtered=bool(filter_target)).get_data()[data_group]
    input_dataframe, expected_output = create_dataframes_with_table_aliases(
        spark_session, **data_set
    )
    output_field_names = data_set["output_field_names"]
    expr_dict = {k: v for k, v in EXPRESSIONS.items() if k in output_field_names}
    business_logic = {"expressions": expr_dict}
    if filter_target:
        business_logic["filter_target"] = filter_target
    result = transform_business_logic_sql(input_dataframe, business_logic)
    assert_df_equality(result, expected_output.select(result.columns))


def test_transform_business_logic_aggregation(spark_session):
    business_logic = {
        "sources": ["empty sources"],
        "transformations": [
            {
                "aggregation": {
                    "alias": "TBL_AGG",
                    "group": "TBLA.col01",
                    "column_mapping": {
                        "max_TBLA_col03": "max(TBLA.col03)",
                        "first_TBLB_col09": "first(TBLB.col09)",
                        "avg_TBLB_col10": "avg(TBLB.col10)",
                        "max_TBLA_TRUE_col4b": "max(TBLA_TRUE.col4b)",
                        "max_TBLA_TRUE_col5b": "max(TBLA_TRUE.col5b)",
                        "median_TBLC_col11": "median(TBLC.col11)",
                        "median_TBLC_col12": "median(TBLC.col12)",
                    },
                }
            }
        ],
    }
    input_columns = [
        "TBLA.col01",
        "TBLA.col03",
        "TBLB.col09",
        "TBLB.col10",
        "TBLA_TRUE.col4b",
        "TBLA_TRUE.col5b",
        "TBLC.col11",
        "TBLC.col12",
    ]
    output_columns = [
        "col01",
        "max_TBLA_col03",
        "first_TBLB_col09",
        "avg_TBLB_col10",
        "max_TBLA_TRUE_col4b",
        "max_TBLA_TRUE_col5b",
        "median_TBLC_col11",
        "median_TBLC_col12",
    ]

    input_data = spark_session.createDataFrame(
        [
            (1, 1, 7, 7, 1, 1, 1, 6),
            (1, 8, 6, 8, 1, 2, 2, 1),
            (1, 1, 9, 6, 3, 3, 1, 9),
            (2, 3, 4, 1, 1, 1, 1, 3),
            (2, 1, 1, 7, 1, 1, 2, 1),
        ],
        schema=input_columns,
    ).transform(get_aliases_on_dataframe)
    expected_output = spark_session.createDataFrame(
        [
            (1, 8, 7, 7.0, 3, 3, 1.0, 6.0),
            (2, 3, 4, 4.0, 1, 1, 1.5, 2.0),
        ],
        schema=output_columns,
    )

    obj = GetIntegratedData(spark=spark_session, business_logic=business_logic)
    last_transformation_step = business_logic["transformations"][-1]["aggregation"]
    agg_data = obj.aggregation(input_data, **last_transformation_step)
    assert_df_equality(agg_data, expected_output)
