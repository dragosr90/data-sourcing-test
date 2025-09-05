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

with Path.open(MAPPING_ROOT_DIR / "business_logic/BCR_CPTY.yml") as stream:
    EXPRESSIONS = yaml.safe_load(stream)["expressions"]


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
