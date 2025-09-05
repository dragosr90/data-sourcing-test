import pytest

from abnamro_bsrc_etl.utils.parse_yaml import parse_yaml


@pytest.mark.parametrize(
    ("delivery_entity", "run_month", "output"),
    [
        (
            "IHUB-FR1",
            "202412",
            {
                "target": "test_catalog.test_schema_202412.ihubfr1_test_target_table",
                "sources": [
                    {
                        "alias": "TBLA",
                        "columns": ["col01", "col02", "col03"],
                        "filter": "col02 = '''IHUB-FR1''' AND col03 = '''IHUB-FR1'''",
                        "source": "schema1.ihubfr1_table_a",
                    },
                    {
                        "alias": "TBLC",
                        "columns": ["col01c", "col11", "col12"],
                        "source": "schema2.ihubfr1_table_c",
                    },
                ],
                "transformations": [
                    {
                        "join": {
                            "left_source": "TBLA",
                            "right_source": "TBLC",
                            "condition": [
                                "TBLA.col01 = TBLC.col01c",
                                "TBLC.col12 = '''IHUB-FR1'''",
                                "TBLC.col12 = '''IHUB-FR1'''",
                            ],
                            "how": "left",
                        }
                    },
                    {
                        "add_variables": {
                            "column_mapping": {
                                "var": "colA in ('''ABC''', '''IHUB-FR1''')"
                            }
                        }
                    },
                ],
                "expressions": {
                    "DeliveryEntity": "'IHUB-FR1'",
                    "DeliveryEntity2": "'IHUB-FR1'",
                    "WrongDeliveryEntity": "ihubfr1",
                    "NewCol01": "TBLA.col01",
                    "NewCol11": "TBLC.col11",
                },
            },
        ),
    ],
)
def test_parse_yaml(delivery_entity, run_month, output):
    """Test Parse YAML file."""
    business_logic = parse_yaml(
        "../test/data/test_parse_yaml.yml",
        parameters={"DELIVERY_ENTITY": delivery_entity, "RUN_MONTH": run_month},
    )
    assert business_logic == output
