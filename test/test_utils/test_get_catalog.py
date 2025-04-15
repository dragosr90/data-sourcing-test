import pytest

from src.utils.get_catalog import get_catalog


@pytest.mark.parametrize(
    ("environment", "catalog_name", "expected_catalog_suffix"),
    [
        (None, "bsrc", "d"),  # Default
        ("Development", "bsrc", "d"),
        ("Test", "bsrc", "t"),
        ("Acceptance", "another_catalog", "a"),
        ("Production", "another_catalog", "p"),
    ],
)
def test_get_catalog(spark_session, environment, catalog_name, expected_catalog_suffix):
    if environment:
        cluster_tag = f'[{{"key": "Environment", "value": "{environment}"}}]'
        spark_session.conf.set(
            "spark.databricks.clusterUsageTags.clusterAllTags",
            cluster_tag,
        )
    assert (
        get_catalog(spark_session, catalog_name)
        == f"{catalog_name}_{expected_catalog_suffix}"
    )
