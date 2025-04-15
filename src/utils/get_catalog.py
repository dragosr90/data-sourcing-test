import json

from pyspark.sql import SparkSession


def get_catalog(spark: SparkSession, catalog_name: str = "bsrc") -> str:
    tag_dicts = json.loads(
        spark.conf.get(  # type: ignore[arg-type]
            "spark.databricks.clusterUsageTags.clusterAllTags",
            '[{"key": "Environment", "value": "Development"}]',
        )
    )

    environment = next(
        (item["value"] for item in tag_dicts if item["key"] == "Environment"), ""
    )
    env = {"Development": "d", "Test": "t", "Acceptance": "a", "Production": "p"}[
        environment
    ]
    return f"{catalog_name}_{env}"
