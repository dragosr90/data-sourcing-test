import json

from pyspark.sql import SparkSession


def get_env(spark: SparkSession) -> str:
    """
    Get the environment from the Databricks cluster tags.
    The environment is determined by the "Environment" tag.
    The function returns a single character representing the environment:
    - Development: "d"
    - Test: "t"
    - Acceptance: "a"
    - Production: "p"
    If the "Environment" tag is not found, it defaults to "Development".

    Args:
        spark (SparkSession): The Spark session object.

    Returns str: A single character representing the environment.
    """
    tag_dicts = json.loads(
        spark.conf.get(  # type: ignore[arg-type]
            "spark.databricks.clusterUsageTags.clusterAllTags",
            '[{"key": "Environment", "value": "Development"}]',
        )
    )

    environment = next(
        (item["value"] for item in tag_dicts if item["key"] == "Environment"), ""
    )
    return {"Development": "d", "Test": "t", "Acceptance": "a", "Production": "p"}[
        environment
    ]


def get_catalog(spark: SparkSession, catalog_name: str = "bsrc") -> str:
    """
    Get the catalog name based on the environment.
    The catalog is determined by the catalog name and the environment tag.

    Args:
        spark (SparkSession): The Spark session object.
        catalog_name (str): The base catalog name. Defaults to "bsrc".

    Returns str: The catalog name in the format "<catalog_name>_<env>".
    """
    return f"{catalog_name}_{get_env(spark)}"


def get_storage_account(
    spark: SparkSession, prefix: str = "bsrc", suffix: str = "adls"
) -> str:
    """
    Get the storage account name based on the environment.
    The storage account is determined by the prefix, environment, and suffix.

    Args:
        spark (SparkSession): The Spark session object.
        prefix (str): The prefix for the storage account. Defaults to "bsrc".
        suffix (str): The suffix for the storage account. Defaults to "adls".

    Returns str: The storage account name in the format "<prefix><env><suffix>"."""
    return f"{prefix}{get_env(spark)}{suffix}"


def get_container_path(
    spark: SparkSession, container: str, prefix: str = "bsrc", suffix: str = "adls"
) -> str:
    """
    Get the container path based on the environment.
    The container path is determined by the container name, storage account,
    prefix, and suffix.

    Args:
        spark (SparkSession): The Spark session object.
        container (str): The container name.
        prefix (str): The prefix for the storage account. Defaults to "bsrc".
        suffix (str): The suffix for the storage account. Defaults to "adls".

    Returns str: The container path in the format
        "abfss://<container>@<storage_account>.dfs.core.windows.net".
    """
    return (
        f"abfss://{container}@{get_storage_account(spark, prefix, suffix)}"
        ".dfs.core.windows.net"
    )
