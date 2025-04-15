from datetime import datetime, timezone

from pyspark.sql import SparkSession

from src.month_setup.setup_new_month import setup_new_month


def new_month_catalog_setup(spark: SparkSession, month_no: str | None = None) -> None:
    """Sets up new schemas and folder structure for a given month
    Usage (two options):
    1. setup_new_month(spark) - derive the month number based on the current date (prod)
    2. setup_new_month(spark, "yyyymm") - use the provided month number (for testing)
    """

    if month_no is None:
        month_no = datetime.now(tz=timezone.utc).strftime("%Y%m")

    setup_new_month(spark, month_no)
