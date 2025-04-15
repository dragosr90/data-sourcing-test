from pyspark.sql import SparkSession
from pyspark.sql.utils import ParseException

from src.month_setup.metadata_log_tables import tables_new_month
from src.utils.get_catalog import get_catalog
from src.utils.get_dbutils import get_dbutils
from src.utils.logging_util import get_logger

logger = get_logger()


def setup_schemas(spark: SparkSession, month_no: str) -> None:  # pragma: no cover
    schema_list = [
        f"metadata_{month_no}",
        f"log_{month_no}",
        f"stg_{month_no}",
        f"int_{month_no}",
        f"enrich_{month_no}",
        f"dist_{month_no}",
    ]
    for schema in schema_list:
        schema_creation = f"CREATE SCHEMA IF NOT EXISTS {schema}"
        try:
            spark.sql(schema_creation)
        except ParseException as e:
            logger.exception(f"Incorrect syntax for: {schema_creation}")
            logger.exception(str(e).split(";")[0])
        logger.info(f"Created schema {schema}")


def setup_folder_structure(
    spark: SparkSession, month_no: str
) -> None:  # pragma: no cover
    folder_list = [
        "sourcing_landing_data/DIAL",
        "sourcing_landing_data/INSTAGRAM",
        "sourcing_landing_data/NME",
        "sourcing_landing_data/FRRD",
        "masked_data",
        "logs",
    ]
    dbutils = get_dbutils(spark)
    for folder in folder_list:
        # Temporary solution to use data blob
        # Final solution will depend on outcome of volume analysis
        if dbutils.fs.mkdirs(f"dbfs:/mnt/dbmount/{month_no}/{folder}"):
            logger.info(f"Created folder {folder} in data container")
        else:
            logger.error(f"Failed to create folder {folder} in data container")
    logger.info(f"Created folders {folder_list} in data container")


def setup_new_month(spark: SparkSession, month_no: str) -> None:  # pragma: no cover
    spark.sql(f"""USE CATALOG {get_catalog(spark)};""")

    setup_schemas(spark, month_no)
    setup_folder_structure(spark, month_no)
    tables_new_month(spark, month_no)
    logger.info(f"Completed setup for new month {month_no}")
