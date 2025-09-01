from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.utils import ParseException

from abnamro_bsrc_etl.month_setup.metadata_log_tables import tables_new_month
from abnamro_bsrc_etl.utils.get_dbutils import get_dbutils
from abnamro_bsrc_etl.utils.get_env import (
    get_catalog,
    get_container_path,
    get_env,
    get_storage_account,
)
from abnamro_bsrc_etl.utils.logging_util import get_logger

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
        "sourcing_landing_data/NON_SSF",
        "sourcing_landing_data/FRRD",
        "masked_data",
        "logs",
    ]
    dbutils = get_dbutils()
    for folder in folder_list:
        if dbutils.fs.mkdirs(f"{get_container_path(spark, month_no)}/{folder}"):
            logger.info(f"Created folder {folder} in data container")
        else:
            logger.error(f"Failed to create folder {folder} in data container")
    logger.info(f"Created folders {folder_list} in data container")


def create_new_month_container(spark: SparkSession, month_no: str) -> None:
    dbutils = get_dbutils()
    storage_account = get_storage_account(spark)
    tenant_id = "3a15904d-3fd9-4256-a753-beb05cdf0c6d"

    client_id = dbutils.secrets.get(
        scope=f"bsrc-{get_env(spark)}-adb-secretscope",
        key=f"bsrc-{get_env(spark)}-kv-genericsp-id",
    )
    client_secret = dbutils.secrets.get(
        scope=f"bsrc-{get_env(spark)}-adb-secretscope",
        key=f"bsrc-{get_env(spark)}-kv-genericsp",
    )

    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=credential,
    )
    try:
        blob_service_client.create_container(month_no)
    except ResourceExistsError:
        logger.info(f"A container with the name {month_no} already exists")


def setup_new_month(spark: SparkSession, month_no: str) -> None:
    spark.sql(f"""USE CATALOG {get_catalog(spark)};""")

    create_new_month_container(spark, month_no)
    setup_schemas(spark, month_no)
    setup_folder_structure(spark, month_no)
    tables_new_month(spark, month_no)
    logger.info(f"Completed setup for new month {month_no}")
