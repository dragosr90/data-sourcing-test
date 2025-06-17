from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from src.config.constants import PROJECT_ROOT_DIRECTORY
from src.utils.get_env import get_container_path
from src.utils.logging_util import get_logger

logger = get_logger()


def export_to_parquet(
    spark: SparkSession,
    data: DataFrame,
    run_month: str,
    folder: str,
    file_name: str,
    container: str = "",
    *,
    local: bool = False,
) -> bool:
    """Exports dataframe to parquet file on the storage account

    Exports the data to the path run_month/folder/file_name on the storage account.

    Args:
        spark: Spark session
        data: dataframe to export
        run_month: yyyymm
        folder: location under run_month on the storage account, can be nested
        file_name: name of the file to export to, without extension
        container: name of the container to export to, defaults to run_month
        local: False, make True to test locally and save to test dir instead of blob

    Returns: bool whether export succeeded
    """
    container = container if container else run_month
    file_location = (
        get_container_path(spark, container)
        if not local
        else f"{PROJECT_ROOT_DIRECTORY}/test/data"
    )
    try:
        data.write.parquet(
            f"{file_location}/{folder}/{file_name}.parquet",
            mode="overwrite",
        )
    except (Py4JJavaError, AnalysisException, PySparkException) as e:
        logger.exception(
            f"Could not write data to parquet {folder}/{file_name}.parquet"
        )
        logger.exception(str(e).splitlines()[0])
        return False

    logger.info(f"Exported to {folder}/{file_name}.parquet")
    return True


def write_table_with_exception(data: DataFrame, path: str) -> None:
    """Write table to catalog with error handling.

    Args:
        data (DataFrame): Input DataFrame
        path (str): Destination path
    """
    try:
        data.write.mode("overwrite").saveAsTable(path)
    except AnalysisException:  # pragma: no cover
        logger.exception(f"Error loading {path}")
