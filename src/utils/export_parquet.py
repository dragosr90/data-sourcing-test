from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from src.config.constants import PROJECT_ROOT_DIRECTORY
from src.utils.logging_util import get_logger

logger = get_logger()


def export_to_parquet(
    run_month: str,
    folder: str,
    file_name: str,
    df: DataFrame | None = None,
    *,
    local: bool = False,
) -> bool:
    """Exports dataframe to parquet file on the storage account

    Exports the data to the path run_month/folder/file_name on the storage account.

    Args
        run_month: yyyymm
        folder: location under run_month on the storage account, can be nested
        df: dataframe to export
        local: False, make True to test locally and save to test dir instead of blob

    Returns: bool whether export succeeded
    """
    if not df:
        logger.error("No Dataframe given")
        return False
    file_location = (
        "dbfs:/mnt/dbmount" if not local else f"{PROJECT_ROOT_DIRECTORY}/test/data"
    )
    try:
        df.write.parquet(
            f"{file_location}/{run_month}/{folder}/{file_name}.parquet",
            mode="overwrite",
        )
    except (Py4JJavaError, AnalysisException, PySparkException) as e:
        logger.exception(
            f"Could not write data to parquet {run_month}/{folder}/{file_name}.parquet"
        )
        logger.exception(str(e).splitlines()[0])
        return False

    logger.info(f"Exported to {run_month}/{folder}/{file_name}.parquet")
    return True
