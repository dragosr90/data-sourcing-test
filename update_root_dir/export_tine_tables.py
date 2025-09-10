import hashlib
import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from abnamro_bsrc_etl.utils.export_parquet import export_to_parquet
from abnamro_bsrc_etl.utils.get_dbutils import get_dbutils
from abnamro_bsrc_etl.utils.get_env import get_catalog, get_container_path
from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()


def create_json_metadata(
    spark: SparkSession,
    table: str,
    month_no: str,
    df: DataFrame,
    folder: str = "tine_output",
    tmp_location: str = "file:/Workspace/tmp",
    folder_ext: str = ".parquet",
) -> None:
    """Creates json metadata file for the exported parquet file

    Args:
        spark: Spark session
        table: name of the tine table
        month_no: yyyymm
        df: dataframe that was exported
        folder: location where the files get placed on the storage account,
            in the run_month container
        tmp_location: temporary location to store the parquet file, for md5 hash
            (md5 cannot be calculated directly from blob storage)
        folder_ext: extension of the folder where the parquet file is stored
    """
    container_path = get_container_path(spark, container=month_no)
    dbutils = get_dbutils()
    file_name, extension, modification_time = next(
        (f.name, ".".join(f.name.split(".")[1:]), f.modificationTime)
        for f in dbutils.fs.ls(f"{container_path}/{folder}/{table}{folder_ext}")
        if f.name.endswith(".parquet")
    )
    # Create local copy of the parquet file to be able to generate md5 hash
    dbutils.fs.cp(
        f"{container_path}/{folder}/{table}{folder_ext}",
        f"{tmp_location}/{table}{folder_ext}",
        recurse=True,
    )
    with Path.open(
        Path(f"{tmp_location.split(':')[-1]}/{table}{folder_ext}/{file_name}"), "rb"
    ) as file_to_check:
        # read contents of the file
        data = file_to_check.read()
        md5 = hashlib.md5(data).hexdigest()  # noqa: S324
    # Remove tmp file
    dbutils.fs.rm(f"{tmp_location}/{table}", recurse=True)
    json_dict = {
        "number_of_records": df.count(),
        "number_of_columns": len(df.columns),
        "md5": md5,
        "file_description": table,
        "file_extension": extension,
        "file_name": f"{table}/{file_name}",
        "snapshot_datetime": df.select("REPORTING_DATE")
        .head(1)[0][0]
        .strftime("%Y%m%d"),
        "extraction_datetime": time.strftime(
            "%Y%m%d%H%M%S", time.gmtime(modification_time)
        ),
    }
    dbutils.fs.put(
        f"{container_path}/{folder}/{table}.json",
        json.dumps(json_dict),
        overwrite=True,
    )


def export_tine_tables(
    spark: SparkSession,
    month_no: str,
    tine_table_list: list[str],
    folder: str = "tine_output",
) -> bool:
    """Exports TINE tables to parquet files on the storage account

    Args:
        spark: Spark session
        month_no: yyyymm
        tine_table_list: list of TINE tables to export
        folder: location where the files get placed on the storage account,
            in the run_month container

    Returns: bool whether all exports succeeded
    """
    table_success = {}
    for table in tine_table_list:
        df = spark.read.table(f"{get_catalog(spark)}.dist_{month_no}.{table}")
        table_success[table] = export_to_parquet(
            spark,
            run_month=month_no,
            folder=folder,
            file_name=table,
            data=df,
        )
        if table_success[table]:
            create_json_metadata(
                spark,
                table=table,
                month_no=month_no,
                df=df,
                folder=folder,
            )
    if not all(table_success.values()):
        logger.error("Failure exporting these TINE tables:")
        logger.error(
            "\n".join(
                [table for table, success in table_success.items() if not success]
            )
        )
        return False
    return True
