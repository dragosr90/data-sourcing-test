from pyspark.sql import SparkSession

from src.utils.export_parquet import export_to_parquet
from src.utils.get_catalog import get_catalog
from src.utils.logging_util import get_logger

logger = get_logger()


def export_tine_tables(
    spark: SparkSession, month_no: str, *, local: bool = False
) -> bool:  # pragma: no cover
    tine_table_list = [
        "tine_npl_collateral_agmt",
        "tine_pl_collateral_agmt",
        "tine_npl_collateral_links",
        "tine_pl_collateral_links",
    ]
    table_success = {}
    for table in tine_table_list:
        df = (
            spark.read.table(f"{get_catalog(spark)}.dist_{month_no}.{table}")
            if not local
            else spark.read.table(f"{table}")
        )
        table_success[table] = export_to_parquet(
            run_month=month_no,
            folder="tine_output",
            file_name=table,
            df=df,
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
