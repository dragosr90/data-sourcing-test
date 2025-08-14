from pyspark.sql import SparkSession

from abnamro_bsrc_etl.utils.export_parquet import export_to_parquet
from abnamro_bsrc_etl.utils.get_env import get_catalog
from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()

TINE_TABLE_LIST = [
    "tine_npl_collateral_agmt",
    "tine_pl_collateral_agmt",
    "tine_npl_collateral_links",
    "tine_pl_collateral_links",
    "tine_npl_credit_fac_agmt",
    "tine_npl_guarantee_agmt",
    "tine_npl_lending_expo_agmt",
    "tine_cpty",
    "tine_credit_rating",
    "tine_market_instruments",
]


def export_tine_tables(
    spark: SparkSession, month_no: str, *, local: bool = False
) -> bool:  # pragma: no cover
    table_success = {}
    for table in TINE_TABLE_LIST:
        df = (
            spark.read.table(f"{get_catalog(spark)}.dist_{month_no}.{table}")
            if not local
            else spark.read.table(f"{table}")
        )
        table_success[table] = export_to_parquet(
            spark,
            run_month=month_no,
            folder="tine_output",
            file_name=table,
            data=df,
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
