"""Script to run the mapping

Args:
  stage
  target_mapping
  run_month
  [delivery_entity]"""

import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pathlib import Path

# from abnamro_bsrc_etl.config.constants import MAPPING_ROOT_DIR
from abnamro_bsrc_etl.extract.master_data_sql import GetIntegratedData
from abnamro_bsrc_etl.transform.table_write_and_comment import write_and_comment
from abnamro_bsrc_etl.transform.transform_business_logic_sql import (
    transform_business_logic_sql,
)
from abnamro_bsrc_etl.utils.get_env import get_catalog
from abnamro_bsrc_etl.utils.logging_util import get_logger
from abnamro_bsrc_etl.utils.parse_yaml import parse_yaml
from abnamro_bsrc_etl.utils.table_logging import write_to_log
from abnamro_bsrc_etl.validate.run_all import validate_business_logic_mapping

MAPPING_ROOT_DIR = Path("/Workspace/Shared/deployment/mappings").resolve()

def run_mapping(
    spark: SparkSession,
    stage: str,
    target_mapping: str,
    run_month: str,
    dq_check_folder: str = "dq_checks",
    delivery_entity: str = "",
    parent_workflow: str = "",
    business_logic_path: Path | str = MAPPING_ROOT_DIR / "business_logic",
    run_id: int = 1,
    *,
    local: bool = False,
) -> None:
    log_info = {
        "RunID": run_id,
        "Timestamp": datetime.now(tz=timezone.utc),
        "Workflow": parent_workflow,
        "Component": target_mapping,
        "Layer": stage,
        "SourceSystem": delivery_entity,
    }
    write_to_log(
        spark=spark,
        run_month=run_month,
        log_table="process_log",
        record={
            **log_info,
            "Status": "Started",
            "Comments": "",
        },
    )

    if not local:
        spark.sql(f"""USE CATALOG {get_catalog(spark)};""")

    business_logic_dict = parse_yaml(
        yaml_path=Path(stage, target_mapping),
        parameters={
            "RUN_MONTH": run_month,
            "DELIVERY_ENTITY": delivery_entity,
        },
    )

    if not validate_business_logic_mapping(spark, business_logic_dict):
        write_to_log(
            spark=spark,
            run_month=run_month,
            log_table="process_log",
            record={
                **log_info,
                "Status": "Failed",
                "Comments": "Mapping validation failed",
            },
        )

        sys.exit(1)

    data = GetIntegratedData(
        spark=spark, business_logic=business_logic_dict
    ).get_integrated_data()

    transformed_data = transform_business_logic_sql(
        data=data,
        business_logic=business_logic_dict,
    )

    if not write_and_comment(
        spark=spark,
        business_logic=business_logic_dict,
        data=transformed_data,
        run_month=run_month,
        dq_check_folder=dq_check_folder,
        source_system=delivery_entity,
        local=local,
    ):
        write_to_log(
            spark=spark,
            run_month=run_month,
            log_table="process_log",
            record={
                **log_info,
                "Status": "Failed",
                "Comments": "DQ validation failed",
            },
        )

        sys.exit(2)

    write_to_log(
        spark=spark,
        run_month=run_month,
        log_table="process_log",
        record={
            **log_info,
            "Status": "Completed",
            "Comments": "",
        },
    )

if __name__ == "__main__":
    logger = get_logger()

    # Get args:
    if len(sys.argv) not in [4, 5]: # First is script name
        logger.error(
            "Incorrect number of parameters, expected 3 or 4: "
            "stage target_mapping run_month[ delivery_entity]"
        )
        sys.exit(-1)

    script, stage, target_mapping, run_month, *del_entity = sys.argv
    delivery_entity = "" if not del_entity else del_entity[0]
    run_mapping(spark, stage, target_mapping, run_month, delivery_entity=delivery_entity)  # type: ignore[name-defined]
