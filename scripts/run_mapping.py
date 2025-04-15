"""Script to run the mapping

Args:
  stage
  target_mapping
  run_month
  [delivery_entity]"""

import sys

from pyspark.sql import SparkSession

from src.config.constants import PROJECT_ROOT_DIRECTORY
from src.extract.master_data_sql import GetIntegratedData
from src.transform.table_write_and_comment import write_and_comment
from src.transform.transform_business_logic_sql import transform_business_logic_sql
from src.utils.get_catalog import get_catalog
from src.utils.logging_util import get_logger
from src.utils.parse_yaml import parse_yaml
from src.utils.process_logging import write_to_log
from src.validate.run_all import validate_business_logic_mapping


def run_mapping(
    spark: SparkSession,
    stage: str,
    target_mapping: str,
    run_month: str,
    dq_check_folder: str = "dq_checks",
    delivery_entity: str = "",
    parent_workflow: str = "",
    run_id: int = 1,
    *,
    local: bool = False,
) -> None:
    write_to_log(
        spark,
        record={
            "process": target_mapping,
            "status": "Started",
            "run_month": run_month,
            "stage": stage,
            "source_system": delivery_entity,
            "run_id": run_id,
            "workflow": parent_workflow,
        },
    )

    if not local:
        spark.sql(f"""USE CATALOG {get_catalog(spark)};""")

    business_logic_dict = parse_yaml(
        yaml_path=PROJECT_ROOT_DIRECTORY / stage / target_mapping,
        parameters={
            "RUN_MONTH": run_month,
            "DELIVERY_ENTITY": delivery_entity,
        },
    )

    if not validate_business_logic_mapping(spark, business_logic_dict):
        write_to_log(
            spark,
            record={
                "process": target_mapping,
                "status": "Failed",
                "run_month": run_month,
                "stage": stage,
                "source_system": delivery_entity,
                "run_id": run_id,
                "workflow": parent_workflow,
                "comments": "Mapping validation failed",
            },
        )
        sys.exit(1)

    data = GetIntegratedData(spark, business_logic_dict).get_integrated_data()

    transformed_data = transform_business_logic_sql(
        data,
        business_logic_dict,
    )

    if not write_and_comment(
        spark,
        business_logic_dict,
        transformed_data,
        run_month=run_month,
        dq_check_folder=dq_check_folder,
        source_system=delivery_entity,
        local=local,
    ):
        write_to_log(
            spark,
            record={
                "process": target_mapping,
                "status": "Failed",
                "run_month": run_month,
                "stage": stage,
                "source_system": delivery_entity,
                "run_id": run_id,
                "workflow": parent_workflow,
                "comments": "DQ validation failed",
            },
        )
        sys.exit(2)

    write_to_log(
        spark,
        record={
            "process": target_mapping,
            "status": "Completed",
            "run_month": run_month,
            "stage": stage,
            "source_system": delivery_entity,
            "run_id": run_id,
            "workflow": parent_workflow,
        },
    )


if __name__ == "__main__":
    logger = get_logger()

    # Get args:
    if len(sys.argv) not in [3, 4]:
        logger.error(
            "Incorrect number of parameters, expected 3 or 4: "
            "stage target_mapping run_month[ delivery_entity]"
        )
        sys.exit(-1)

    stage, target_mapping, run_month, *del_entity = sys.argv
    delivery_entity = "" if not del_entity else del_entity[0]
    run_mapping(spark, stage, target_mapping, run_month, delivery_entity)  # type: ignore[name-defined]
