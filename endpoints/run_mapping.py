"""
Final fix for run_mapping.py - Working around the package path bug
"""

# ============================================
# endpoints/run_mapping.py - FINAL FIX
# ============================================
import sys
from pathlib import Path

# ======== WORKAROUND FOR PACKAGE PATH BUG ========
# The package has a bug where run_mapping passes:
#   yaml_path=PROJECT_ROOT_DIRECTORY / stage / target_mapping
# But parse_yaml then does:
#   PROJECT_ROOT_DIRECTORY / "business_logic" / yaml_path
# 
# This causes a double path issue. We need to patch this.
import abnamro_bsrc_etl.config.constants
import abnamro_bsrc_etl.scripts.run_mapping as run_mapping_module

# Set the correct workspace path
WORKSPACE_PATH = "/Workspace/Users/dragos-cosmin.raduta@nl.abnamro.com/bsrc-etl"
abnamro_bsrc_etl.config.constants.PROJECT_ROOT_DIRECTORY = Path(WORKSPACE_PATH)

# Monkey-patch the run_mapping function to fix the path issue
original_run_mapping = run_mapping_module.run_mapping

def patched_run_mapping(
    spark,
    stage,
    target_mapping,
    run_month,
    dq_check_folder="dq_checks",
    delivery_entity="",
    parent_workflow="",
    run_id=1,
    *,
    local=False
):
    """Patched version that passes just the stage/target_mapping to parse_yaml"""
    from datetime import datetime, timezone
    from abnamro_bsrc_etl.extract.master_data_sql import GetIntegratedData
    from abnamro_bsrc_etl.transform.table_write_and_comment import write_and_comment
    from abnamro_bsrc_etl.transform.transform_business_logic_sql import transform_business_logic_sql
    from abnamro_bsrc_etl.utils.get_env import get_catalog
    from abnamro_bsrc_etl.utils.parse_yaml import parse_yaml
    from abnamro_bsrc_etl.utils.table_logging import write_to_log
    from abnamro_bsrc_etl.validate.run_all import validate_business_logic_mapping
    
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

    # FIX: Pass just stage/target_mapping, not the full path
    business_logic_dict = parse_yaml(
        yaml_path=Path(stage) / target_mapping,  # Just the relative path
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

# Replace the original function with our patched version
run_mapping_module.run_mapping = patched_run_mapping
# ========================================================

from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()

MIN_ARGS = 4  # script + stage + target_mapping + run_month
MAX_ARGS = 7  # + delivery_entity + parent_workflow + run_id

if __name__ == "__main__":
    if len(sys.argv) not in range(MIN_ARGS, MAX_ARGS + 1):
        logger.error(
            "Incorrect number of parameters, expected 3-6: "
            "stage, target_mapping, run_month, "
            "[delivery_entity, parent_workflow, run_id]"
        )
        sys.exit(-1)

    script, stage, target_mapping, run_month = sys.argv[:4]
    remaining_args = sys.argv[4:]

    delivery_entity = "" if len(remaining_args) < 1 else remaining_args[0]
    parent_workflow = "" if len(remaining_args) < 2 else remaining_args[1]
    run_id = 1 if len(remaining_args) < 3 else int(remaining_args[2])

    logger.info(f"Running with parameters: stage={stage}, target_mapping={target_mapping}, run_month={run_month}")
    
    # Use the patched function
    patched_run_mapping(
        spark=spark,  # type: ignore[name-defined]  # noqa: F821
        stage=stage,
        target_mapping=target_mapping,
        run_month=run_month,
        delivery_entity=delivery_entity,
        parent_workflow=parent_workflow,
        run_id=run_id,
    )


# ============================================
# Alternative: Simpler approach - Just call parse_yaml correctly
# ============================================
import sys
from pathlib import Path

# Set up the path
import abnamro_bsrc_etl.config.constants
WORKSPACE_PATH = "/Workspace/Users/dragos-cosmin.raduta@nl.abnamro.com/bsrc-etl"
abnamro_bsrc_etl.config.constants.PROJECT_ROOT_DIRECTORY = Path(WORKSPACE_PATH)

from datetime import datetime, timezone
from abnamro_bsrc_etl.extract.master_data_sql import GetIntegratedData
from abnamro_bsrc_etl.transform.table_write_and_comment import write_and_comment
from abnamro_bsrc_etl.transform.transform_business_logic_sql import transform_business_logic_sql
from abnamro_bsrc_etl.utils.get_env import get_catalog
from abnamro_bsrc_etl.utils.logging_util import get_logger
from abnamro_bsrc_etl.utils.parse_yaml import parse_yaml
from abnamro_bsrc_etl.utils.table_logging import write_to_log
from abnamro_bsrc_etl.validate.run_all import validate_business_logic_mapping

logger = get_logger()

MIN_ARGS = 4
MAX_ARGS = 7

if __name__ == "__main__":
    if len(sys.argv) not in range(MIN_ARGS, MAX_ARGS + 1):
        logger.error(
            "Incorrect number of parameters, expected 3-6: "
            "stage, target_mapping, run_month, "
            "[delivery_entity, parent_workflow, run_id]"
        )
        sys.exit(-1)

    script, stage, target_mapping, run_month = sys.argv[:4]
    remaining_args = sys.argv[4:]

    delivery_entity = "" if len(remaining_args) < 1 else remaining_args[0]
    parent_workflow = "" if len(remaining_args) < 2 else remaining_args[1]
    run_id = 1 if len(remaining_args) < 3 else int(remaining_args[2])

    # Run the mapping logic directly here instead of calling the buggy function
    log_info = {
        "RunID": run_id,
        "Timestamp": datetime.now(tz=timezone.utc),
        "Workflow": parent_workflow,
        "Component": target_mapping,
        "Layer": stage,
        "SourceSystem": delivery_entity,
    }
    
    write_to_log(
        spark=spark,  # type: ignore[name-defined]  # noqa: F821
        run_month=run_month,
        log_table="process_log",
        record={
            **log_info,
            "Status": "Started",
            "Comments": "",
        },
    )

    spark.sql(f"""USE CATALOG {get_catalog(spark)};""")  # type: ignore[name-defined]  # noqa: F821

    # Call parse_yaml with just the relative path
    business_logic_dict = parse_yaml(
        yaml_path=Path(stage) / target_mapping,
        parameters={
            "RUN_MONTH": run_month,
            "DELIVERY_ENTITY": delivery_entity,
        },
    )

    if not validate_business_logic_mapping(spark, business_logic_dict):  # type: ignore[name-defined]  # noqa: F821
        write_to_log(
            spark=spark,  # type: ignore[name-defined]  # noqa: F821
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
        spark=spark, business_logic=business_logic_dict  # type: ignore[name-defined]  # noqa: F821
    ).get_integrated_data()

    transformed_data = transform_business_logic_sql(
        data=data,
        business_logic=business_logic_dict,
    )

    if not write_and_comment(
        spark=spark,  # type: ignore[name-defined]  # noqa: F821
        business_logic=business_logic_dict,
        data=transformed_data,
        run_month=run_month,
        dq_check_folder="dq_checks",
        source_system=delivery_entity,
        local=False,
    ):
        write_to_log(
            spark=spark,  # type: ignore[name-defined]  # noqa: F821
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
        spark=spark,  # type: ignore[name-defined]  # noqa: F821
        run_month=run_month,
        log_table="process_log",
        record={
            **log_info,
            "Status": "Completed",
            "Comments": "",
        },
    )
