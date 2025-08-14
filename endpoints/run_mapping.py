import sys

from abnamro_bsrc_etl.scripts.run_mapping import run_mapping
from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()

if __name__ == "__main__":
    # Get args:
    if len(sys.argv) not in [4, 5]:  # First argument is script name
        logger.error(
            "Incorrect number of parameters, expected 3 or 4: "
            "stage, target_mapping, run_month, "
            "[ delivery_entity, parent_workflow, run_id ]"
        )
        sys.exit(-1)

    script, stage, target_mapping, run_month, *delivery_entity_li = sys.argv
    delivery_entity = "" if not delivery_entity_li else int(delivery_entity_li[0])
    run_mapping(
        spark,  # type: ignore[name-defined]
        stage=stage,
        run_month=run_month,
        delivery_entity=delivery_entity,
    )
