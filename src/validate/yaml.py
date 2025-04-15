from src.utils.logging_util import get_logger

logger = get_logger()


class Yaml:
    def __init__(self, business_logic: dict) -> None:
        self.business_logic = business_logic

    def validate(self) -> bool:
        required = {
            "target",
            "sources",
            "expressions",
        }
        optional = {
            "transformations",
            "filter_target",
            "drop_duplicates",
        }
        business_logic_set = set(self.business_logic.keys())
        has_required = required.issubset(business_logic_set)
        valid_keys = required.union(optional)
        no_extras = business_logic_set.issubset(valid_keys)
        if not (has_required and no_extras):
            logger.error("Structure of the business logic is incorrect")
            logger.error(
                f"Expected sections: {list(required)} and optionally {list(optional)}"
            )
            logger.error(f"Received sections: {list(self.business_logic.keys())}")
            return False
        logger.info("YAML format validated successfully")
        return True
