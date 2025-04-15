from collections.abc import Callable

from pyspark.sql import SparkSession

from src.utils.logging_util import get_logger
from src.validate.expressions import Expressions
from src.validate.sources import Sources
from src.validate.transformations import Transformations
from src.validate.yaml import Yaml

logger = get_logger()


def validate_business_logic_mapping(spark: SparkSession, business_logic: dict) -> bool:
    """Validate business logic mapping.

    Using:
    - Overall structure YAML file
    - Sources
    - Transformations
        - Joins
        - Aggregations
        - Pivots
        - Unions
        - Variables
    - Expressions


    Args:
        spark (SparkSession): SparkSession
        business_logic (dict): Business logic mapping

    Returns:
        bool: True, if business logic mapping is succesfully validated.
    """

    def _validate_logic_element(
        cls: Callable, spark: SparkSession, business_logic: dict
    ) -> bool:
        if cls.__name__.lower() == "yaml":
            return cls(business_logic).validate()
        return cls(spark, business_logic).validate()

    return all(
        _validate_logic_element(c, spark, business_logic)
        for c in [
            Yaml,
            Sources,
            Transformations,
            Expressions,
        ]
    )
