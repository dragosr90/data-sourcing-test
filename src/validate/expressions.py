from pyspark.sql import SparkSession

from src.config.business_logic import SourceConfig
from src.utils.logging_util import get_logger
from src.utils.sources_util import (
    keep_unique_sources,
    update_join_sources_transformations,
    update_sources,
    update_variables,
)
from src.validate.base import BaseValidate
from src.validate.validate_sql import validate_sql_expressions

logger = get_logger()


class Expressions(BaseValidate):
    def __init__(
        self,
        spark: SparkSession,
        business_logic: dict,
    ) -> None:
        super().__init__(spark, business_logic)
        self.sources: list[SourceConfig] = self.business_logic["sources"]
        self.expressions = self.business_logic["expressions"]
        self.transformations = self.business_logic.get("transformations")

    def validate(self) -> bool:
        """Validate expressions from business logic mapping."""
        available_variables: list[str] = []
        updated_sources: list[SourceConfig] = self.sources

        if self.transformations:
            for tf in self.transformations:
                # Take first key of the dictionary, the transformation step name
                # transformation mapping should not be empty
                tf_step = next(iter(tf))
                if tf_step == "join":
                    left_source, right_source = (
                        tf.get("join").get(f"{s}_source") for s in ["left", "right"]
                    )
                    updated_sources = update_join_sources_transformations(
                        self.sources, updated_sources, left_source, right_source
                    )
                    available_variables = update_variables(available_variables, tf=tf)

                # Update available variables and sources after last transformation step
                updated_sources = update_sources(
                    input_sources=updated_sources, tf=tf, tf_step=tf_step
                )
                # Update 'data dictionary' with new aliases
                self.sources = keep_unique_sources(updated_sources, self.sources)

                # If left source is specified, drop all updated variables
                # If tf_step in aggregation, pivot, union, drop all variables
                available_variables = update_variables(available_variables, tf=tf)

            if tf[tf_step].get("alias"):
                updated_sources = [
                    source
                    for source in updated_sources
                    if source["alias"] == tf[tf_step].get("alias")
                ]

        if not validate_sql_expressions(
            self.spark,
            updated_sources,
            self.expressions,
            available_variables if available_variables else None,
        ):
            return False
        logger.info("Target expressions validated successfully")
        return True
