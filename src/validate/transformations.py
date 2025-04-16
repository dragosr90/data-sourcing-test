from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from src.utils.logging_util import get_logger
from src.utils.sources_util import (
    get_required_arguments,
    get_source,
    keep_unique_sources,
    update_join_sources_transformations,
    update_sources,
    update_variables,
)
from src.validate.base import BaseValidate
from src.validate.validate_sql import validate_sql_expressions

logger = get_logger()


JOIN_ON_OPTIONS = [
    "inner",
    "cross",
    "outer",
    "full",
    "fullouter",
    "full_outer",
    "left",
    "leftouter",
    "left_outer",
    "right",
    "rightouter",
    "right_outer",
    "semi",
    "leftsemi",
    "left_semi",
    "anti",
    "leftanti",
    "left_anti",
]


class Transformations(BaseValidate):
    def __init__(
        self,
        spark: SparkSession,
        business_logic: dict,
    ) -> None:
        super().__init__(spark, business_logic)
        self.sources = self.business_logic["sources"]
        self.transformations = self.business_logic.get("transformations")

    def validate_keys(self) -> bool:
        valid_transformation_steps = [
            "join",
            "add_variables",
            "aggregation",
            "pivot",
            "union",
            "filter",
        ]
        if self.transformations:
            for tf in self.transformations:
                all_valid_keys = set(tf.keys()).issubset(
                    set(valid_transformation_steps)
                )
                if not all_valid_keys or set(tf.keys()) == set():
                    logger.error("Structure of transformation steps is incorrect")
                    logger.error(f"Expected sections: {valid_transformation_steps}")
                    logger.error(f"Received sections: {list(tf.keys())}")
                    return False
        return True

    def validate_initial_source(self) -> bool:
        if self.transformations:
            first_tf_step = self.transformations[0]
            if not get_source(first_tf_step):
                logger.error("Initial source should be specified")
                return False
        return True

    def validate_ambiguous_join(self) -> bool:
        """Validate risk on ambiguous columns from incorrect joins.

        In this function we loop over the list of transformations.
        - Every time when left_source is specified we start over again
        - Every time when right_source is specified, include this source in the
          list of potential duplicate sources.
        - Evaluate every time whether there are duplicates in the list
        - If there are duplicates add it to the overal list of duplicates
        - At the end, log all potential ambiguous columns from duplicate table aliases

        Returns:
            bool: True if there is no risk on ambiguous columns, False otherwise
        """

        if not self.transformations:
            return True

        right_sources: list[str] = []
        ambiguous_risks: list[list[str]] = [
            []
            for tf in self.transformations
            if tf.get("join") and tf.get("join").get("left_source")
        ]
        duplicates_idx = 0
        for idx, tf in enumerate(self.transformations):
            if not tf.get("join"):
                continue

            # If there is no left source specified in 1st step, select 1st source
            if idx == 0 and not tf.get("join").get("left_source"):
                right_sources = [self.sources[0]["alias"]]
                ambiguous_risks = [[]]
            # If there is a left_source specified, re-initiate the right_sources
            if tf.get("join").get("left_source"):
                right_sources = [tf.get("join").get("left_source")]
                duplicates_idx = duplicates_idx + 1
            # Add right sources to list
            right_sources = [*right_sources, tf.get("join").get("right_source")]

            # Get duplicate right source in list of right sources
            duplicate = list(
                {
                    string
                    for index, string in enumerate(right_sources)
                    if string in right_sources[index + 1 :]
                }
            )

            # If there are any duplicates, starting from left_source, add this to
            # duplicates sublist
            if duplicate:
                ambiguous_risks[duplicates_idx - 1].append(next(iter(duplicate)))
                right_sources = list(set(right_sources))

        # Drop indices without duplicates starting from left_source
        ambiguous_risks = [d for d in ambiguous_risks if d]

        # For every duplicate starting from a left source, show the table aliases that
        # Have a risk on ambiguous columns
        for d in ambiguous_risks:
            logger.error(
                f"Risk on ambiguous columns, using multiple times the right_source(s) {sorted(set(d))}"  # noqa: E501
            )
        return not any(ambiguous_risks)

    def validate_structure(self) -> bool:
        # Sequential structure validation checks. If one fails, return False and stop
        if self.transformations:
            for check in [
                self.validate_keys,
                self.validate_initial_source,
                self.validate_ambiguous_join,
            ]:
                if not check():
                    return False
        return True

    def validate(self) -> bool:
        validations: list[bool] = []
        available_variables: list[str] = []
        updated_sources: list[dict[str, str | list[str]]] = self.sources

        if not self.transformations:
            logger.info("No transformation steps included")
            return True

        # Sequential structure validation checks. If one fails, return False and stop
        if not self.validate_structure():
            return False

        for tf in self.transformations:
            # Take first key of the dictionary, the transformation step name
            # transformation mapping should not be empty
            tf_step = next(iter(tf))

            # Required before validation, since the data should be created and tested
            # on the join conditions
            if tf_step == "join":
                left_source, right_source = (
                    tf.get("join").get(f"{s}_source") for s in ["left", "right"]
                )
                updated_sources = update_join_sources_transformations(
                    self.sources, updated_sources, left_source, right_source
                )
                available_variables = update_variables(available_variables, tf=tf)

            validations.append(
                self.validate_transformation_step(
                    tf,
                    available_sources=updated_sources,
                    available_variables=available_variables
                    if available_variables
                    else None,
                )
            )

            # Update available variables and sources after last transformation step
            if tf_step in ["aggregation", "pivot", "union"]:
                updated_sources = update_sources(
                    input_sources=updated_sources, tf=tf, tf_step=tf_step
                )
                # Update 'data dictionary' with new aliases
                self.sources = keep_unique_sources(updated_sources, self.sources)

            # If left source is specified, drop all updated variables
            # If tf_step in aggregation, pivot, union, drop all variables
            available_variables = update_variables(available_variables, tf=tf)

        if all(validations):
            logger.info("Transformations validated successfully")
        return all(validations)

    def validate_transformation_step(
        self,
        tf: dict,
        available_sources: list,
        available_variables: list[str] | None = None,
    ) -> bool:
        tf_step = next(iter(tf))
        f_name = f"validate_{tf_step}"
        kwgs = get_required_arguments(tf, self, f_name)
        return getattr(self, f_name)(
            **kwgs,
            available_sources=available_sources,
            available_variables=available_variables,
        )

    def validate_join_source(
        self,
        available_sources: list[dict[str, str | list[str]]],
        left_or_right: str,
        alias: str | None = None,
    ) -> bool:
        available_alias = [
            src["alias"] for src in available_sources if src.get("alias")
        ]
        if alias and alias not in available_alias:
            logger.error("Problem with join")
            logger.error(
                f"No {left_or_right.title()} {alias}.",
            )
            logger.error(f"Available alias: {available_alias}")
            return False
        return True

    def validate_join(
        self,
        right_source: str,
        condition: list[str],
        available_sources: list[dict[str, str | list[str]]],
        left_source: str | None = None,
        how: str = "left",
        available_variables: list[str] | None = None,
    ) -> bool:
        if not self.validate_join_source(available_sources, "left", left_source):
            return False
        if not self.validate_join_source(available_sources, "right", right_source):
            return False
        if not validate_join_conditions(
            spark=self.spark,
            sources=available_sources,
            conditions=condition,
            available_variables=available_variables,
        ):
            return False
        if how not in JOIN_ON_OPTIONS:
            logger.error("Problem with join")
            logger.error(f"how option '{how}' is not a valid option")
            logger.error(f"Possible values for 'how': {JOIN_ON_OPTIONS}")
            return False
        logger.info("Joins validated successfully")
        return True

    def validate_aggregation(
        self,
        column_mapping: dict,
        alias: str,
        group: list[str] | None,
        available_sources: list[dict[str, str | list[str]]],
        available_variables: list[str] | None = None,
    ) -> bool:
        if not validate_sql_expressions(
            spark=self.spark,
            sources=available_sources,
            expressions=column_mapping,
            extra_cols=available_variables,
            group=group,
        ):
            logger.error(f"Issue with aggregated data {alias}")
            return False
        logger.info("Aggregations validated successfully")
        return True

    def validate_pivot(
        self,
        available_sources: list[dict],
        group_cols: list[str],
        pivot_col: str,
        pivot_value_col: str,
        available_variables: list[str] | None,
    ) -> bool:
        available_variables_set = (
            set(available_variables) if available_variables else set()
        )
        available_columns = set()
        for source in available_sources:
            src_alias = source["alias"]
            if "columns" in source:
                available_columns.update(
                    [f"{src_alias}.{col}" for col in source["columns"]]
                )
            else:
                try:
                    df = self.spark.read.table(source["source"])
                    available_columns.update(
                        [f"{src_alias}.{col}" for col in df.columns]
                    )
                except AnalysisException:
                    logger.exception(
                        f"Failed to retrieve columns for source {source['source']}"
                    )
        cols_to_check = {*group_cols, pivot_col, pivot_value_col}
        if not cols_to_check.issubset(available_columns | available_variables_set):
            not_in_list = cols_to_check - available_columns
            logger.error(f"Problem with pivot columns {not_in_list}")
            logger.error(f"Available columns: {available_columns}")
            return False
        logger.info("Pivot validated successfully")
        return True

    def validate_add_variables(
        self,
        available_sources: list,
        column_mapping: dict[str, str],
        available_variables: list[str] | None = None,
    ) -> bool:
        if not validate_sql_expressions(
            spark=self.spark,
            sources=available_sources,
            expressions=column_mapping,
            extra_cols=available_variables,
        ):
            return False
        logger.info("Variables validated successfully")
        return True

    def validate_union(
        self,
        available_sources: list,
        alias: str,
        column_mapping: dict,
        available_variables: list[str] | None = None,
    ) -> bool:
        union_source_tables = column_mapping.keys()
        source_tables = [source["alias"] for source in available_sources]
        if not set(union_source_tables).issubset(set(source_tables)):
            error_tables = list(set(union_source_tables) - set(source_tables))
            logger.error(f"Source table(s) {error_tables} in {alias} not loaded.")
            return False
        target_column_names = [list(v.keys()) for v in column_mapping.values()]
        if not all(
            column_mapping_names == target_column_names[0]
            for column_mapping_names in target_column_names
        ):
            logger.error(
                f"Column mapping names are not identical: {target_column_names}"
            )
            return False

        # Unravel all SQL (column) expressions from the nested dictionary
        if not all(
            validate_sql_expressions(
                self.spark,
                [s for s in available_sources if s["alias"] == table_name],
                table_mapping,
                available_variables,
            )
            for table_name, table_mapping in column_mapping.items()
        ):
            return False

        logger.info("Union expressions validated successfully")
        return True


def validate_filter(
    self,  # noqa: ANN001
    available_sources: list,
    conditions: list[str],
    available_variables: list[str] | None = None,
) -> bool:
    """Validate filter conditions.

    Args:
        self: self
        available_sources (list): List of available sources
        conditions (list[str]): List of filter conditions
        available_variables: List of available variables

    Returns:
        bool: True if conditions are valid, False otherwise
    """
    if not validate_filter_conditions(
        self.spark, available_sources, conditions, available_variables
    ):
        return False
    logger.info("Filter conditions validated successfully")
    return True


def validate_join_conditions(
    spark: SparkSession,
    sources: list[dict[str, str | list[str]]],
    conditions: list[str],
    available_variables: list[str] | None = None,
) -> bool:
    """Validate join conditions from business logic mapping.

    Args:
        spark (SparkSession): SparkSession
        sources (list): Business logic mapping
        conditions (list): List of conditions to validate

    Returns:
        bool: True, if join conditions are succesfully validated.
    """
    cond_parts = {
        f"join_{cond_idx}_{part}": conditions[cond_idx].split("=")[part]
        for cond_idx in range(len(conditions))
        for part in range(2)
    }
    if validate_sql_expressions(
        spark, sources, cond_parts, extra_cols=available_variables
    ):
        logger.debug("Join condition expressions validated successfully")
        return True
    return False


def validate_filter_conditions(
    spark: SparkSession,
    sources: list[dict[str, str | list[str]]],
    conditions: list[str],
    available_variables: list[str] | None = None,
) -> bool:
    """Validate that filter conditions can be executed on the available sources.

    Args:
        spark: SparkSession to use for validation
        sources: List of source dictionaries with alias and columns
        conditions: List of condition strings
        available_variables: List of available variables

    Returns:
        bool: True if all conditions are valid, False otherwise
    """
    if not conditions:
        logger.warning("No filter conditions provided")
        return True
    if not validate_sql_expressions(
        spark,
        sources,
        {f"filter_condition_{i}": cond for i, cond in enumerate(conditions)},
        extra_cols=available_variables,
    ):
        return False
    logger.info("Filter conditions validated successfully")
    return True
