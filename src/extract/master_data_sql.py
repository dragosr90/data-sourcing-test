import re
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, expr

from src.utils.logging_util import get_logger
from src.utils.sources_util import get_source

logger = get_logger()


class GetIntegratedData:
    """Get Integrated data, using configuration of `business_logic` input file"""

    def __init__(
        self,
        spark: SparkSession,
        business_logic: dict,
    ) -> None:
        """
        Args:
            spark (SparkSession): Spark session
            business_logic (dict): Configuration for data processing
        """
        self.spark = spark
        self.sources = business_logic["sources"]
        self.transformations = business_logic.get("transformations")

    def get_integrated_data(self) -> DataFrame:
        """Get integrated dataset by reading, filtering, joining and aggregating."""
        data_dict = self.read_source_data()
        return self.transform_data(data_dict=data_dict)

    def read_source_data(self) -> dict[str, DataFrame]:
        """Read and (optionally) filter all source data from catalog."""
        return {
            source["alias"]: (
                self.spark.read.table(source["source"])
                .transform(
                    lambda x: x.filter(source["filter"])  # noqa: B023
                    if "filter" in source.keys()  # noqa: B023
                    else x
                )
                .selectExpr(source.get("columns", "*"))
                .alias(source["alias"])
            )
            for source in self.sources
        }

    def transform_data(self, data_dict: dict[str, DataFrame]) -> DataFrame:
        """Join all data from `data_dict` according to config in business_logic."""
        # If there are no transformations, return first table
        first_table = next(iter(data_dict.values()))
        if not self.transformations:
            return first_table

        # Initialize transformed_data with first table
        transformed_data = first_table

        # Get Integrated dataset from sequential list of transformations
        for tf in self.transformations:
            # Get the transformation type and parameters
            tf_step = next(iter(tf))
            tf_params = tf[tf_step]

            # Check if new starting source is specified
            # If not, keep the already existing transformed_data
            new_source = get_source(tf)
            if new_source:
                transformed_data = data_dict[new_source]
            # Apply the transformation
            transformed_data = self._apply_transformation(
                tf_step=tf_step,
                tf_params=tf_params,
                transformed_data=transformed_data,
                data_dict=data_dict,
            )
            # Add alias to original source data dictionary if specified
            if tf_params.get("alias"):
                data_dict[tf_params["alias"]] = transformed_data
        return transformed_data

    def _apply_transformation(
        self,
        tf_step: str | tuple,
        tf_params: dict,
        transformed_data: DataFrame,
        data_dict: dict[str, DataFrame],
    ) -> DataFrame:
        """Apply a specific transformation step to the data.

        Args:
            tf_step (str | tuple): Type of transformation (join, add_variables, etc.)
            tf_params (dict): Parameters for the transformation
            transformed_data (DataFrame): Current state of the data
            data_dict (dict[str, DataFrame]): Dictionary of all available data sources

        Returns:
            DataFrame: Data after applying the transformation
        """
        # If tf_step is a tuple, extract the actual step name (like "join")
        actual_step = tf_step[0] if isinstance(tf_step, tuple) else tf_step
        # Apply the appropriate transformation based on type
        result = transformed_data  # Default to unchanged data

        if actual_step == "join":
            # For join, need to pass data_dict
            result = self.join(data=transformed_data, data_dict=data_dict, **tf_params)
        elif actual_step == "add_variables":
            result = self.add_variables(transformed_data, **tf_params)
        elif actual_step == "aggregation":
            result = self.aggregation(transformed_data, **tf_params)
        elif actual_step == "pivot":
            result = self.pivot(transformed_data, **tf_params)
        elif actual_step == "filter":
            result = self.filter(transformed_data, **tf_params)
        elif actual_step == "union":
            data_union_keys = list(tf_params["column_mapping"].keys())[1:]
            union_dataframes = [data_dict[k] for k in data_union_keys]
            result = self.union(transformed_data, union_dataframes, **tf_params)
        else:
            # This should never happen as we validate transformation types
            logger.warning(f"Unknown transformation type: {tf_step}")
        return result

    @staticmethod
    def join(
        data: DataFrame,
        data_dict: dict[str, DataFrame],
        condition: list[str],
        right_source: str,
        left_source: str | None = None,
        how: str = "left",
    ) -> DataFrame:
        join_conditions = [parse_join_condition(c) for c in condition]
        data_ = data_dict[left_source] if left_source else data
        return data_.join(
            data_dict[right_source],
            on=join_conditions,
            how=how,
        )

    @staticmethod
    def add_variables(
        data: DataFrame, column_mapping: dict[str, str], alias: str | None = None
    ) -> DataFrame:
        for var, var_expression in column_mapping.items():
            data = data.selectExpr(["*", f"{var_expression} AS {var}"])
        return data.alias(alias) if alias else data

    @staticmethod
    def aggregation(
        data: DataFrame, group: list, column_mapping: dict, alias: str
    ) -> DataFrame:
        """Aggregate Integrated Dataset.

        Since columns are aggregated, table aliases will be lost.
        Therefore the output columns will be renamed based on the
        table alias and the aggregation function. So `max('TBLA.col1')`
        will be renamed to `'max_TBLA_col1'`.

        Args:
            data (DataFrame): Input (Integrated) DataFrame
            group (list[str]): List of column names to group data
            column_mapping (dict): Configuration for aggregation, with the keys as
                input columns and the values as aggregation function
                from `pyspark.sql.functions`
            alias (str): Alias of new aggregated dataset

        Returns:
            DataFrame: Aggregated integrated dataset
        """
        return (
            data.groupBy(group)
            .agg(*[expr(v).alias(k) for k, v in column_mapping.items()])
            .alias(alias)
        )

    @staticmethod
    def pivot(
        data: DataFrame,
        group_cols: list[str],
        pivot_col: str,
        pivot_value_col: str,
        alias: str,
        column_mapping: dict[str, str] | None = None,
        value_suffix: str | None = None,
    ) -> DataFrame:
        """Pivot input data

        Args:
            data (DataFrame): Input DataFrame
            group_cols (list[str]): Column name(s) of group level, so we can run
                aggregations
            pivot_col (str): Name of the column to pivot
            pivot_value_col (str): Column name of values for new columns
            alias (str): Alias of new pivoted dataset
            column_mapping (dict): Mapping of column of values that will be translated
                to columns in the output DataFrame with aggregation function
            value_suffix (str | None, optional): Adding suffix to output column names.
                Defaults to None.

        Returns:
            DataFrame: Pivoted DataFrame
        """
        # Get set of aggregation function(s)
        value_mapping = (
            column_mapping
            if column_mapping
            else {
                row[pivot_col]: "min"
                for row in data.select(pivot_col).distinct().collect()
            }
        )
        agg_funcs = list(set(value_mapping.values()))
        return (
            data.groupBy(group_cols)
            .pivot(pivot_col, list(value_mapping.keys()) if value_mapping else None)
            # Get number of agg_functions x number of values in values list columns
            .agg(*[expr(f"{agg_func}({pivot_value_col})") for agg_func in agg_funcs])
            .select(
                *[col(c) for c in group_cols],
                # Select the grouping columns + renaming pivoted columns
                *[
                    col(f"{k}_{v}({pivot_value_col.split('.')[-1]})").alias(
                        f"{k}{'_' + value_suffix if value_suffix else ''}"
                    )
                    if len(agg_funcs) > 1
                    else col(k).alias(
                        f"{k}{'_' + value_suffix if value_suffix else ''}"
                    )
                    for k, v in value_mapping.items()
                ],
            )
            .alias(alias)
        )

    @staticmethod
    def union(
        data: DataFrame,
        data_union: list[DataFrame],
        alias: str,
        column_mapping: dict[str, dict[str, str]],
    ) -> DataFrame:
        """Update input data dictionary, with unioned datasets.

        Args:
            data_dict (dict[str, DataFrame]): Input sources as dictionary of DataFrames.
            unions (dict[str, dict[str, dict[str, str]]): Nested dictionary with
                unions configurations

        Returns:
            dict[str, DataFrame]: Input sources and unioned sources.
        """
        keys_unions = list(column_mapping.keys())
        input_dataframes = [data, *data_union]
        data_frames = [
            # 1st union from transformed_data. 2nd one from data_union
            d.selectExpr(
                [
                    f"{col_expr} AS {col_name}"
                    for col_name, col_expr in column_mapping[k].items()
                ]
            )
            for d, k in zip(input_dataframes, keys_unions, strict=False)
        ]
        return reduce(DataFrame.unionAll, data_frames).alias(alias)

    @staticmethod
def filter(
    data: DataFrame, 
    conditions: list[str], 
    *, 
    log_reductions: bool = False,
    alias: str | None = None
) -> DataFrame:
    """Filter a dataframe based on one or more conditions.

    Args:
        data (DataFrame): Input DataFrame
        conditions (list[str]): List of condition strings to filter by
        log_reductions (bool, optional): Whether to log row counts before
            and after filtering. Defaults to False as counting
            is an expensive operation.
        alias (str | None, optional): Alias for the filtered DataFrame.
            Defaults to None.

    Returns:
        DataFrame: Filtered DataFrame
    """
    if not conditions:
        logger.warning(
            "No filter conditions provided, returning original dataframe"
        )
        return data.alias(alias) if alias else data

    filtered_df = data
    
    # Only count once at the beginning if log_reductions is True
    if log_reductions:
        input_count = filtered_df.count()
    
    # Apply all filter conditions
    for condition in conditions:
        # Apply the filter condition
        filtered_df = filtered_df.filter(condition)
        # Log the applied condition (always)
        logger.debug(f"Applied filter condition: '{condition}'")
    
    # Only count once at the end and log the reduction if log_reductions is True
    if log_reductions:
        output_count = filtered_df.count()
        logger.info(
            f"Filter conditions reduced rows from {input_count} to {output_count}"
        )
    
    # Return the DataFrame with alias if specified
    return filtered_df.alias(alias) if alias else filtered_df


def parse_join_condition(
    condition_str: str, pattern: str = "(CASE WHEN.*?END)"
) -> Column:
    """Parse join condition.

    Currently support for:
        - CASE WHEN END on left, right or both sides of equal (=) sign.
        - Statements with one equal sign

    Args:
        condition_str (str): Condition to be parsed

    Raises:
        ValueError: If condition string is not supported with pattern

    Returns:
        Column: PySpark column with condition
    """
    conditions = re.findall(pattern, condition_str, re.IGNORECASE)
    case_one_side = 1
    case_two_sides = 2
    no_case = 0
    if len(conditions) == case_two_sides:
        return expr(conditions[0]) == expr(conditions[1])
    if len(conditions) == case_one_side:
        pattern_left_case = f"{pattern} = (.*)"
        pattern_right_case = f"(.*) = {pattern}"
        if re.match(pattern_left_case, condition_str, re.IGNORECASE):
            condition_ = re.search(
                pattern_left_case, condition_str, re.IGNORECASE
            ).groups()  # type: ignore[union-attr]
        elif re.match(pattern_right_case, condition_str, re.IGNORECASE):
            condition_ = re.search(
                pattern_right_case, condition_str, re.IGNORECASE
            ).groups()  # type: ignore[union-attr]
        else:
            msg = f"Seems like the input condition : {condition_str} is not correct"
            raise ValueError(msg)
        return expr(condition_[0]) == expr(condition_[1])
    if len(conditions) == no_case:
        return expr(condition_str.split("=")[0].strip()) == expr(
            condition_str.split("=")[1].strip()
        )
    msg = (
        f"Seems like the input condition : {condition_str} is not correct. "
        f"Regex pattern split the condition in {len(conditions)}: "
        f"{conditions}"
    )
    raise ValueError(msg)

