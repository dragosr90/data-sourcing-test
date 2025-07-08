from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr

from src.config.business_logic import BusinessLogicConfig
from src.utils.logging_util import get_logger
from src.utils.transformations_util import (
    get_required_arguments,
    get_source,
    get_table_name,
    get_tf_step,
)

logger = get_logger()


class GetIntegratedData:
    """Get Integrated data, using configuration of `business_logic` input file"""

    def __init__(
        self,
        spark: SparkSession,
        business_logic: BusinessLogicConfig,
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
            tf_step = get_tf_step(tf)
            tf_params = tf[tf_step]
            tf_params_req = get_required_arguments(tf, self, tf_step)

            # Check if new starting source is specified
            # If not, keep the already existing transformed_data
            new_source = get_source(tf)
            if new_source:
                transformed_data = data_dict[new_source]
            # Apply the transformation
            transformed_data = self._apply_transformation(
                tf_step=tf_step,
                tf_params=tf_params_req,
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
            union_dataframes = [
                data_dict[
                    get_table_name(union_key)
                    if isinstance(union_key, dict)
                    else union_key
                ]
                for union_key in tf_params["column_mapping"]
            ]
            result = self.union(union_dataframes, **tf_params)
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
        alias: str | None = None,
    ) -> DataFrame:
        join_conditions = [expr(c) for c in condition]
        data_ = data_dict[left_source] if left_source else data
        data_ = data_.join(
            data_dict[right_source],
            on=join_conditions,
            how=how,
        )
        return data_.alias(alias) if alias else data_

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
        data_union: list[DataFrame],
        alias: str,
        column_mapping: list[str | dict[str, dict[str, str]]],
        *,
        allow_missing_columns: bool = False,
    ) -> DataFrame:
        """Perform a union on a list of DataFrames with optional column mapping.

        This method unions multiple DataFrames, optionally renaming or mapping
        columns based on the provided `column_mapping`. If `allow_missing_columns`
        is set to True, missing columns in some DataFrames will be filled with null
        values.

        Args:
            data_union (list[DataFrame]): A list of DataFrames to be unioned.
            alias (str): The alias for the resulting unioned DataFrame.
            column_mapping (list[str | dict[str, dict[str, str]]]): A list where
                each element corresponds to a DataFrame in `data_union`. Each
                element can either be:
                - A string (indicating no column mapping for the corresponding
                  DataFrame).
                - A dictionary where the key is the table name and the value is
                  another dictionary mapping column names to their expressions
                  (used for renaming or transforming columns).
            allow_missing_columns (bool, optional): Whether to allow missing
                columns during the union. Defaults to False.

        Returns:
            DataFrame: The resulting unioned DataFrame with the specified alias.
        """
        dataframe_list = []
        for idx, d in enumerate(data_union):
            table_map = column_mapping[idx]
            if isinstance(table_map, dict):
                table_name = get_table_name(table_map)
                dataframe_list.append(
                    d.selectExpr(
                        [
                            f"{col_expr} AS {col_name}"
                            for col_name, col_expr in table_map[table_name].items()
                        ]
                    )
                )
            elif isinstance(table_map, str):
                dataframe_list.append(d)
        return reduce(
            lambda df1, df2: df1.unionByName(
                df2, allowMissingColumns=allow_missing_columns
            ),
            dataframe_list,
        ).alias(alias)

    @staticmethod
    def filter(
        data: DataFrame,
        conditions: list[str],
        *,
        log_reductions: bool = False,
        alias: str | None = None,
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
