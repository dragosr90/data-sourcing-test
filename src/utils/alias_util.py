import re
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

from src.config.schema import get_schema


def create_dataframe(
    spark: SparkSession, data: list[tuple], field_names: list[str] | list[Column]
) -> DataFrame:
    """Greate PySpark DataFrame from specified schema."""
    field_names = [
        c._jc.toString() if isinstance(c, Column) else c  # noqa: SLF001
        for c in field_names
    ]
    schema_output = get_schema(field_names=field_names)
    return spark.createDataFrame(
        data,
        schema=schema_output,
    )


def get_aliases_on_dataframe(
    data: DataFrame,
    input_field_names: list[str] | None = None,
) -> DataFrame:
    """Get aliases on PySpark DataFrame.

    This function set table aliases on test DataFrames, so you can select
    ambiguous columns.

    Args:
        spark (SparkSession): Spark Session
        data (DataFrame): Input DataFrame
        input_field_names (list): List of expected column names with format
            `"{table_alias}.{col_alias}"`

    Note:
        `alias_config` is an dictionary and the total number of elements in the list
        values should match the number of columns in the input DataFrame.

    Example:

        >>> from src.utils.alias_util import get_aliases_on_dataframe
        >>> expected_schema = ["TBL1.col1", "TBL2.col2", "TBL2.col1"]
        >>> df = spark.createDataFrame(
        ...     [(1, "A", 2), (2, "B", 4)], schema=expected_schema
        ... ).transform(get_aliases_on_dataframe)
        >>> df.select(
        ...     "TBL1.col1",
        ...     "TBL2.col2",
        ...     "TBL2.col1",
        ... ).collect()
        [Row(col1=1, col2='A', col1=2), Row(col1=2, col2='B', col1=4)]
    """
    input_field_names = input_field_names if input_field_names else data.columns
    # All table aliases, preserve order
    tbl_aliases = list(
        dict.fromkeys([c.split(".")[0] for c in input_field_names if "." in c])
    )

    # Get list of separate DataFrames, refering to 'original' table
    data_frame_list = [
        data.select(
            [
                # Use tick notation "`col_name`" to select column
                col(f"`{c}`").alias(c.split(".")[1])
                for c in input_field_names
                if re.match(rf"^{tbl_alias}\..*", c)
            ]
        )
        .withColumn("idx", monotonically_increasing_id())
        .alias(tbl_alias)
        for tbl_alias in tbl_aliases
    ]
    return reduce(
        lambda left, right: left.join(right, "idx", how="left"), data_frame_list
    ).drop("idx")


def create_dataframes_with_table_aliases(
    spark: SparkSession,
    data: list[tuple],
    input_field_names: list[str],
    output_field_names: list[str] | list[Column],
) -> tuple[DataFrame, DataFrame]:
    """Create DataFrames with table aliases

    Args:
        spark (SparkSession): Spark session
        data (DataFrame): PySpark DataFrame with artifical column names
        input_field_names (list):  List of expected column names with format
            `"{table_alias}.{col_alias}"`
        output_field_names (list): List of derived attributes

    Returns:
        tuple[DataFrame, DataFrame]: In - and output DataFrames
    """
    output_dataframe = (
        create_dataframe(spark, data, [*input_field_names, *output_field_names])
        .withColumn("id", monotonically_increasing_id())
        .sort("id")
    )
    input_dataframe = output_dataframe.select(
        [f"`{c}`" for c in input_field_names]
    ).transform(get_aliases_on_dataframe, input_field_names=input_field_names)
    return input_dataframe, output_dataframe.select(
        [
            # Rename column "TBL_A.c1" to "c1" to ensure matching schemas
            *[col(f"`{c}`").alias(c.split(".")[1]) for c in input_field_names],
            *output_field_names,
        ]
    )
