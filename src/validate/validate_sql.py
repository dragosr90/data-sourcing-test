import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr as pyspark_expr
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException, ParseException

from src.utils.alias_util import get_aliases_on_dataframe
from src.utils.logging_util import get_logger

logger = get_logger()


def generate_dummy_dataframe(
    spark: SparkSession,
    sources: list[dict],
    extra_cols: list[str] | None = None,
) -> DataFrame:
    if extra_cols is None:
        extra_cols = []
    tablecolumn_list = []
    for source in sources:
        alias = source["alias"]
        if "columns" in source:
            pattern = r"\bas\s+([\w]+)"  # Select new column names after 'as'
            tablecolumn_list.extend(
                [
                    f"{alias}.{re.search(pattern, col).group(1) if re.search(pattern, col) else col }"  # type:ignore[union-attr]   # noqa: E501
                    for col in source["columns"]
                ]
            )
        else:
            try:
                df = spark.read.table(source["source"])
                tablecolumn_list.extend([f"{alias}.{col}" for col in df.columns])
            except AnalysisException:
                logger.exception(
                    f"Failed to retrieve columns for source {source['source']}"
                )
    if not tablecolumn_list and not extra_cols:
        return spark.createDataFrame([], StructType([]))
    extra_cols_with_alias = [c for c in extra_cols if "." in c]
    extra_cols_no_alias = set(extra_cols) - set(extra_cols_with_alias)
    empty_data = spark.createDataFrame(
        [],
        ", ".join(
            [f"`{col}`: string" for col in tablecolumn_list + extra_cols_with_alias]
        ),
    )
    data = get_aliases_on_dataframe(
        data=empty_data, input_field_names=tablecolumn_list + extra_cols_with_alias
    )
    return (
        data.withColumns({extra_col: lit("") for extra_col in extra_cols_no_alias})
        if extra_cols_no_alias
        else data
    )


def validate_expressions(
    data: DataFrame,
    expressions: dict[str, str],
    group: list[str] | None = None,
) -> dict[str, str]:
    errors = {}
    for column, logic in expressions.items():
        if len(column) == 0:
            errors[str(logic)] = "Missing column name"
            continue
        if len(str(logic)) == 0:
            errors[str(column)] = "No value/expression given"
            continue
        expr = str(logic) + " as " + column
        try:
            # Force evaluation
            if group:
                data.groupBy(group).agg(pyspark_expr(expr)).take(1)
            else:
                data.selectExpr("*", expr).take(1)
                data = data.selectExpr("*", expr)
        except ParseException as e:
            errors[str(logic).strip()] = str(e).splitlines()[1]
        except AnalysisException as e:
            errors[str(logic).strip()] = str(e).split(";")[0]
    return errors


def validate_sql_expressions(
    spark: SparkSession,
    sources: list[dict],
    expressions: dict[str, str],
    extra_cols: list[str] | None = None,
    group: list[str] | None = None,
) -> bool:
    """Validate the SQL expressions from an input list of sources and expressions.

    First, create dummy dataframe with all columns available for the expressions.

    Args:
        spark (SparkSession): SparkSession
        sources (list): sources as list from `business_logic["sources"]`
        expressions (dict): mapping of target columns and input columns
        extra_cols (list, optional): Optional addititional columns. Defaults to None.

    Returns:
        bool: True if SQL expressions are successfully
    """
    data = generate_dummy_dataframe(spark, sources, extra_cols)
    errors = validate_expressions(data, expressions, group)
    if errors:
        logger.error("Problem with expression(s):")
        logger.error("\n".join([f"{k}: {v}" for k, v in errors.items()]))
        return False
    return True
