from pyspark.sql import DataFrame

from src.utils.logging_util import get_logger

logger = get_logger()


def transform_business_logic_sql(
    data: DataFrame,
    business_logic: dict,
) -> DataFrame:
    combined_filter = " AND ".join(business_logic.get("filter_target", []))
    transformed_data = data.selectExpr(
        [
            logic + " as " + column
            for column, logic in business_logic["expressions"].items()
        ]
    )
    transformed_data = (
        transformed_data.filter(combined_filter)
        if combined_filter
        else transformed_data
    )
    return (
        transformed_data.drop_duplicates()
        if business_logic.get("drop_duplicates", False)
        else transformed_data
    )
