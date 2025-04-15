from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, ParseException

from src.utils.logging_util import get_logger
from src.validate.base import BaseValidate

logger = get_logger()


class Sources(BaseValidate):
    def __init__(self, spark: SparkSession, business_logic: dict) -> None:
        super().__init__(spark, business_logic)
        self.sources = self.business_logic["sources"]

    def validate(self) -> bool:
        """Validate sources from list of sources.

        Args:
            spark (SparkSession): SparkSession
            sources (list): List of sources to validate

        Returns:
            bool: True, if sources are succesfully validated.
        """
        if not self.validate_unique_alias():
            return False

        try:
            for source in self.sources:
                columns = source.get("columns", "*")
                (
                    self.spark.read.table(source["source"])
                    .transform(
                        lambda x: x.filter(source["filter"])  # noqa: B023
                        if "filter" in source.keys()  # noqa: B023
                        else x
                    )
                    .selectExpr(columns)
                    .alias(source["alias"])
                    .take(1)
                )
        except ParseException as e:
            logger.exception(f"Incorrect syntax for source {source}")
            logger.exception(str(e).split(";")[0])
            return False
        except AnalysisException as e:
            logger.exception(f"Problem with table/columns for source {source}")
            logger.exception(str(e).split(";")[0])
            return False
        logger.info("Sources validated successfully")
        return True

    def validate_unique_alias(self) -> bool:
        alias = [src["alias"] for src in self.sources]
        duplicates = list(
            {
                string
                for index, string in enumerate(alias)
                if string in alias[index + 1 :]
            }
        )
        if duplicates:
            logger.error(
                f"Risk on ambiguous columns, duplicate of source with alias: {sorted(duplicates)}"  # noqa: E501
            )
            return False
        return True
