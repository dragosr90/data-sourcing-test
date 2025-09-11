from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from abnamro_bsrc_etl.config.constants import MAPPING_ROOT_DIR
from abnamro_bsrc_etl.month_setup.metadata_log_tables import get_log_table_schemas
from abnamro_bsrc_etl.utils.logging_util import get_logger

logger = get_logger()


class DQValidation:
    """Perform Data Quality Validations

    Runs available DQ validation checks for a given table,
    if a relevant checks file exists.
    Every validation returns None if no checks available, else result (bool).

    Usage: DQValidation(table_name, schema_name, run_month[, source_system])

    Args:
        table_name: Name of the table to validate, should match filename of checks yml
        schema_name: Schema of the table to validate, generic name, without run month
        run_month: yyyymm as in schema names
        [source_system: Should match source system in table name
            to allow generic checks]
        [catalog: Spark catalog to use]
        [local: True when testing locally]

    """

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        schema_name: str,
        run_month: str,
        source_system: str = "",
        dq_check_folder: str = "dq_checks",
        *,
        local: bool = False,
    ) -> None:
        self.spark = spark
        self.run_month = run_month
        self.local = local
        if not local:  # pragma: no cover
            self.schema = f"{schema_name}_{run_month}"
            self.table = self.spark.read.table(f"{self.schema}.{table_name}")
        else:
            self.schema = ""
            self.table = self.spark.read.table(f"{table_name}")
        self.table_name = table_name

        if source_system:
            self.source_system = source_system
            generic_checks_name = table_name.replace(f"{self.source_system}", "[]")
            path = (
                MAPPING_ROOT_DIR
                / dq_check_folder
                / schema_name
                / f"{generic_checks_name}.yml"
            )
            try:
                with Path.open(path) as yaml_file:
                    generic_checks = yaml.safe_load(yaml_file.read())
            except FileNotFoundError:
                logger.info(
                    "No generic checks file available for "
                    f"{schema_name}.{table_name} at {path}"
                )
                generic_checks = {}
        else:
            self.source_system = ""
            generic_checks = {}

        try:
            path = (
                MAPPING_ROOT_DIR / dq_check_folder / schema_name / f"{table_name}.yml"
            )
            with Path.open(path) as yaml_file:
                specific_checks_dict = yaml.safe_load(yaml_file.read())
        except FileNotFoundError:
            logger.info(
                f"No specific checks file available for {schema_name}.{table_name} at "
                f"{path}"
            )
            specific_checks_dict = {}
        self.columns: dict | int = (
            specific_checks_dict.get("columns", {})
            if "columns" in specific_checks_dict
            else generic_checks.get("columns", {})
        )
        self.pk: list[str] = [
            *specific_checks_dict.get("PK", []),
            *generic_checks.get("PK", []),
        ]
        self.notnulls: list[str] = [
            *specific_checks_dict.get("not_null", []),
            *generic_checks.get("not_null", []),
        ]
        self.unique_cols: list[str] = [
            *specific_checks_dict.get("unique", []),
            *generic_checks.get("unique", []),
        ]
        self.refchecks: list = [
            *specific_checks_dict.get("referential_checks", []),
            *generic_checks.get("referential_checks", []),
        ]

    def log_dq_table(
        self, check_type: str, check_details: str, result: bool | None, error: str = ""
    ) -> None:  # pragma: no cover
        if self.local:
            return
        schema = get_log_table_schemas("log_dq_validation")["log_dq_validation"]
        first_n_chars = 300
        log_entry = [
            {
                "Timestamp": datetime.now(tz=timezone.utc),
                "SourceSystem": self.source_system,
                "Schema": self.schema,
                "TableName": self.table_name,
                "ProcessLogID": None,
                "CheckType": check_type,
                "CheckDetails": check_details,
                "CheckResult": {True: "PASS", False: "FAIL", None: "NA"}.get(result),
                "Error": error[:first_n_chars]
                + ("..." if len(error) > first_n_chars else ""),
            }
        ]

        log_entry_df = self.spark.createDataFrame(
            log_entry,
            schema=schema,
        )
        try:
            log_entry_df.write.mode("append").saveAsTable(
                f"log_{self.run_month}.log_dq_validation"
            )
        except AnalysisException as e:
            logger.exception(f"Error writing to log_{self.run_month}.log_dq_validation")
            logger.exception(str(e).split(";")[0])

    def checks(self, *, functional: bool = False) -> bool | None:
        """Run all available DQ checks.

        For all checks in the object, runs the relevant check function.

        Args:
            functional: If True, run referential integrity checks,
                        else only technical checks

        Returns:
            True if all checks are successful
            False if any check fails
            None if no checks are found

        """
        validation_result = [
            self.columns_datatypes().get("result", None),
            self.primary_key().get("result", None),
            self.not_null().get("result", None),
            self.unique(individual=True).get("result", None),
            self.referential_integrity().get("result", None) if functional else None,
        ]
        if all(v is None for v in validation_result):
            logger.info("No checks done")
            self.log_dq_table(
                check_type="result",
                check_details="",
                result=None,
            )
            return None
        result = all(v for v in validation_result if v is not None)
        if result:
            logger.info("Checks completed successfully")
            self.log_dq_table(
                check_type="result",
                check_details="",
                result=True,
            )
        else:
            self.log_dq_table(
                check_type="result",
                check_details="",
                result=False,
            )
            logger.error("Checks completed - DQ issues found")
        return result

    def columns_datatypes(self) -> dict:
        """Check number of columns and names/datatypes

        Given a number or dictionary {name:type} in self.columns, checks:
        - Number of columns matches with tables (both for dict and number)
        - Column names and datatypes (only for dictionary)

        Returns dictionary containing
        "result":
            True if columns match specification
            False if there is a mismatch
            None if self.columns is empty, no column check done

        """
        if not self.columns:
            logger.info("No columns to check")
            return {}

        error_msg = ""
        if isinstance(self.columns, dict):
            expected_cols = len(self.columns.keys())
        else:
            expected_cols = self.columns
        num_cols_match = len(self.table.columns) == expected_cols
        if num_cols_match:
            logger.info(
                f"Number of columns matches: {expected_cols} expected and received"
            )
        else:
            error_msg += f"Received {len(self.table.columns)} columns\n"
            logger.error(
                "Number of columns incorrect: "
                f"expected {expected_cols}, "
                f"received {len(self.table.columns)}"
            )

        if isinstance(self.columns, dict):
            # Note: this requires very specific matches: int != integer
            datatypes_match = dict(self.table.dtypes) == self.columns
            if datatypes_match:
                logger.info("Datatypes match")
            else:
                expected_diff = sorted(
                    set(self.columns.items()) - set(self.table.dtypes)
                )
                received_diff = sorted(
                    set(self.table.dtypes) - set(self.columns.items())
                )
                error_msg += (
                    "Column differences: "
                    f"expected {expected_diff}, received {received_diff}"
                )
                logger.error(
                    "Mismatch in datatypes, differences: "
                    f"expected {expected_diff}, received {received_diff}"
                )
        else:
            logger.info("No datatypes checked")
            datatypes_match = True

        self.log_dq_table(
            "columns_datatypes",
            f"Expected {expected_cols} columns",
            num_cols_match and datatypes_match,
            error_msg,
        )
        return {"result": num_cols_match and datatypes_match}

    def primary_key(self) -> dict:
        """Check the primary key for uniqueness and nulls

        Given a primary key as list of columns in self.pk, checks:
        - Is the combination of the columns unique?
        - Are there any null values in any of the PK columns?

        Returns dictionary containing
        "result":
            True if unique and no nulls
            False if there is a duplicate or null value
            None if self.pk is empty, no primary key check done

        """
        if not self.pk:
            logger.info("No Primary Key to check")
            return {}

        pk_check = self.unique(unique_cols=self.pk, individual=False, log=False)

        not_null_check = self.not_null(notnulls=self.pk, log=False)

        if pk_check.get("result", None) and not_null_check.get("result", None):
            logger.info(f"Primary key {self.pk} validated successfully")
            result = True
        else:
            logger.error(f"Issues found in primary key {self.pk}")
            result = False

        self.log_dq_table(
            "Primary Key",
            str(self.pk),
            result,
            pk_check.get("error", "") + not_null_check.get("error", ""),
        )
        return {"result": result}

    def unique(
        self,
        unique_cols: list | None = None,
        *,
        individual: bool = False,
        log: bool = True,
    ) -> dict:
        """Check uniqueness of columns

        Given a list of columns that need to be unique, checks whether the columns
        are unique or whether there are any duplicates.

        Args:
            unique_cols: List of columns that need to be unique
            individual: If True every column needs to be unique by itself
                        If False, the combination of the columns needs to be unique
            log: Bool whether to log to table
                 (True when running separately, False when checked from primary_key)

        Returns dictionary containing
        "result":
            True if all unique
            False if any duplicates are found
            None if unique_cols is empty, no uniqueness check done
        "error":
            String containing details for the failure
            Passed to primary_key check to log in dq table

        """

        if not unique_cols:
            unique_cols = self.unique_cols
        if not unique_cols:
            logger.info("No unique columns to check")
            return {}
        if not individual or len(unique_cols) == 1:
            uniqueness = (
                self.table.select(unique_cols).distinct().count() == self.table.count()
            )
            if uniqueness:
                logger.info(f"{unique_cols} is unique")
                error_msg = ""
            else:
                duplicates = (
                    self.table.select(unique_cols)
                    .groupBy(unique_cols)
                    .count()
                    .filter(col("count") > 1)
                )
                error_msg = f"Duplicates: {duplicates.collect()}\n"
                logger.error(
                    f"Duplicates found in {unique_cols}: {duplicates.collect()}"
                )
        else:
            uniqueness = True
            error_msg = ""
            for c in unique_cols:
                unique_check = self.unique(individual=True, unique_cols=[c])
                uniqueness = all([uniqueness, unique_check.get("result", None)])
                error_msg += unique_check.get("error", "")
        if log:
            self.log_dq_table("Unique", str(unique_cols), uniqueness, error_msg)
        return {"result": uniqueness, "error": error_msg}

    def not_null(self, notnulls: list | None = None, *, log: bool = True) -> dict:
        """Check nulls in not nullable columns

        Given a list of not nullable columns, checks if any nulls are found.

        Args:
            notnulls: List of columns that should not contain null
            log: Bool whether to log to table
                 (True when running separately, False when checked from primary_key)

        Returns dictionary containing
        "result":
            True if no nulls
            False if there is a null value in any of the columns
            None if notnulls is empty, no not nullable check done
        "error":
            String containing details for the failure
            Passed to primary_key check to log in dq table

        """
        if not notnulls:
            notnulls = self.notnulls
        if not notnulls:
            logger.info("No not nullable columns to check")
            return {}
        error_msg = ""
        not_null = {
            not_null_col: self.table.where(col(not_null_col).isNull()).count() == 0
            for not_null_col in notnulls
        }
        if all(not_null.values()):
            logger.info(f"No nulls found in {notnulls}")
        else:
            columns_with_null = [c for c in not_null if not not_null[c]]
            error_msg += f"Nulls in: {columns_with_null}"
            logger.error(f"Nulls found in not nullable column(s): {columns_with_null}")
        if log:
            self.log_dq_table(
                "Not Null", str(notnulls), all(not_null.values()), error_msg
            )
        return {"result": all(not_null.values()), "error": error_msg}

    def referential_integrity(self) -> dict:
        """Check the referential integrity rules

        Given a check in self.refchecks, with
            column: Column in current table to be validated
            [filter: Filter on current table to ignore certain values]
            reference_schema: Schema for reference table (month independent)
            reference_table: Reference table name
            reference_column: Lookup column in reference table
        Checks whether all values from table.column
        with optional filter
        are in reference_schema.reference_table.reference_column

        Returns dictionary containing
        "result":
            True if all values are found
            False if any value is not present in the reference
            None if self.refchecks is empty, no referential integrity check done

        """
        if not self.refchecks:
            logger.info("No referential integrity checks")
            return {}
        checks_results = []
        for ref_check in self.refchecks:
            error_msg = ""
            main_table = (
                self.table.transform(
                    lambda x, check=ref_check: x.filter(check["filter"])
                    if "filter" in check.keys()
                    else x
                )
                .select(ref_check["column"])
                .withColumnRenamed(ref_check["column"], ref_check["reference_column"])
                .drop_duplicates()
                .alias("MAIN")
            )
            if not self.local:  # pragma: no cover
                reference_schema = f"{ref_check['reference_schema']}_{self.run_month}."
            else:
                reference_schema = ""
            reference_table = (
                self.spark.read.table(
                    f"{reference_schema}{ref_check['reference_table']}"
                )
                .select(ref_check["reference_column"])
                .drop_duplicates()
                .alias("REF")
            )
            join_not_found = main_table.join(
                reference_table, on=ref_check["reference_column"], how="left"
            ).where(f"REF.{ref_check['reference_column']} IS NULL")
            result = join_not_found.count() == 0
            filter_statement = (
                f" with filter '{ref_check['filter']}'" if "filter" in ref_check else ""
            )

            if result:
                logger.info(
                    "Referential check successful, "
                    f"all values from {ref_check['column']}{filter_statement} "
                    "are present in "
                    f"{ref_check['reference_table']}.{ref_check['reference_column']}"
                )
            else:
                missing_vals = str(
                    sorted(
                        [
                            r[ref_check["reference_column"]]
                            for r in join_not_found.selectExpr(
                                f"MAIN.{ref_check['reference_column']}"
                            ).collect()
                        ]
                    )
                )
                error_msg += f"{join_not_found.count()} missing values: {missing_vals}"
                logger.error(
                    "Referential check failed, "
                    f"not all values from {ref_check['column']}{filter_statement}"
                    " are present in "
                    f"{ref_check['reference_table']}.{ref_check['reference_column']}\n"
                    f"{join_not_found.count()} values not available: {missing_vals}"
                )
            self.log_dq_table(
                "Referential Integrity",
                f"All {ref_check['column']} in {ref_check['reference_schema']}."
                f"{ref_check['reference_table']}.{ref_check['reference_column']}"
                + filter_statement,
                result,
                error_msg,
            )
            checks_results.append(result)
        return {"result": all(checks_results)}
