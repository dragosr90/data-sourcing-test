from pathlib import Path

from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from src.config.constants import PROJECT_ROOT_DIRECTORY
from src.utils.export_parquet import export_to_parquet


def test_export_df_happy(spark_session: SparkSession, caplog):
    df = spark_session.createDataFrame([(1, 2, 3), (2, 7, 9)], schema=["a", "b", "c"])
    assert export_to_parquet(
        run_month="999912",
        folder="test_export",
        file_name="test_df",
        df=df,
        local=True,
    )

    assert Path.is_dir(
        PROJECT_ROOT_DIRECTORY / "test/data/999912/test_export/test_df.parquet"
    )
    assert caplog.messages == ["Exported to 999912/test_export/test_df.parquet"]

    assert_df_equality(
        spark_session.read.parquet(
            f"{PROJECT_ROOT_DIRECTORY}/test/data/999912/test_export/test_df.parquet"
        ),
        df,
    )

    for file in Path.iterdir(
        PROJECT_ROOT_DIRECTORY / "test/data/999912/test_export/test_df.parquet"
    ):
        Path.unlink(file)
    Path.rmdir(PROJECT_ROOT_DIRECTORY / "test/data/999912/test_export/test_df.parquet")


def test_export_unhappy_no_data(spark_session: SparkSession, caplog):
    assert not export_to_parquet(
        run_month="999912",
        folder="test_export",
        file_name="no_data",
        local=True,
    )
    assert not Path.is_dir(
        PROJECT_ROOT_DIRECTORY / "test/data/999912/test_export/no_data.parquet"
    )
    assert caplog.messages == ["No Dataframe given"]


def test_export_unhappy_empty_file(spark_session: SparkSession, caplog):
    df = spark_session.createDataFrame([], StructType([]))
    assert not export_to_parquet(
        run_month="999912",
        folder="test_export",
        file_name="emptyfile",
        df=df,
        local=True,
    )
    assert not Path.is_dir(
        PROJECT_ROOT_DIRECTORY / "test/data/999912/test_export/emptyfile.parquet"
    )
    assert caplog.messages == [
        "Could not write data to parquet 999912/test_export/emptyfile.parquet",
        "Datasource does not support writing empty or nested empty schemas. "
        "Please make sure the data schema has at least one or more column(s).",
    ]
