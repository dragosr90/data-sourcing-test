import json
from datetime import date
from pathlib import Path

import pytest

from abnamro_bsrc_etl.config.constants import MAPPING_ROOT_DIR
from abnamro_bsrc_etl.scripts.export_tine_tables import (
    create_json_metadata,
    export_tine_tables,
)
from abnamro_bsrc_etl.utils.export_parquet import export_to_parquet

TINE_TABLE_LIST = [
    "tine_cpty",
    "tine_npl_credit_fac_agmt",
    "tine_npl_derivative",
    "tine_npl_guarantee_agmt",
    "tine_npl_guarantee_links",
    "tine_npl_lending_expo_agmt",
    "tine_npl_market_instruments",
    "tine_npl_security_finance",
    "tine_credit_agmt",
    "tine_npl_liability_agmt",
    "tine_npl_lla_agmt",
    "tine_pl_collateral_agmt",
    "tine_pl_collateral_links",
    "tine_pl_cpp_segment",
    "tine_pl_credit_fac_agmt",
    "tine_pl_guarantee_agmt",
    "tine_pl_guarantee_links",
    "tine_pl_lending_expo_agmt",
    "tine_pl_lla_agmt",
    "tstg_npl_collateral_links",
    "tstg_npl_guarantee_links",
    "tine_npl_collateral_agmt",
    "tine_npl_collateral_links",
    "tine_credit_rating",
    "tine_npl_fi_n_equity",
    "tmisc_npl_credit_agmt_links",
]

SCENARIO_FULL_TRUE = [True for _ in TINE_TABLE_LIST]
SCENARIO_FULL_FALSE = [False for _ in TINE_TABLE_LIST]
SCENARIO_SINGLE_FALSE = [i != "tine_cpty" for i in TINE_TABLE_LIST]


class FileInfoMock(dict):
    """Allows to access dictionary keys as attributes.
    And adds a simple isDir method."""

    __getattr__ = dict.get

    def isDir(self):  # noqa: N802
        return bool(self.name.endswith("/"))


def put_mock(file, contents, overwrite):
    """Mock function for dbutils.fs.put"""
    with Path.open(MAPPING_ROOT_DIR / "test/data" / Path(file), "w+") as f:
        f.write(contents)


@pytest.fixture
def mock_get_catalog(mocker):
    """Fixture to mock get_catalog."""
    return mocker.patch("abnamro_bsrc_etl.scripts.export_tine_tables.get_catalog")


@pytest.fixture
def mock_export_to_parquet(mocker):
    return mocker.patch("abnamro_bsrc_etl.scripts.export_tine_tables.export_to_parquet")


@pytest.mark.parametrize(
    ("result_export_to_parquet", "expected_output", "expected_log"),
    [
        (SCENARIO_FULL_TRUE, True, []),
        (
            SCENARIO_FULL_FALSE,
            False,
            ["Failure exporting these TINE tables:", "\n".join(TINE_TABLE_LIST)],
        ),
        (
            SCENARIO_SINGLE_FALSE,
            False,
            ["Failure exporting these TINE tables:", "tine_cpty"],
        ),
    ],
)
def test_export_tine_tables(
    mocker,
    mock_spark,
    mock_get_catalog,
    mock_export_to_parquet,
    caplog,
    result_export_to_parquet,
    expected_output,
    expected_log,
):
    mock_get_catalog.return_value = "dev"
    mock_export_to_parquet.side_effect = result_export_to_parquet
    mock_json = mocker.patch(
        "abnamro_bsrc_etl.scripts.export_tine_tables.create_json_metadata"
    )
    assert export_tine_tables(mock_spark, "202403", TINE_TABLE_LIST) == expected_output
    assert mock_json.call_count == sum(result_export_to_parquet)
    assert caplog.messages == expected_log


def test_create_json_metadata(
    spark_session,
    mocker,
):
    mocker.patch(
        "abnamro_bsrc_etl.scripts.export_tine_tables.get_container_path",
        return_value="../data",
    )

    df = spark_session.createDataFrame(
        [(1, 2, date(9999, 12, 31)), (2, 7, date(9999, 12, 31))],
        schema=["a", "b", "REPORTING_DATE"],
    )
    assert export_to_parquet(
        spark_session,
        run_month="999912",
        folder="test_export",
        file_name="test_table",
        data=df,
        local=True,
    )
    # Get name of part file in test_export/test_table.parquet folder
    folder = f"{MAPPING_ROOT_DIR}/test/data/test_export/test_table.parquet"
    file_name = next(
        f for f in Path(folder).iterdir() if f.name.endswith(".parquet")
    ).name

    mock_dbutils = mocker.patch(
        "abnamro_bsrc_etl.utils.get_dbutils.WorkspaceClient.dbutils", autospec=True
    )
    mock_dbutils.fs.ls.return_value = [
        FileInfoMock(
            {
                "name": file_name,
                "modificationTime": 1735689600,  # Jan 1 2025
            }
        )
    ]

    mock_dbutils.fs.put.side_effect = put_mock

    create_json_metadata(
        spark=spark_session,
        table="test_table",
        month_no="999912",
        df=df,
        folder="test_export",
        tmp_location=f"{MAPPING_ROOT_DIR}/test/data/test_export",
    )
    with Path.open(
        MAPPING_ROOT_DIR / "test/data/test_export/test_table.json", "r"
    ) as file:
        json_dict = json.load(file)

    expected_json = {
        "number_of_records": 2,
        "number_of_columns": 3,
        "file_description": "test_table",
        "file_extension": "snappy.parquet",
        "file_name": f"test_table/{file_name}",
        "snapshot_datetime": "99991231",
        "extraction_datetime": "20250101000000",
    }

    del json_dict["md5"]  # Removed md5, because that changes every time the test is run
    assert json_dict == expected_json
