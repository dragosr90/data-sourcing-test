import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from src.staging.extract_ssf_data import ExtractSSFData
from src.staging.status import SSFStepStatus


class SparkLocal:
    def __init__(self, spark_session):
        self.spark = spark_session

    def spark_read_local(self, table_name: str):
        return self.spark.createDataFrame(
            {
                "metadata_ssf_entities": [
                    {
                        "SSFEntityName": "CollateralSecuresCreditFacility",
                        "SSFTableName": "cfxcol",
                        "DeliveryEntity": "ALFAM",
                        "DeliverySet": "5F",
                        "FileDeliveryStatus": "Expected",
                        "FileDeliveryStep": 0,
                    },
                    {
                        "SSFEntityName": "Collateral",
                        "SSFTableName": "col",
                        "DeliveryEntity": "ALFAM",
                        "DeliverySet": "5F",
                        "FileDeliveryStatus": "Expected",
                        "FileDeliveryStep": 0,
                    },
                    # Extra record to test empty table creations, to create cf for ALFAM
                    {
                        "SSFEntityName": "CreditFacility",
                        "SSFTableName": "cf",
                        "DeliveryEntity": "FBS",
                        "DeliverySet": "5F",
                        "FileDeliveryStatus": "Expected",
                        "FileDeliveryStep": 0,
                    },
                ],
                "log_ssf_entities": [
                    {
                        "SourceSystem": "TESTSRCSYSTEM",
                        "SourceFileName": "TESTSSFFILENAME",
                        "RedeliveryNumber": 1,
                        "FileDeliveryStep": 0,
                        "FileDeliveryStatus": "Expected",
                        "Result": "Success",
                        "LastUpdatedDateTimestamp": "2025-05-25",
                        "Comment": "Test comment",
                    }
                ],
                "run_process_table": [
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "ALFAM",
                        "FileName": "ALFAM_COL_20240331000000_PAYLOAD.zip",
                        "SSFTableName": "COL",
                        "FileReportingPeriod": "EOM",
                        "PeriodVersion": "V1",
                        "DeliverySet": "5F",
                        "XSDVersion": "2024Q2V1",
                        "RedeliveryNumber": 0,
                        "ActualRowCount": 2,
                        "FileReportingDate": "9999-12-31",
                    },
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "ALFAM",
                        "FileName": "ALFAM_CFXCOL_20240331000000_PAYLOAD.zip",
                        "SSFTableName": "CFXCOL",
                        "FileReportingPeriod": "EOM",
                        "PeriodVersion": "V1",
                        "DeliverySet": "5F",
                        "XSDVersion": "2024Q2V1",
                        "RedeliveryNumber": 0,
                        "ActualRowCount": 2,
                        "FileReportingDate": "9999-12-31",
                    },
                ],
                "metadata_ssf_xsd_change": [
                    {
                        "NewXSDVersion": "2024Q4V2",
                        "OldXSDVersion": "2024Q2V1",
                        "ChangeType": "Add",
                        "NewEntityName": "col",
                        "OldEntityName": "col",
                        "NewAttributeName": "realestatepurposetype",
                        "OldAttributeName": None,
                        "NewEntityKey": None,
                        "OldEntityKey": None,
                    },
                    {
                        "NewXSDVersion": "2024Q4V2",
                        "OldXSDVersion": "2024Q2V1",
                        "ChangeType": "Remove",
                        "NewEntityName": "col",
                        "OldEntityName": "col",
                        "NewAttributeName": None,
                        "OldAttributeName": "buildingarea",
                        "NewEntityKey": None,
                        "OldEntityKey": None,
                    },
                    {
                        "NewXSDVersion": "2024Q4V2",
                        "OldXSDVersion": "2024Q2V1",
                        "ChangeType": "Rename",
                        "NewEntityName": "col",
                        "OldEntityName": "col",
                        "NewAttributeName": "numberofhousingunits",
                        "OldAttributeName": "numberofhousing",
                        "NewEntityKey": None,
                        "OldEntityKey": None,
                    },
                    {
                        "NewXSDVersion": "2024Q4V2",
                        "OldXSDVersion": "2024Q2V1",
                        "ChangeType": "Move",
                        "NewEntityName": "cfxcol",
                        "OldEntityName": "col",
                        "NewAttributeName": "maturitydate",
                        "OldAttributeName": "maturitydate",
                        "NewEntityKey": "collateral",
                        "OldEntityKey": "CollateralIdentifier",
                    },
                ],
                "metadata_ssf_abc_list": [
                    {
                        "SSFEntityName": "Collateral",
                        "AttributeName": "CollateralIdentifier",
                        "Key": "Collateral-CollateralIdentifier",
                        "SSFTableName": "col",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "Collateral",
                        "AttributeName": "BuildingArea",
                        "Key": "Collateral-BuildingArea",
                        "SSFTableName": "col",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "Collateral",
                        "AttributeName": "NumberOfHousing",
                        "Key": "Collateral-NumberOfHousing",
                        "SSFTableName": "col",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "Collateral",
                        "AttributeName": "MaturityDate",
                        "Key": "Collateral-MaturityDate",
                        "SSFTableName": "col",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "CollateralSecuresCreditFacility",
                        "AttributeName": "CreditFacilityIdentifier",
                        "Key": "CollateralFacility-CreditFacilityIdentifier",
                        "SSFTableName": "cfxcol",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "CollateralSecuresCreditFacility",
                        "AttributeName": "Collateral",
                        "Key": "CollateralSecuresCreditFacility-Collateral",
                        "SSFTableName": "cfxcol",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "CreditFacility",
                        "AttributeName": "FacilityIdentifier",
                        "Key": "CreditFacility-FacilityIdentifier",
                        "SSFTableName": "cf",
                        "XSDVersion": "2024Q2V1",
                    },
                    {
                        "SSFEntityName": "CreditFacility",
                        "AttributeName": "CreditFacilityIdentifier",
                        "Key": "CreditFacility-CreditFacilityIdentifier",
                        "SSFTableName": "cf",
                        "XSDVersion": "2025Q2V1",
                    },
                ],
                "cfxcol": [
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "ALFAM",
                        "CreditFacilityIdentifier": 1,
                        "Collateral": "A1",
                        "FileReportingDate": "9999-12-31",
                    },
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "ALFAM",
                        "CreditFacilityIdentifier": 2,
                        "Collateral": "B2",
                        "FileReportingDate": "9999-12-31",
                    },
                ],
                "col": [
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "ALFAM",
                        "CollateralIdentifier": "A1",
                        "BuildingArea": "NL",
                        "NumberOfHousing": 2,
                        "MaturityDate": "2024-12-31",
                        "FileReportingDate": "9999-12-31",
                    },
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "ALFAM",
                        "CollateralIdentifier": "B2",
                        "BuildingArea": "UK",
                        "NumberOfHousing": 1,
                        "MaturityDate": "2025-01-01",
                        "FileReportingDate": "9999-12-31",
                    },
                ],
                "cf": [
                    {
                        "RunID": 4,
                        "FileDeliveryEntity": "FBS",
                        "CreditFacilityIdentifier": 1,
                        "FileReportingDate": "9999-12-31",
                    },
                    {
                        "RunID": 5,
                        "FileDeliveryEntity": "FBS",
                        "FacilityIdentifier": 3,
                        "FileReportingDate": "9999-12-31",
                    },
                ],
            }[table_name.split(".")[-1]]
        )


@pytest.fixture(autouse=True)
def mock_spark_write(mocker, spark_session):
    return mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")


@pytest.fixture(autouse=True)
def mock_spark_catalog(mocker):
    return mocker.patch("pyspark.sql.Catalog.tableExists")


@pytest.fixture(autouse=True)
def mock_write_parquet(mocker):
    return mocker.patch("pyspark.sql.DataFrameWriter.parquet")


@pytest.fixture(autouse=True)
def mock_spark_read(spark_session, mocker):
    return mocker.patch(
        "pyspark.sql.DataFrameReader.table",
        wraps=SparkLocal(spark_session).spark_read_local,
        spark_session=spark_session,
    )


@pytest.fixture(autouse=True)
def spy_spark_select(mocker):
    return mocker.spy(DataFrame, "select")


def test_get_updated_delivery_entities(
    spark_session,
):
    assert ExtractSSFData(
        spark_session, run_month="999912"
    ).get_updated_delivery_entities() == ["ALFAM"]


def test_extract_from_view(
    spark_session,
):
    extracted_data = ExtractSSFData(
        spark_session, run_month="999912"
    ).extract_from_view("ALFAM")
    assert set(extracted_data.keys()) == {"col", "cfxcol"}
    assert_df_equality(
        extracted_data["col"],
        SparkLocal(spark_session)
        .spark_read_local("col")
        .select(
            "FileDeliveryEntity",
            "FileReportingDate",
            "BuildingArea",
            "NumberOfHousing",
            "MaturityDate",
            "CollateralIdentifier",
        ),
        ignore_column_order=True,
        ignore_row_order=True,
    )
    assert_df_equality(
        extracted_data["cfxcol"],
        SparkLocal(spark_session)
        .spark_read_local("cfxcol")
        .select(
            "FileDeliveryEntity",
            "FileReportingDate",
            "CreditFacilityIdentifier",
            "Collateral",
        ),
        ignore_column_order=True,
        ignore_row_order=True,
    )


def test_check_counts(spark_session):
    assert ExtractSSFData(spark_session, run_month="999912").validate_record_counts(
        "ALFAM", "col", SparkLocal(spark_session).spark_read_local("col")
    )


def test_check_counts_unhappy(spark_session):
    col_data = SparkLocal(spark_session).spark_read_local("col")
    assert (
        ExtractSSFData(spark_session, run_month="999912").validate_record_counts(
            "ALFAM", "col", col_data.union(col_data.limit(1))
        )
        is False
    )


def test_export_to_parquet(
    spark_session,
):
    assert ExtractSSFData(spark_session, run_month="999912").export_to_storage(
        spark_session, "ALFAM", "col", SparkLocal(spark_session).spark_read_local("col")
    )


def test_xsd_conversion(spark_session):
    unconverted_data = {
        "col": SparkLocal(spark_session).spark_read_local("col"),
        "cfxcol": SparkLocal(spark_session).spark_read_local("cfxcol"),
    }

    converted_data = ExtractSSFData(
        spark_session, run_month="999912"
    ).convert_to_latest_xsd(
        "ALFAM",
        unconverted_data,
    )

    cfxcol_expected = spark_session.createDataFrame(
        [
            {
                "RunID": 4,
                "FileDeliveryEntity": "ALFAM",
                "CreditFacilityIdentifier": 1,
                "Collateral": "A1",
                "maturitydate": "2024-12-31",
                "FileReportingDate": "9999-12-31",
            },
            {
                "RunID": 4,
                "FileDeliveryEntity": "ALFAM",
                "CreditFacilityIdentifier": 2,
                "Collateral": "B2",
                "maturitydate": "2025-01-01",
                "FileReportingDate": "9999-12-31",
            },
        ]
    )

    col_expected = spark_session.createDataFrame(
        [
            {
                "RunID": 4,
                "FileDeliveryEntity": "ALFAM",
                "CollateralIdentifier": "A1",
                "numberofhousingunits": 2,
                "FileReportingDate": "9999-12-31",
            },
            {
                "RunID": 4,
                "FileDeliveryEntity": "ALFAM",
                "CollateralIdentifier": "B2",
                "numberofhousingunits": 1,
                "FileReportingDate": "9999-12-31",
            },
        ],
    ).withColumn(
        "realestatepurposetype",
        lit(None).cast(StringType()),
    )

    assert_df_equality(
        cfxcol_expected,
        converted_data["cfxcol"],
        ignore_column_order=True,
        ignore_row_order=True,
        allow_nan_equality=True,
    )
    assert_df_equality(
        col_expected,
        converted_data["col"],
        ignore_column_order=True,
        ignore_row_order=True,
        allow_nan_equality=True,
    )


def test_dq_checks(spark_session):
    assert ExtractSSFData(spark_session, run_month="999912").validate_data_quality(
        delivery_entity="ALFAM",
        ssf_table="col",
        stg_table_name="col",
    )


def test_staging_load(spark_session, mock_spark_write):
    assert ExtractSSFData(spark_session, run_month="999912").save_to_stg_table(
        data=SparkLocal(spark_session).spark_read_local("col"),
        stg_table_name="ssf_alfam_col",
        delivery_entity="ALFAM",
        ssf_table="col",
    )
    mock_spark_write.assert_any_call("bsrc_d.stg_999912.ssf_alfam_col")


def test_update_log_metadata(spark_session, mock_spark_write):
    mock_spark_write.reset_mock()
    extraction = ExtractSSFData(spark_session, run_month="999912")
    delivery_entity = "ALFAM"
    lookup = extraction.get_lookup(delivery_entity, "col")
    log_info = extraction.get_updated_log_info(lookup)
    extraction.update_log_metadata(
        key=delivery_entity,
        file_delivery_status=SSFStepStatus.EXTRACTED,
        ssf_table="col",
        **log_info,
    )

    mock_spark_write.assert_any_call("bsrc_d.log_999912.log_ssf_entities")
    mock_spark_write.assert_any_call("bsrc_d.metadata_999912.metadata_ssf_entities")


class SameDataFrame:
    def __init__(self, df: DataFrame):
        self.df = df

    def __eq__(self, other):
        # Simple check to see it is querying the correct table
        return self.df.columns == other.columns


def test_create_empty_tables(spark_session, mock_spark_write, spy_spark_select):
    ExtractSSFData(spark_session, run_month="999912").create_empty_tables("ALFAM")

    mock_spark_write.assert_any_call("bsrc_d.stg_999912.ssf_alfam_cf")
    mock_spark_write.assert_any_call("bsrc_d.log_999912.log_ssf_entities")

    spy_spark_select.assert_any_call(
        SameDataFrame(spark_session.read.table("instap_a.ez_basel_vw.cf")),
        [
            "FileDeliveryEntity",
            "FileReportingDate",
            "CreditFacilityIdentifier",
        ],
    )
