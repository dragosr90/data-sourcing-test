from pyspark.sql.types import IntegerType, StringType, StructField, StructType

SCHEMA_FIELDS = {
    # "<Input column name>": [<data_type>, <nullable>]
    "o_SNAPSHOTDATE": [IntegerType(), True],
    "o_GLOBAL_ID": [IntegerType(), True],
    "o_global_id": [IntegerType(), True],
    "o_PRTY_TYP": [StringType(), True],
    "o_PRTY_NM": [StringType(), True],
    "o_MAIN_COC": [IntegerType(), True],
    "o_GLOBAL_D1": [IntegerType(), True],
    "o_GLOBAL_ID2": [IntegerType(), True],
    "o_RLTNSHP_TYP": [IntegerType(), True],
    "o_RLTNSHP_STTS": [IntegerType(), True],
    "o_CNTRY_OF_INCORP": [StringType(), True],
    "o_CLSFCTN_STS": [IntegerType(), True],
    "o_CLSFCTN_TYP": [IntegerType(), True],
    "o_CLSFCTN_VAL": [StringType(), True],
    "o_ADRES_TYPE": [StringType(), True],
    "o_CNTRY": [StringType(), True],
    "o_LOCL_SRC_SYSTM": [StringType(), True],
    "o_LOCL_ID": [StringType(), True],
    "o_EXTRN_ID_TYP": [StringType(), True],
    "o_EXTRN_ID_VAL": [StringType(), True],
    "o_accnt_data_level": [IntegerType(), True],
    "o_annl_trnover": [IntegerType(), True],
    "o_accnt_crrncy": [StringType(), True],
    # "<Output column name>": [<data_type>, <nullable>]
    "SnapshotDate": [IntegerType(), True],
    "GlobalCounterpartyIdentifier": [IntegerType(), True],
    "PartyType": [StringType(), True],
    "CounterpartyName": [StringType(), True],
    "NationalIdentifier": [IntegerType(), True],
    "CountryOfIncorporation": [StringType(), True],
    "UltimateParentOrganisation": [IntegerType(), True],
    "HeadOfficeOrganisation": [IntegerType(), True],
    "ImmediateParentOrganisation": [IntegerType(), True],
    "OFFICIAL_SBI_CODE": [StringType(), True],
    "OFFICIAL_NACE": [StringType(), True],
    "REAL_SBI_CODE": [StringType(), True],
    "SME_INDICATOR": [StringType(), True],
    "CountryOfResidence": [StringType(), True],
    "InsighterId": [StringType(), True],
    "LeiCode": [StringType(), True],
    "Group_Annual_Turnover": [IntegerType(), True],
    "Group_Annual_Turnover_Currency": [StringType(), True],
}


def get_schema(field_names: None | list[str] = None) -> StructType:
    field_names = field_names if field_names else list(SCHEMA_FIELDS.keys())
    return StructType(
        [
            StructField(k, *SCHEMA_FIELDS[k.split(".")[1] if "." in k else k])  # type: ignore[arg-type]
            for k in field_names
        ]
    )
