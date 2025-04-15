class TransformTestData:
    def __init__(self, *, filtered: bool = False):
        self.filtered = filtered

    def get_data(self) -> dict:
        data = {
            "epr_party": {
                "input_field_names": [
                    "EPR_ENT_PARTY.o_GLOBAL_ID",
                    "EPR_ENT_PARTY.o_SNAPSHOTDATE",
                    "EPR_ENT_PARTY.o_PRTY_TYP",
                    "EPR_ENT_PARTY.o_PRTY_NM",
                ],
                "output_field_names": [
                    "SnapshotDate",
                    "GlobalCounterpartyIdentifier",
                    "PartyType",
                    "CounterpartyName",
                ],
                "data": [
                    (20241023, 20191201, "ORG", 5, 20191201, 20241023, "ORG", 5),
                    (20241024, 20191202, "ORG", "af", 20191202, 20241024, "ORG", "af"),
                    (20241025, 20191205, "ORG", "ad", 20191205, 20241025, "ORG", "ad"),
                ],
            },
            "epr_org": {
                "input_field_names": [
                    "EPR_ENT_PARTY.o_PRTY_TYP",
                    "EPR_ORG.o_MAIN_COC",
                    "EPR_ORG.o_CNTRY_OF_INCORP",
                ],
                "output_field_names": ["NationalIdentifier", "CountryOfIncorporation"],
                "data": [
                    ("ORG", 111, "NL", 111, "NL"),
                    ("NO_ORG", 222, "NL", None, "NL"),
                    ("ORG", 333, "GB", 333, "GB"),
                ],
            },
            "epr_relation": {
                "input_field_names": [
                    "EPR_ENT_PARTY_UP.o_GLOBAL_ID",
                    "EPR_RLTN_HO.o_RLTNSHP_TYP",
                    "EPR_ENT_PARTY_HO.o_GLOBAL_ID",
                    "EPR_RLTN_DP.o_RLTNSHP_TYP",
                    "EPR_ENT_PARTY_DP.o_GLOBAL_ID",
                ],
                "output_field_names": [
                    "UltimateParentOrganisation",
                    "HeadOfficeOrganisation",
                    "ImmediateParentOrganisation",
                ],
                "data": [
                    # o_RLTNSHP_TYP columns not in 101 or 103, so
                    # ImmediateParentOrganisation and HeadOfficeOrganisation are None
                    (1234, 102, 4321, 109, 6789, 1234, None, None),
                    # EPR_RLTN_HO.o_RLTNSHP_TYP == 101, so HeadOfficeOrganisation = 4321
                    (1234, 103, 4321, 109, 6789, 1234, 4321, None),
                    # EPR_RLTN_DP.o_RLTNSHP_TYP -> 101, so
                    # ImmediateParentOrganisation -> 6789
                    (1234, 100, 4321, 101, 6789, 1234, None, 6789),
                ],
            },
            "epr_class": {
                "input_field_names": [
                    "EPR_CLASS_SBI_01.o_CLSFCTN_TYP",
                    "EPR_CLASS_SBI_01.o_CLSFCTN_VAL",
                    "EPR_CLASS_02_NACE.o_CLSFCTN_TYP",
                    "EPR_CLASS_02_NACE.o_CLSFCTN_VAL",
                    "EPR_CLASS_SBI_03.o_CLSFCTN_TYP",
                    "EPR_CLASS_SBI_03.o_CLSFCTN_VAL",
                    "EPR_CLASS_VAL.o_CLSFCTN_VAL",
                ],
                "output_field_names": [
                    "OFFICIAL_SBI_CODE",
                    "OFFICIAL_NACE",
                    "REAL_SBI_CODE",
                    "SME_INDICATOR",
                ],
                "data": [
                    # OFFICIAL_SBI_CODE
                    (1, "XXX", 9, "YYY", 6, "ZZZ", 8, "XXX", None, None, None),
                    # OFFICIAL_NACE
                    (8, "XXX", 2, "YYY", 6, "ZZZ", 8, None, "YYY", None, None),
                    # REAL SBI CODE
                    (8, "XXX", 9, "YYY", 3, "ZZZ", 8, None, None, "ZZZ", None),
                    # CLASS_VAL 1
                    (8, "XXX", 9, "YYY", 6, "ZZZ", 1, None, None, None, "Y"),
                    # CLASS_VAL 2
                    (8, "XXX", 9, "YYY", 6, "ZZZ", 2, None, None, None, "N"),
                    # Only None values
                    (8, "XXX", 9, "YYY", 6, "ZZZ", 8, None, None, None, None),
                ],
            },
            "epr_address": {
                "input_field_names": [
                    "EPR_ADDRESS_PREF.o_ADRES_TYPE",
                    "EPR_ADDRESS_PREF.o_CNTRY",
                ],
                "output_field_names": ["CountryOfResidence"],
                "data": [
                    ("PREF", "NL", "NL"),
                    ("PREF", "GB", "GB"),
                ],
            },
            "epr_loc_id": {
                "input_field_names": [
                    "EPR_LOC_ID_INSIGHT.o_LOCL_SRC_SYSTM",
                    "EPR_LOC_ID_INSIGHT.o_LOCL_ID",
                ],
                "output_field_names": ["InsighterId"],
                "data": [
                    ("INSIGHTER", "000YY6076984", "000YY6076984"),
                    ("NO_INSIGHTER", "000006076984", None),
                    ("INSIGHTER", "000XX6076984", "000XX6076984"),
                ],
            },
            "epr_ext_id": {
                "input_field_names": [
                    "EPR_EXT_ID_LEI.o_EXTRN_ID_TYP",
                    "EPR_EXT_ID_LEI.o_EXTRN_ID_VAL",
                ],
                "output_field_names": ["LeiCode"],
                "data": [
                    ("LEI", "7ABCDTR592BPAKXX25", "7ABCDTR592BPAKXX25"),
                    ("NO_LEI", "7ABCDTR592BPAKXX25", None),
                    ("LEI", "7ABCDTR592BPAKXX25", "7ABCDTR592BPAKXX25"),
                ],
            },
            "epr_finan": {
                "input_field_names": [
                    "EPR_RLTN_UP.o_RLTNSHP_TYP",
                    "EPR_RLTN_UP.o_RLTNSHP_STTS",
                    "EPR_FINANCIAL_DET.o_accnt_data_level",
                    "EPR_FINANCIAL_DET.o_annl_trnover",
                    "EPR_FINANCIAL_DET.o_accnt_crrncy",
                ],
                "output_field_names": [
                    "Group_Annual_Turnover",
                    "Group_Annual_Turnover_Currency",
                ],
                "data": [
                    (102, 2, 3, 567000, "EUR", None, None),
                    (102, 1, 3, 567000, "EUR", 567000, "EUR"),
                    (102, 2, 4, 567000, "EUR", None, None),
                    (1002, 1, 3, 567000, "EUR", None, None),
                ],
            },
        }
        return self.get_filtered_data(data) if self.filtered else data

    def get_filtered_data(self, test_data_dict: dict) -> dict:
        test_data_dict["epr_org"]["data"] = [
            ("ORG", 111, "NL", 111, "NL"),
            ("NO_ORG", 222, "NL", None, "NL"),
        ]
        test_data_dict["epr_relation"]["data"] = [
            # EPR_RLTN_HO.o_RLTNSHP_TYP == 101, so HeadOfficeOrganisation == 4321
            (1234, 103, 4321, 109, 6789, 1234, 4321, None),
        ]
        return test_data_dict
