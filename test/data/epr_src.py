"""Dummy data for all source dasets required for the EPR output
- enterprise_party_delt (EPR_ENT_PARTY)
- epr_party_relationship_delt (EPR_RLTN)
- epr_party_external_identifier_delt (EPR_EXT_ID)
- epr_party_classification_delt (EPR_CLASS)
- epr_organisation_delt (EPR_ORG)
- epr_party_local_identifier_delt (EPR_LOC_ID)
- epr_party_address_delt (EPR_ADDRESS)
- organisation_financial_details_delt (EPR_FINANCIAL_DET)
"""

EPR_SRC_DATA = {
    "enterprise_party_delt": {
        "o_GLOBAL_ID": [20241023, 20241024, 20241025],
        "o_SNAPSHOTDATE": [20191201, 20191202, 20191205],
        "o_PRTY_TYP": ["ORG", "ORG", "ORG"],
        "o_PRTY_NM": [5, "afjkd", "afjkd"],
    },
    "epr_party_relationship_delt": {
        "o_RLTNSHP_TYP": [102, 103, 101, 102, 103, 101],
        "o_RLTNSHP_STTS": [1, 1, 1, 1, 1, 1],
        "o_GLOBAL_D1": [20241023, 20241024, 20241025, 111, 222, 333],
        "o_GLOBAL_ID2": [1100, 2200, 3300, 20241023, 20241024, 20241025],
    },
    "epr_party_external_identifier_delt": {
        "o_GLOBAL_ID": [20241023, 20241024, 20241025],
        "o_EXTRN_ID_TYP": ["LEI", "LEI", "NO_LEI"],
        "o_EXTRN_ID_VAL": ["7ABCD", "7ABCF", "7ABCF"],
    },
    "epr_party_classification_delt": {
        "o_GLOBAL_ID": [20241023, 20241024, 20241025, 20241025],
        "o_CLSFCTN_TYP": [2, 3, 1, 18],
        "o_CLSFCTN_VAL": ["CLIENT", "CORP", "CLIENT", "CLIENT"],
        "o_CLSFCTN_STS": [1, 1, 1, 1],
    },
    "epr_organisation_delt": {
        "o_GLOBAL_ID": [20241023, 20241024, 20241025],
        "o_MAIN_COC": [111, 222, 333],
        "o_CNTRY_OF_INCORP": ["NL", "BE", "GB"],
    },
    "epr_party_local_identifier_delt": {
        "o_GLOBAL_ID": [20241023, 20241024, 20241025],
        "o_LOCL_SRC_SYSTM": ["INSIGHTER", "NO_INSIGHTER", "INSIGHTER"],
        "o_LOCL_ID": ["000YY6076984", "000XX6076984", "000XX6076984"],
    },
    "epr_party_address_delt": {
        "o_GLOBAL_ID": [20241023, 20241025, 20241024, 20241026],
        "o_ADRES_TYPE": ["PREF", "NO_PREF", "PREF", "PREF"],
        "o_CNTRY": ["NL", "NL", "BE", "BE"],
    },
    "organisation_financial_details_delt": {
        "o_global_id": [20241023, 20241025],
        "o_accnt_data_level": [3, 3],
        "o_annl_trnover": [5000, 3000],
        "o_accnt_crrncy": ["EUR", "BRP"],
    },
}
