from src.utils.sources_util import (
    aggregation_in_sources_format,
    pivot_in_sources_format,
    union_in_sources_format,
)


def test_union_in_sources_format():
    unions = {
        "alias": "TABLE_A_B",
        "column_mapping": {
            "TABLE_A": {"c01_ab": "c01", "c02_ab": "c02"},
            "TABLE_B": {"c01_ab": "c01", "c02_ab": "c02"},
        },
    }
    updated_sources = union_in_sources_format(**unions)
    assert updated_sources == {"alias": "TABLE_A_B", "columns": ["c01_ab", "c02_ab"]}


def test_aggregation_in_sources_format():
    aggregation = {
        "alias": "TBL_AGG",
        "group": ["TBL_A.col00", "TBL_B.col00"],
        "column_mapping": {
            "MaxCol01": "max(TBLA.col01)",
            "MinCol02": "min(TBLA.col02)",
        },
    }
    updated_sources = aggregation_in_sources_format(**aggregation)
    assert updated_sources == {
        "alias": "TBL_AGG",
        "columns": ["col00", "col00", "MaxCol01", "MinCol02"],
    }


def test_pivot_in_sources_format():
    pivot = {
        "alias": "TBL_PIVOT",
        "group_cols": ["TBL_A.col00", "TBL_B.col00"],
        "pivot_value_col": "TBL_D.col04",
        "pivot_col": "TBL_C.col03",
        "column_mapping": {"X": "min", "Y": "first", "Z": "max"},
    }

    updated_sources = pivot_in_sources_format(
        **{
            k: v
            for k, v in pivot.items()
            if k in ["alias", "group_cols", "column_mapping"]
        }
    )
    assert updated_sources == {
        "alias": "TBL_PIVOT",
        "columns": ["col00", "col00", "X", "Y", "Z"],
    }
