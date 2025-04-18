import inspect


def union_in_sources_format(
    alias: str,
    column_mapping: dict[str, dict[str, str]],
) -> dict[str, str | list[str]]:
    """Get union transformation step in sources format

    This helper function is used to update the original list of mappings that contain
    at least an `'alias'` and a list of `'columns'`. In the union step, new sources
    are added to the input data dictionary. In the validation these unioned sources
    should be validated as well.

    Example:

        >>> from src.utils.union_util import union_in_sources_format
        >>> sources = [
        ...     {"alias": "TABLE_A", "columns": ["c01a", "c02a"]},
        ...     {"alias": "TABLE_B", "columns": ["c01b", "c02b"]},
        ... ]
        >>> union = {
        ...     "alias": "TABLE_A_B"
        ...     "column_mapping": {
        ...         "TABLE_A": {"c01_ab": "c01a", "c02_ab": "c02a"},
        ...         "TABLE_B": {"c01_ab": "c01b", "c02_ab": "c02b"},
        ...     }
        ... }
        >>> union_in_sources_format(**union)
        {'alias': 'TABLE_A_B', 'columns': ['c01_ab', 'c02_ab']}

    Args:
        alias (str): Alias of new union data set
        column_mapping (dict[str, dict[str, str]]): Mapping containing original source
            tables and column mappings

    Returns:
        dict[str, str | list[str]]: Union in Sources format dictionary
    """
    return next(
        iter(
            [
                {
                    "alias": alias,
                    "columns": list(table_mapping.keys()),
                }
                for table_mapping in column_mapping.values()
            ]
        )
    )


def aggregation_in_sources_format(
    alias: str, group: list[str], column_mapping: dict[str, str]
) -> dict[str, str | list[str]]:
    """Get aggregation transformation step in sources format."""
    return {
        "alias": alias,
        "columns": [
            *[c.split(".")[1] if "." in c else c for c in group],
            *column_mapping.keys(),
        ],
    }


def join_in_sources_format(
    alias: str, updated_sources: list[dict[str, str | list[str]]]
) -> dict[str, str | list[str]]:
    available_columns = sorted({c for s in updated_sources for c in s["columns"]})
    return {
        "alias": alias,
        "columns": available_columns,
    }


def add_variables_in_sources_format(
    alias: str, updated_sources: list[dict[str, str | list[str]]]
) -> dict[str, str | list[str]]:
    available_columns = sorted({c for s in updated_sources for c in s["columns"]})
    return {
        "alias": alias,
        "columns": available_columns,
    }


def pivot_in_sources_format(
    alias: str,
    group_cols: list[str],
    column_mapping: dict[str, str],
) -> dict[str, str | list[str]]:
    """Get pivot transformation step in sources format."""
    return {
        "alias": alias,
        "columns": [
            *[c.split(".")[1] if "." in c else c for c in group_cols],
            *list(column_mapping),
        ],
    }


def keep_unique_sources(
    input_sources: list[dict[str, str | list[str]]],
    reference_sources: list[dict[str, str | list[str]]],
) -> list[dict[str, str | list[str]]]:
    """Helper function to deduplicate list of sources."""
    return [
        *reference_sources,
        *[s for s in input_sources if s not in reference_sources],
    ]


def update_join_sources_transformations(
    input_sources: list[dict[str, str | list[str]]],
    updated_sources: list[dict[str, str | list[str]]],
    left_source: str | None,
    right_source: str,
) -> list[dict[str, str | list[str]]]:
    """Update sources list in transformation validation."""
    new_sources = [
        s for s in input_sources if s.get("alias") in [left_source, right_source]
    ]
    updated_sources = [] if left_source else updated_sources
    return keep_unique_sources(new_sources, updated_sources)


def update_join_sources_expressions(
    sources: list[dict[str, str | list[str]]],
    reference_sources: list[dict[str, str | list[str]]],
    left_source: str,
    right_source: str,
) -> list[dict[str, str | list[str]]]:
    """Update sources list in expressions validation."""
    return (
        # Only keep left and right source as available sources if
        # left_source is specified
        [
            source
            for source in reference_sources
            if source["alias"] in [left_source, right_source]
        ]
        if left_source
        # else take all unique sources that are used
        # until this transformation step
        else keep_unique_sources(
            [source for source in reference_sources if source["alias"] == right_source],
            sources,
        )
    )


def update_sources(
    input_sources: list[dict[str, str | list[str]]], tf: dict, tf_step: str
) -> list[dict[str, str | list[str]]]:
    """Update sources list based on transformation step."""
    tf_func = globals()[f"{tf_step}_in_sources_format"]
    # Only pass the arguments that are required for the formatting functions
    arg_names = list(inspect.signature(tf_func).parameters)
    kwgs = {k: v for k, v in tf[tf_step].items() if k in arg_names}
    if tf_step in ["join", "add_variables"]:
        # If there is a join or add_variables, we can use all columns
        # From the updated_sources from all previous transformation steps
        return (
            keep_unique_sources(
                [tf_func(updated_sources=input_sources, **kwgs)],
                input_sources,
            )
            if "alias" in kwgs
            else input_sources
        )
    # If there is a pivot, union or aggregation we can use
    # the new generated columns from the corresponding functions
    return keep_unique_sources(
        [tf_func(**kwgs)],
        input_sources,
    )


def update_variables(variables: list[str], tf: dict | None) -> list[str]:
    """Update variable list based on transformation step."""
    if not tf:
        return []
    tf_step = next(iter(tf))
    # Reset variables if left_source is specified or for certain operations
    if tf[tf_step].get("left_source") or tf_step in [
        "pivot",
        "union",
        "aggregation",
    ]:
        return []
    # Add new variables for add_variables step
    if tf_step == "add_variables":
        return [
            *variables,
            *list(tf["add_variables"]["column_mapping"].keys()),
        ]
    # Default: preserve existing variables (includes "filter" and other steps)
    return variables


def get_source(tf: dict[str, dict[str, str]]) -> str | None:
    tf_step: str = next(iter(tf))
    if tf_step in ["aggregation", "pivot", "union", "add_variables", "filter"]:
        return tf[tf_step].get("source")
    if tf_step == "join":
        return tf[tf_step].get("left_source")
    return None


def get_required_arguments(tf: dict, obj: object, func_name: str) -> dict:
    tf_func = getattr(obj, func_name)
    arg_names = list(inspect.signature(tf_func).parameters)
    return {k: v for k, v in tf[next(iter(tf))].items() if k in arg_names}
