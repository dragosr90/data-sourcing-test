from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from src.config.business_logic import (
    BusinessLogicConfig,
    SourceConfig,
    TransformationStep,
)
from src.dq.dq_validation import DQValidation
from src.utils.logging_util import get_logger
from src.utils.parameter_utils import standardize_delivery_entity
from src.utils.transformations_util import get_table_name

logger = get_logger()


def write_to_table(
    spark: SparkSession, table_name: str, data: DataFrame, mode: str = "overwrite"
) -> None:
    """Write data to table in Unity Catalog.
    If the table already exists and the schema is different, it drops
    the table and recreates it."""
    # Check if the table exists
    try:
        # Try to read the existing table
        existing_table = spark.read.table(table_name)
        # Table exists, compare schemas
        existing_schema = existing_table.schema
        new_schema = data.schema
        # Check if schemas are different
        if str(existing_schema) != str(new_schema):
            logger.info(
                f"Schema changed for table {table_name}. Dropping and recreating."
            )
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            data.write.saveAsTable(table_name)
        else:
            # Same schema, use normal overwrite
            data.write.mode(mode).saveAsTable(table_name)
    except AnalysisException:
        # Table doesn't exist, create it
        data.write.saveAsTable(table_name)


def transformation_dict_to_string(tf: TransformationStep) -> str:
    """Transformation business logic transformation dictionary to string values."""
    transformation_steps = [
        "join",
        "aggregation",
        "pivot",
        "union",
        "add_variables",
        "filter",
    ]
    if tf.get("join"):
        conditions = tf["join"]["condition"]
        how = tf["join"].get("how", "left")
        md_conditions = (
            "\n".join(conditions)
            if len(conditions) == 1
            else "\n".join([f"  - {c}" for c in conditions])
        )
        return f"- Join ({how}):\n{md_conditions}"
    if tf.get("aggregation"):
        return (
            f"- Aggregate:\n"
            f"  - group by {tf['aggregation']['group']}\n"
            + "".join(
                [
                    f"  - {col} = {agg_expr}\n"
                    for col, agg_expr in tf["aggregation"]["column_mapping"].items()
                ]
            )
        )
    if tf.get("pivot"):
        val_mapping_str = (
            (
                "  - using values:\n"
                + "".join(
                    [
                        f"    - {agg_func}({val})\n"
                        for val, agg_func in tf["pivot"]["column_mapping"].items()
                    ]
                )
            )
            if tf["pivot"].get("column_mapping")
            else ""
        )
        return (
            f"- Pivot:\n"
            f"  - group by {tf['pivot']['group_cols']}\n"
            f"  - pivot column {tf['pivot']['pivot_col']}\n"
            f"  - values from column {tf['pivot']['pivot_value_col']}\n"
            f"{val_mapping_str}"
        )
    if tf.get("add_variables"):
        return "- Variable:\n" + "".join(
            [
                f"  - {var} = {v_expr}\n"
                for var, v_expr in tf["add_variables"]["column_mapping"].items()
            ]
        )
    if tf.get("union"):
        union_str = union_dict_to_string(
            tf["union"]["alias"], tf["union"]["column_mapping"]
        )
        return f"- Union:\n{union_str}"
    if tf.get("filter"):
        conditions = tf["filter"].get("conditions", [])
        alias = tf["filter"].get("alias")
        alias_str = f" (alias: {alias})" if alias else ""
        return (
            "- Filter"
            + alias_str
            + ":\n"
            + "".join([f"  - {cond}\n" for cond in conditions])
        )

    msg = (
        f"Input `tf` should be a dict with one of the following keys: "
        f"{transformation_steps},  input keys are: {list(tf.keys())}"
    )
    raise ValueError(msg)


def source_dict_to_string(source: SourceConfig) -> str:
    """Source business logic transformation dictionary to string values."""
    fltr_cnd = f"  \nFilter: {source['filter']}" if "filter" in source else ""
    return f"- {source['alias']} = {source['source']}{fltr_cnd}"


def union_dict_to_string(
    alias: str, column_mapping: list[str | dict[str, dict[str, str]]]
) -> str:
    """Union mapping to string values."""
    # Alias with 2 indentations, source tables with column mapping with 4 indentation
    union_list = []
    for table_map in column_mapping:
        if isinstance(table_map, dict):
            table_name = get_table_name(table_map)
            column_map_str = ": " + ", ".join(
                [f"{v} as {k}" for k, v in table_map[table_name].items()]
            )
        else:
            table_name = table_map
            column_map_str = ""

        union_list.append(f"    - {table_name}" + column_map_str)
    return f"  - {alias}:\n" + "\n".join(union_list)


def table_summary(
    spark: SparkSession,
    business_logic: BusinessLogicConfig,
    sources_title: str = "\n#### Sources\n",
    transformations_title: str = "#### Transformations\n",
) -> None:
    """Create summary as comment on table."""

    description = (
        "\n" + business_logic["description"] if "description" in business_logic else ""
    )

    sources = sources_title + "\n".join(
        [source_dict_to_string(source) for source in business_logic["sources"]]
    )

    transformations = (
        "\n\n"
        + transformations_title
        + "\n".join(
            [
                transformation_dict_to_string(tf)
                for tf in business_logic["transformations"]
            ]
        )
        if business_logic.get("transformations")
        else ""
    )
    filter_target = (
        "\n**Filter Target**\n- " + "\n- ".join(business_logic["filter_target"])
        if "filter_target" in business_logic
        else ""
    )

    drop_dups = (
        f"\n\nDrop duplicates = {business_logic['drop_duplicates']}"
        if "drop_duplicates" in business_logic
        else ""
    )

    comment_string = (
        f"{description}{sources}{transformations}{filter_target}{drop_dups}"
    ).replace('"', '\\"')
    spark.sql(f'COMMENT ON TABLE {business_logic["target"]} IS "{comment_string}"')


def target_expression_comments(
    spark: SparkSession, business_logic: BusinessLogicConfig
) -> None:
    """Add logic as comments to columns for data lineage."""
    for tgt_col in business_logic["expressions"].keys():
        tgt_table = business_logic["target"]
        expression = business_logic["expressions"][tgt_col].replace('"', '\\"')
        spark.sql(
            f'ALTER TABLE {tgt_table} ALTER COLUMN {tgt_col} COMMENT "{expression}"'
        )


def write_and_comment(
    spark: SparkSession,
    business_logic: BusinessLogicConfig,
    data: DataFrame,
    run_month: str,
    dq_check_folder: str = "dq_checks",
    source_system: str = "",
    schema: str = "",
    *,
    local: bool = False,
) -> bool:
    """Write data to Unity Catalog and add table & column comments from logic."""
    write_to_table(spark=spark, table_name=business_logic["target"], data=data)
    table_summary(spark=spark, business_logic=business_logic)
    target_expression_comments(spark=spark, business_logic=business_logic)
    if not schema:
        schema = business_logic["target"].split(".")[-1].split("_")[0]
    dq_validation = DQValidation(
        spark,
        table_name=business_logic["target"].split(".")[-1],
        schema_name=schema,
        dq_check_folder=dq_check_folder,
        run_month=run_month,
        source_system=standardize_delivery_entity(source_system),
        local=local,
    )
    check_result = dq_validation.checks(functional=True)
    return check_result is None or check_result
