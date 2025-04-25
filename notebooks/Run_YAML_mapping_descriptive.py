# Databricks notebook source
# DBTITLE 1,Initialize notebook
import os
import sys

import pandas as pd
from py4j.protocol import Py4JError
from pyspark.sql.utils import (
    AnalysisException,  # Fixed typo from Analysis to AnalysisException
)

from src.extract.master_data_sql import GetIntegratedData
from src.transform.table_write_and_comment import write_and_comment
from src.transform.transform_business_logic_sql import transform_business_logic_sql
from src.utils.get_env import get_catalog
from src.utils.parameter_utils import parse_delivery_entity
from src.utils.parse_yaml import parse_yaml
from src.validate.run_all import validate_business_logic_mapping

# Constants
MAX_DISPLAY_CATEGORIES = 20  # Maximum number of distinct values to display for a column

user = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)

working_dir = f"/Workspace/Users/{user}/bsrc-etl"
sys.path.append(working_dir)
sys.path.append(f"{working_dir}/test")

spark.sql(f"""USE CATALOG {get_catalog(spark)};""")

dbutils.widgets.dropdown(
    "stage", "integration", ["staging", "integration", "enrichment"], "1. Stage"
)
dbutils.widgets.dropdown(
    "target_mapping",
    "",
    ["", *os.listdir(f"{working_dir}/business_logic/{dbutils.widgets.get('stage')}")],
    "2. Target mapping",
)
dbutils.widgets.text(
    "run_month",
    "202408",
    "3. Run Month",
)
dbutils.widgets.text(
    "delivery_entity",
    "",
    "4. Delivery Entity",
)

stage = dbutils.widgets.get("stage")
target_mapping = dbutils.widgets.get("target_mapping")
run_month = dbutils.widgets.get("run_month")
delivery_entity = dbutils.widgets.get("delivery_entity")

# Get both original and standardized delivery entity in one call
original_delivery_entity, standardized_delivery_entity = parse_delivery_entity(
    delivery_entity
)

# COMMAND ----------

# DBTITLE 1,Read business logic YAML
business_logic_dict = parse_yaml(
    yaml_path=f"{stage}/{target_mapping}",
    parameters={
        "RUN_MONTH": run_month,
        "DELIVERY_ENTITY": standardized_delivery_entity,
    },
)

# COMMAND ----------

# DBTITLE 1,Target delta table
print(business_logic_dict["target"])

# COMMAND ----------

# DBTITLE 1,Sources
display(pd.DataFrame(business_logic_dict["sources"]).fillna(""))

# COMMAND ----------

# DBTITLE 1,Joins
if "transformations" in business_logic_dict and any(
    "join" in tf for tf in business_logic_dict["transformations"]
):
    display(
        pd.DataFrame(
            [
                d["join"]
                for d in business_logic_dict["transformations"]
                if "join" in d.keys()
            ]
        )
    )
else:
    print("No join transformations found")

# COMMAND ----------

# DBTITLE 1,Transformations (with filter)
if "transformations" in business_logic_dict:
    filter_transformations = [
        d["filter"] for d in business_logic_dict["transformations"] if "filter" in d
    ]
    if filter_transformations:
        print(f"Found {len(filter_transformations)} filter transformation(s)")

        # Create a df for better visualization
        filter_data = []
        for i, filter_config in enumerate(filter_transformations, 1):
            for j, condition in enumerate(filter_config.get("conditions", []), 1):
                filter_data.append(
                    {
                        "Filter #": i,
                        "Condition #": j,
                        "Condition": condition,
                        "Source": filter_config.get("source", "Previous result"),
                        "Log Reductions": filter_config.get("log_reductions", False),
                    }
                )
        if filter_data:
            display(pd.DataFrame(filter_data))
        else:
            print("No filter transformations found.")

# COMMAND ----------

# DBTITLE 1,Aggregation
if "transformations" in business_logic_dict and any(
    "aggregation" in tf for tf in business_logic_dict["transformations"]
):
    display(
        pd.DataFrame(
            [
                d["aggregation"]
                for d in business_logic_dict["transformations"]
                if "aggregation" in d
            ]
        )
    )
else:
    print("No aggregation transformations found.")

# COMMAND ----------

# DBTITLE 1,Variables
if "variables" in business_logic_dict:
    display(
        pd.DataFrame.from_dict(business_logic_dict["variables"], orient="index")
        .reset_index()
        .rename(columns={"index": "Variable Name", 0: "Expression"})
    )

# COMMAND ----------

# DBTITLE 1,Add variables (from transformations)
if "transformations" in business_logic_dict and any(
    "add_variables" in tf for tf in business_logic_dict["transformations"]
):
    add_variables_data = []

    for tf in business_logic_dict["transformations"]:
        if "add_variables" in tf:
            for var_name, expression in (
                tf["add_variables"].get("column_mapping", {}).items()
            ):
                add_variables_data.append(
                    {
                        "Variable Name": var_name,
                        "Expression": expression,
                        "Source": tf["add_variables"].get("source", "Previous result"),
                    }
                )

    if add_variables_data:
        display(pd.DataFrame(add_variables_data))
    else:
        print("No add_variables transformations found.")
else:
    print("No add_variables transformations found.")

# COMMAND ----------

# DBTITLE 1,Expressions
display(
    pd.DataFrame.from_dict(business_logic_dict["expressions"], orient="index")
    .reset_index()
    .rename(columns={"index": "Target Column", 0: "Expression"})
)

# COMMAND ----------

# DBTITLE 1,Validate YAML
validate_business_logic_mapping(spark, business_logic_dict)

# COMMAND ----------

# DBTITLE 1,Load sources and apply filters and joins
data = GetIntegratedData(spark, business_logic_dict).get_integrated_data()


# COMMAND ----------

# DBTITLE 1,Apply filter impact analysis
if "transformations" in business_logic_dict:
    filter_transformations = [
        d["filter"]
        for d in business_logic_dict["transformations"]
        if "filter" in d.keys()
    ]
    if filter_transformations:
        print("Filter Impact Analysis:")
        print("============================")

        total_rows = data.count()
        print(f"Final row count after all filters: {total_rows:,}")

        try:
            sample_columns = data.columns[:10]

            for col in sample_columns:
                try:
                    distinct_count = data.select(col).distinct().count()
                    if 1 < distinct_count <= MAX_DISPLAY_CATEGORIES:
                        print(f"\nDistribution by {col}:")
                        try:
                            display(
                                data.groupBy(col)
                                .count()
                                .orderBy("count", ascending=False)
                            )
                            break
                        except (AnalysisException, Py4JError, ValueError) as exc:
                            print(
                                f"Could not display distribution for column {col}: {exc}"
                            )
                            continue
                except (AnalysisException, Py4JError) as exc:
                    print(f"Could not analyze column {col}: {exc}")
                    continue
        except (AnalysisException, Py4JError, ValueError) as exc:
            print(f"Error analyzing column distributions: {exc}")

        if total_rows > 0:
            limit_rows = min(5, total_rows)
            print(f"\nSample of {limit_rows} rows that passed all filters:")
            display(data.limit(limit_rows))

# COMMAND ----------

# DBTITLE 1,Apply expressions
transformed_data = transform_business_logic_sql(
    data,
    business_logic_dict,
)

# COMMAND ----------

# DBTITLE 1,Show output for validation
display(transformed_data)

# COMMAND ----------

# DBTITLE 1,Write output to Delta table and comment lineage information
write_and_comment(
    spark,
    business_logic_dict,
    transformed_data,
    run_month=run_month,
    source_system=standardized_delivery_entity,
)
