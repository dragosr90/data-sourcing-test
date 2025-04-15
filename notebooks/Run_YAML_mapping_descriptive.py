# Databricks notebook source
# DBTITLE 1,Initialize notebook
import os
import sys

import pandas as pd

from src.extract.master_data_sql import GetIntegratedData
from src.transform.table_write_and_comment import write_and_comment
from src.transform.transform_business_logic_sql import transform_business_logic_sql
from src.utils.get_catalog import get_catalog
from src.utils.parameter_utils import parse_delivery_entity
from src.utils.parse_yaml import parse_yaml
from src.validate.run_all import validate_business_logic_mapping

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
if (
    "transformations" in business_logic_dict
    and "join" in business_logic_dict["transformations"][0]
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

# COMMAND ----------

# DBTITLE 1,Aggregation
if (
    "transformations" in business_logic_dict
    and "aggregation" in business_logic_dict["transformations"][-1]
):
    display(pd.DataFrame([business_logic_dict["transformations"][-1]["aggregation"]]))

# COMMAND ----------

# DBTITLE 1,Variables
if "variables" in business_logic_dict:
    display(
        pd.DataFrame.from_dict(business_logic_dict["variables"], orient="index")
        .reset_index()
        .rename(columns={"index": "Variable Name", 0: "Expression"})
    )

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
