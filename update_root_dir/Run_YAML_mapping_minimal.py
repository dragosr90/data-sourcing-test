# Databricks notebook source
# DBTITLE 1,Initialize notebook
import os
import sys
from pathlib import Path

import abnamro_bsrc_etl.config.constants

user = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)
working_dir = f"/Workspace/Users/{user}/bsrc-etl"
abnamro_bsrc_etl.config.constants.PROJECT_ROOT_DIRECTORY = Path(working_dir)
from abnamro_bsrc_etl.extract.master_data_sql import GetIntegratedData
from abnamro_bsrc_etl.transform.table_write_and_comment import write_and_comment
from abnamro_bsrc_etl.transform.transform_business_logic_sql import (
    transform_business_logic_sql,
)
from abnamro_bsrc_etl.utils.get_env import get_catalog
from abnamro_bsrc_etl.utils.parse_yaml import parse_yaml
from abnamro_bsrc_etl.validate.run_all import validate_business_logic_mapping

sys.path.append(working_dir)
sys.path.append(f"{working_dir}/test")

spark.sql(f"""USE CATALOG {get_catalog(spark)};""")

dbutils.widgets.dropdown(
    "stage",
    "integration",
    ["staging", "integration", "enrichment", "distribution", "comparison"],
    "1. Stage",
)
dbutils.widgets.dropdown(
    "target_mapping",
    "",
    [
        "",
        *sorted(
            os.listdir(f"{working_dir}/business_logic/{dbutils.widgets.get('stage')}")
        ),
    ],
    "2. Target mapping",
)
dbutils.widgets.text(
    "run_month",
    "202412",
    "3. Run Month",
)

delivery_entities = [
    "AACF",
    "ALFAM",
    "BEAM",
    "BRDWH",
    "ENTRACARD",
    "FBS",
    "FRP",
    "IHUB-BE1",
    "IHUB-DE2",
    "IHUB-FR1",
    "LEASE",
    "MAINBANK",
    "VIENNA",
]

dbutils.widgets.dropdown(
    "delivery_entity",
    "",
    ["", *sorted(delivery_entities)],
    "4. Delivery Entity",
)

stage = dbutils.widgets.get("stage")
target_mapping = dbutils.widgets.get("target_mapping")
run_month = dbutils.widgets.get("run_month")
delivery_entity = dbutils.widgets.get("delivery_entity")

# COMMAND ----------

# DBTITLE 1,Read business logic YAML

business_logic_dict = parse_yaml(
    yaml_path=f"{stage}/{target_mapping}",
    parameters={
        "RUN_MONTH": run_month,
        "DELIVERY_ENTITY": delivery_entity,
    },
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

# DBTITLE 1,Write output to Delta table and comment lineage information

write_and_comment(
    spark,
    business_logic_dict,
    transformed_data,
    run_month=run_month,
    source_system=delivery_entity,
)
