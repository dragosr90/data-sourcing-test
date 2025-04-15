from pyspark.sql import SparkSession


def get_dbutils(spark: SparkSession):  # noqa: ANN201 # pragma: no cover
    """Getting dbutils based on local or databricks"""
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils
