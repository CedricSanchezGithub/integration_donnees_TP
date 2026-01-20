from pyspark.sql import SparkSession
from etl.shared.config import SPARK_CONFIG


def get_spark_session(app_name: str = "OFF_ETL") -> SparkSession:
    """Crée et retourne une session Spark configurée."""
    builder = SparkSession.builder.appName(app_name)

    for key, val in SPARK_CONFIG.items():
        builder = builder.config(key, val)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark