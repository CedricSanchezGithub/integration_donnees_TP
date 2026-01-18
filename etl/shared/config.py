from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
JARS_DIR = PROJECT_ROOT / "jars"
MYSQL_JAR_PATH = list(JARS_DIR.glob("mysql-connector-j*.jar"))[0]

SPARK_CONFIG = {
    "spark.app.name": "OFF_ETL",
    "spark.master": "local[*]",
    "spark.driver.memory": "4g",
    "spark.jars": str(MYSQL_JAR_PATH),
    "spark.sql.warehouse.dir": str(PROJECT_ROOT / "spark-warehouse")
}