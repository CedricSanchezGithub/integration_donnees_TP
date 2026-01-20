from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
JARS_DIR = PROJECT_ROOT / "jars"

try:
    MYSQL_JAR_PATH = list(JARS_DIR.glob("mysql-connector-j*.jar"))[0]
except IndexError:
    raise FileNotFoundError(f"❌ Le driver JDBC MySQL est introuvable dans {JARS_DIR}. Télécharge-le !")

SPARK_CONFIG = {
    "spark.app.name": "OFF_ETL",
    "spark.master": "local[*]",
    "spark.driver.memory": "4g",
    "spark.jars": str(MYSQL_JAR_PATH),
    "spark.driver.extraClassPath": str(MYSQL_JAR_PATH),
    "spark.sql.warehouse.dir": str(PROJECT_ROOT / "spark-warehouse")
}

MYSQL_CONFIG = {
    "url": "jdbc:mysql://localhost:3306/openfoodfacts",
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

DEV_MODE = True
SAMPLE_FRACTION = 0.01