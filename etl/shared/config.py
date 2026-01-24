import json
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

MARIADB_CONFIG = {
    "database": "OFF",
    "url": "jdbc:mysql://localhost:3306/OFF",
    "user": "root",
    "password": "rootpassword",
    "driver": "com.mysql.cj.jdbc.Driver",
    "collation": "utf8mb4_general_ci"
}

SAMPLE_FRACTION = 0.01
DEFAULT_DEV_CONFIG = {
    "enabled": True,
    "sample_fraction": 0.05,
    "seed": 42
}

CONFIG_FILE = PROJECT_ROOT / "config.json"

if CONFIG_FILE.exists():
    try:
        with open(CONFIG_FILE, "r") as f:
            DEV_CONFIG = json.load(f)
            # Sécurité : on s'assure que les clés existent
            if "enabled" not in DEV_CONFIG: DEV_CONFIG = DEFAULT_DEV_CONFIG
    except Exception:
        print("⚠️ Erreur de lecture du config.json, utilisation des valeurs par défaut.")
        DEV_CONFIG = DEFAULT_DEV_CONFIG
else:
    DEV_CONFIG = DEFAULT_DEV_CONFIG