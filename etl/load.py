import time
import mysql.connector
from mysql.connector import Error
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from etl.shared.config import MARIADB_CONFIG
from etl.shared.metrics import MetricsCollector


# --- 1. FACTORISATION DE LA CONNEXION (NOUVEAU HELPER) ---
def _get_db_connection():
    """
    Crée une connexion MySQL avec les bons paramètres et une logique de Retry automatique.
    Centralise la config (collation, user, etc.).
    """
    max_retries = 10
    delay = 5

    for i in range(max_retries):
        try:
            conn = mysql.connector.connect(
                host="localhost",
                port=3306,
                user=MARIADB_CONFIG["user"],
                password=MARIADB_CONFIG["password"],
                database=MARIADB_CONFIG["database"],
                collation=MARIADB_CONFIG["collation"]
            )
            if conn.is_connected():
                return conn
        except Error as e:
            if i == max_retries - 1:
                print(f"❌ Erreur critique de connexion MySQL après {max_retries} essais.")
                raise e
            print(f"⏳ MySQL indisponible ({e}). Nouvelle tentative dans {delay}s... ({i + 1}/{max_retries})")
            time.sleep(delay)
    return None


def init_database(reset=False):
    """Crée les tables (DDL)."""
    print("🛠 Initialisation de la BDD...")

    # Appel simplifié grâce au helper
    conn = _get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SET GLOBAL max_allowed_packet=67108864")

        if reset:
            tables_to_drop = [
                "fact_nutrition_snapshot", "bridge_product_category",
                "dim_category", "dim_product",
                "staging_products_to_close", "staging_facts",
                "staging_categories", "staging_bridge"
            ]
            for t in tables_to_drop:
                cursor.execute(f"DROP TABLE IF EXISTS {t}")

        # 1. DDL - Dimension Produit
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS dim_product
                       (
                           product_sk
                           INT
                           AUTO_INCREMENT
                           PRIMARY
                           KEY,
                           code
                           VARCHAR
                       (
                           255
                       ) NOT NULL,
                           product_name TEXT,
                           brands TEXT,
                           categories TEXT,
                           row_hash CHAR
                       (
                           64
                       ) NOT NULL,
                           effective_from DATETIME,
                           effective_to DATETIME,
                           is_current BOOLEAN,
                           INDEX idx_code
                       (
                           code
                       ),
                           INDEX idx_current
                       (
                           is_current
                       )
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                       """)

        # 2. DDL - Dimension Catégorie
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS dim_category
                       (
                           category_sk
                           INT
                           AUTO_INCREMENT
                           PRIMARY
                           KEY,
                           category_name
                           VARCHAR
                       (
                           255
                       ) NOT NULL,
                           UNIQUE KEY idx_category_name
                       (
                           category_name
                       )
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                       """)

        # 3. DDL - Bridge
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS bridge_product_category
                       (
                           product_sk
                           INT
                           NOT
                           NULL,
                           category_sk
                           INT
                           NOT
                           NULL,
                           PRIMARY
                           KEY
                       (
                           product_sk,
                           category_sk
                       ),
                           FOREIGN KEY
                       (
                           product_sk
                       ) REFERENCES dim_product
                       (
                           product_sk
                       ),
                           FOREIGN KEY
                       (
                           category_sk
                       ) REFERENCES dim_category
                       (
                           category_sk
                       )
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                       """)

        # 4. DDL - Faits
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot
                       (
                           fact_sk
                           INT
                           AUTO_INCREMENT
                           PRIMARY
                           KEY,
                           product_sk
                           INT
                           NOT
                           NULL,
                           date_sk
                           INT
                           NOT
                           NULL,
                           nutriscore_grade
                           VARCHAR
                       (
                           50
                       ),
                           additives_n INT,
                           ecoscore_grade VARCHAR
                       (
                           50
                       ),
                           nova_group INT,
                           energy_kcal_100g FLOAT,
                           sugars_100g FLOAT,
                           salt_100g FLOAT,
                           proteins_100g FLOAT,
                           FOREIGN KEY
                       (
                           product_sk
                       ) REFERENCES dim_product
                       (
                           product_sk
                       ),
                           UNIQUE KEY idx_unique_fact
                       (
                           product_sk,
                           date_sk
                       )
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                       """)

        print("✅ Tables initialisées.")

    finally:
        conn.close()


def load_dimension_scd2(spark: SparkSession, df_source: DataFrame, metrics: MetricsCollector = None):
    """Gère le SCD2."""
    print("🔄 Démarrage du processus SCD2...")

    df_target = spark.read.format("jdbc") \
        .option("url", MARIADB_CONFIG["url"]) \
        .option("dbtable", "(SELECT code, row_hash as target_hash FROM dim_product WHERE is_current = 1) as t") \
        .option("user", MARIADB_CONFIG["user"]) \
        .option("password", MARIADB_CONFIG["password"]) \
        .option("driver", MARIADB_CONFIG["driver"]) \
        .load()

    df_joined = df_source.join(df_target, on="code", how="left")

    condition_new = col("target_hash").isNull()
    condition_changed = col("target_hash") != col("row_hash")

    df_to_insert = df_joined.filter(condition_new | condition_changed) \
        .select(
        col("code"), col("product_name"), col("brands"), col("categories"),
        col("row_hash"),
        col("last_modified_ts").alias("effective_from"),
        lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_to"),
        lit(True).alias("is_current")
    )

    df_to_close = df_joined.filter(condition_changed).select("code")

    count_insert = df_to_insert.count()
    count_close = df_to_close.count()

    print(f"📊 Analyse SCD2 : {count_insert} à insérer / {count_close} à fermer.")

    if metrics:
        metrics.set_metric("dim_products_inserted", count_insert)
        metrics.set_metric("dim_products_closed", count_close)

    if count_close > 0:
        print("🔒 Fermeture des anciennes versions...")
        try:
            _write_jdbc(df_to_close, "staging_products_to_close", mode="overwrite")
            _close_products_via_staging()
        except Exception as e:
            print(f"❌ Erreur fermeture : {e}")
            raise

    if count_insert > 0:
        print("🚀 Insertion des nouvelles versions...")
        try:
            _write_jdbc(df_to_insert, "dim_product", mode="append")
        except Exception as e:
            print(f"❌ Erreur insertion : {e}")
            raise


def load_facts(df: DataFrame, metrics: MetricsCollector = None):
    """Charge la table de faits (Upsert)."""
    print("🚚 Chargement de fact_nutrition_snapshot...")

    count = df.count()
    if metrics:
        metrics.set_metric("facts_loaded", count)

    if count == 0:
        return

    try:
        _write_jdbc(df, "staging_facts", mode="overwrite")
        _upsert_facts_via_staging()
        print(f"✅ {count} Faits traités.")
    except Exception as e:
        print(f"❌ Erreur chargement faits : {e}")
        raise


def load_dim_categories(df: DataFrame):
    """Charge les catégories uniques."""
    print("📂 Chargement de dim_category...")
    try:
        _write_jdbc(df, "staging_categories", mode="overwrite")
        _execute_sql_staging("""
                             INSERT
                             IGNORE INTO dim_category (category_name)
                             SELECT DISTINCT category_name
                             FROM staging_categories
                             WHERE category_name IS NOT NULL
                               AND category_name != ''
                             """, "staging_categories")
        print("✅ Catégories chargées.")
    except Exception as e:
        print(f"❌ Erreur catégories : {e}")
        raise


def load_bridge(df: DataFrame):
    """Charge le Bridge."""
    print("🔗 Chargement du Bridge...")
    try:
        _write_jdbc(df, "staging_bridge", mode="overwrite")
        _execute_sql_staging("""
                             INSERT
                             IGNORE INTO bridge_product_category (product_sk, category_sk)
                             SELECT product_sk, category_sk
                             FROM staging_bridge
                             """, "staging_bridge")
        print("✅ Bridge chargé.")
    except Exception as e:
        print(f"❌ Erreur Bridge : {e}")
        raise


# --- HELPERS SIMPLIFIÉS ---

def _execute_sql_staging(sql: str, staging_table_to_drop: str = None):
    conn = _get_db_connection()  # Appel au helper unique
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        if staging_table_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table_to_drop}")
            conn.commit()
    finally:
        conn.close()


def _close_products_via_staging():
    conn = _get_db_connection()  # Appel au helper unique
    cursor = conn.cursor()
    try:
        query = """
                UPDATE dim_product d
                    JOIN staging_products_to_close s \
                ON d.code = s.code
                    SET d.is_current = 0, d.effective_to = NOW()
                WHERE d.is_current = 1 \
                """
        cursor.execute(query)
        conn.commit()
        cursor.execute("DROP TABLE IF EXISTS staging_products_to_close")
        conn.commit()
    finally:
        conn.close()


def _upsert_facts_via_staging():
    conn = _get_db_connection()  # Appel au helper unique
    cursor = conn.cursor()
    try:
        query = """
                INSERT INTO fact_nutrition_snapshot (product_sk, date_sk, nutriscore_grade, additives_n, \
                                                     ecoscore_grade, nova_group, energy_kcal_100g, \
                                                     sugars_100g, salt_100g, proteins_100g)
                SELECT product_sk, \
                       date_sk, \
                       nutriscore_grade, \
                       additives_n, \
                       ecoscore_grade, \
                       nova_group, \
                       energy_kcal_100g, \
                       sugars_100g, \
                       salt_100g, \
                       proteins_100g
                FROM staging_facts ON DUPLICATE KEY \
                UPDATE \
                    nutriscore_grade = \
                VALUES (nutriscore_grade), additives_n = \
                VALUES (additives_n), ecoscore_grade = \
                VALUES (ecoscore_grade), nova_group = \
                VALUES (nova_group), energy_kcal_100g = \
                VALUES (energy_kcal_100g), sugars_100g = \
                VALUES (sugars_100g), salt_100g = \
                VALUES (salt_100g), proteins_100g = \
                VALUES (proteins_100g) \
                """
        cursor.execute(query)
        conn.commit()
        cursor.execute("DROP TABLE IF EXISTS staging_facts")
        conn.commit()
    finally:
        conn.close()


def _write_jdbc(df: DataFrame, table: str, mode: str = "append"):
    df.write.jdbc(
        url=MARIADB_CONFIG["url"],
        table=table,
        mode=mode,
        properties={
            "user": MARIADB_CONFIG["user"],
            "password": MARIADB_CONFIG["password"],
            "driver": MARIADB_CONFIG["driver"],
            "batchsize": "5000"
        },
    )


def fetch_product_keys(spark: SparkSession) -> DataFrame:
    return spark.read.format("jdbc") \
        .option("url", MARIADB_CONFIG["url"]) \
        .option("dbtable", "dim_product") \
        .option("user", MARIADB_CONFIG["user"]) \
        .option("password", MARIADB_CONFIG["password"]) \
        .option("driver", MARIADB_CONFIG["driver"]) \
        .load().filter(col("is_current") == True).select("product_sk", "code")


def fetch_category_keys(spark: SparkSession) -> DataFrame:
    return spark.read.format("jdbc") \
        .option("url", MARIADB_CONFIG["url"]) \
        .option("dbtable", "dim_category") \
        .option("user", MARIADB_CONFIG["user"]) \
        .option("password", MARIADB_CONFIG["password"]) \
        .option("driver", MARIADB_CONFIG["driver"]) \
        .load()