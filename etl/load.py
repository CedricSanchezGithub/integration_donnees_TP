import time
import mysql.connector
from mysql.connector import Error
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from etl.shared.config import MYSQL_CONFIG


def init_database(reset=False):
    """Crée les tables (DDL) avec un mécanisme de Retry."""
    print("🛠 Initialisation de la BDD...")

    max_retries = 10
    delay = 5
    conn = None

    for i in range(max_retries):
        try:
            conn = mysql.connector.connect(
                host="localhost",
                port=3306,
                user=MYSQL_CONFIG["user"],
                password=MYSQL_CONFIG["password"],
                database=MYSQL_CONFIG["database"],
                collation=MYSQL_CONFIG["collation"]
            )
            if conn.is_connected():
                print("✅ Connexion MySQL établie.")
                break
        except Error as e:
            if i == max_retries - 1:
                print(f"❌ Impossible de se connecter après {max_retries} essais.")
                raise e
            print(f"⏳ MySQL indisponible ({e}). Nouvelle tentative dans {delay}s... ({i + 1}/{max_retries})")
            time.sleep(delay)

    cursor = conn.cursor()

    # Tuning pour gros paquets
    cursor.execute("SET GLOBAL max_allowed_packet=67108864")

    # NOTE: En SCD2 réel, on NE DROP PAS les tables à chaque fois
    if reset:
        cursor.execute("DROP TABLE IF EXISTS fact_nutrition_snapshot")
        cursor.execute("DROP TABLE IF EXISTS dim_product")

    # DDL - Dimension Produit
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

    # DDL - Faits
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
                   )
                       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                   """)
    conn.close()


def load_dimension_scd2(spark: SparkSession, df_source: DataFrame):
    """
    Gère le Slowly Changing Dimension Type 2 (SCD2).
    Compare la source et la cible pour détecter les changements.
    """
    print("🔄 Démarrage du processus SCD2...")

    # 1. Charger les données ACTUELLES de MySQL (Target)
    # On ne veut comparer qu'avec les produits actifs
    df_target = spark.read.format("jdbc") \
        .option("url", MYSQL_CONFIG["url"]) \
        .option("dbtable", "(SELECT code, row_hash as target_hash FROM dim_product WHERE is_current = 1) as t") \
        .option("user", MYSQL_CONFIG["user"]) \
        .option("password", MYSQL_CONFIG["password"]) \
        .option("driver", MYSQL_CONFIG["driver"]) \
        .load()

    # 2. Comparaison (Diff)
    # Left Join : On garde tout de la source, on matche avec la cible
    df_joined = df_source.join(df_target, on="code", how="left")

    # Définition des cas
    # Cas A : Nouveau produit (pas de correspondance dans Target)
    condition_new = col("target_hash").isNull()
    # Cas B : Produit modifié (le hash a changé)
    condition_changed = col("target_hash") != col("row_hash")

    # Produits à INSÉRER (Nouveaux + Modifiés)
    df_to_insert = df_joined.filter(condition_new | condition_changed) \
        .select(
        col("code"), col("product_name"), col("brands"), col("categories"),
        col("row_hash"),
        col("last_modified_ts").alias("effective_from"),
        lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_to"),
        lit(True).alias("is_current")
    )

    # Produits à FERMER (Seulement ceux qui existaient et ont changé)
    # Note : On ne ferme pas si c'est nouveau
    df_to_close = df_joined.filter(condition_changed).select("code")

    count_insert = df_to_insert.count()
    count_close = df_to_close.count()

    print(f"📊 Analyse SCD2 : {count_insert} à insérer / {count_close} à fermer (mise à jour).")

    # 3. Exécuter la fermeture (UPDATE SQL)
    if count_close > 0:
        print("🔒 Fermeture des anciennes versions...")
        try:
            # On collecte les codes driver-side (OK pour volumes < 1M, sinon utiliser temp table)
            codes_to_update = [row.code for row in df_to_close.collect()]
            _close_products_in_mysql(codes_to_update)
            print(f"✅ {count_close} versions fermées dans dim_product.")
        except Exception as e:
            print(f"❌ Erreur lors de la fermeture des versions : {e}")
            raise

    # 4. Exécuter l'insertion (APPEND Spark)
    if count_insert > 0:
        print("🚀 Insertion des nouvelles versions...")
        try:
            _write_jdbc(df_to_insert, "dim_product")
            print(f"✅ {count_insert} enregistrements insérés dans dim_product.")
        except Exception as e:
            print(f"❌ Erreur lors de l'insertion dans dim_product : {e}")
            raise
    else:
        print("✅ Aucune modification détectée.")


def load_facts(df: DataFrame):
    """Charge la table de faits."""
    print("🚚 Chargement de fact_nutrition_snapshot...")
    try:
        _write_jdbc(df, "fact_nutrition_snapshot")
        print(f"✅ Faits insérés dans fact_nutrition_snapshot.")
    except Exception as e:
        print(f"❌ Erreur lors du chargement des faits : {e}")
        raise


def _close_products_in_mysql(codes: list):
    """Ferme les lignes obsolètes via une requête SQL brute."""
    if not codes:
        return

    conn = mysql.connector.connect(
        host="localhost",
        port=3306,
        user=MYSQL_CONFIG["user"], password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        collation=MYSQL_CONFIG["collation"]
    )
    cursor = conn.cursor()

    batch_size = 1000
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        format_strings = ','.join(['%s'] * len(batch))

        query = f"""
            UPDATE dim_product 
            SET is_current = 0, effective_to = NOW() 
            WHERE is_current = 1 AND code IN ({format_strings})
        """
        cursor.execute(query, tuple(batch))
        conn.commit()

    conn.close()


def _write_jdbc(df: DataFrame, table: str):
    """Helper privé pour l'écriture JDBC."""
    df.write.jdbc(
        url=MYSQL_CONFIG["url"],
        table=table,
        mode="append",
        properties={
            "user": MYSQL_CONFIG["user"],
            "password": MYSQL_CONFIG["password"],
            "driver": MYSQL_CONFIG["driver"],
            "batchsize": "1000"
        },
    )


def fetch_product_keys(spark: SparkSession) -> DataFrame:
    """Récupère le mapping code -> product_sk depuis MySQL."""
    return spark.read.format("jdbc") \
        .option("url", MYSQL_CONFIG["url"]) \
        .option("dbtable", "dim_product") \
        .option("user", MYSQL_CONFIG["user"]) \
        .option("password", MYSQL_CONFIG["password"]) \
        .option("driver", MYSQL_CONFIG["driver"]) \
        .load() \
        .filter(col("is_current") == True) \
        .select("product_sk", "code")