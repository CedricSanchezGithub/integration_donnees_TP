from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, from_unixtime, sha2, concat_ws, date_format, when, isnan

def clean_data(df_raw: DataFrame) -> DataFrame:
    """Applique le nettoyage Silver (Typage, Trim)."""
    print("🧹 Nettoyage des données (Silver)...")

    # TODO: En prod, retirer le sample ou le gérer via config
    # df_raw = df_raw.sample(withReplacement=False, fraction=0.01, seed=42)

    df_cleaned = df_raw \
        .select(
        trim(col("code")).alias("code"),
        trim(col("product_name")).alias("product_name"),
        from_unixtime(col("last_modified_t")).cast("timestamp").alias("last_modified_ts"),
        from_unixtime(col("created_t")).cast("timestamp").alias("created_ts"),
        col("countries_tags"),
        trim(col("brands")).alias("brands"),
        trim(col("categories")).alias("categories"),
        trim(col("nutriscore_grade")).alias("nutriscore_grade"),
        trim(col("ecoscore_grade")).alias("ecoscore_grade"),
        col("nova_group").cast("integer").alias("nova_group"),
        col("nutriments.energy-kcal_100g").alias("energy_kcal_100g"),
        col("nutriments.sugars_100g").alias("sugars_100g"),
        col("nutriments.salt_100g").alias("salt_100g"),
        col("nutriments.proteins_100g").alias("proteins_100g")
    ) \
        .filter(col("code").isNotNull() & (col("code") != ""))

    print(f"✅ Nettoyage terminé.")
    return df_cleaned


def add_technical_hash(df_silver: DataFrame) -> DataFrame:
    """Ajoute le hash technique pour le SCD2."""
    print("🔑 Calcul des empreintes (Hash)...")

    columns_to_hash = [
        "product_name", "brands", "categories", "countries_tags",
        "nutriscore_grade", "nova_group", "ecoscore_grade",
        "energy_kcal_100g", "sugars_100g", "salt_100g", "proteins_100g"
    ]

    return df_silver.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[col(c) for c in columns_to_hash]), 256)
    )

def prepare_fact_table(df_hashed: DataFrame, df_dim_products: DataFrame) -> DataFrame:
    """Prépare la table de faits en joignant avec la dimension et en nettoyant les métriques."""
    print("📊 Préparation de la Fact Table...")

    # 1. Jointure pour récupérer product_sk
    df_joined = df_hashed.join(df_dim_products, on="code", how="inner")

    # 2. Liste des colonnes numériques à risque
    metrics = ["energy_kcal_100g", "sugars_100g", "salt_100g", "proteins_100g"]

    # 3. Nettoyage : On remplace Infinity/NaN par NULL (None)
    for metric in metrics:
        df_joined = df_joined.withColumn(
            metric,
            when(isnan(col(metric)) | (col(metric) == float("inf")) | (col(metric) == float("-inf")), None)
            .otherwise(col(metric))
        )

    return df_joined.select(
        col("product_sk"),
        date_format(col("last_modified_ts"), "yyyyMMdd").cast("integer").alias("date_sk"),
        col("nutriscore_grade"),
        col("ecoscore_grade"),
        col("nova_group"),
        col("energy_kcal_100g"),
        col("sugars_100g"),
        col("salt_100g"),
        col("proteins_100g")
    )