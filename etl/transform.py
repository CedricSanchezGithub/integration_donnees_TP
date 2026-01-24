from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, trim, from_unixtime, sha2, concat_ws, date_format, when, isnan, row_number

from etl.shared.config import SAMPLE_FRACTION


def clean_data(df_raw: DataFrame) -> DataFrame:
    """Applique le nettoyage Silver (Typage, Trim) et DÉDOUBLONNE."""

    print("🧹 Nettoyage des données (Silver)...")

    df_raw = df_raw.sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=42)

    df_clean = df_raw \
        .select(
        trim(col("code")).alias("code"),
        trim(col("product_name")).alias("product_name"),
        col("last_modified_t"),
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
        col("nutriments.proteins_100g").alias("proteins_100g"),
        col("additives_n").cast("integer").alias("additives_n"),
    ) \
        .filter(col("code").isNotNull() & (col("code") != ""))

    window_spec = Window.partitionBy("code").orderBy(col("last_modified_t").desc())

    df_deduplicated = df_clean.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn", "last_modified_t")

    return df_deduplicated


def add_technical_hash(df_silver: DataFrame) -> DataFrame:
    """Ajoute le hash technique pour le SCD2."""
    print("🔑 Calcul des empreintes (Hash)...")

    columns_to_hash = [
        "product_name", "brands", "categories", "countries_tags",
        "nutriscore_grade", "nova_group", "ecoscore_grade",
        "energy_kcal_100g", "sugars_100g", "salt_100g", "proteins_100g", "additives_n",
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
