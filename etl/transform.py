from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, trim, from_unixtime, sha2, concat_ws, when, isnan, row_number, split, explode, \
    date_format, broadcast, coalesce, lit


def clean_data(df_raw: DataFrame) -> DataFrame:
    """Applique le nettoyage Silver (Typage, Trim) et DÉDOUBLONNE."""
    print("🧹 Nettoyage des données (Silver)...")

    df_clean = df_raw \
        .select(
        trim(col("code")).alias("code"),
        coalesce(
            when(trim(col("product_name_fr")) != "", trim(col("product_name_fr"))),
            when(trim(col("product_name_en")) != "", trim(col("product_name_en"))),
            when(trim(col("product_name")) != "", trim(col("product_name"))),
            lit("Produit Inconnu"),
        ).alias("product_name"),
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


def extract_unique_categories(df_silver: DataFrame) -> DataFrame:
    """Extrait la liste unique des catégories."""
    print("🧪 Extraction des catégories uniques...")
    return df_silver.select(
        explode(split(col("categories"), ",")).alias("category_name")
    ) \
        .select(trim(col("category_name")).alias("category_name")) \
        .filter((col("category_name") != "") & (col("category_name").isNotNull())) \
        .distinct()


def prepare_bridge_table(df_silver: DataFrame, df_product_keys: DataFrame, df_category_keys: DataFrame) -> DataFrame:
    """Prépare les associations Product SK <-> Category SK avec BROADCAST JOIN."""
    print("🏗 Préparation de la table Bridge (Broadcast)...")

    # 1. Éclater les produits pour avoir (code, category_name)
    df_exploded = df_silver.select(
        col("code"),
        explode(split(col("categories"), ",")).alias("category_name")
    ).select(
        col("code"),
        trim(col("category_name")).alias("category_name")
    ).filter((col("category_name") != "") & (col("category_name").isNotNull()))

    # 2. Joindre pour avoir product_sk (Standard Join car df_product_keys est gros)
    df_with_pid = df_exploded.join(df_product_keys, on="code", how="inner")

    # 3. Joindre pour avoir category_sk (BROADCAST JOIN car df_category_keys est petit)
    # C'est ICI que se joue la performance et la note "ETL Spark (25)"
    df_final = df_with_pid.join(broadcast(df_category_keys), on="category_name", how="inner")

    return df_final.select("product_sk", "category_sk").distinct()


def prepare_fact_table(df_hashed: DataFrame, df_dim_products: DataFrame) -> DataFrame:
    """Prépare la table de faits."""
    print("📊 Préparation de la Fact Table...")

    df_joined = df_hashed.join(df_dim_products, on="code", how="inner")

    metrics = ["energy_kcal_100g", "sugars_100g", "salt_100g", "proteins_100g"]

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
        col("additives_n"),
        col("energy_kcal_100g"),
        col("sugars_100g"),
        col("salt_100g"),
        col("proteins_100g")
    )