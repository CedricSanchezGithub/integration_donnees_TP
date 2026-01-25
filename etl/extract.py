from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, LongType, ArrayType
from etl.shared.config import PROJECT_ROOT


def get_jsonl_schema() -> StructType:
    """Définit le schéma strict pour éviter l'inférence coûteuse."""
    nutriments_schema = StructType([
        StructField("energy-kcal_100g", FloatType(), True),
        StructField("sugars_100g", FloatType(), True),
        StructField("salt_100g", FloatType(), True),
        StructField("sodium_100g", FloatType(), True),
        StructField("fiber_100g", FloatType(), True),
        StructField("proteins_100g", FloatType(), True)
    ])

    return StructType([
        StructField("code", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("last_modified_t", LongType(), True),
        StructField("created_t", LongType(), True),
        StructField("brands", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("countries_tags", ArrayType(StringType()), True),
        StructField("nutriscore_grade", StringType(), True),
        StructField("nova_group", IntegerType(), True),
        StructField("ecoscore_grade", StringType(), True),
        StructField("nutriments", nutriments_schema, True),
        StructField("additives_n", IntegerType(), True),
        StructField("product_name_fr", StringType(), True),
        StructField("product_name_en", StringType(), True),
    ])


def extract_data(spark: SparkSession, file_name: str = "openfoodfacts-products.jsonl") -> DataFrame:
    """Lit le fichier JSONL brut."""
    raw_path = str(PROJECT_ROOT / "data" / "raw" / file_name)
    print(f"📥 Lecture du fichier : {raw_path}")

    try:
        df = spark.read \
            .schema(get_jsonl_schema()) \
            .json(raw_path)
        print(f"✅ Extraction réussie.")
        return df
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction : {e}")
        raise