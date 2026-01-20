from etl.shared.utils import get_spark_session
from etl.extract import extract_data
from etl.transform import clean_data, add_technical_hash, prepare_fact_table
from etl.load import init_database, load_dimension_scd2, load_facts, fetch_product_keys


def main():
    print("🚀 Démarrage du Pipeline ETL OpenFoodFacts (Mode SCD2)")

    try:
        spark = get_spark_session()
        init_database()

        # 1. Extraction
        df_raw = extract_data(spark)

        # 2. Transformation (Dimension)
        df_silver = clean_data(df_raw)

        count = df_silver.count()
        print(f"📊 Données source : {count} produits.")

        df_hashed = add_technical_hash(df_silver)

        # 3. Chargement Dimension (SCD2 Intelligent)
        load_dimension_scd2(spark, df_hashed)

        # 4. Préparation & Chargement Faits
        df_keys = fetch_product_keys(spark)
        df_facts = prepare_fact_table(df_hashed, df_keys)
        load_facts(df_facts)

        print("✅ Pipeline terminé avec succès.")
    except Exception as e:
        print(f"❌ Erreur critique dans le pipeline : {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()