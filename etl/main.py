import time
from etl.shared.utils import get_spark_session
from etl.extract import extract_data
from etl.transform import clean_data, add_technical_hash, prepare_fact_table
from etl.load import init_database, load_dimension_scd2, load_facts, fetch_product_keys
from etl.visualization import visualize_input, visualize_output


def main():
    start_time = time.time()
    print("🚀 Démarrage du Pipeline ETL OpenFoodFacts (Mode SCD2 + Viz)")

    try:
        spark = get_spark_session()
        init_database()

        # 1. Extraction
        df_raw = extract_data(spark)
        visualize_input(df_raw)

        # 2. Transformation (Dimension)
        df_silver = clean_data(df_raw)
        count = df_silver.count()
        print(f"📊 Données source : {count} produits.")

        df_hashed = add_technical_hash(df_silver)
        load_dimension_scd2(spark, df_hashed)

        df_keys = fetch_product_keys(spark)
        df_facts = prepare_fact_table(df_hashed, df_keys)
        load_facts(df_facts)
        visualize_output()


        elapsed_time = time.time() - start_time
        print(f"✅ Pipeline terminé avec succès en {elapsed_time:.2f}s.")

    except Exception as e:
        print(f"❌ Erreur critique dans le pipeline : {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()