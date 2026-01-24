import time
from etl.shared.utils import get_spark_session
from etl.shared.config import PROJECT_ROOT, DEV_CONFIG
from etl.shared.metrics import MetricsCollector

from etl.extract import extract_data
from etl.transform import (
    clean_data,
    add_technical_hash,
    prepare_fact_table,
    extract_unique_categories,
    prepare_bridge_table
)
from etl.load import (
    init_database,
    load_dimension_scd2,
    load_facts,
    fetch_product_keys,
    load_dim_categories,
    fetch_category_keys,
    load_bridge
)
from etl.visualization import visualize_input, visualize_output


def main():
    # 1. Initialisation des Métriques
    metrics = MetricsCollector()
    print("🚀 Démarrage du Pipeline ETL OpenFoodFacts (Mode SCD2 + Bridge + Viz)")

    try:
        spark = get_spark_session()

        # ⚠️ IMPORTANT :
        # - Mettre à True pour recréer les tables (Schema change / Reset propre)
        # - Mettre à False pour conserver l'historique et tester le SCD2
        init_database(reset=False)

        # 2. Extraction
        df_raw = extract_data(spark)

        # --- BLOC SAMPLING (MODE DEV) ---
        # Permet de travailler sur un échantillon pour accélérer les tests
        if DEV_CONFIG["enabled"]:
            fraction = DEV_CONFIG["sample_fraction"]
            seed = DEV_CONFIG["seed"]
            print(f"⚠️ MODE DEV ACTIVÉ : Échantillonnage de {fraction * 100}% des données (Seed={seed}).")

            # On écrase df_raw avec la version réduite
            df_raw = df_raw.sample(withReplacement=False, fraction=fraction, seed=seed)
        # -------------------------------

        # Métrique : Données traitées (après sampling)
        count_raw = df_raw.count()
        metrics.set_metric("rows_read", count_raw)
        print(f"📥 Données traitées pour ce run : {count_raw} lignes.")

        visualize_input(df_raw)

        # 3. Transformation (Silver) - Nettoyage
        df_silver = clean_data(df_raw)

        # Métrique : Données nettoyées
        count_silver = df_silver.count()
        metrics.set_metric("rows_processed", count_silver)
        metrics.set_metric("rows_filtered", count_raw - count_silver)
        print(f"📊 Données nettoyées : {count_silver} produits.")

        # Ajout du Hash Technique pour le SCD2
        df_hashed = add_technical_hash(df_silver)

        # 4. Chargement Dimension Produit (SCD2)
        # On passe 'metrics' pour compter les insertions et fermetures
        load_dimension_scd2(spark, df_hashed, metrics)

        # --- GESTION DES CATÉGORIES ET BRIDGE ---
        print("🔗 Traitement du modèle relationnel avancé (Bridge)...")

        # A. Extraire et charger les catégories uniques
        df_cats = extract_unique_categories(df_silver)
        load_dim_categories(df_cats)

        # B. Récupérer les clés techniques (SK) nécessaires au Bridge
        # On a besoin des product_sk (générés par MySQL) et des category_sk
        df_prod_keys = fetch_product_keys(spark)
        df_cat_keys = fetch_category_keys(spark)

        # C. Construire et charger la table de liaison (Bridge)
        df_bridge = prepare_bridge_table(df_silver, df_prod_keys, df_cat_keys)
        load_bridge(df_bridge)
        # -------------------------------------------------------

        # 5. Chargement de la Table de Faits
        # Optimisation : On réutilise df_prod_keys pour lier les faits aux produits
        df_facts = prepare_fact_table(df_hashed, df_prod_keys)
        load_facts(df_facts, metrics)

        # 6. Visualisation finale
        visualize_output()

        # Finalisation du rapport
        metrics.finalize(success=True)
        print(f"✅ Pipeline terminé avec succès en {metrics.data['duration_seconds']}s.")

    except Exception as e:
        # En cas d'erreur, on loggue l'anomalie et on marque le run comme FAILED
        metrics.finalize(success=False)
        metrics.add_anomaly("CRITICAL_FAILURE", str(e))
        print(f"❌ Erreur critique dans le pipeline : {e}")
        raise
    finally:
        # 7. Sauvegarde systématique du rapport JSON (même en cas de crash)
        report_dir = PROJECT_ROOT / "reports"
        metrics.save_report(report_dir)
        spark.stop()


if __name__ == "__main__":
    main()