import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from etl.shared.config import MYSQL_CONFIG
from sqlalchemy import create_engine


def visualize_input(df_spark: DataFrame):
    """Génère un graphique sur les données brutes (Input)."""
    print("🎨 Génération du graphique d'entrée (Distribution Nutri-Score)...")

    # Agrégation Spark
    df_distrib = df_spark.groupBy("nutriscore_grade").count().toPandas()

    # Nettoyage
    df_distrib = df_distrib[df_distrib["nutriscore_grade"].notnull()]
    df_distrib = df_distrib.sort_values("nutriscore_grade")

    plt.figure(figsize=(10, 6))

    sns.barplot(
        data=df_distrib,
        x="nutriscore_grade",
        y="count",
        palette="viridis",
        hue="nutriscore_grade",
        legend=False
    )

    plt.title("Distribution des Nutri-Scores (Données Brutes)")
    plt.xlabel("Grade")
    plt.ylabel("Nombre de produits")

    output_path = "input_nutriscore_distrib.png"
    plt.savefig(output_path)
    print(f"✅ Graphique d'entrée sauvegardé : {output_path}")
    plt.close()


def visualize_output():
    """Génère un graphique analytique depuis MySQL (Output)."""
    print("🎨 Génération du graphique de sortie (Top 10 Marques Sucrées)...")

    connection_string = f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@localhost:3306/openfoodfacts"
    engine = create_engine(connection_string)

    query = """
            SELECT d.brands           as brand, \
                   AVG(f.sugars_100g) as avg_sugar
            FROM fact_nutrition_snapshot f
                     JOIN dim_product d ON f.product_sk = d.product_sk
            WHERE f.sugars_100g IS NOT NULL
              AND d.brands IS NOT NULL
              AND d.brands != ''
      AND d.brands != 'nan'
            GROUP BY d.brands
            HAVING COUNT(*) > 50
            ORDER BY avg_sugar DESC
                LIMIT 10 \
            """

    try:
        df_kpi = pd.read_sql(query, engine)

        if df_kpi.empty:
            print("⚠️ Pas assez de données en base (count > 50) pour le graphique de sortie.")
            print("   -> Tentative avec seuil réduit pour le mode DEV...")
            query_dev = query.replace("COUNT(*) > 50", "COUNT(*) > 5")
            df_kpi = pd.read_sql(query_dev, engine)

        if not df_kpi.empty:
            plt.figure(figsize=(12, 8))
            sns.barplot(
                data=df_kpi,
                y="brand",
                x="avg_sugar",
                palette="rocket",
                hue="brand",
                legend=False
            )
            plt.title("Top Marques les plus sucrées (Moyenne)")
            plt.xlabel("Sucre (g/100g)")
            plt.ylabel("Marque")

            output_path = "output_top_sugar_brands.png"
            plt.savefig(output_path)
            print(f"✅ Graphique de sortie sauvegardé : {output_path}")
            plt.close()
        else:
            print("⚠️ Toujours pas de données significatives à afficher.")

    except Exception as e:
        print(f"❌ Erreur lors de la viz MySQL : {e}")