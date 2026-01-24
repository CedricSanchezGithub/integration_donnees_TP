import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from etl.shared.config import MYSQL_CONFIG

# 1. Configuration de la page
st.set_page_config(page_title="OpenFoodFacts Dashboard", layout="wide")
st.title("🥑 OpenFoodFacts Analytics")


# 2. Connexion à la BDD
@st.cache_resource  # Permet de ne pas se reconnecter à chaque clic
def get_db_connection():
    collation = MYSQL_CONFIG.get("collation", "utf8mb4_general_ci")
    connection_string = (
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
        f"@localhost:3306/{MYSQL_CONFIG['database']}"
        f"?collation={collation}"
    )
    return create_engine(connection_string)


engine = get_db_connection()

# 3. Sidebar (Filtres)
st.sidebar.header("🔍 Filtres")
min_products = st.sidebar.slider("Nombre min. de produits par marque", 10, 500, 50)

# 4. KPI Principaux
col1, col2 = st.columns(2)
with col1:
    count = pd.read_sql("SELECT COUNT(*) as c FROM dim_product WHERE is_current=1", engine).iloc[0]['c']
    st.metric("Produits Actifs", f"{count:,}")
with col2:
    avg_sugar = pd.read_sql("SELECT AVG(sugars_100g) as s FROM fact_nutrition_snapshot", engine).iloc[0]['s']
    st.metric("Sucre Moyen (g/100g)", f"{avg_sugar:.2f} g")

# 5. Graphique : Top Marques Sucrées (Interactif)
st.subheader(f"🍬 Top 10 Marques les plus sucrées (> {min_products} produits)")

query = f"""
    SELECT d.brands as Marque, AVG(f.sugars_100g) as Sucre_Moyen
    FROM fact_nutrition_snapshot f
    JOIN dim_product d ON f.product_sk = d.product_sk
    WHERE f.sugars_100g IS NOT NULL 
      AND d.brands != '' 
      AND d.brands != 'nan'
    GROUP BY d.brands
    HAVING COUNT(*) > {min_products}
    ORDER BY Sucre_Moyen DESC
    LIMIT 10
"""

df_chart = pd.read_sql(query, engine)

if not df_chart.empty:
    st.bar_chart(df_chart.set_index("Marque"))

    with st.expander("Voir les données brutes"):
        st.dataframe(df_chart)
else:
    st.warning("Aucune donnée avec ces filtres.")

    st.subheader("🧪 Top Catégories avec le plus d'additifs")

query_additives = """
                  SELECT d.categories       as Category, \
                         AVG(f.additives_n) as Avg_Additives, \
                         COUNT(*)           as Count_Products
                  FROM fact_nutrition_snapshot f
                           JOIN dim_product d ON f.product_sk = d.product_sk
                  WHERE f.additives_n IS NOT NULL
                    AND d.categories IS NOT NULL
                    AND d.categories != ''
                  GROUP BY d.categories
                  HAVING COUNT(*) > 10
                  ORDER BY Avg_Additives DESC
                      LIMIT 10 \
                  """

df_additives = pd.read_sql(query_additives, engine)

if not df_additives.empty:
    st.bar_chart(df_additives.set_index("Category")["Avg_Additives"])

    with st.expander("Voir les détails"):
        st.dataframe(df_additives)
else:
    st.info("Pas encore de données sur les additifs (avez-vous relancé l'ETL avec le nouveau code ?)")

st.subheader("🧪 Top Catégories avec le plus d'additifs")

# 1. Debugging : Vérifions d'abord si on a des données tout court
check_query = "SELECT COUNT(*) as total, COUNT(additives_n) as with_additives FROM fact_nutrition_snapshot"
df_check = pd.read_sql(check_query, engine)
total = df_check.iloc[0]['total']
with_additives = df_check.iloc[0]['with_additives']

st.markdown(f"**Diagnostic Base de Données :**")
st.info(f"Produits totaux : **{total}** | Produits avec additifs renseignés : **{with_additives}**")

if with_additives == 0:
    st.error(
        "🚨 La colonne `additives_n` est vide pour tous les produits. Vérifiez votre fichier `extract.py` et `transform.py`.")
else:
    # 2. La requête principale avec un seuil dynamique
    # Si on a peu de données, on baisse le seuil à 1 produit pour voir quelque chose
    threshold = 10 if total > 10000 else 1

    query_additives = f"""
        SELECT 
            d.categories as Category, 
            AVG(f.additives_n) as Avg_Additives,
            COUNT(*) as Count_Products
        FROM fact_nutrition_snapshot f
        JOIN dim_product d ON f.product_sk = d.product_sk
        WHERE f.additives_n IS NOT NULL 
          AND d.categories IS NOT NULL 
          AND d.categories != ''
        GROUP BY d.categories
        HAVING COUNT(*) >= {threshold} 
        ORDER BY Avg_Additives DESC
        LIMIT 10
    """

    df_additives = pd.read_sql(query_additives, engine)

    if not df_additives.empty:
        st.bar_chart(df_additives.set_index("Category")["Avg_Additives"])
        with st.expander("Voir les données brutes"):
            st.dataframe(df_additives)
    else:
        st.warning(f"Aucune catégorie n'a plus de {threshold} produits avec additifs (Mode échantillon trop petit ?).")