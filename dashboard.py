import streamlit as st
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import create_engine
from etl.shared.config import MARIADB_CONFIG

# 1. Configuration de la page
st.set_page_config(page_title="OpenFoodFacts Dashboard", layout="wide")
st.title("🥑 OpenFoodFacts Analytics & Monitoring")


# 2. Connexion à la BDD
@st.cache_resource
def get_db_connection():
    collation = MARIADB_CONFIG.get("collation", "utf8mb4_general_ci")
    connection_string = (
        f"mysql+mysqlconnector://{MARIADB_CONFIG['user']}:{MARIADB_CONFIG['password']}"
        f"@localhost:3306/{MARIADB_CONFIG['database']}"
        f"?collation={collation}"
    )
    return create_engine(connection_string)


engine = get_db_connection()

# --- CONTROLES ETL (Sidebar) ---
st.sidebar.divider()
st.sidebar.header("⚙️ Configuration ETL")

# 1. Lire la config actuelle pour pré-remplir les widgets
config_path = Path("config.json")
current_conf = {"enabled": True, "sample_fraction": 0.05, "seed": 42}

if config_path.exists():
    try:
        with open(config_path, "r") as f:
            current_conf = json.load(f)
    except:
        pass

# 2. Widgets
dev_mode_active = st.sidebar.toggle("Mode DEV (Échantillonnage)", value=current_conf.get("enabled", True))
sample_frac = st.sidebar.slider(
    "Taille de l'échantillon (%)",
    min_value=1, max_value=100,
    value=int(current_conf.get("sample_fraction", 0.05) * 100)
)
seed_val = st.sidebar.number_input("Seed (Reproductibilité)", value=current_conf.get("seed", 42))

# 3. Bouton de Sauvegarde
if st.sidebar.button("💾 Sauvegarder Config ETL"):
    new_conf = {
        "enabled": dev_mode_active,
        "sample_fraction": sample_frac / 100.0,
        "seed": seed_val
    }
    with open(config_path, "w") as f:
        json.dump(new_conf, f)
    st.sidebar.success("Config mise à jour !")

# --- CRÉATION DES ONGLETS ---
tab_business, tab_monitoring = st.tabs(["📊 Analyse Métier", "🛠 Monitoring ETL"])

# ==============================================================================
# ONGLET 1 : ANALYSE MÉTIER (Ton dashboard existant)
# ==============================================================================
with tab_business:
    # 3. Sidebar (Filtres) - Spécifique à l'onglet métier
    st.header("Analyse Nutritionnelle")
    min_products = st.slider("Nombre min. de produits par marque", 10, 500, 50)

    # 4. KPI Principaux
    col1, col2 = st.columns(2)
    with col1:
        try:
            count = pd.read_sql("SELECT COUNT(*) as c FROM dim_product WHERE is_current=1", engine).iloc[0]['c']
            st.metric("Produits Actifs", f"{count:,}")
        except Exception:
            st.error("Impossible de lire dim_product.")
            count = 0

    with col2:
        try:
            avg_sugar = pd.read_sql("SELECT AVG(sugars_100g) as s FROM fact_nutrition_snapshot", engine).iloc[0]['s']
            val = f"{avg_sugar:.2f} g" if avg_sugar else "N/A"
            st.metric("Sucre Moyen (g/100g)", val)
        except Exception:
            st.metric("Sucre Moyen", "N/A")

    # 5. Graphique : Top Marques Sucrées
    st.subheader(f"🍬 Top 10 Marques les plus sucrées (> {min_products} produits)")

    if count > 0:
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
        try:
            df_chart = pd.read_sql(query, engine)
            if not df_chart.empty:
                st.bar_chart(df_chart.set_index("Marque"))
                with st.expander("Voir les données brutes"):
                    st.dataframe(df_chart)
            else:
                st.warning("Aucune donnée avec ces filtres.")
        except Exception as e:
            st.error(f"Erreur SQL : {e}")

        # 6. Graphique : Additifs
        st.subheader("🧪 Top Catégories avec le plus d'additifs")
        check_query = "SELECT COUNT(*) as total, COUNT(additives_n) as with_additives FROM fact_nutrition_snapshot"
        df_check = pd.read_sql(check_query, engine)
        total_p = df_check.iloc[0]['total']

        # Seuil dynamique
        threshold = 10 if total_p > 10000 else 1

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
        try:
            df_additives = pd.read_sql(query_additives, engine)
            if not df_additives.empty:
                st.bar_chart(df_additives.set_index("Category")["Avg_Additives"])
            else:
                st.info("Pas assez de données sur les additifs.")
        except Exception:
            pass

# ==============================================================================
# ONGLET 2 : MONITORING ETL (Nouvelle fonctionnalité)
# ==============================================================================
with tab_monitoring:
    st.header("🛠 Journal d'exécution du Pipeline")

    # 1. Lister les fichiers JSON dans reports/
    report_dir = Path("reports")
    if not report_dir.exists():
        st.warning("Aucun dossier 'reports/' trouvé. Lancez le pipeline ETL d'abord.")
    else:
        # Récupère tous les fichiers json et les trie par nom (qui contient la date) décroissant
        report_files = sorted(report_dir.glob("report_*.json"), key=lambda f: f.name, reverse=True)

        if not report_files:
            st.info("Aucun rapport d'exécution trouvé.")
        else:
            # Sélecteur de rapport
            selected_file = st.selectbox(
                "Choisir un run :",
                report_files,
                format_func=lambda f: f.name.replace("report_", "").replace(".json", "")
            )

            # 2. Charger et afficher le JSON
            with open(selected_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # En-tête statut
            status = data.get("status", "UNKNOWN")
            if status == "SUCCESS":
                st.success(f"✅ Run du {data['timestamp']} (Durée : {data['duration_seconds']}s)")
            else:
                st.error(f"❌ Run du {data['timestamp']} (ECHEC)")

            # Métriques Clés
            m = data.get("metrics", {})
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            col_m1.metric("Lignes Lues", f"{m.get('rows_read', 0):,}")
            col_m2.metric("Lignes Traitées", f"{m.get('rows_processed', 0):,}")
            col_m3.metric("Nouv. Produits (SCD2)", f"{m.get('dim_products_inserted', 0):,}")
            col_m4.metric("Fermetures (SCD2)", f"{m.get('dim_products_closed', 0):,}")

            st.divider()

            col_m5, col_m6 = st.columns(2)
            col_m5.metric("Faits Chargés", f"{m.get('facts_loaded', 0):,}")
            col_m6.metric("Lignes Filtrées/Rejetées", f"{m.get('rows_filtered', 0):,}")

            # 3. Anomalies
            anomalies = data.get("anomalies", [])
            if anomalies:
                st.subheader("🚨 Anomalies Détectées")
                st.dataframe(pd.DataFrame(anomalies))
            else:
                st.info("👍 Aucune anomalie critique détectée.")

            # JSON Brut (pour debug)
            with st.expander("Voir le JSON brut"):
                st.json(data)