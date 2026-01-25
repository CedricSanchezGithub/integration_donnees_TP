# 🥑 OpenFoodFacts ETL (M1 Data Engineering)

Pipeline ETL (Extract-Transform-Load) développé en Python/Spark pour traiter les données OpenFoodFacts et alimenter un Datamart MySQL.

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Spark](https://img.shields.io/badge/PySpark-3.5-orange)
![MySQL](https://img.shields.io/badge/MySQL-8.0-lightgrey)

---

## 🏗 Architecture Technique

**Approche :** Bronze (Raw) → Silver (Clean/Deduplicate) → Gold (Star Schema).

| Étape | Technologies | Choix Techniques Clés |
| :--- | :--- | :--- |
| **Extract** | PySpark | **Schéma strict** (`StructType`) pour optimiser la lecture JSON. |
| **Transform** | PySpark | **Dédoublonnage** via Fenêtrage (Code + Date modif). Gestion **Multilingue** (FR > EN > Defaut). |
| **SCD2** | PySpark | **Hash SHA256** sur colonnes métiers pour détecter les changements et gérer l'historique. |
| **Load** | JDBC / MySQL | Utilisation de **Staging Tables** + `INSERT ON DUPLICATE KEY UPDATE` pour la performance. |
| **Viz** | Streamlit | Dashboard interactif pour le monitoring et l'analyse métier. |

---

## 🚀 Installation & Lancement

1. **Démarrer l'infrastructure** (MariaDB) :

    docker compose up -d

2. **Installer les dépendances** :

    poetry install

3. **Lancer le Pipeline ETL** :
   (Assurez-vous d'avoir un fichier .jsonl dans data/raw/)

    poetry run python -m etl.main

4. **Accéder au Dashboard** :

    poetry run streamlit run dashboard.py

---

## 🗂 Modèle de Données (Datamart)

* **fact_nutrition_snapshot** : Mesures nutritionnelles (sucre, sel, nutriscore) à un instant T.
* **dim_product** : Dimension SCD2 (Code, Nom, Marque, Dates d'effet).
* **dim_category** : Référentiel des catégories nettoyées.
* **bridge_product_category** : Table de liaison (Produits <-> Catégories).

---

## 📁 Structure du Projet

* **etl/** : Code source Spark (extract, transform, load).
* **docs/** : Documentation (ADR, Qualité).
* **reports/** : Logs d'exécution au format JSON.
* **dashboard.py** : Application de visualisation.