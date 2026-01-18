# 🗺️ Feuille de Route : Projet ETL OpenFoodFacts

Ce document vulgarise les spécifications techniques du projet pour servir de guide de développement.  
Objectif : Transformer des données brutes (Big Data) en informations décisionnelles via un pipeline industriel.

---

## 🏗️ Mission 1 : L'Usine de Tri (Ingestion & Nettoyage)
**Le problème :** Le fichier source OpenFoodFacts est massif et "sale" (champs manquants, formats instables).  
**L'objectif :** Lire les données sans que le pipeline ne plante en production.

* [ ] **Lecture Robuste :** Interdiction d'utiliser `inferSchema=True`.
* [ ] **Schéma Explicite :** Définir manuellement les types (String, Float, Date) pour chaque colonne critique.
* [ ] **Stratégie :** Si une ligne ne respecte pas le schéma -> Elle est gérée (mise de côté ou nullifiée), mais le script ne s'arrête pas.

---

## ⏳ Mission 2 : La Machine à Remonter le Temps (SCD2)
**Le problème :** Si la composition du Nutella change demain, on ne doit pas écraser l'ancienne recette. On doit pouvoir dire "En 2022, c'était mieux".  
**L'objectif :** Gérer l'historique des modifications (Slowly Changing Dimension Type 2).

* [ ] **Fingerprinting :** Calculer un `hash` (empreinte) unique pour chaque produit basé sur ses attributs clés.
* [ ] **Comparaison :**
    * Si le hash change : On ferme l'ancienne ligne (`is_current=False`, `end_date=Now`) et on en crée une nouvelle (`is_current=True`).
    * Si le hash est identique : On ne fait rien (Optimisation).
* [ ] **Clés Techniques :** Utiliser des `product_sk` (Surrogate Keys) et non juste le code-barres.

---

## 🏪 Mission 3 : Le Magasin Rangé (Datamart MySQL)
**Le problème :** Spark est puissant pour le calcul, mais inadapté pour l'affichage rapide dans un dashboard.  
**L'objectif :** Stocker les données "propres" dans une base de données relationnelle optimisée pour l'analyse.

* [ ] **Modélisation en Étoile (Star Schema) :**
    * **Centre (Faits) :** Les mesures chiffrées (ex: `fact_nutrition` avec taux de sucre, sel...).
    * **Autour (Dimensions) :** Les axes d'analyse (ex: `dim_brand`, `dim_time`, `dim_category`).
* [ ] **Performance :** L'analyste doit pouvoir faire des `GROUP BY` et des `JOIN` instantanés.

---

## 📊 Mission 4 : Le Tableau de Bord (Observabilité)
**Le problème :** Comment savoir si le chargement de cette nuit a fonctionné correctement ?  
**L'objectif :** Générer des métriques de qualité à chaque exécution.

* [ ] **Rapport d'exécution :** À la fin du script, produire un résumé :
    * Nombre de lignes lues.
    * Nombre de lignes rejetées (qualité).
    * Nombre de produits mis à jour vs nouveaux.
* [ ] **Logs :** Tracer les erreurs sans arrêter le programme.

---

## 🚀 Stack Technique
* **Langage :** Python (PySpark)
* **Moteur de calcul :** Apache Spark
* **Stockage final :** MySQL 8
* **Conteneurisation :** Docker
* **Qualité de code :** Poetry, Pytest, Typing