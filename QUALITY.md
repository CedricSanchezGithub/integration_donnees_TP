# 🛡 Cahier de Qualité & Règles de Gestion

Ce document recense les règles appliquées lors du traitement des données OpenFoodFacts.

## 1. Règles de Nettoyage (Silver)

| Champ | Règle appliquée | Justification |
| :--- | :--- | :--- |
| **Noms Produits** | Priorité : `product_name_fr` > `en` > `product_name`. | Maximiser la complétude en français pour un usage local. |
| **Textes** | `TRIM()` + Remplacement vides par `NULL`. | Éviter les doublons dus aux espaces et normaliser les manquants. |
| **Doublons** | Fenêtre `ROW_NUMBER()` sur `code` trié par `last_modified_t DESC`. | Ne conserver que la version la plus récente fournie par OFF. |
| **Nutriments** | Remplacement `Infinity` / `NaN` par `NULL`. | Éviter les crashs lors des agrégations SQL (AVG, SUM). |

## 2. Règles d'Historisation (SCD2)

* **Détection de changement** : Calcul d'un `row_hash` (SHA256) sur la concaténation de toutes les colonnes métiers (Nom, Marque, Scores, Nutriments).
* **Logique** :
    * Si Hash identique : Ignorer.
    * Si Hash différent : Fermer l'ancienne ligne (`is_current=0`, `effective_to=NOW()`) et insérer la nouvelle (`is_current=1`).

## 3. Métriques & Suivi

Un rapport JSON est généré à chaque exécution dans le dossier `reports/` contenant :
* **Volumétrie** : Lignes lues, insérées, rejetées.
* **Performance** : Temps d'exécution global.
* **Anomalies** : Liste des produits critiques rejetés (ex: sans code-barres).