# 🏗️ Architecture Decision Record : Choix de la Source de Données

**Décision :** Utilisation de l'export **JSONL (JSON Lines)**.

## 📊 Comparatif des options d'ingestion

| Source / Format | Type | Compatibilité Spark | Gestion Données Imbriquées | Avantages | Inconvénients | Décision |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **JSONL (JSON Lines)** | Texte Semi-Structuré | ⭐⭐⭐⭐⭐ (Natif) | ✅ Excellente (Preserve la hiérarchie) | • **Splittable :** Lecture parallèle native par Spark.<br>• **Complet :** Contient 100% des données brutes.<br>• **Robuste :** 1 ligne = 1 objet, pas de décalage de colonnes. | • Verbeux (Fichier plus lourd que du binaire). | **✅ RETENU** |
| **CSV** | Texte Tabulaire | ⭐ (Médiocre) | ❌ Nulle (Aplatissement total) | • Lisible par Excel (pour petits échantillons). | • **Enfer du Parsing :** Tabulations/Virgules dans les champs textes cassent la structure.<br>• **Perte de sens :** Les objets `nutriments` deviennent `nutriments_sugars_100g` (plus de 1000 colonnes). | ⛔ REJETÉ |
| **MongoDB Dump** | Binaire (BSON) | ⭐⭐ (Complexe) | ✅ Excellente | • Format natif d'OpenFoodFacts. | • **Complexité Infra :** Nécessite de monter un serveur Mongo ou d'utiliser des librairies tierces instables.<br>• Trop lourd pour l'exercice. | ⛔ REJETÉ |
| **Parquet (HuggingFace)** | Binaire Colonnaire | ⭐⭐⭐⭐⭐ (Excellente) | ✅ Excellente | • Ultra-performant (lecture rapide).<br>• Schéma déjà typé. | • **Trop "Propre" :** Données déjà filtrées par OpenFoodFacts.<br>• Masque la complexité de l'ingestion "Bronze" (objectif pédagogique du TP). | ⛔ REJETÉ |
| **API REST** | HTTP (JSON) | ⭐ (Nulle) | ✅ Excellente | • Données temps réel. | • **Lenteur extrême :** Inadapté au traitement par lots (Batch).<br>• Risque de ban (Rate Limiting). | ⛔ REJETÉ |

## 📝 Justification du choix

Nous avons retenu le format **JSONL (`.jsonl.gz`)** car il représente le meilleur compromis pour un pipeline Big Data pédagogique :
1.  **Réalisme :** Il confronte l'ingénieur aux vrais problèmes de volumétrie et de typage (contrairement au Parquet aseptisé).
2.  **Stabilité :** Il évite les erreurs de parsing aléatoires du CSV sur les champs textuels libres (ingrédients).
3.  **Scalabilité :** Il est nativement géré par Spark qui peut le découper pour paralléliser la lecture sur plusieurs cœurs.