# 📂 Documentation : etl/transform.py

### 📄 En bref
Ce fichier est l'usine de recyclage du projet : il prend les données brutes, les nettoie, retire les doublons, calcule des empreintes numériques (hash) pour détecter les modifications et prépare les différentes tables pour la base de données.

---

### 🎯 Pourquoi ce fichier ?
Les données brutes extraites (étape *Extract*) sont rarement utilisables telles quelles :
1.  **Elles sont "sales"** : Espaces en trop, types incorrects (dates en chiffres bizarres), valeurs manquantes.
2.  **Elles contiennent des doublons** : Un même produit peut apparaître plusieurs fois si le fichier source a été mis à jour.
3.  **Elles ne sont pas relationnelles** : Les catégories sont souvent une longue liste de texte ("Chips, Snacks, Salé") qu'il faut découper pour les analyser proprement.

Ce fichier transforme le "Bronze" (donnée brute) en "Silver" (donnée propre) et prépare le "Gold" (donnée prête à l'analyse).

---

### ⚙️ Comment ça marche ?

Le code est découpé en plusieurs fonctions spécialisées :

#### 1. Le grand nettoyage (`clean_data`)
C'est la première étape obligatoire.
* **Typage** : On transforme les dates (format "Unix timestamp") en vraies dates lisibles.
* **Extraction** : On va chercher les nutriments (sucre, sel, etc.) qui étaient cachés dans des sous-structures du fichier.
* **Déduplication intelligente** : Si le produit "Nutella" apparaît 3 fois, on utilise une "fenêtre" (`Window`) pour ne garder que la version la plus récente (celle avec le `last_modified_t` le plus grand).

#### 2. La signature numérique (`add_technical_hash`)
Pour gérer l'historisation (SCD2), on a besoin de savoir si un produit a changé.
Au lieu de comparer les 50 colonnes une par une (ce qui est lent), on colle toutes les valeurs importantes ensemble et on calcule un **Hash (SHA256)**. C'est une empreinte digitale unique : si une seule virgule change dans le produit, le Hash change radicalement.

#### 3. La gestion des catégories (`extract_unique_categories` & `prepare_bridge_table`)
Dans le fichier source, un produit a une liste de catégories : "Boissons, Sucré, Sodas".
* **Explosion** : La fonction `explode` fait "éclater" cette liste. Le produit se retrouve dupliqué pour chaque catégorie.
* **Table Bridge** : On crée une table de liaison qui dit simplement "Le Produit X est lié à la Catégorie Y". C'est indispensable pour faire des filtres précis plus tard.

#### 4. La préparation finale (`prepare_fact_table`)
On prépare la table centrale d'analyse (Table de Faits). On nettoie les valeurs aberrantes (comme les "Infinity" qui font planter les calculs) pour les remplacer par du vide (`NULL`), garantissant des graphiques propres à la fin.

---

### 💡 Le coin de l'expert

**Pourquoi dédoublonner avec une `Window` et `row_number()` ?**

Tu verras souvent `df.dropDuplicates()` dans les tutos Spark. C'est bien, mais dangereux : cela garde une ligne *au hasard* parmi les doublons.
Ici, on utilise une méthode pro :
1.  On groupe par code produit.
2.  On trie par date de modification décroissante.
3.  On numérote les lignes (1, 2, 3...).
4.  On ne garde que la n°1.
Cela garantit mathématiquement qu'on conserve toujours la **dernière version** des données.