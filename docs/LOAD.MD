# 📂 Documentation : etl/load.py

### 📄 En bref
C'est le **livreur** du projet : ce fichier prend les données transformées et les dépose proprement dans la base de données (MySQL). Il ne se contente pas de "jeter" les données, il les range méticuleusement en gérant l'historique des changements.

---

### 🎯 Pourquoi ce fichier ?
Charger des données, ce n'est pas juste faire un "copier-coller". Dans un projet Data professionnel, on a deux défis majeurs résolus ici :
1.  **L'historisation (SCD2)** : Si la composition du Nutella change demain, on veut garder l'ancienne version pour nos statistiques passées ET avoir la nouvelle. On ne doit pas écraser l'information, mais *ajouter* une nouvelle page à l'histoire du produit.
2.  **La cohérence** : On doit remplir plusieurs tables (Produits, Catégories, Faits) sans casser les liens entre elles (Clés étrangères).

---

### ⚙️ Comment ça marche ?

Le fichier contient plusieurs mécanismes ingénieux :

#### 1. L'initialisation du terrain (`init_database`)
Avant de charger quoi que ce soit, le script vérifie si les tables existent. Si non, il les crée avec le bon format (colonnes, types, index). C'est ici qu'on définit l'architecture "en étoile" (Dimension Produit, Dimension Catégorie, Table de Faits).

#### 2. La gestion intelligente de l'histoire (`load_dimension_scd2`)
C'est le cœur du réacteur.
* **Comparaison** : Il compare le "Hash" (l'empreinte) du produit qui arrive avec celui déjà en base.
* **Arbitrage** :
    * *Rien n'a changé ?* On ne fait rien.
    * *C'est nouveau ?* On insère le produit.
    * *Ça a changé ?* On "ferme" l'ancienne ligne (on met une date de fin `effective_to`) et on insère la nouvelle ligne active.

#### 3. La technique du "Staging" (`load_facts`, `_close_products_via_staging`)
Pour mettre à jour la base de données rapidement, Spark écrit d'abord les données dans une table temporaire (table de "brouillon" ou *staging*).
Ensuite, une requête SQL ultra-rapide déplace les données du brouillon vers la table finale. Cela permet de faire des opérations complexes (comme des mises à jour massives) que Spark gère mal en direct.

#### 4. La connexion robuste (`_get_db_connection`)
Si la base de données a un micro-hoquet (coupure réseau de 1 seconde), le script ne plante pas. Il attend un peu et réessaie jusqu'à 10 fois. C'est ce qu'on appelle la résilience.

---

### 💡 Le coin de l'expert

**Pourquoi utiliser des tables de "Staging" temporaires ?**

Tu verras des fonctions comme `_close_products_via_staging` ou `_upsert_facts_via_staging`.
Spark est très fort pour *ajouter* des données (Insert), mais très mauvais pour les *modifier* (Update) ligne par ligne.
L'astuce de pro ici est de :
1.  Utiliser Spark pour écrire en masse dans une table temporaire (`staging_...`).
2.  Lancer une seule commande SQL (`INSERT ... ON DUPLICATE KEY UPDATE` ou `UPDATE ... JOIN`) pour appliquer les changements.
C'est souvent 10 à 100 fois plus rapide qu'une mise à jour classique ! 🚀