# 📂 Documentation : etl/visualization.py

### 📄 En bref
Ce fichier est le "photographe" du projet : il capture des instantanés des données avant (Input) et après traitement (Output) sous forme d'images statiques (PNG), permettant une validation visuelle rapide sans interface complexe.

---

### 🎯 Pourquoi ce fichier ?
Dans une chaîne de traitement automatisée, on ne peut pas toujours ouvrir un tableau de bord interactif pour vérifier que tout va bien.
Ce script a deux fonctions vitales :
1.  **Contrôle Qualité (Input)** : Vérifier dès le début si les données brutes "tiennent la route" (ex: a-t-on reçu des Nutri-Scores ?).
2.  **Rapport Décisionnel (Output)** : Fournir une "preuve" visuelle du résultat final (ex: Top 10 des marques) facile à partager par email ou à archiver.

---

### ⚙️ Comment ça marche ?

Le code distingue deux moments clés :

#### 1. La photo de départ (`visualize_input`)
* **Agrégation Spark** : On demande d'abord à Spark de compter les produits par catégorie (Nutri-Score A, B, C...).
* **Dessin** : On récupère ce petit résumé (très léger) pour créer un diagramme en barres avec **Seaborn**.
* **Sauvegarde** : Le graphique est enregistré sur le disque (`input_nutriscore_distrib.png`).

#### 2. La photo d'arrivée (`visualize_output`)
* **Connexion MySQL** : On se connecte à la base de données finale.
* **Requête Analytique** : On exécute une requête SQL pour trouver les marques les plus sucrées.
    * *Note* : Le code est intelligent ; si on est en "mode test" avec peu de données, il abaisse ses critères de filtrage pour afficher quand même quelque chose.
* **Sauvegarde** : Le résultat est enregistré (`output_top_sugar_brands.png`).

---

### 💡 Le coin de l'expert (Astuces)

**Pourquoi agréger avant de dessiner ?**
Le code fait `df_spark.groupBy(...).count().toPandas()`.
C'est une règle d'or en Big Data : ne jamais envoyer des millions de lignes brutes vers la librairie de dessin (Matplotlib), cela ferait exploser la mémoire. On fait toujours travailler le moteur puissant (Spark) pour résumer les données *avant* de les dessiner.