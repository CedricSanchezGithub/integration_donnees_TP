# 📂 Documentation : dashboard.py

### 📄 En bref
C'est la tour de contrôle du projet. Une application web interactive (construite avec **Streamlit**) qui permet à la fois d'explorer les données nutritionnelles (Vue Business) et de surveiller la santé technique du pipeline (Vue Tech).

---

### 🎯 Pourquoi ce fichier ?
Les rapports statiques (images) sont utiles, mais limités. Ici, on offre de l'**interactivité** pour deux publics :
* **L'Analyste Métier** : Il veut pouvoir filtrer, changer les seuils ("Et si je regarde les marques avec +100 produits ?") et voir les KPIs en temps réel.
* **Le Data Engineer** : Il a besoin de vérifier si le chargement de la nuit s'est bien passé, combien de lignes ont été insérées et s'il y a eu des erreurs, sans aller fouiller dans les logs serveurs.

---

### ⚙️ Comment ça marche ?

L'application est structurée en plusieurs zones :

#### 1. La Configuration (Barre latérale)
C'est une télécommande pour le projet. Elle permet de modifier le fichier `config.json` directement via l'interface (activer le mode DEV, changer l'échantillonnage) sans toucher au code.

#### 2. Onglet "Analyse Métier"
* **Connexion BDD** : Se connecte à MySQL pour afficher les chiffres clés (Nombre de produits, Sucre moyen).
* **Interactivité** : Les curseurs (sliders) modifient directement les requêtes SQL envoyées à la base. Les graphiques se mettent à jour instantanément.

#### 3. Onglet "Monitoring ETL"
* **Lecture des Logs** : Le dashboard scanne le dossier `reports/` pour trouver les fichiers JSON générés par l'ETL.
* **Bulletin de Santé** : Il affiche clairement le statut du dernier "Run" (Succès/Échec), le temps d'exécution, et les volumes de données traités (SCD2 : insertions vs fermetures).

---

### 💡 Le coin de l'expert (Astuces)

**Le cache pour la performance (`@st.cache_resource`)**
Ouvrir une connexion à une base de données est coûteux en temps.
Le décorateur `@st.cache_resource` permet à Streamlit de garder la connexion MySQL ouverte en mémoire. Ainsi, quand l'utilisateur joue avec les filtres, l'application réagit immédiatement sans se reconnecter à chaque fois.