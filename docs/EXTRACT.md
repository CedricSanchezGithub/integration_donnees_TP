# 📂 Documentation : etl/extract.py

### 📄 En bref
Ce fichier est le point d'entrée des données : il lit le fichier brut **OpenFoodFacts** (format JSONL) en appliquant une grille de lecture stricte pour transformer ce texte en un tableau manipulable (DataFrame Spark).

---

### 🎯 Pourquoi ce fichier ?
Dans un projet Data, l'étape d'**Extraction** est critique. On ne peut pas simplement "ouvrir" un fichier de plusieurs giga-octets comme un Excel classique.
Ce script a deux rôles majeurs :
1.  **Charger les données** depuis le disque dur vers le moteur de calcul (Spark).
2.  **Imposer une structure** dès le début. Les fichiers JSON sont "flexibles" (parfois une colonne existe, parfois non). Ici, on définit exactement quelles informations nous intéressent pour ne pas perdre de temps à tout charger.

---

### ⚙️ Comment ça marche ?

Le code se divise en deux étapes logiques :

#### Étape 1 : Définir la carte d'identité des données (`get_jsonl_schema`)
Avant de lire le fichier, on prévient Spark de ce qu'il va trouver. On liste les colonnes attendues et leur type (texte, nombre entier, nombre à virgule...).
* **Exemple** : On précise que `code` est une chaîne de caractères et que `additives_n` (nombre d'additifs) est un entier.
* **Le cas spécial** : Les nutriments (sucre, sel, énergie) sont regroupés dans une "boîte" à l'intérieur du JSON. On définit donc une sous-structure (`nutriments_schema`) pour aller chercher ces infos imbriquées proprement.

#### Étape 2 : L'extraction proprement dite (`extract_data`)
C'est la fonction qui fait le travail :
1.  **Localisation** : Elle construit le chemin d'accès vers le dossier `data/raw` où est stocké le fichier.
2.  **Lecture optimisée** : Elle demande à Spark de lire le fichier en utilisant le **schéma** défini à l'étape 1.
3.  **Sécurité** : Si le fichier est introuvable ou illisible, elle capture l'erreur et affiche un message clair au lieu de faire planter tout le programme silencieusement.

---

### 💡 Le coin de l'expert

**Pourquoi définir un schema strict (`StructType`) au lieu de laisser Spark deviner ?**

C'est une astuce de performance majeure.
Par défaut, Spark utilise le `schema inference` : il doit lire **tout le fichier une première fois** juste pour deviner si la colonne "sucre" contient des chiffres ou du texte, puis le relire pour charger les données.
Sur un fichier massif comme OpenFoodFacts (plusieurs Go), cela doublerait le temps de chargement ! En lui donnant le schéma, Spark lit le fichier une seule fois. Gain de temps immédiat. 🚀