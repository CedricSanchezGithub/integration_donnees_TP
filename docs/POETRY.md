# 📦 Guide Rapide : Gestion des dépendances avec Poetry

Ce projet utilise **Poetry** pour la gestion des dépendances et de l'environnement virtuel.
C'est le standard moderne qui remplace le classique (et fragile) `requirements.txt`.

## 🧐 Pourquoi Poetry ?

1.  **Isolation Totale :** Il crée automatiquement un environnement virtuel dédié au projet. Pas de pollution de ton Python global.
2.  **Reproductibilité (Lockfile) :** Le fichier `poetry.lock` fige les versions exactes de toutes les librairies (et de leurs sous-dépendances).
3.  **Séparation Propre :** Il distingue les outils de production (`pyspark`, `pandas`) des outils de développement (`pytest`, `black`).

---

## 🚀 Cheatsheet (Commandes utiles)

### 1. Installation initiale (après un git clone)

```bash
poetry install
```

### 2. Ajouter une librairie
Ne jamais faire `pip install`. Plutôt :

* **Pour le code du projet (Prod) :**
    ```bash
    poetry add nom_librairie
    # Exemple : poetry add pyspark
    ```

* **Pour les outils de dev (Tests, Linter) :**
    ```bash
    poetry add --group dev nom_librairie
    # Exemple : poetry add --group dev pytest
    ```

### 3. Lancer une commande
Pas besoin d'activer manuellement l'environnement virtuel. Poetry le fait pour toi avec `run` :

```bash
# Lancer un script Python
poetry run python etl/main.py

# Lancer les tests
poetry run pytest

# Lancer l'interface Spark (si configurée)
poetry run pyspark
```

### 4. Entrer dans le shell (Optionnel)
Si tu veux activer l'environnement virtuel dans ton terminal pour taper plusieurs commandes :

```bash
poetry shell
# Pour sortir : exit
```

---

## 📂 Fichiers Clés

* `pyproject.toml` : La "recette" de haut niveau (ex: "Je veux pandas"). C'est ici qu'on configure aussi les outils comme `pytest` ou `black`.
* `poetry.lock` : Le "plat cuisiné et congelé". Il contient les versions exactes (ex: "pandas 2.0.3 avec numpy 1.24.3..."). **Ne jamais modifier ce fichier à la main.**