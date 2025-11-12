# 🚀 Projet Paris2055 : Migration SQLite → MongoDB et Tableau de Bord

**Contexte** : Analyse et migration d'une base de données relationnelle (SQLite) vers un modèle NoSQL (MongoDB), suivie de la création d'un tableau de bord interactif pour visualiser les données.

---

## 📌 Organisation du Projet

### 🧩 **Phase 1 — Exploration et Requêtes SQL**
**Objectif** : Analyser la base `Paris2055.sqlite` et produire des indicateurs de référence.

#### Étapes :
1. **Connexion à la base SQLite**
   - Utiliser `sqlite3` et `pandas` pour lire les tables.
   - Lister les tables et leurs colonnes pour comprendre le schéma.
2. **Réalisation des requêtes SQL (a → n)**
   - Créer et tester 14 requêtes (moyennes, taux, corrélations, etc.).
   - Stocker chaque résultat dans un DataFrame, puis exporter en CSV (`resultat_a.csv`, `resultat_b.csv`, etc.).
   - Vérifier l'ordonnancement et la cohérence des résultats.
3. **Sauvegarde et documentation**
   - Créer un notebook ou un script `partie1.py`.
   - Ajouter des commentaires pour expliquer la logique des requêtes.

#### Livrables :
- Script Python : [`partie1.py`](partie1.py)
- 14 fichiers CSV des résultats : `resultat_a.csv`, `resultat_b.csv`, etc.

#### Outils :
`sqlite3`, `pandas`, `matplotlib` (optionnel pour graphiques rapides).

---

### 🧱 **Phase 2 — Migration vers MongoDB**
**Objectif** : Transformer le modèle relationnel en modèle document et écrire le script de migration.

#### Étapes :
1. **Analyse du schéma relationnel**
   - Identifier les entités principales (ex: `Ligne`, `Arret`, `Vehicule`, `Capteur`).
   - Déterminer les relations (1-n, n-n) pour prévoir les imbrications JSON.
2. **Conception du modèle NoSQL**
   - Proposer les collections : `Lignes`, `Arrets`, `Vehicules`, `Capteurs`, `Quartiers`.
   - Pour chaque collection :
     - Définir les champs et sous-documents.
     - Écrire un exemple JSON de document type.
3. **Écriture du script de migration**
   - Charger les tables SQLite avec `pandas`.
   - Créer des DataFrames imbriqués (exemple : `Gymnase`).
   - Insérer dans MongoDB avec `insert_many()`.
4. **Vérification dans MongoDB Compass**
   - Vérifier la structure et l'insertion des documents.

#### Livrables :
- Script Python : [`partie2_migration.py`](partie2_migration.py)
- Schéma NoSQL : [`schema_nosql.json`](schema_nosql.json) (document texte ou JSON illustratif).

#### Outils :
`pandas`, `sqlite3`, `pymongo`, `MongoDB Compass`.

---

### 📊 **Phase 3 — Requêtes Tests sur MongoDB**
**Objectif** : Reproduire les requêtes SQL de la Phase 1 avec MongoDB pour comparer les résultats.

#### Étapes :
1. **Connexion à MongoDB**
   - Utiliser `pymongo` pour se connecter à la base migrée.
2. **Traduction des requêtes SQL en MongoDB**
   - Utiliser des requêtes d’agrégation (`$group`, `$avg`, `$match`, `$lookup`).
   - Comparer les résultats aux CSV de la Phase 1.
3. **Validation**
   - Vérifier la cohérence des résultats.
   - Documenter les équivalences SQL ↔ MongoDB.

#### Livrables :
- Script Python : [`partie3_requetesMongo.py`](partie3_requetesMongo.py)
- Tableau comparatif : [`comparaison_sql_mongodb.md`](comparaison_sql_mongodb.md).

#### Outils :
`pymongo`, `pandas`.

---

### 🌍 **Phase 4 — Tableau de Bord et Cartographie**
**Objectif** : Créer un tableau de bord interactif connecté à MongoDB.

#### Étapes :
1. **Connexion et extraction**
   - Lire les données MongoDB directement dans l'application (via `pymongo`).
2. **Création des graphiques**
   - Histogramme : retards moyens par ligne.
   - Courbe : tendance CO₂.
   - Diagramme circulaire : répartition des véhicules.
   - Autres graphiques pertinents (ex: corrélation pollution/trafic).
3. **Cartographie avec Folium**
   - Carte choroplèthe : niveau moyen de CO₂ par quartier.
   - Carte à marqueurs filtrable :
     - Chaque arrêt = marqueur.
     - Couleur selon pollution.
     - Popup : nom, nombre de lignes, bruit, température.
     - Filtre : visualiser les arrêts d’une ligne spécifique.
4. **Interface**
   - Interface `Streamlit` (pages, filtres, sélecteurs, graphiques dynamiques).

#### Livrables :
- Script : [`partie4_dashboard.py`](partie4_dashboard.py)
- (Optionnel) Dossier `/data` pour les CSV intermédiaires.
- Capture d’écran du tableau de bord : [`dashboard_screenshot.png`](dashboard_screenshot.png).

#### Outils :
`streamlit` ou `plotly`, `folium`, `pandas`, `pymongo`, `geopandas`.

---

## 📦 **Livraison Finale**
**Date limite** : 11 janvier à 23h59 (dépôt sur Updago).

#### Fichiers à fournir :
- [`partie1.py`](partie1.py)
- [`partie2_migration.py`](partie2_migration.py)
- [`partie3_requetesMongo.py`](partie3_requetesMongo.py)
- [`partie4_dashboard.py`](partie4_dashboard.py)
- Fichiers CSV intermédiaires (si applicable).
- Fichiers web (HTML, JS) ou captures d’écran (optionnel).

---

## 🛠 **Installation et Prérequis**
1. **Cloner le dépôt** :
   ```bash
   git clone https://github.com/votre-utilisateur/paris2055.git

2. **Utiliser DB Browser for SQLite pour visualiser la base SQL** :
   ```bash
   https://sqlitebrowser.org/dl/

3. **Penser à push le code uniquement sur la branch develop** :
   ```bash
   git checkout develop (passer sur la branch develop)
   git checkout -b develop origin/develop (Si Git te dit que la branche n’existe pas localement, tu peux la créer à partir de celle du dépôt distant)
   git pull origin develop (récupérer le code de la branch develop sur le repo)
   git add .
   git commit -m "commenter la modif"
   git push origin develop 
   