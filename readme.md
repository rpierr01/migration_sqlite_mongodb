# Guide d'utilisation du logiciel et organisation du projet

## Organisation du projet

- Travail collaboratif sur GitHub.
- Taux d'utilisation de l'IA estimé à **70%**.
- Nous avons décidé de travailler avec Dash car c'est un framework que nous trouvons parfaitement adapté bien que cet outil ne sit pas explicitement proposé sur le sujet.

### Membres de l'équipe

- **Romain Faucher**
- **Jules Dubuy**
- **Rémi Pierron**

---

## Utilisation du logiciel

Le programme `run_all.py` permet de lancer le logiciel Dash dans son intégralité.

### Partie 1 : Requête SQL

- **Script** : `requetes_sql/requete_sql.py`
- **Description** : Lançable depuis l'interface Dash.
- **Détail** : Requête 'n' : Nous avons choisi de classer les retards en trois catégories.

### Partie 2 : Migration SQL -> MongoDB

- **Script** : `migration/migration.py`
- **Description** : Lançable depuis l'interface Dash.
- **Nombre de collections** : 5 (voir `migration/info_collection.txt`).
- **Bonus 1** : La page 'View results' permet de visualiser dynamiquement les fichier d'export csv des requêtes SQL et MongoDB de façon à constater que la migration est parfaitement exécutée.
- **Bonus 2** : Importation d'open data pour les quartiers de Paris afin d'améliorer la carte choroplète (les données de base affichaient uniquement des rectangles sur la carte).

### Partie 3 : Requête MongoDB

- **Script** : `requetes_mongodb/requete_mongo.py`
- **Info** : La requête i est longue à exécuter.

### Partie 4 : Tableau de Bord

- **Script** : `dashboard/dashboard.py`
- **Description** : Accessible depuis l'interface Dash.

---

## Bug connu

- **Problème** : Si les fichiers d'export CSV sont supprimés et que les programmes de migration et de requêtes sont lancés depuis l'interface Dash (`run_all.py`), la partie "tableau de bord" ne charge pas.
- **Solution temporaire** : Relancer le logiciel.

---

## Piste d'amélioration

- **Prediction de flux de passagers** : Nous avons pensé à intégrer un programme d'apprentissage supervisé pour prédire les flux de passagers durant 2055. 