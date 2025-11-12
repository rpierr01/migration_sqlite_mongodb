"""
==============================================================
🚀 Partie 2 — Migration SQLite → MongoDB (Projet Paris2055)
==============================================================

Objectif :
    Migrer la base relationnelle Paris2055.sqlite vers un modèle
    documentaire MongoDB, avec imbrication logique des entités.

Nouveauté :
    - Ajout d'un résumé détaillé des entités migrées :
      lignes, arrêts, capteurs, véhicules, mesures, incidents
==============================================================
"""

import sqlite3
import pandas as pd
from pymongo import MongoClient

# --- CONFIGURATION ---
SQLITE_PATH = "../data/Paris2055.sqlite"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "Paris2055"

# --- CONNEXIONS ---
print("🔗 Connexion à la base SQLite...")
conn = sqlite3.connect(SQLITE_PATH)

print("🔗 Connexion à MongoDB...")
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# Nettoyage préalable de la collection MongoDB
db.Lignes.drop()

# --- CHARGEMENT DES TABLES ---
print("📥 Chargement des tables SQLite...")
tables = {
    "lignes": pd.read_sql_query("SELECT * FROM Ligne", conn),
    "quartiers": pd.read_sql_query("SELECT * FROM Quartier", conn),
    "arrets": pd.read_sql_query("SELECT * FROM Arret", conn),
    "arret_quartier": pd.read_sql_query("SELECT * FROM ArretQuartier", conn),
    "chauffeurs": pd.read_sql_query("SELECT * FROM Chauffeur", conn),
    "vehicules": pd.read_sql_query("SELECT * FROM Vehicule", conn),
    "horaires": pd.read_sql_query("SELECT * FROM Horaire", conn),
    "capteurs": pd.read_sql_query("SELECT * FROM Capteur", conn),
    "mesures": pd.read_sql_query("SELECT * FROM Mesure", conn),
    "trafics": pd.read_sql_query("SELECT * FROM Trafic", conn),
    "incidents": pd.read_sql_query("SELECT * FROM Incident", conn)
}

print("✅ Tables chargées :", ", ".join(tables.keys()))

# --- MIGRATION DES LIGNES ---
print("\n🚧 Début de la migration vers MongoDB...")

documents_lignes = []
# Compteurs globaux
total_arrets = 0
total_capteurs = 0
total_mesures = 0
total_vehicules = 0
total_incidents = 0

for _, ligne in tables["lignes"].iterrows():
    id_ligne = ligne["id_ligne"]

    # --- Arrets de la ligne ---
    df_arrets = tables["arrets"][tables["arrets"]["id_ligne"] == id_ligne]
    arrets_docs = []
    for _, arret in df_arrets.iterrows():
        id_arret = arret["id_arret"]

        # Quartiers associés
        quartiers_ids = tables["arret_quartier"][tables["arret_quartier"]["id_arret"] == id_arret]["id_quartier"]
        quartiers_data = tables["quartiers"][tables["quartiers"]["id_quartier"].isin(quartiers_ids)]
        quartiers_docs = quartiers_data[["id_quartier", "nom"]].to_dict(orient="records")

        # Capteurs et mesures
        df_capteurs = tables["capteurs"][tables["capteurs"]["id_arret"] == id_arret]
        capteurs_docs = []
        for _, capteur in df_capteurs.iterrows():
            id_capteur = capteur["id_capteur"]
            mesures = tables["mesures"][tables["mesures"]["id_capteur"] == id_capteur]
            mesures_docs = mesures[["horodatage", "valeur", "unite"]].to_dict(orient="records")
            total_mesures += len(mesures_docs)
            capteurs_docs.append({
                "id_capteur": int(id_capteur),
                "type_capteur": capteur["type_capteur"],
                "latitude": capteur["latitude"],
                "longitude": capteur["longitude"],
                "mesures": mesures_docs
            })

        # Horaires
        horaires = tables["horaires"][tables["horaires"]["id_arret"] == id_arret]
        horaires_docs = horaires[["id_vehicule", "heure_prevue", "heure_effective", "passagers_estimes"]].to_dict(orient="records")

        total_capteurs += len(capteurs_docs)
        arrets_docs.append({
            "id_arret": int(id_arret),
            "nom": arret["nom"],
            "latitude": arret["latitude"],
            "longitude": arret["longitude"],
            "quartiers": quartiers_docs,
            "capteurs": capteurs_docs,
            "horaires": horaires_docs
        })

    # --- Véhicules ---
    df_vehicules = tables["vehicules"][tables["vehicules"]["id_ligne"] == id_ligne]
    vehicules_docs = []
    for _, vehicule in df_vehicules.iterrows():
        chauffeur = tables["chauffeurs"][tables["chauffeurs"]["id_chauffeur"] == vehicule["id_chauffeur"]]
        chauffeur_doc = None
        if not chauffeur.empty:
            chauffeur_doc = {
                "id_chauffeur": int(chauffeur.iloc[0]["id_chauffeur"]),
                "nom": chauffeur.iloc[0]["nom"],
                "date_embauche": chauffeur.iloc[0]["date_embauche"]
            }
        vehicules_docs.append({
            "id_vehicule": int(vehicule["id_vehicule"]),
            "immatriculation": vehicule["immatriculation"],
            "type_vehicule": vehicule["type_vehicule"],
            "capacite": int(vehicule["capacite"]),
            "chauffeur": chauffeur_doc
        })
    total_vehicules += len(vehicules_docs)

    # --- Trafic ---
    df_trafic = tables["trafics"][tables["trafics"]["id_ligne"] == id_ligne]
    trafic_docs = []
    for _, trafic in df_trafic.iterrows():
        id_trafic = trafic["id_trafic"]
        incidents = tables["incidents"][tables["incidents"]["id_trafic"] == id_trafic]
        incidents_docs = incidents[["description", "gravite", "horodatage"]].to_dict(orient="records")
        total_incidents += len(incidents_docs)

        trafic_docs.append({
            "id_trafic": int(id_trafic),
            "horodatage": trafic["horodatage"],
            "retard_minutes": int(trafic["retard_minutes"]),
            "evenement": trafic["evenement"],
            "incidents": incidents_docs
        })

    total_arrets += len(arrets_docs)

    # --- Document final de la ligne ---
    doc = {
        "id_ligne": int(id_ligne),
        "nom_ligne": ligne["nom_ligne"],
        "type": ligne["type"],
        "frequentation_moyenne": int(ligne["frequentation_moyenne"]),
        "arrets": arrets_docs,
        "vehicules": vehicules_docs,
        "trafic": trafic_docs
    }
    documents_lignes.append(doc)

# --- INSERTION ---
if documents_lignes:
    db.Lignes.insert_many(documents_lignes)
    print(f"✅ {len(documents_lignes)} lignes migrées vers MongoDB !")
else:
    print("⚠️ Aucune ligne trouvée à migrer.")

# --- RÉSUMÉ GÉNÉRAL ---
print("\n📊 RÉSUMÉ DE LA MIGRATION")
print("=" * 50)
print(f"🚌 Lignes migrées     : {len(documents_lignes)}")
print(f"🚏 Total arrêts       : {total_arrets}")
print(f"📡 Total capteurs     : {total_capteurs}")
print(f"📈 Total mesures      : {total_mesures}")
print(f"🚐 Total véhicules    : {total_vehicules}")
print(f"⚠️  Total incidents    : {total_incidents}")
print("=" * 50)

# --- FERMETURE ---
conn.close()
client.close()
print("🏁 Migration terminée avec succès.")