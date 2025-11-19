"""
==============================================================
🚀 Partie 2 — Migration SQLite → MongoDB (Projet Paris2055)
==============================================================

Objectif :
    Migrer la base relationnelle Paris2055.sqlite vers un modèle
    documentaire MongoDB avec plusieurs collections :
    - Lignes (infos de base)
    - Arrets (avec capteurs, mesures, quartiers, horaires imbriqués)
    - Vehicules (avec chauffeurs imbriqués)
    - Trafic (avec incidents imbriqués)

Avantages :
    - Meilleure modularité
    - Requêtes plus performantes
    - Possibilité de requêter chaque entité indépendamment
==============================================================
"""

import sqlite3
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

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

# Nettoyage préalable des collections MongoDB
print("🧹 Nettoyage des collections existantes...")
db.Lignes.drop()
db.Arrets.drop()
db.Vehicules.drop()
db.Trafic.drop()

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

# --- COMPTEURS GLOBAUX ---
total_lignes = 0
total_arrets = 0
total_capteurs = 0
total_mesures = 0
total_vehicules = 0
total_trafic = 0
total_incidents = 0

# =============================================================================
# COLLECTION 1 : LIGNES (informations de base uniquement)
# =============================================================================
print("\n🚌 Migration de la collection LIGNES...")
documents_lignes = []

for _, ligne in tqdm(tables["lignes"].iterrows(), desc="Lignes", total=len(tables["lignes"])):
    doc = {
        "id_ligne": int(ligne["id_ligne"]),
        "nom_ligne": ligne["nom_ligne"],
        "type": ligne["type"],
        "frequentation_moyenne": int(ligne["frequentation_moyenne"])
    }
    documents_lignes.append(doc)

if documents_lignes:
    db.Lignes.insert_many(documents_lignes)
    total_lignes = len(documents_lignes)
    print(f"✅ {total_lignes} lignes migrées")

# =============================================================================
# COLLECTION 2 : ARRETS (avec capteurs, mesures, quartiers, horaires)
# =============================================================================
print("\n🚏 Migration de la collection ARRETS...")
documents_arrets = []

for _, arret in tqdm(tables["arrets"].iterrows(), desc="Arrêts", total=len(tables["arrets"])):
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
    
    doc = {
        "id_arret": int(id_arret),
        "nom": arret["nom"],
        "latitude": arret["latitude"],
        "longitude": arret["longitude"],
        "id_ligne": int(arret["id_ligne"]),  # Référence à la ligne
        "quartiers": quartiers_docs,
        "capteurs": capteurs_docs,
        "horaires": horaires_docs
    }
    documents_arrets.append(doc)

if documents_arrets:
    db.Arrets.insert_many(documents_arrets)
    total_arrets = len(documents_arrets)
    print(f"✅ {total_arrets} arrêts migrés")

# =============================================================================
# COLLECTION 3 : VEHICULES (avec chauffeurs imbriqués)
# =============================================================================
print("\n🚐 Migration de la collection VEHICULES...")
documents_vehicules = []

for _, vehicule in tqdm(tables["vehicules"].iterrows(), desc="Véhicules", total=len(tables["vehicules"])):
    chauffeur = tables["chauffeurs"][tables["chauffeurs"]["id_chauffeur"] == vehicule["id_chauffeur"]]
    chauffeur_doc = None
    if not chauffeur.empty:
        chauffeur_doc = {
            "id_chauffeur": int(chauffeur.iloc[0]["id_chauffeur"]),
            "nom": chauffeur.iloc[0]["nom"],
            "date_embauche": chauffeur.iloc[0]["date_embauche"]
        }
    
    doc = {
        "id_vehicule": int(vehicule["id_vehicule"]),
        "immatriculation": vehicule["immatriculation"],
        "id_ligne": int(vehicule["id_ligne"]),  # Référence à la ligne
        "type_vehicule": vehicule["type_vehicule"],
        "capacite": int(vehicule["capacite"]),
        "chauffeur": chauffeur_doc
    }
    documents_vehicules.append(doc)

if documents_vehicules:
    db.Vehicules.insert_many(documents_vehicules)
    total_vehicules = len(documents_vehicules)
    print(f"✅ {total_vehicules} véhicules migrés")

# =============================================================================
# COLLECTION 4 : TRAFIC (avec incidents imbriqués)
# =============================================================================
print("\n⚠️  Migration de la collection TRAFIC...")
documents_trafic = []

for _, trafic in tqdm(tables["trafics"].iterrows(), desc="Événements trafic", total=len(tables["trafics"])):
    id_trafic = trafic["id_trafic"]
    incidents = tables["incidents"][tables["incidents"]["id_trafic"] == id_trafic]
    incidents_docs = incidents[["description", "gravite", "horodatage"]].to_dict(orient="records")
    total_incidents += len(incidents_docs)

    doc = {
        "id_trafic": int(id_trafic),
        "id_ligne": int(trafic["id_ligne"]),  # Référence à la ligne
        "horodatage": trafic["horodatage"],
        "retard_minutes": int(trafic["retard_minutes"]),
        "evenement": trafic["evenement"],
        "incidents": incidents_docs
    }
    documents_trafic.append(doc)

if documents_trafic:
    db.Trafic.insert_many(documents_trafic)
    total_trafic = len(documents_trafic)
    print(f"✅ {total_trafic} événements de trafic migrés")

# --- CRÉATION D'INDEX POUR OPTIMISER LES REQUÊTES ---
print("\n🔍 Création des index...")
db.Lignes.create_index("id_ligne")
db.Arrets.create_index("id_ligne")
db.Arrets.create_index("id_arret")
db.Vehicules.create_index("id_ligne")
db.Vehicules.create_index("id_vehicule")
db.Trafic.create_index("id_ligne")
db.Trafic.create_index("horodatage")
print("✅ Index créés")

# --- RÉSUMÉ GÉNÉRAL ---
print("\n" + "=" * 60)
print("📊 RÉSUMÉ DE LA MIGRATION")
print("=" * 60)
print(f"🚌 Lignes migrées          : {total_lignes}")
print(f"🚏 Arrêts migrés           : {total_arrets}")
print(f"📡 Capteurs (imbriqués)    : {total_capteurs}")
print(f"📈 Mesures (imbriquées)    : {total_mesures}")
print(f"🚐 Véhicules migrés        : {total_vehicules}")
print(f"⚠️  Événements trafic      : {total_trafic}")
print(f"🚨 Incidents (imbriqués)   : {total_incidents}")
print("=" * 60)
print("\n📦 COLLECTIONS CRÉÉES :")
print(f"  • Lignes     : {db.Lignes.count_documents({})} documents")
print(f"  • Arrets     : {db.Arrets.count_documents({})} documents")
print(f"  • Vehicules  : {db.Vehicules.count_documents({})} documents")
print(f"  • Trafic     : {db.Trafic.count_documents({})} documents")
print("=" * 60)

# --- FERMETURE ---
conn.close()
client.close()
print("🏁 Migration terminée avec succès.")