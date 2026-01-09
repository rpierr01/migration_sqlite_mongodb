"""
==============================================================
🚀 Partie 2 — Migration SQLite → MongoDB (OPTIMISÉE)
==============================================================

Améliorations par rapport à la v1 :
1. GeoJSON : Ajout du champ 'location' pour la cartographie (Partie 4).
2. Dates : Conversion en objets datetime natifs (plus de strings).
3. Performance : Utilisation de groupby() pour éviter les lenteurs.

==============================================================
"""

import sqlite3
import pandas as pd
from pymongo import MongoClient, GEOSPHERE
from tqdm import tqdm
import sys

# --- CONFIGURATION ---
SQLITE_PATH = "data/Paris2055.sqlite"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "Paris2055"

# --- CONNEXIONS ---
print("🔗 Connexion à la base SQLite...")
conn = sqlite3.connect(SQLITE_PATH)

print("🔗 Connexion à MongoDB...")
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# Nettoyage préalable
print("🧹 Nettoyage des collections existantes...")
db.Lignes.drop()
db.Arrets.drop()
db.Vehicules.drop()
db.Trafic.drop()

# --- CHARGEMENT ET PRÉ-TRAITEMENT DES DONNÉES ---
print("📥 Chargement et pré-traitement des DataFrames...")

# Fonction pour convertir les dates en objets datetime Python
def convert_dates(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df

# 1. Chargement brut
lignes = pd.read_sql_query("SELECT * FROM Ligne", conn)
quartiers = pd.read_sql_query("SELECT * FROM Quartier", conn)
arrets = pd.read_sql_query("SELECT * FROM Arret", conn)
arret_quartier = pd.read_sql_query("SELECT * FROM ArretQuartier", conn)
chauffeurs = pd.read_sql_query("SELECT * FROM Chauffeur", conn)
vehicules = pd.read_sql_query("SELECT * FROM Vehicule", conn)
horaires = pd.read_sql_query("SELECT * FROM Horaire", conn)
capteurs = pd.read_sql_query("SELECT * FROM Capteur", conn)
mesures = pd.read_sql_query("SELECT * FROM Mesure", conn)
trafics = pd.read_sql_query("SELECT * FROM Trafic", conn)
incidents = pd.read_sql_query("SELECT * FROM Incident", conn)

# 2. Conversion des dates (CRUCIAL pour les requêtes temporelles)
convert_dates(chauffeurs, ["date_embauche"])
convert_dates(mesures, ["horodatage"])
convert_dates(trafics, ["horodatage"])
convert_dates(incidents, ["horodatage"])
# Pour les horaires, c'est souvent juste des heures (HH:MM), on laisse ou on convertit selon le format exact
# Si c'est "HH:MM:SS", on peut laisser en string ou convertir en datetime complet si besoin.

# 3. Préparation des Groupes (OPTIMISATION N+1)
# Au lieu de filtrer dans la boucle, on prépare des dictionnaires de DataFrames
print("⚡ Indexation des données en mémoire pour accélération...")

# Groupement Quartiers par Arret
df_aq_full = arret_quartier.merge(quartiers, on="id_quartier")
groups_quartiers = df_aq_full.groupby("id_arret")

# Groupement Horaires par Arret
groups_horaires = horaires.groupby("id_arret")

# Groupement Mesures par Capteur
groups_mesures = mesures.groupby("id_capteur")

# Groupement Capteurs par Arret
groups_capteurs = capteurs.groupby("id_arret")

# Groupement Incidents par Trafic
groups_incidents = incidents.groupby("id_trafic")

# Jointure Chauffeurs sur Véhicules (plus simple que le groupby pour du 1-1)
vehicules_full = vehicules.merge(chauffeurs, on="id_chauffeur", how="left")

print("✅ Pré-traitement terminé.")

# --- COMPTEURS ---
total_stats = {
    "lignes": 0, "arrets": 0, "vehicules": 0, 
    "trafic": 0, "mesures": 0, "incidents": 0
}

# =============================================================================
# COLLECTION 1 : LIGNES
# =============================================================================
print("\n🚌 Migration LIGNES...")
docs_lignes = lignes.to_dict(orient="records")
if docs_lignes:
    db.Lignes.insert_many(docs_lignes)
    total_stats["lignes"] = len(docs_lignes)

# =============================================================================
# COLLECTION 2 : ARRETS (Complexe : GeoJSON + Imbrications)
# =============================================================================
print("🚏 Migration ARRETS (avec GeoJSON)...")
docs_arrets = []

# Disable tqdm progress bars
for _, arret in tqdm(arrets.iterrows(), total=len(arrets), disable=True):
    id_arret = arret["id_arret"]
    
    # 1. Récupération optimisée des Quartiers
    list_quartiers = []
    if id_arret in groups_quartiers.groups:
        list_quartiers = groups_quartiers.get_group(id_arret)[["id_quartier", "nom"]].to_dict(orient="records")

    # 2. Récupération optimisée des Horaires
    list_horaires = []
    if id_arret in groups_horaires.groups:
        list_horaires = groups_horaires.get_group(id_arret)[["id_vehicule", "heure_prevue", "heure_effective", "passagers_estimes"]].to_dict(orient="records")

    # 3. Récupération optimisée Capteurs + Mesures
    list_capteurs = []
    if id_arret in groups_capteurs.groups:
        sub_capteurs = groups_capteurs.get_group(id_arret)
        for _, cap in sub_capteurs.iterrows():
            id_cap = cap["id_capteur"]
            
            # Mesures du capteur
            list_mesures = []
            if id_cap in groups_mesures.groups:
                list_mesures = groups_mesures.get_group(id_cap)[["horodatage", "valeur", "unite"]].to_dict(orient="records")
                total_stats["mesures"] += len(list_mesures)

            list_capteurs.append({
                "id_capteur": int(id_cap),
                "type_capteur": cap["type_capteur"],
                # GeoJSON optionnel pour le capteur
                "location": {"type": "Point", "coordinates": [cap["longitude"], cap["latitude"]]},
                "mesures": list_mesures
            })

    # Construction du document Arret
    doc = {
        "id_arret": int(id_arret),
        "nom": arret["nom"],
        "id_ligne": int(arret["id_ligne"]),
        # GEOJSON STANDARD (Longitude d'abord !)
        "location": {
            "type": "Point",
            "coordinates": [arret["longitude"], arret["latitude"]]
        },
        # On garde les champs plats pour compatibilité si besoin
        "latitude": arret["latitude"],
        "longitude": arret["longitude"],
        "quartiers": list_quartiers,
        "capteurs": list_capteurs,
        "horaires": list_horaires
    }
    docs_arrets.append(doc)

if docs_arrets:
    db.Arrets.insert_many(docs_arrets)
    total_stats["arrets"] = len(docs_arrets)

# =============================================================================
# COLLECTION 3 : VEHICULES (avec Chauffeur)
# =============================================================================
print("🚐 Migration VEHICULES...")
docs_vehicules = []

# On utilise le DataFrame déjà fusionné 'vehicules_full'
for _, row in tqdm(vehicules_full.iterrows(), total=len(vehicules_full)):
    chauffeur_doc = {
        "id_chauffeur": int(row["id_chauffeur"]),
        "nom": row["nom"], # Vient de la jointure
        "date_embauche": row["date_embauche"] # Vient de la jointure (déjà en datetime)
    }
    
    doc = {
        "id_vehicule": int(row["id_vehicule"]),
        "immatriculation": row["immatriculation"],
        "id_ligne": int(row["id_ligne"]),
        "type_vehicule": row["type_vehicule"],
        "capacite": int(row["capacite"]),
        "chauffeur": chauffeur_doc
    }
    docs_vehicules.append(doc)

if docs_vehicules:
    db.Vehicules.insert_many(docs_vehicules)
    total_stats["vehicules"] = len(docs_vehicules)

# =============================================================================
# COLLECTION 4 : TRAFIC (avec Incidents)
# =============================================================================
print("⚠️  Migration TRAFIC...")
docs_trafic = []

for _, row in tqdm(trafics.iterrows(), total=len(trafics)):
    id_trafic = row["id_trafic"]
    
    list_incidents = []
    if id_trafic in groups_incidents.groups:
        list_incidents = groups_incidents.get_group(id_trafic)[["description", "gravite", "horodatage"]].to_dict(orient="records")
        total_stats["incidents"] += len(list_incidents)
    
    doc = {
        "id_trafic": int(id_trafic),
        "id_ligne": int(row["id_ligne"]),
        "horodatage": row["horodatage"], # Déjà datetime
        "retard_minutes": int(row["retard_minutes"]),
        "evenement": row["evenement"],
        "incidents": list_incidents
    }
    docs_trafic.append(doc)

if docs_trafic:
    db.Trafic.insert_many(docs_trafic)
    total_stats["trafic"] = len(docs_trafic)

# --- INDEXATION ---
print("\n🔍 Création des index (dont Géospatial)...")
# Index Géospatial pour les cartes (Partie 4)
db.Arrets.create_index([("location", GEOSPHERE)])

# Index standards pour la performance
db.Lignes.create_index("id_ligne")
db.Arrets.create_index("id_ligne")
db.Arrets.create_index("quartiers.id_quartier") # Pour chercher par quartier
db.Vehicules.create_index("id_ligne")
db.Trafic.create_index("id_ligne")
db.Trafic.create_index("horodatage") # Pour les séries temporelles

# --- RÉSUMÉ ---
print("\n" + "="*60)
print("✅ MIGRATION TERMINÉE AVEC SUCCÈS")
print("="*60)
print(f"🚌 Lignes      : {total_stats['lignes']}")
print(f"🚏 Arrêts      : {total_stats['arrets']} (Index GeoSphere activé)")
print(f"🚐 Véhicules   : {total_stats['vehicules']}")
print(f"⚠️  Trafic      : {total_stats['trafic']}")
print(f"📈 Mesures     : {total_stats['mesures']} (imbriquées)")
print(f"🚨 Incidents   : {total_stats['incidents']} (imbriqués)")
conn.close()
client.close()