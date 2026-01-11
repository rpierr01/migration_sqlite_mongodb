"""
==============================================================
Partie 2 — Migration SQLite → MongoDB (OPTIMISÉE)
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

# --- CONFIGURATION ---
SQLITE_PATH = "data/Paris2055.sqlite"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "Paris2055"

# --- CONNEXIONS ---
print("Connexion à la base SQLite...")
conn = sqlite3.connect(SQLITE_PATH)

print("Connexion à MongoDB...")
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# Nettoyage préalable
print("Nettoyage des collections existantes...")
db.Lignes.drop()
db.Arrets.drop()
db.Vehicules.drop()
db.Trafic.drop()
db.Quartiers.drop()  # Nouvelle collection

# --- CHARGEMENT ET PRÉ-TRAITEMENT DES DONNÉES ---
print("Chargement et pré-traitement des DataFrames...")

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

# 3. Préparation des Groupes (OPTIMISATION N+1)
print("Indexation des données en mémoire pour accélération...")

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

# Jointure Chauffeurs sur Véhicules
vehicules_full = vehicules.merge(chauffeurs, on="id_chauffeur", how="left", validate="m:1")

print("Pré-traitement terminé.")

# --- COMPTEURS ---
total_stats = {
    "lignes": 0, "arrets": 0, "vehicules": 0, 
    "trafic": 0, "mesures": 0, "incidents": 0, "quartiers": 0
}

# Insert helper to process collections in chunks to reduce memory and driver overhead
def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

# =============================================================================
# COLLECTION 1 : LIGNES
# =============================================================================
print("\nMigration LIGNES...")
docs_lignes = lignes.to_dict(orient="records")
if docs_lignes:
    db.Lignes.insert_many(docs_lignes)
    total_stats["lignes"] = len(docs_lignes)

# =============================================================================
# COLLECTION 1.5 : QUARTIERS (avec GeoJSON RÉEL de Paris)
# =============================================================================
print("\nMigration QUARTIERS (avec GeoJSON réel de Paris)...")
docs_quartiers = []

# Charger le fichier GeoJSON réel
import json
geojson_path = "data/paris_quartiers_real.geojson"

try:
    with open(geojson_path, 'r', encoding='utf-8') as f:
        paris_geojson = json.load(f)
    
    print(f"✓ Fichier GeoJSON chargé : {len(paris_geojson['features'])} quartiers réels trouvés")
    
    # Créer un mapping entre les quartiers synthétiques et réels
    # On va mapper par proximité de nom ou utiliser un index
    quartiers_sqlite = pd.read_sql_query("SELECT id_quartier, nom FROM Quartier", conn)
    
    # Pour chaque quartier réel de Paris
    for idx, feature in enumerate(paris_geojson['features']):
        props = feature['properties']
        
        # Utiliser l'index comme id_quartier (ou mapper par nom si possible)
        # Ajuster selon votre logique métier
        id_quartier_mapping = idx + 1  # Commence à 1
        
        # Si vous voulez mapper par nom (correspondance approximative)
        nom_quartier_reel = props.get('l_qu', f'Quartier-{idx+1}')
        
        docs_quartiers.append({
            "id_quartier": int(id_quartier_mapping),
            "nom": nom_quartier_reel,
            "nom_officiel": nom_quartier_reel,
            "code_quartier": props.get('c_qu', ''),
            "arrondissement": props.get('c_ar', 0),
            "geometry": feature['geometry'],
            "surface": props.get('surface', 0),
            "perimetre": props.get('perimetre', 0),
            "is_real_paris": True
        })
    
    print(f"✓ {len(docs_quartiers)} quartiers réels préparés pour insertion")
    
except FileNotFoundError:
    print(f"⚠️ Fichier {geojson_path} non trouvé. Utilisation du fallback SQLite...")
    # Fallback vers l'ancienne méthode si le fichier n'existe pas
    for _, row in tqdm(quartiers.iterrows(), total=len(quartiers), desc="Quartiers"):
        geom_str = row['geojson']
        
        try:
            if geom_str.startswith('POLYGON'):
                if geom_str.startswith('MULTIPOLYGON'):
                    coords_str = geom_str.replace('MULTIPOLYGON(((', '').replace(')))', '')
                else:
                    coords_str = geom_str.replace('POLYGON((', '').replace('))', '')
                
                points = coords_str.split(',')
                coordinates = []
                
                for point in points:
                    point = point.strip()
                    if ' ' in point:
                        parts = point.split()
                        lon = float(parts[0])
                        lat = float(parts[1])
                        coordinates.append([lon, lat])
                
                if coordinates[0] != coordinates[-1]:
                    coordinates.append(coordinates[0])
                
                geometry = {
                    "type": "Polygon",
                    "coordinates": [coordinates]
                }
            else:
                import json
                geometry = json.loads(geom_str)
            
            docs_quartiers.append({
                "id_quartier": int(row['id_quartier']),
                "nom": row['nom'],
                "geometry": geometry,
                "is_real_paris": False
            })
            
        except Exception as e:
            print(f"Erreur parsing quartier {row['id_quartier']}: {e}")
            continue

if docs_quartiers:
    db.Quartiers.insert_many(docs_quartiers)
    total_stats["quartiers"] = len(docs_quartiers)
    real_count = sum(1 for q in docs_quartiers if q.get('is_real_paris', False))
    print(f"📊 Quartiers réels de Paris : {real_count}/{len(docs_quartiers)}")

# =============================================================================
# COLLECTION 2 : ARRETS (Complexe : GeoJSON + Imbrications)
# =============================================================================
print("Migration ARRETS (avec GeoJSON)...")
docs_arrets = []

# Pré-calcul des données imbriquées pour limiter les groupby répétés
quartiers_by_arret = {k: v[["id_quartier", "nom"]].to_dict(orient="records") for k, v in groups_quartiers}
horaires_by_arret = {k: v[["id_vehicule", "heure_prevue", "heure_effective", "passagers_estimes"]].to_dict(orient="records") for k, v in groups_horaires}
mesures_by_capteur = {k: v[["horodatage", "valeur", "unite"]].to_dict(orient="records") for k, v in groups_mesures}
total_stats["mesures"] = sum(len(v) for v in mesures_by_capteur.values())

capteurs_by_arret = {}
for k, sub_capteurs in groups_capteurs:
    caps = []
    for _, cap in sub_capteurs.iterrows():
        id_cap = cap["id_capteur"]
        caps.append({
            "id_capteur": int(id_cap),
            "type_capteur": cap["type_capteur"],
            "location": {"type": "Point", "coordinates": [cap["longitude"], cap["latitude"]]},
            "mesures": mesures_by_capteur.get(id_cap, [])
        })
    capteurs_by_arret[k] = caps

# Construction des documents Arrets via dicts pré-calculés (plus rapide que iterrows)
for arret in tqdm(arrets.to_dict(orient="records"), total=len(arrets), disable=False):
    id_arret = arret["id_arret"]
    doc = {
        "id_arret": int(id_arret),
        "nom": arret["nom"],
        "id_ligne": int(arret["id_ligne"]),
        "location": {"type": "Point", "coordinates": [arret["longitude"], arret["latitude"]]},
        "latitude": arret["latitude"],
        "longitude": arret["longitude"],
        "quartiers": quartiers_by_arret.get(id_arret, []),
        "capteurs": capteurs_by_arret.get(id_arret, []),
        "horaires": horaires_by_arret.get(id_arret, [])
    }
    docs_arrets.append(doc)

if docs_arrets:
    for batch in chunked(docs_arrets, 5000):
        db.Arrets.insert_many(batch, ordered=False, bypass_document_validation=True)
    total_stats["arrets"] = len(docs_arrets)

# =============================================================================
# COLLECTION 3 : VEHICULES (avec Chauffeur)
# =============================================================================
print("Migration VEHICULES...")
docs_vehicules = []

for row in tqdm(vehicules_full.itertuples(index=False), total=len(vehicules_full)):
    chauffeur_doc = {
        "id_chauffeur": int(row.id_chauffeur),
        "nom": row.nom,
        "date_embauche": row.date_embauche
    }
    docs_vehicules.append({
        "id_vehicule": int(row.id_vehicule),
        "immatriculation": row.immatriculation,
        "id_ligne": int(row.id_ligne),
        "type_vehicule": row.type_vehicule,
        "capacite": int(row.capacite),
        "chauffeur": chauffeur_doc
    })

if docs_vehicules:
    for batch in chunked(docs_vehicules, 10000):
        db.Vehicules.insert_many(batch, ordered=False, bypass_document_validation=True)
    total_stats["vehicules"] = len(docs_vehicules)

# =============================================================================
# COLLECTION 4 : TRAFIC (avec Incidents)
# =============================================================================
print("Migration TRAFIC...")
docs_trafic = []

incidents_by_trafic = {k: v[["description", "gravite", "horodatage"]].to_dict(orient="records") for k, v in groups_incidents}
total_stats["incidents"] = sum(len(v) for v in incidents_by_trafic.values())

for row in tqdm(trafics.itertuples(index=False), total=len(trafics)):
    docs_trafic.append({
        "id_trafic": int(row.id_trafic),
        "id_ligne": int(row.id_ligne),
        "horodatage": row.horodatage,
        "retard_minutes": int(row.retard_minutes),
        "evenement": row.evenement,
        "incidents": incidents_by_trafic.get(row.id_trafic, [])
    })

if docs_trafic:
    for batch in chunked(docs_trafic, 10000):
        db.Trafic.insert_many(batch, ordered=False, bypass_document_validation=True)
    total_stats["trafic"] = len(docs_trafic)

# --- INDEXATION ---
print("\nCréation des index (dont Géospatial)...")
db.Arrets.create_index([("location", GEOSPHERE)])
db.Quartiers.create_index([("geometry", GEOSPHERE)])  # Index géospatial pour les quartiers
db.Quartiers.create_index("id_quartier")
db.Lignes.create_index("id_ligne")
db.Arrets.create_index("id_ligne")
db.Arrets.create_index("quartiers.id_quartier")
db.Vehicules.create_index("id_ligne")
db.Trafic.create_index("id_ligne")
db.Trafic.create_index("horodatage")

# --- RÉSUMÉ ---
print("\n" + "="*60)
print("MIGRATION TERMINÉE AVEC SUCCÈS")
print("="*60)
print(f"Lignes      : {total_stats['lignes']}")
print(f"Quartiers   : {total_stats['quartiers']} (Index GeoSphere activé)")
print(f"Arrêts      : {total_stats['arrets']} (Index GeoSphere activé)")
print(f"Véhicules   : {total_stats['vehicules']}")
print(f"Trafic      : {total_stats['trafic']}")
print(f"Mesures     : {total_stats['mesures']} (imbriquées)")
print(f"Incidents   : {total_stats['incidents']} (imbriqués)")
conn.close()
client.close()