"""
Script pour créer un mapping entre les quartiers synthétiques 
de votre simulation et les vrais quartiers de Paris
"""

import sqlite3
import json
import pandas as pd
from pymongo import MongoClient

# Configuration
SQLITE_PATH = "data/Paris2055.sqlite"
GEOJSON_PATH = "data/paris_quartiers_real.geojson"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "Paris2055"

# Connexions
conn = sqlite3.connect(SQLITE_PATH)
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

# Charger les données
quartiers_sqlite = pd.read_sql_query("SELECT id_quartier, nom FROM Quartier", conn)

with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
    paris_geojson = json.load(f)

print("\n" + "="*60)
print("MAPPING DES QUARTIERS")
print("="*60)
print(f"Quartiers synthétiques (SQLite) : {len(quartiers_sqlite)}")
print(f"Quartiers réels (Paris)         : {len(paris_geojson['features'])}")
print("="*60)

# Créer un mapping personnalisé
# Option 1 : Mapping simple par index (1 à 1)
mapping = {}
for i, (_, row) in enumerate(quartiers_sqlite.iterrows()):
    if i < len(paris_geojson['features']):
        real_quartier = paris_geojson['features'][i]['properties']
        mapping[int(row['id_quartier'])] = {
            'nom_synthetique': row['nom'],
            'nom_reel': real_quartier.get('l_qu', ''),
            'arrondissement': real_quartier.get('c_ar', 0),
            'index_geojson': i
        }
        print(f"ID {row['id_quartier']:3d} : {row['nom']:20s} → {real_quartier.get('l_qu', 'N/A')}")

# Sauvegarder le mapping
mapping_path = "data/quartier_mapping.json"
with open(mapping_path, 'w', encoding='utf-8') as f:
    json.dump(mapping, f, ensure_ascii=False, indent=2)

print(f"\n✓ Mapping sauvegardé dans {mapping_path}")

# Mettre à jour MongoDB avec le mapping
print("\nMise à jour de la collection Quartiers...")
for id_quartier, info in mapping.items():
    idx = info['index_geojson']
    feature = paris_geojson['features'][idx]
    
    db.Quartiers.update_one(
        {"id_quartier": id_quartier},
        {
            "$set": {
                "nom_synthetique": info['nom_synthetique'],
                "nom_reel": info['nom_reel'],
                "arrondissement": info['arrondissement'],
                "geometry": feature['geometry']
            }
        },
        upsert=True
    )

print(f"✓ {len(mapping)} quartiers mis à jour dans MongoDB")

conn.close()
client.close()
