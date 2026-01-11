import sqlite3
import pandas as pd

conn = sqlite3.connect("data/Paris2055.sqlite")
quartiers = pd.read_sql_query("SELECT id_quartier, nom, geojson FROM Quartier LIMIT 3", conn)
conn.close()

print("Exemple de format GeoJSON dans la base :")
for _, row in quartiers.iterrows():
    print(f"\n=== Quartier {row['id_quartier']}: {row['nom']} ===")
    print(f"Format: {row['geojson'][:200]}...")
