import sqlite3
import pandas as pd

# --- Connexion à la base SQLite ---
db_path = "data/Paris2055.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("🔗 Connexion réussie à", db_path)
print("=" * 60)

# --- 1️⃣ Lister les tables ---
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]
print("📋 Tables présentes dans la base :")
for t in tables:
    print(" -", t)

print("=" * 60)

# --- 2️⃣ Explorer la structure de chaque table ---
for table in tables:
    print(f"🧱 Structure de la table '{table}' :")
    cursor.execute(f"PRAGMA table_info({table});")
    columns = cursor.fetchall()
    df_cols = pd.DataFrame(columns, columns=["cid", "name", "type", "notnull", "default_value", "pk"])
    print(df_cols[["name", "type", "pk"]])
    print("-" * 40)

print("=" * 60)

# --- 3️⃣ Rechercher les clés étrangères ---
for table in tables:
    cursor.execute(f"PRAGMA foreign_key_list({table});")
    fkeys = cursor.fetchall()
    if fkeys:
        print(f"🔗 Clés étrangères dans '{table}' :")
        df_fk = pd.DataFrame(fkeys, columns=["id", "seq", "table_ref", "from_col", "to_col", "on_update", "on_delete", "match"])
        print(df_fk[["from_col", "table_ref", "to_col"]])
        print("-" * 40)

print("=" * 60)

# --- 4️⃣ Afficher un aperçu des données ---
for table in tables:
    print(f"📊 Aperçu des données dans '{table}' :")
    df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 5;", conn)
    print(df)
    print("-" * 60)

conn.close()
print("✅ Exploration terminée.")