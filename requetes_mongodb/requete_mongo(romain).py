# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 15:30:39 2025

@author: rfaucher
"""
import pymongo as mg
import pandas as pd
import os

print("Début des requêtes MongoDB...")

connex = mg.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.Paris2055

# Create export directory
EXPORT_DIR = "requetes_mongodb/resultat_requetes_mongodb"
os.makedirs(EXPORT_DIR, exist_ok=True)

#%%a
result_a=pd.DataFrame(db.Trafic.aggregate([{"$group": {"_id": "$id_ligne", "retard_moyen": {"$avg": "$retard_minutes"}}}
    ]))
print(result_a)
result_a.to_csv(os.path.join(EXPORT_DIR, "requete_a.csv"), index=False)

#%%b pas fini
result_b=pd.DataFrame(db.Trafic.aggregate([{"$group": {"_id": "$id_ligne", "retard_moyen": {"$avg": "$passagers_estimes"}}}]))
result_b.to_csv(os.path.join(EXPORT_DIR, "requete_b.csv"), index=False)

#%%C
result_c=pd.DataFrame(db.Trafic.aggregate([{"$group": {"_id": "$id_ligne", "nb_incident": {"$avg": "$retard_minutes"}}}
    ]))
print(result_c)
result_c.to_csv(os.path.join(EXPORT_DIR, "requete_c.csv"), index=False)

print("Fin des requêtes MongoDB.")
