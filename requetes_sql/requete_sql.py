# -*- coding: utf-8 -*-
"""
Created on Thu Nov 27 14:13:16 2025

@author: rfaucher
"""


import sqlite3
import pandas as pd
#from pymongo import MongoClient, GEOSPHERE
from tqdm import tqdm
from math import sqrt
import os


SQLITE_PATH = "data/Paris2055.sqlite"
#MONGO_URI = "mongodb://localhost:27017/"
#MONGO_DB_NAME = "Paris2055"

EXPORT_DIR = "resultat_requetes_sql"
os.makedirs(EXPORT_DIR, exist_ok=True)

#
conn = sqlite3.connect(SQLITE_PATH)

#ajout de la fonction racine au requete
conn.create_function("SQRT", 1, sqrt)
#
curseur = conn.cursor()

#%%a
curseur.execute("""
    SELECT nom_ligne, AVG(retard_minutes)
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    GROUP BY Ligne.id_ligne
""")
result_a = curseur.fetchall()
pd.DataFrame(result_a, columns=["nom_ligne", "avg_retard_minutes"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_a.csv"), index=False
)

#%%b
curseur.execute("""
    SELECT nom_ligne,
           SUBSTR(heure_prevue, 1, 6),
           AVG(passagers_estimes)
    FROM Horaire
    LEFT JOIN Arret ON Horaire.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    GROUP BY Ligne.id_ligne, SUBSTR(heure_prevue, 1, 6)
""")
result_b = curseur.fetchall()
pd.DataFrame(result_b, columns=["nom_ligne", "heure_prevue", "avg_passagers_estimes"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_b.csv"), index=False
)

#%%c
curseur.execute("""
    SELECT nom_ligne, COUNT(id_incident)
    FROM Incident
    LEFT JOIN Trafic ON Incident.id_trafic = Trafic.id_trafic
    LEFT JOIN Ligne ON Trafic.id_ligne = Ligne.id_ligne
    GROUP BY Ligne.id_ligne
""")
result_c = curseur.fetchall()
pd.DataFrame(result_c, columns=["nom_ligne", "incident_count"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_c.csv"), index=False
)

#%%d
curseur.execute("""
    SELECT immatriculation, type_vehicule, AVG(valeur)
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    LEFT JOIN Vehicule ON Ligne.id_ligne = Vehicule.id_ligne
    WHERE type_capteur = 'CO2'
    GROUP BY id_vehicule
""")
result_d = curseur.fetchall()
pd.DataFrame(result_d, columns=["immatriculation", "type_vehicule", "avg_valeur"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_d.csv"), index=False
)

#%%e
curseur.execute("""
    SELECT Quartier.nom, AVG(valeur)
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN ArretQuartier ON Arret.id_arret = ArretQuartier.id_arret
    LEFT JOIN Quartier ON ArretQuartier.id_quartier = Quartier.id_quartier
    WHERE type_capteur = 'Bruit'
    GROUP BY Quartier.id_quartier
    ORDER BY AVG(valeur) DESC
    LIMIT 5
""")
result_e = curseur.fetchall()
pd.DataFrame(result_e, columns=["quartier_nom", "avg_valeur"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_e.csv"), index=False
)

#%%f
curseur.execute("""
    SELECT nom_ligne
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
    WHERE retard_minutes > 10 AND id_incident IS NULL
""")
result_f = curseur.fetchall()
pd.DataFrame(result_f, columns=["nom_ligne"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_f.csv"), index=False
)

#%%g
curseur.execute("""
    SELECT (COUNT(CASE WHEN retard_minutes = 0 THEN 1 END) * 1.0 / COUNT(*)) AS taux_sans_retard
    FROM Trafic
""")
result_g = curseur.fetchall()
pd.DataFrame(result_g, columns=["taux_sans_retard"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_g.csv"), index=False
)

#%%h
curseur.execute("""
    SELECT Quartier.nom, COUNT(id_arret)
    FROM Quartier
    LEFT JOIN ArretQuartier ON Quartier.id_quartier = ArretQuartier.id_quartier
    GROUP BY Quartier.id_quartier
""")
result_h = curseur.fetchall()
pd.DataFrame(result_h, columns=["quartier_nom", "arret_count"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_h.csv"), index=False
)

#%%i pas sur de moi
curseur.execute("""
    SELECT nom_ligne,
           (SUM(valeur * retard_minutes) - (SUM(valeur) * SUM(retard_minutes)) / COUNT(*)) /
           SQRT((SUM(valeur * valeur) - (SUM(valeur) * SUM(valeur)) / COUNT(*)) *
                (SUM(retard_minutes * retard_minutes) - (SUM(retard_minutes) * SUM(retard_minutes)) / COUNT(*)))
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    WHERE type_capteur = 'CO2'
    GROUP BY Ligne.id_ligne
""")
result_i = curseur.fetchall()
pd.DataFrame(result_i, columns=["nom_ligne", "correlation"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_i.csv"), index=False
)

#%%j
curseur.execute("""
    SELECT nom_ligne, AVG(valeur)
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    WHERE type_capteur = 'Temperature'
    GROUP BY Ligne.id_ligne
""")
result_j = curseur.fetchall()
pd.DataFrame(result_j, columns=["nom_ligne", "avg_temperature"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_j.csv"), index=False
)

#%%k
curseur.execute("""
    SELECT nom, AVG(retard_minutes)
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    LEFT JOIN Vehicule ON Ligne.id_ligne = Vehicule.id_ligne
    LEFT JOIN Chauffeur ON Vehicule.id_chauffeur = Chauffeur.id_chauffeur
    GROUP BY Chauffeur.id_chauffeur
""")
result_k = curseur.fetchall()
pd.DataFrame(result_k, columns=["chauffeur_nom", "avg_retard_minutes"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_k.csv"), index=False
)

#%%l
curseur.execute("""
    SELECT (COUNT(CASE WHEN type_vehicule = 'Electrique' THEN 1 END) * 1.0 / COUNT(*)) AS taux_sans_retard
    FROM Vehicule
""")
result_l = curseur.fetchall()
pd.DataFrame(result_l, columns=["taux_sans_retard"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_l.csv"), index=False
)

#%%m 
curseur.execute("""
    SELECT valeur,
           CASE
               WHEN valeur < 400 THEN 'faible'
               WHEN valeur < 500 THEN 'moyen'
               ELSE 'élevé'
           END AS niveau_pollution
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    WHERE type_capteur = 'CO2'
""")
result_m = curseur.fetchall()
pd.DataFrame(result_m, columns=["valeur", "niveau_pollution"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_m.csv"), index=False
)

#%%n
curseur.execute("""
    SELECT nom_ligne,
           CASE
               WHEN AVG(retard_minutes) = 0 THEN 'aucun retard'
               WHEN AVG(retard_minutes) < 5 THEN 'retard moyen inf a 5min'
               ELSE 'retard moyen sup a 5min'
           END AS classification_retard
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    GROUP BY Ligne.id_ligne
""")
result_n = curseur.fetchall()
pd.DataFrame(result_n, columns=["nom_ligne", "classification_retard"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_n.csv"), index=False
)




