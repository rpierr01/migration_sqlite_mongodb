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

EXPORT_DIR = "requetes_sql/resultat_requetes_sql"
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

#%%b - Nombre moyen de passagers transportés par jour et par ligne 

# Modifié -> La modification remplace l'extraction de 6 caractères par la fonction DATE(), car SUBSTR(..., 1, 6) regroupait 
# les données par tranches de mois incohérentes (ex: "2053-0") alors que l'énoncé exige une moyenne calculée 
# par journée calendrier complète.

curseur.execute("""
    SELECT 
        nom_ligne,
        DATE(heure_prevue) AS jour,
        AVG(passagers_estimes) AS avg_passagers_estimes
    FROM Horaire
    LEFT JOIN Arret ON Horaire.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    GROUP BY Ligne.id_ligne, jour
    ORDER BY nom_ligne, jour
""")
result_b = curseur.fetchall()

pd.DataFrame(result_b, columns=["nom_ligne", "jour", "avg_passagers_estimes"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_b.csv"), index=False
)

#%%c

# Modifié -> J'ai transformé le comptage en taux pour mieux refléter l'énoncé qui demande le "taux d'incidents par ligne".

curseur.execute("""
    SELECT 
        nom_ligne, 
        CAST(COUNT(id_incident) AS FLOAT) / COUNT(Trafic.id_trafic) AS taux_incident
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
    GROUP BY Ligne.id_ligne
""")
result_c = curseur.fetchall()
pd.DataFrame(result_c, columns=["nom_ligne", "incident_taux"]).to_csv(
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

#%%f - Liste des lignes sans incident mais avec retards > 10 min

# Modifié -> Ajout de DISTINCT pour éviter les doublons dans le résultat.

curseur.execute("""
    SELECT DISTINCT nom_ligne
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
    WHERE retard_minutes > 10 AND id_incident IS NULL
    ORDER BY nom_ligne
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

#%%i - Corrélation entre trafic et pollution (CO2 quand retard augmente) par ligne
curseur.execute("""
    SELECT 
        Ligne.nom_ligne,
        (SUM(Mesure.valeur * Trafic.retard_minutes) - (SUM(Mesure.valeur) * SUM(Trafic.retard_minutes)) / COUNT(*)) /
        SQRT(
            (SUM(Mesure.valeur * Mesure.valeur) - (SUM(Mesure.valeur) * SUM(Mesure.valeur)) / COUNT(*)) *
            (SUM(Trafic.retard_minutes * Trafic.retard_minutes) - (SUM(Trafic.retard_minutes) * SUM(Trafic.retard_minutes)) / COUNT(*))
        ) AS correlation
    FROM Mesure
    JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    JOIN Arret ON Capteur.id_arret = Arret.id_arret
    JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne 
        AND DATE(Mesure.horodatage) = DATE(Trafic.horodatage)
    WHERE Capteur.type_capteur = 'CO2'
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

#%%l - % de véhicules électriques par ligne de bus

# modif -> La requête a été modifiée pour passer d'un taux global à un détail par ligne de transport 
# en ajoutant un regroupement par ligne et un filtre spécifique sur le type de véhicule "Bus".

curseur.execute("""
    SELECT 
        Ligne.nom_ligne,
        (COUNT(CASE WHEN Vehicule.type_vehicule = 'Electrique' THEN 1 END) * 1.0 / COUNT(*)) AS taux_electrique
    FROM Vehicule
    JOIN Ligne ON Vehicule.id_ligne = Ligne.id_ligne
    WHERE Ligne.type = 'Bus'
    GROUP BY Ligne.id_ligne
    ORDER BY nom_ligne
""")
result_l = curseur.fetchall()

pd.DataFrame(result_l, columns=["nom_ligne", "taux_electrique"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_l.csv"), index=False
)

#%%m - Classification du niveau de pollution par capteur avec sa localisation

# La sélection a été enrichie avec l'identifiant du capteur et ses coordonnées GPS (latitude et longitude) 
# afin de rendre les données de pollution exploitables pour une visualisation géographique.

curseur.execute("""
    SELECT 
        Capteur.id_capteur,
        Capteur.latitude,
        Capteur.longitude,
        Mesure.valeur,
        CASE
            WHEN Mesure.valeur < 400 THEN 'faible'
            WHEN Mesure.valeur < 500 THEN 'moyen'
            ELSE 'élevé'
        END AS niveau_pollution
    FROM Mesure
    JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    WHERE Capteur.type_capteur = 'CO2'
""")
result_m = curseur.fetchall()

pd.DataFrame(result_m, columns=["id_capteur", "latitude", "longitude", "valeur", "niveau_pollution"]).to_csv(
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




