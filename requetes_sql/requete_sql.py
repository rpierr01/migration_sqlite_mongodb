# -*- coding: utf-8 -*-
"""
Created on Thu Nov 27 14:13:16 2025
Updated for consistency: Sorting and column matching.
@author: rfaucher
"""

print("Début des requêtes SQL sur SQLite...")

import sqlite3
import pandas as pd
from math import sqrt
import os

SQLITE_PATH = "data/Paris2055.sqlite"
EXPORT_DIR = "requetes_sql/resultat_requetes_sql"
os.makedirs(EXPORT_DIR, exist_ok=True)

conn = sqlite3.connect(SQLITE_PATH)
conn.create_function("SQRT", 1, sqrt)
curseur = conn.cursor()

#%% a - Moyenne des retards par ligne
# Tri : Alphabétique par nom de ligne
curseur.execute("""
    SELECT nom_ligne, AVG(retard_minutes) AS avg_retard
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    GROUP BY Ligne.id_ligne
    ORDER BY nom_ligne ASC
""")
result_a = curseur.fetchall()
pd.DataFrame(result_a, columns=["nom_ligne", "avg_retard"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_a.csv"), index=False
)

#%% b - Nombre moyen de passagers par jour et par ligne
# Tri : Alphabétique par ligne, puis chronologique
curseur.execute("""
    SELECT 
        nom_ligne,
        DATE(heure_prevue) AS jour,
        AVG(passagers_estimes) AS avg_passagers
    FROM Horaire
    LEFT JOIN Arret ON Horaire.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    GROUP BY Ligne.id_ligne, jour
    ORDER BY nom_ligne ASC, jour ASC
""")
result_b = curseur.fetchall()
pd.DataFrame(result_b, columns=["nom_ligne", "jour", "avg_passagers"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_b.csv"), index=False
)

#%% c - Taux d'incidents par ligne (Corrigé)
# Correction : Utilisation de COUNT(DISTINCT Trafic.id_trafic) pour le dénominateur
# afin d'éviter de compter plusieurs fois le même trajet s'il a plusieurs incidents.

curseur.execute("""
    SELECT 
        nom_ligne, 
        CAST(COUNT(id_incident) AS FLOAT) / NULLIF(COUNT(DISTINCT Trafic.id_trafic), 0) AS incident_taux
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
    GROUP BY Ligne.id_ligne
    ORDER BY nom_ligne ASC
""")
result_c = curseur.fetchall()
pd.DataFrame(result_c, columns=["nom_ligne", "incident_taux"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_c.csv"), index=False
)

#%% d - Emissions moyennes CO2 par véhicule
# Tri : Par immatriculation
curseur.execute("""
    SELECT immatriculation, type_vehicule, AVG(valeur) AS avg_co2
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    LEFT JOIN Vehicule ON Ligne.id_ligne = Vehicule.id_ligne
    WHERE type_capteur = 'CO2'
    GROUP BY id_vehicule
    ORDER BY immatriculation ASC
""")
result_d = curseur.fetchall()
pd.DataFrame(result_d, columns=["immatriculation", "type_vehicule", "avg_co2"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_d.csv"), index=False
)

#%% e - Top 5 quartiers bruyants
# Tri : Valeur décroissante, puis nom de quartier (pour égalité)
curseur.execute("""
    SELECT Quartier.nom, AVG(valeur) AS avg_bruit
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN ArretQuartier ON Arret.id_arret = ArretQuartier.id_arret
    LEFT JOIN Quartier ON ArretQuartier.id_quartier = Quartier.id_quartier
    WHERE type_capteur = 'Bruit'
    GROUP BY Quartier.id_quartier
    ORDER BY avg_bruit DESC, Quartier.nom ASC
    LIMIT 5
""")
result_e = curseur.fetchall()
pd.DataFrame(result_e, columns=["quartier_nom", "avg_bruit"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_e.csv"), index=False
)

#%% f - Lignes sans incident mais retards > 10 min
# Tri : Alphabétique par nom de ligne
curseur.execute("""
    SELECT DISTINCT nom_ligne
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
    WHERE retard_minutes > 10 AND id_incident IS NULL
    ORDER BY nom_ligne ASC
""")
result_f = curseur.fetchall()
pd.DataFrame(result_f, columns=["nom_ligne"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_f.csv"), index=False
)

#%% g - Taux de ponctualité global
curseur.execute("""
    SELECT (COUNT(CASE WHEN retard_minutes = 0 THEN 1 END) * 1.0 / COUNT(*)) AS taux_sans_retard
    FROM Trafic
""")
result_g = curseur.fetchall()
pd.DataFrame(result_g, columns=["taux_sans_retard"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_g.csv"), index=False
)

#%% h - Nombre d'arrêts par quartier
# Tri : Nombre d'arrêts décroissant, puis nom quartier
curseur.execute("""
    SELECT Quartier.nom, COUNT(id_arret) AS arret_count
    FROM Quartier
    LEFT JOIN ArretQuartier ON Quartier.id_quartier = ArretQuartier.id_quartier
    GROUP BY Quartier.id_quartier
    ORDER BY arret_count DESC, Quartier.nom ASC
""")
result_h = curseur.fetchall()
pd.DataFrame(result_h, columns=["quartier_nom", "arret_count"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_h.csv"), index=False
)

#%% i - Corrélation Trafic/Pollution
# Tri : Alphabétique par nom de ligne
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
    ORDER BY nom_ligne ASC
""")
result_i = curseur.fetchall()
pd.DataFrame(result_i, columns=["nom_ligne", "correlation"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_i.csv"), index=False
)

#%% j - Température moyenne par ligne
# Tri : Alphabétique par nom de ligne
curseur.execute("""
    SELECT nom_ligne, AVG(valeur) AS avg_temperature
    FROM Mesure
    LEFT JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    LEFT JOIN Arret ON Capteur.id_arret = Arret.id_arret
    LEFT JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    WHERE type_capteur = 'Temperature'
    GROUP BY Ligne.id_ligne
    ORDER BY nom_ligne ASC
""")
result_j = curseur.fetchall()
pd.DataFrame(result_j, columns=["nom_ligne", "avg_temperature"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_j.csv"), index=False
)

#%% k - Retard moyen par chauffeur (Corrigé)
# Correction : 
# 1. On part de la table Vehicule (comme en Mongo) pour assurer le même périmètre.
# 2. On utilise INNER JOIN sur Trafic pour ne garder que les lignes ayant réellement circulé
#    (similaire au comportement par défaut de $unwind en Mongo qui supprime les vides).

curseur.execute("""
    SELECT 
        Chauffeur.nom, 
        AVG(Trafic.retard_minutes) AS avg_retard_minutes
    FROM Vehicule
    INNER JOIN Chauffeur ON Vehicule.id_chauffeur = Chauffeur.id_chauffeur
    INNER JOIN Ligne ON Vehicule.id_ligne = Ligne.id_ligne
    INNER JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    GROUP BY Chauffeur.id_chauffeur
    ORDER BY Chauffeur.nom ASC
""")
result_k = curseur.fetchall()
pd.DataFrame(result_k, columns=["chauffeur_nom", "avg_retard_minutes"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_k.csv"), index=False
)

#%% l - % véhicules électriques par ligne de bus
# Tri : Alphabétique par nom de ligne
curseur.execute("""
    SELECT 
        Ligne.nom_ligne,
        (COUNT(CASE WHEN Vehicule.type_vehicule = 'Electrique' THEN 1 END) * 1.0 / COUNT(*)) AS taux_electrique
    FROM Vehicule
    JOIN Ligne ON Vehicule.id_ligne = Ligne.id_ligne
    WHERE Ligne.type = 'Bus'
    GROUP BY Ligne.id_ligne
    ORDER BY nom_ligne ASC
""")
result_l = curseur.fetchall()
pd.DataFrame(result_l, columns=["nom_ligne", "taux_electrique"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_l.csv"), index=False
)

#%% m - Classification pollution
# Tri : Par ID Capteur pour cohérence
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
    ORDER BY Capteur.id_capteur ASC
""")
result_m = curseur.fetchall()
pd.DataFrame(result_m, columns=["id_capteur", "latitude", "longitude", "valeur", "niveau_pollution"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_m.csv"), index=False
)

#%% n - Classification retard par ligne
# Tri : Alphabétique par nom de ligne
curseur.execute("""
    SELECT nom_ligne,
           CASE
               WHEN AVG(retard_minutes) < 6.5 THEN 'retard moyen inf a 6min30'
               WHEN AVG(retard_minutes) < 7 THEN 'retard moyen inf a 7min'
               ELSE 'retard moyen sup a 7min'
           END AS classification_retard
    FROM Ligne
    LEFT JOIN Trafic ON Ligne.id_ligne = Trafic.id_ligne
    GROUP BY Ligne.id_ligne
    ORDER BY nom_ligne ASC
""")
result_n = curseur.fetchall()
pd.DataFrame(result_n, columns=["nom_ligne", "classification_retard"]).to_csv(
    os.path.join(EXPORT_DIR, "requete_n.csv"), index=False
)

print("Fin des requêtes SQL sur SQLite.")