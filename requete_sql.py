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


SQLITE_PATH = "../data/Paris2055.sqlite"
#MONGO_URI = "mongodb://localhost:27017/"
#MONGO_DB_NAME = "Paris2055"




#
conn = sqlite3.connect(SQLITE_PATH)

#ajout de la fonction racine au requete
conn.create_function("SQRT", 1, sqrt)
#
curseur = conn.cursor()

#%%a
curseur.execute("select nom_ligne,avg(retard_minutes ) from Ligne left join Trafic on Ligne.id_ligne = Trafic.id_ligne  group by Ligne.id_ligne")

result_a = curseur.fetchall()    
#%%b
curseur.execute("select nom_ligne,substr(heure_prevue,1,6),avg(passagers_estimes) from Horaire left join Arret on Horaire.id_arret = Arret.id_arret left join Ligne on Arret.id_ligne = Ligne.id_ligne group by Ligne.id_ligne, substr(heure_prevue,1,6)")
result_b = curseur.fetchall()

#%%c
curseur.execute("Select nom_ligne,count(id_incident) from Incident left join Trafic on Incident.id_trafic = Trafic.id_trafic left join Ligne on Trafic.id_ligne = Ligne.id_ligne  group by Ligne.id_ligne")
result_c = curseur.fetchall()
#%%d
curseur.execute("Select immatriculation,type_vehicule,avg(valeur) from Mesure left join Capteur on Mesure.id_capteur = Capteur.id_capteur left join Arret on Capteur.id_arret = Arret.id_arret left join Ligne on Arret.id_ligne = Ligne.id_ligne left join  Vehicule on Ligne.id_ligne = Vehicule.id_ligne where type_capteur = 'CO2' group by id_vehicule")
result_d = curseur.fetchall()
#%%e
curseur.execute("Select Quartier.nom,avg(valeur) from Mesure left join Capteur on Mesure.id_capteur = Capteur.id_capteur left join Arret on Capteur.id_arret = Arret.id_arret left join ArretQuartier on Arret.id_arret = ArretQuartier.id_arret left join Quartier on ArretQuartier.id_quartier = Quartier.id_quartier where type_capteur = 'Bruit'  group by Quartier.id_quartier ORDER BY avg(valeur) DESC LIMIT 5")

result_e = curseur.fetchall()
#%%f
curseur.execute("select nom_ligne from Ligne left join Trafic on Ligne.id_ligne = Trafic.id_ligne left join Incident on Trafic.id_trafic = Incident.id_trafic where retard_minutes > 10 and id_incident is null")

result_f = curseur.fetchall()
#%%g
curseur.execute("select (count(case when retard_minutes = 0 then 1 end) * 1.0 / count(*)) as taux_sans_retard from trafic ")

result_g = curseur.fetchall()
#%%h
curseur.execute("select Quartier.nom,count(id_arret) from Quartier left join ArretQuartier on Quartier.id_quartier = ArretQuartier.id_quartier group by Quartier.id_quartier ")

result_h = curseur.fetchall()  
#%%i pas sur de moi
curseur.execute("select nom_ligne, (SUM(valeur * retard_minutes) - (SUM(valeur) * SUM(retard_minutes)) / COUNT(*))/SQRT((SUM(valeur * valeur) - (SUM(valeur) * SUM(valeur)) / COUNT(*)) * (SUM(retard_minutes * retard_minutes) - (SUM(retard_minutes) * SUM(retard_minutes)) / COUNT(*))) from Mesure left join Capteur on Mesure.id_capteur = Capteur.id_capteur left join Arret on Capteur.id_arret = Arret.id_arret left join Ligne on Arret.id_ligne = Ligne.id_ligne left join Trafic on Ligne.id_ligne = Trafic.id_ligne where type_capteur = 'CO2' group by Ligne.id_ligne ")

result_i = curseur.fetchall() 
#%%j
curseur.execute("Select nom_ligne,avg(valeur) from Mesure left join Capteur on Mesure.id_capteur = Capteur.id_capteur left join Arret on Capteur.id_arret = Arret.id_arret left join Ligne on Arret.id_ligne = Ligne.id_ligne where type_capteur = 'Temperature' group by Ligne.id_ligne")

result_j = curseur.fetchall() 
#%%k
curseur.execute("select nom,avg(retard_minutes ) from Ligne left join Trafic on Ligne.id_ligne = Trafic.id_ligne left join Vehicule on Ligne.id_ligne=Vehicule.id_ligne left join Chauffeur on Vehicule.id_chauffeur = Chauffeur.id_chauffeur group by Chauffeur.id_chauffeur")

result_k = curseur.fetchall() 
#%%l
curseur.execute("select (count(case when type_vehicule = 'Electrique' then 1 end) * 1.0 / count(*)) as taux_sans_retard from vehicule ")

result_l = curseur.fetchall() 
#%%m 
curseur.execute("select valeur, case when valeur < 400 then 'faible' when valeur < 500 then 'moyen' else 'élevé' end as niveau_pollution from Mesure left join Capteur on Mesure.id_capteur = Capteur.id_capteur where type_capteur = 'CO2';")

result_m = curseur.fetchall() 
#%%n
#classifiction retard moyen par ligne

curseur.execute("select nom_ligne,case when avg(retard_minutes )==0 then 'aucun retard' when avg(retard_minutes ) < 5 then 'retard moyen inf a 5min' else 'retard moyen sup a 5min' from Ligne left join Trafic on Ligne.id_ligne = Trafic.id_ligne  group by Ligne.id_ligne")

result_n = curseur.fetchall() 




