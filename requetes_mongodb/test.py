import pandas as pd
from pymongo import MongoClient
import os
from datetime import datetime

# Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Paris2055"
EXPORT_DIR = "requetes_mongodb/resultat_requetes_mongodb"
os.makedirs(EXPORT_DIR, exist_ok=True)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def export_to_csv(cursor, filename, columns):
    """
    Export MongoDB cursor data to a CSV file with specified columns.
    Ensures consistent structure with SQLite exports.
    """
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df = df[columns]  # Ensure consistent column order
    df.to_csv(os.path.join(EXPORT_DIR, filename), index=False)
    print(f"✅ Exporté : {filename}")

# --- REQUÊTES ---

# a. Moyenne des retards par ligne
# On utilise la collection Trafic car id_ligne y est présent.
res_a = db.Trafic.aggregate([
    {"$group": {"_id": "$id_ligne", "avg_retard": {"$avg": "$retard_minutes"}}},
    {"$lookup": {"from": "Lignes", "localField": "_id", "foreignField": "id_ligne", "as": "ligne_info"}},
    {"$unwind": "$ligne_info"},
    {"$project": {"nom_ligne": "$ligne_info.nom_ligne", "avg_retard": 1}}
])
export_to_csv(res_a, "mongo_requete_a.csv", ["nom_ligne", "avg_retard"])

# b. Nombre moyen de passagers par jour et par ligne
# Note: Dans votre migration, 'heure_prevue' est un objet datetime dans la collection 'Arrets.horaires'
res_b = db.Arrets.aggregate([
    {"$unwind": "$horaires"},
    {"$addFields": {
        "horaires.heure_prevue": {
            "$dateFromString": {
                "dateString": "$horaires.heure_prevue",
                "onError": None,  # Handle invalid date strings gracefully
                "onNull": None
            }
        }
    }},
    {"$group": {
        "_id": {
            "id_ligne": "$id_ligne", 
            "jour": {"$dateToString": {"format": "%Y-%m-%d", "date": "$horaires.heure_prevue"}}
        },
        "avg_passagers": {"$avg": "$horaires.passagers_estimes"}
    }},
    {"$lookup": {"from": "Lignes", "localField": "_id.id_ligne", "foreignField": "id_ligne", "as": "l"}},
    {"$unwind": "$l"},
    {"$project": {"nom_ligne": "$l.nom_ligne", "jour": "$_id.jour", "avg_passagers": 1}}
])
export_to_csv(res_b, "mongo_requete_b.csv", ["nom_ligne", "jour", "avg_passagers"])

# c. Taux d'incidents par ligne (Ratio Incidents / Trafic total)
res_c = db.Trafic.aggregate([
    {"$group": {
        "_id": "$id_ligne",
        "total_releves": {"$sum": 1},
        "total_incidents": {"$sum": {"$size": "$incidents"}}
    }},
    {"$project": {
        "id_ligne": "$_id",
        "incident_taux": {"$divide": ["$total_incidents", "$total_releves"]}
    }},
    {"$lookup": {
        "from": "Lignes", 
        "localField": "id_ligne", 
        "foreignField": "id_ligne", 
        "as": "l"
    }},
    {"$unwind": "$l"},
    {"$project": {
        "nom_ligne": "$l.nom_ligne", 
        "incident_taux": 1
    }},
    {"$sort": {"nom_ligne": 1}}
])
export_to_csv(res_c, "mongo_requete_c.csv", ["nom_ligne", "incident_taux"])

# d. Emissions moyennes de CO2 par véhicule
res_d = db.Vehicules.aggregate([
    {"$lookup": {
        "from": "Arrets",
        "localField": "id_ligne",
        "foreignField": "id_ligne",
        "as": "arrets_ligne"
    }},
    {"$unwind": "$arrets_ligne"},
    {"$unwind": "$arrets_ligne.capteurs"},
    {"$match": {"arrets_ligne.capteurs.type_capteur": "CO2"}},
    {"$unwind": "$arrets_ligne.capteurs.mesures"},
    {"$group": {
        "_id": "$id_vehicule",
        "immatriculation": {"$first": "$immatriculation"},
        "type_vehicule": {"$first": "$type_vehicule"},
        "avg_co2": {"$avg": "$arrets_ligne.capteurs.mesures.valeur"}
    }}
])
export_to_csv(res_d, "mongo_requete_d.csv", ["immatriculation", "type_vehicule", "avg_co2"])

# e. Top 5 des quartiers les plus bruyants
res_e = db.Arrets.aggregate([
    {"$unwind": "$quartiers"},
    {"$unwind": "$capteurs"},
    {"$match": {"capteurs.type_capteur": "Bruit"}},
    {"$unwind": "$capteurs.mesures"},
    {"$group": {"_id": "$quartiers.nom", "avg_bruit": {"$avg": "$capteurs.mesures.valeur"}}},
    {"$sort": {"avg_bruit": -1}},
    {"$limit": 5},
    {"$project": {"quartier_nom": "$_id", "avg_bruit": 1}}  # Rename _id to quartier_nom
])
export_to_csv(res_e, "mongo_requete_e.csv", ["quartier_nom", "avg_bruit"])

# f. Liste des lignes sans incident mais avec retards > 10 min
res_f = db.Trafic.aggregate([
    {"$match": {"retard_minutes": {"$gt": 10}, "incidents": {"$size": 0}}},
    {"$lookup": {"from": "Lignes", "localField": "id_ligne", "foreignField": "id_ligne", "as": "l"}},
    {"$unwind": "$l"},
    {"$group": {"_id": "$l.nom_ligne"}},
    {"$project": {"nom_ligne": "$_id"}}
])
export_to_csv(res_f, "mongo_requete_f.csv", ["nom_ligne"])

# g. Taux de ponctualité global
total = db.Trafic.count_documents({})
sans_retard = db.Trafic.count_documents({"retard_minutes": 0})
taux = sans_retard / total if total > 0 else 0
pd.DataFrame([{"taux_sans_retard": taux}]).to_csv(
    os.path.join(EXPORT_DIR, "mongo_requete_g.csv"), index=False
)

# h. Nombre d'arrêts par quartier
res_h = db.Arrets.aggregate([
    {"$unwind": "$quartiers"},
    {"$group": {"_id": "$quartiers.nom", "arret_count": {"$sum": 1}}},
    {"$project": {"quartier_nom": "$_id", "arret_count": 1}}
])
export_to_csv(res_h, "mongo_requete_h.csv", ["quartier_nom", "arret_count"])

# i. Corrélation (CO2 vs Retard par ligne et jour)
data_i = db.Trafic.aggregate([
    {"$project": {"id_ligne": 1, "retard": "$retard_minutes", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$horodatage"}}}},
    {"$lookup": {
        "from": "Arrets",
        "let": {"l_id": "$id_ligne", "t_date": "$date"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$id_ligne", "$$l_id"]}}},
            {"$unwind": "$capteurs"},
            {"$match": {"capteurs.type_capteur": "CO2"}},
            {"$unwind": "$capteurs.mesures"},
            {"$match": {"$expr": {"$eq": [{"$dateToString": {"format": "%Y-%m-%d", "date": "$capteurs.mesures.horodatage"}}, "$$t_date"]}}}
        ],
        "as": "mesures_sync"
    }},
    {"$unwind": "$mesures_sync"},
    {"$group": {
        "_id": "$id_ligne",
        "valeurs": {"$push": "$mesures_sync.capteurs.mesures.valeur"},
        "retards": {"$push": "$retard"}
    }}
])
# Calcul de corrélation via Pandas
list_i = []
for doc in data_i:
    if len(doc['valeurs']) > 1:
        corr = pd.Series(doc['valeurs']).corr(pd.Series(doc['retards']))
        list_i.append({"id_ligne": doc['_id'], "correlation": corr})
pd.DataFrame(list_i, columns=["id_ligne", "correlation"]).to_csv(
    os.path.join(EXPORT_DIR, "mongo_requete_i.csv"), index=False
)

# j. Moyenne de température par ligne
res_j = db.Arrets.aggregate([
    {"$unwind": "$capteurs"},
    {"$match": {"capteurs.type_capteur": "Temperature"}},
    {"$unwind": "$capteurs.mesures"},
    {"$group": {"_id": "$id_ligne", "avg_temp": {"$avg": "$capteurs.mesures.valeur"}}},
    {"$lookup": {"from": "Lignes", "localField": "_id", "foreignField": "id_ligne", "as": "l"}},
    {"$unwind": "$l"},
    {"$project": {"nom_ligne": "$l.nom_ligne", "avg_temperature": "$avg_temp"}}
])
export_to_csv(res_j, "mongo_requete_j.csv", ["nom_ligne", "avg_temperature"])

# k. Performance chauffeur (retard moyen) - Correction du GroupBy
res_k = db.Vehicules.aggregate([
    {"$lookup": {
        "from": "Trafic",
        "localField": "id_ligne",
        "foreignField": "id_ligne",
        "as": "t"
    }},
    {"$unwind": "$t"},
    {"$group": {
        # On groupe par l'ID pour ne pas mélanger les homonymes
        "_id": "$chauffeur.id_chauffeur", 
        "nom": {"$first": "$chauffeur.nom"},
        "avg_retard": {"$avg": "$t.retard_minutes"}
    }},
    {"$project": {
        "chauffeur_nom": "$nom", 
        "avg_retard_minutes": "$avg_retard"
    }},
    {"$sort": {"chauffeur_nom": 1}}
])
export_to_csv(res_k, "mongo_requete_k.csv", ["chauffeur_nom", "avg_retard_minutes"])

# l. % véhicules électriques par ligne de bus
res_l = db.Lignes.aggregate([
    {"$match": {"type": "Bus"}},
    {"$lookup": {"from": "Vehicules", "localField": "id_ligne", "foreignField": "id_ligne", "as": "v"}},
    {"$project": {
        "nom_ligne": 1,
        "taux_electrique": {
            "$cond": [
                {"$gt": [{"$size": "$v"}, 0]},
                {"$divide": [
                    {"$size": {"$filter": {"input": "$v", "cond": {"$eq": ["$$this.type_vehicule", "Electrique"]}}}},
                    {"$size": "$v"}
                ]},
                0
            ]
        }
    }}
])
export_to_csv(res_l, "mongo_requete_l.csv", ["nom_ligne", "taux_electrique"])

# m. Classification pollution avec localisation (Correction Latitude/Longitude)
res_m = db.Arrets.aggregate([
    {"$unwind": "$capteurs"},
    {"$match": {"capteurs.type_capteur": "CO2"}},
    {"$unwind": "$capteurs.mesures"},
    {"$project": {
        "id_capteur": "$capteurs.id_capteur",
        # Accès correct aux coordonnées GeoJSON [longitude, latitude]
        "latitude": {"$arrayElemAt": ["$capteurs.location.coordinates", 1]},
        "longitude": {"$arrayElemAt": ["$capteurs.location.coordinates", 0]},
        "valeur": "$capteurs.mesures.valeur",
        "niveau_pollution": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$capteurs.mesures.valeur", 400]}, "then": "faible"},
                    {"case": {"$lt": ["$capteurs.mesures.valeur", 500]}, "then": "moyen"}
                ],
                "default": "élevé"
            }
        }
    }}
])
export_to_csv(res_m, "mongo_requete_m.csv", ["id_capteur", "latitude", "longitude", "valeur", "niveau_pollution"])

# n. Classification des lignes par retard moyen
res_n = db.Trafic.aggregate([
    {"$group": {"_id": "$id_ligne", "avg_r": {"$avg": "$retard_minutes"}}},
    {"$lookup": {"from": "Lignes", "localField": "_id", "foreignField": "id_ligne", "as": "l"}},
    {"$unwind": "$l"},
    {"$project": {
        "nom_ligne": "$l.nom_ligne",
        "classification_retard": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$avg_r", 0]}, "then": "aucun retard"},
                    {"case": {"$lt": ["$avg_r", 5]}, "then": "retard moyen < 5min"}
                ],
                "default": "retard moyen > 5min"
            }
        }
    }}
])
export_to_csv(res_n, "mongo_requete_n.csv", ["nom_ligne", "classification_retard"])

client.close()