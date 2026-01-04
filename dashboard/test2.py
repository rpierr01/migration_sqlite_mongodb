import dash
from dash import dcc, html, dash_table
import plotly.express as px
import pandas as pd
import os
import folium
from folium.plugins import HeatMap, MarkerCluster
from pymongo import MongoClient

# --- CONFIGURATION MONGODB ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Paris2055"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --- CHARGEMENT DES DONNÉES CSV (Pour les graphiques) ---
DATA_DIR = "requetes_mongodb/resultat_requetes_mongodb"

def load_csv(file):
    path = os.path.join(DATA_DIR, file)
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

# Chargement des DataFrames pour les onglets analytics
df_a = load_csv("mongo_requete_a.csv")
df_b = load_csv("mongo_requete_b.csv")
df_e = load_csv("mongo_requete_e.csv")
df_f = load_csv("mongo_requete_f.csv")
df_g = load_csv("mongo_requete_g.csv")
df_n = load_csv("mongo_requete_n.csv")

# --- FONCTIONS DE RÉCUPÉRATION INSPIRÉES DU CODE STREAMLIT ---

def get_heatmap_data():
    """Récupère les points [lat, lon, valeur] pour la Heatmap"""
    if db is None: return []
    pipeline = [
        {"$match": {"location": {"$ne": None}}},
        {"$unwind": "$capteurs"},
        {"$match": {"capteurs.type_capteur": {"$regex": "CO2|Pollution", "$options": "i"}}},
        {"$unwind": "$capteurs.mesures"},
        {"$group": {
            "_id": "$_id",
            "lat": {"$first": {"$arrayElemAt": ["$location.coordinates", 1]}},
            "lon": {"$first": {"$arrayElemAt": ["$location.coordinates", 0]}},
            "valeur": {"$avg": "$capteurs.mesures.valeur"}
        }}
    ]
    data = list(db.Arrets.aggregate(pipeline))
    return [[d['lat'], d['lon'], d['valeur']] for d in data if d['valeur'] > 0]

def get_arrets_full_details():
    """Récupère les détails complets des arrêts (Nom, CO2, Bruit, Temp)"""
    if db is None: return pd.DataFrame()
    
    cursor = db.Arrets.find({"location": {"$ne": None}})
    data = []
    for arret in cursor:
        co2, bruit, temp = None, None, None
        if "capteurs" in arret:
            for c in arret["capteurs"]:
                ctype = c.get("type_capteur", "")
                measures = c.get("mesures", [])
                if measures:
                    avg = sum(m["valeur"] for m in measures) / len(measures)
                    if "CO2" in ctype: co2 = avg
                    elif "Bruit" in ctype: bruit = avg
                    elif "Temp" in ctype: temp = avg
        
        # Correction : On teste 'nom_arret' PUIS 'nom' pour éviter le "Inconnu"
        nom_final = arret.get("nom_arret") or arret.get("nom") or "Arrêt sans nom"
        
        data.append({
            "nom": nom_final,
            "lat": arret["location"]["coordinates"][1],
            "lon": arret["location"]["coordinates"][0],
            "ligne": arret.get("id_ligne", "N/A"),
            "co2": co2,
            "bruit": bruit,
            "temp": temp
        })
    return pd.DataFrame(data)

# --- CRÉATION DE LA CARTE ---

def create_combined_map():
    m = folium.Map(location=[48.8566, 2.3522], zoom_start=12, tiles="cartodbdark_matter")

    # 1. Heatmap (Pollution)
    h_data = get_heatmap_data()
    if h_data:
        HeatMap(h_data, radius=15, blur=10, max_zoom=1, name="Densité Pollution").add_to(m)

    # 2. Marqueurs avec détails (comme Streamlit)
    df_markers = get_arrets_full_details()
    if not df_markers.empty:
        cluster = MarkerCluster(name="Arrêts de bus").add_to(m)
        for _, row in df_markers.iterrows():
            # Couleur dynamique selon CO2
            color = "green"
            val_co2 = row['co2'] if pd.notnull(row['co2']) else 0
            if val_co2 > 400: color = "orange"
            if val_co2 > 500: color = "red"
            
            # Popup HTML riche
            popup_content = f"""
            <div style='font-family: Arial; font-size: 12px; width: 160px;'>
                <b>{row['nom']}</b><br>
                Ligne: {row['ligne']}<br><hr>
                🌫️ CO2: {f"{row['co2']:.1f}" if pd.notnull(row['co2']) else 'N/A'}<br>
                🔊 Bruit: {f"{row['bruit']:.1f}" if pd.notnull(row['bruit']) else 'N/A'} dB<br>
                🌡️ Temp: {f"{row['temp']:.1f}" if pd.notnull(row['temp']) else 'N/A'} °C
            </div>
            """
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_content, max_width=200),
                icon=folium.Icon(color=color, icon="bus", prefix="fa")
            ).add_to(cluster)

    folium.LayerControl().add_to(m)
    return m._repr_html_()

# --- LAYOUT DASH ---

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Paris 2055 - Dashboard Expert (Dash + MongoDB)", 
            style={'textAlign': 'center', 'color': 'white', 'backgroundColor': '#1a1a1a', 'padding': '20px', 'margin': '0'}),

    dcc.Tabs([
        dcc.Tab(label='🗺️ Carte Environnementale', children=[
            html.Iframe(srcDoc=create_combined_map(), style={'width': '100%', 'height': '700px', 'border': 'none'})
        ]),
        
        dcc.Tab(label='📊 Statistiques Réseau', children=[
            html.Div([
                dcc.Graph(figure=px.bar(df_a.sort_values("avg_retard").head(10), x="nom_ligne", y="avg_retard", title="Top 10 Ponctualité")),
                dcc.Graph(figure=px.pie(df_n, names="classification_retard", title="Répartition de la Fiabilité"))
            ], style={'padding': '20px'})
        ]),
    ])
])

if __name__ == '__main__':
    app.run(debug=True, port=8050)