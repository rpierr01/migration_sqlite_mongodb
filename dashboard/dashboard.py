import dash
from dash import dcc, html, dash_table
import plotly.express as px
import pandas as pd
import os
import folium
from folium.plugins import HeatMap, MarkerCluster
from pymongo import MongoClient

# --- CONFIGURATION DES CHEMINS ---
DATA_DIR = "requetes_mongodb/resultat_requetes_mongodb"

def load(file):
    path = os.path.join(DATA_DIR, file)
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

# --- CONFIGURATION MONGODB ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Paris2055"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --- FONCTIONS DE RÉCUPÉRATION DES DONNÉES MONGODB ---
def get_heatmap_data():
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

# --- CHARGEMENT DES DONNÉES ---
df_a = load("mongo_requete_a.csv")
df_b = load("mongo_requete_b.csv")
df_c = load("mongo_requete_c.csv")
df_d = load("mongo_requete_d.csv")
df_e = load("mongo_requete_e.csv") # Contient Top 5 ET Bottom 5
df_f = load("mongo_requete_f.csv")
df_g = load("mongo_requete_g.csv")
df_h = load("mongo_requete_h.csv")
df_i = load("mongo_requete_i.csv")
df_j = load("mongo_requete_j.csv")
df_k = load("mongo_requete_k.csv")
df_l = load("mongo_requete_l.csv")
df_m = load("mongo_requete_m.csv")
df_n = load("mongo_requete_n.csv")

# --- FONCTION POUR CRÉER LA CARTE FOLIUM ---
def create_combined_map():
    m = folium.Map(location=[48.8566, 2.3522], zoom_start=12, tiles="cartodbdark_matter")
    h_data = get_heatmap_data()
    if h_data:
        HeatMap(h_data, radius=15, blur=10, max_zoom=1, name="Densité Pollution").add_to(m)
    df_markers = get_arrets_full_details()
    if not df_markers.empty:
        cluster = MarkerCluster(name="Arrêts de bus").add_to(m)
        for _, row in df_markers.iterrows():
            color = "green"
            val_co2 = row['co2'] if pd.notnull(row['co2']) else 0
            if val_co2 > 400: color = "orange"
            if val_co2 > 500: color = "red"
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

# --- CRÉATION DES FIGURES (PLOTLY) ---

# --- CORRECTION GRAPHE E et J ---
# On vérifie si la colonne 'segment' existe (créée par la nouvelle requête Mongo)
if not df_e.empty and 'segment' in df_e.columns:
    # 1. Top 5 (Les plus bruyants)
    df_e_top = df_e[df_e['segment'] == 'top5'].sort_values("avg_valeur", ascending=False)
    
    # 2. Bottom 5 (Les MOINS bruyants)
    df_e_bot = df_e[df_e['segment'] == 'bottom5'].sort_values("avg_valeur", ascending=True)
else:
    # Fallback si l'utilisateur n'a pas relancé la requête Mongo
    # Cela va probablement afficher des données incorrectes mais évite le crash
    df_e_top = df_e.sort_values("avg_valeur", ascending=False).head(5)
    df_e_bot = df_e.sort_values("avg_valeur", ascending=True).head(5)

# Construction Figure E
fig_e_bruit = px.bar(df_e_top, x="quartier_nom", y="avg_valeur", title="e. Top 5 Quartiers bruyants (+)")
if not df_e_top.empty:
    e_min = df_e_top["avg_valeur"].min()
    e_max = df_e_top["avg_valeur"].max()
    fig_e_bruit.update_layout(yaxis=dict(range=[e_min * 0.98, e_max * 1.01]))

# Construction Figure J
fig_j_new = px.bar(df_e_bot, x="quartier_nom", y="avg_valeur", 
                   title="j. Top 5 Quartiers les MOINS bruyants (-)",
                   color_discrete_sequence=['#2ecc71']) 
if not df_e_bot.empty:
    j_min = df_e_bot["avg_valeur"].min()
    j_max = df_e_bot["avg_valeur"].max()
    fig_j_new.update_layout(yaxis=dict(range=[j_min * 0.98, j_max * 1.01]))

# --- Autres Graphiques ---
df_a_subset = df_a.sort_values("avg_retard", ascending=False).head(15) if not df_a.empty else df_a
fig_a_retard = px.bar(df_a_subset, x="nom_ligne", y="avg_retard", title="Retards par ligne")
if not df_a_subset.empty:
    a_min = df_a_subset["avg_retard"].min()
    a_max = df_a_subset["avg_retard"].max()
    fig_a_retard.update_layout(yaxis=dict(range=[a_min * 0.90, a_max * 1.05]))

df_k_subset = df_k.sort_values("avg_retard_minutes").head(10) if not df_k.empty else df_k
fig_k_chauffeurs = px.bar(df_k_subset, x="chauffeur_nom", y="avg_retard_minutes", title="Top Chauffeurs")
if not df_k_subset.empty:
    k_min = df_k_subset["avg_retard_minutes"].min()
    k_max = df_k_subset["avg_retard_minutes"].max()
    fig_k_chauffeurs.update_layout(yaxis=dict(range=[k_min * 0.90, k_max * 1.05]))

fig_d_co2 = px.histogram(df_d, x="avg_co2", color="type_vehicule", title="d. Émissions CO2", barmode="overlay")
fig_n_pie = px.pie(df_n, names="classification_retard", title="n. Fiabilité")
fig_c_incidents = px.scatter(df_c, x="nom_ligne", y="incident_taux", size="incident_taux")

if not df_b.empty:
    fig_b_passagers = px.line(df_b.groupby("jour")["avg_passagers"].mean().reset_index(), x="jour", y="avg_passagers")
else:
    fig_b_passagers = px.line(title="Pas de données passagers")

fig_l_elec = px.bar(df_l.sort_values("taux_electrique", ascending=False), x="nom_ligne", y="taux_electrique")
fig_h_quartiers = px.treemap(df_h, path=['quartier_nom'], values='arret_count')
fig_i_corr = px.bar(df_i.sort_values("correlation"), x="id_ligne", y="correlation")

# --- LAYOUT DASH ---
<<<<<<< HEAD
app = dash.Dash(__name__)
=======
app = dash.Dash(__name__, assets_folder="assets")  # Specify the assets folder for CSS

# On génère l'HTML de la carte une seule fois au lancement
>>>>>>> 7a07fdd9ea58b36ad2fcb385e429fb5e2a0549b2
map_html = create_combined_map()
ponctualite = df_g.iloc[0,0]*100 if not df_g.empty else 0

app.layout = html.Div([
    html.H1("Paris 2055 - Dashboard Intégral (Folium & Dash)", style={'textAlign': 'center', 'margin': '30px'}),
    html.Div([
        html.H2(f"Taux de Ponctualité Global : {ponctualite:.2f}%", 
                style={'textAlign': 'center', 'color': 'white', 'backgroundColor': '#2c3e50', 'padding': '15px'})
    ], style={'margin': '20px'}),

    dcc.Tabs([
        dcc.Tab(label='🌍 Environnement', children=[
            html.Div([
                html.H3("m. Carte de Chaleur Folium (Pollution CO2)", style={'textAlign': 'center'}),
                html.Iframe(srcDoc=map_html, style={'width': '100%', 'height': '600px', 'border': 'none'}),
                html.Div([
                    dcc.Graph(figure=fig_e_bruit, style={'width': '50%', 'display': 'inline-block'}),
                    dcc.Graph(figure=fig_j_new, style={'width': '50%', 'display': 'inline-block'}),
                ]),
                dcc.Graph(figure=fig_d_co2)
            ])
        ]),
        dcc.Tab(label='⏱️ Performance Trafic', children=[
            html.Div([
                dcc.Graph(figure=fig_a_retard),
                html.Div([
                    dcc.Graph(figure=fig_n_pie, style={'width': '50%', 'display': 'inline-block'}),
                    dcc.Graph(figure=fig_c_incidents, style={'width': '50%', 'display': 'inline-block'}),
                ]),
                html.H3("f. Retards sans incidents", style={'textAlign': 'center'}),
                dash_table.DataTable(
                    data=df_f.to_dict('records') if not df_f.empty else [],
                    columns=[{"name": "Lignes Concernées", "id": "nom_ligne"}],
                    page_size=10,
                    style_cell={'textAlign': 'center'}
                )
            ])
        ]),
        dcc.Tab(label='🚍 Exploitation & Flotte', children=[
            html.Div([
                dcc.Graph(figure=fig_b_passagers),
                dcc.Graph(figure=fig_l_elec),
                dcc.Graph(figure=fig_k_chauffeurs),
            ])
        ]),
        dcc.Tab(label='📊 Analyses Avancées', children=[
            html.Div([
                dcc.Graph(figure=fig_h_quartiers),
                dcc.Graph(figure=fig_i_corr),
            ])
        ]),
    ])
])

if __name__ == '__main__':
    app.run(debug=True)