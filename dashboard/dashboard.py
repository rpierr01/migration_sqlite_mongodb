import dash
from dash import dcc, html, dash_table, Input, Output
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
    else:
        print(f"⚠️ Fichier introuvable : {path}")
    return pd.DataFrame()

# --- CONFIGURATION MONGODB ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Paris2055"

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    db = client[DB_NAME]
    client.server_info()
except:
    db = None
    print("⚠️ Attention : Impossible de se connecter à MongoDB.")

# =============================================================================
# PRÉ-CHARGEMENT ET OPTIMISATIONS
# =============================================================================

LINE_VEHICLE_MAP = {}
STOP_COUNTS_MAP = {}

if db is not None:
    # 1. Map Ligne -> Véhicule
    vehs = list(db.Vehicules.find({}, {"id_ligne": 1, "type_vehicule": 1}))
    for v in vehs:
        if v["id_ligne"] not in LINE_VEHICLE_MAP:
            LINE_VEHICLE_MAP[v["id_ligne"]] = v["type_vehicule"]

    # 2. Map Hubs (Correspondances)
    print("Pré-calcul des correspondances...")
    pipeline_global_count = [
        {"$group": {"_id": "$nom", "lignes_ids": {"$addToSet": "$id_ligne"}}},
        {"$project": {"nom": "$_id", "nb_lignes": {"$size": "$lignes_ids"}}}
    ]
    counts = list(db.Arrets.aggregate(pipeline_global_count))
    STOP_COUNTS_MAP = {c["nom"]: c["nb_lignes"] for c in counts}
    print("Terminé.")

def get_vehicle_options():
    if not LINE_VEHICLE_MAP: return []
    types = sorted(list(set(LINE_VEHICLE_MAP.values())))
    return [{"label": t, "value": t} for t in types]

# --- FONCTIONS DE RÉCUPÉRATION DES DONNÉES MONGODB ---
def get_liste_lignes():
    if db is None: return []
    try:
        lignes = list(db.Lignes.find({}, {"id_ligne": 1, "nom_ligne": 1, "_id": 0}).sort("nom_ligne", 1))
        return [{"label": l["nom_ligne"], "value": l["id_ligne"]} for l in lignes]
    except:
        return []

def get_filtered_data(id_ligne=None, vehicle_type=None, co2_level='all'):
    """Récupère les données filtrées pour la carte et le tableau"""
    if db is None: return pd.DataFrame()

    match_stage = {}
    
    # Filtres Ligne & Véhicule
    target_lignes = []
    if vehicle_type:
        target_lignes = [lid for lid, vtype in LINE_VEHICLE_MAP.items() if vtype == vehicle_type]
        if id_ligne:
            if id_ligne in target_lignes:
                match_stage["id_ligne"] = id_ligne
            else:
                return pd.DataFrame() 
        else:
            match_stage["id_ligne"] = {"$in": target_lignes}
    elif id_ligne:
        match_stage["id_ligne"] = id_ligne

    pipeline = [
        {"$match": match_stage},
        {"$project": {"nom": 1, "location": 1, "quartiers": 1, "capteurs": 1, "id_ligne": 1}}
    ]
    
    data = list(db.Arrets.aggregate(pipeline))
    if not data: return pd.DataFrame()

    formatted = []
    for d in data:
        co2, bruit, temp = 0, None, None
        
        if "capteurs" in d:
            for c in d["capteurs"]:
                mesures = c.get("mesures", [])
                if mesures:
                    avg = sum(m["valeur"] for m in mesures) / len(mesures)
                    ctype = c.get("type_capteur", "")
                    if "CO2" in ctype: co2 = avg
                    elif "Bruit" in ctype or "db" in ctype.lower(): bruit = avg
                    elif "Temp" in ctype: temp = avg
        
        # Filtre CO2 Python
        keep = True
        if co2_level != 'all' and co2_level is not None:
            if co2_level == "low" and co2 > 400: keep = False
            elif co2_level == "medium" and not (400 <= co2 <= 480): keep = False
            elif co2_level == "high" and co2 <= 480: keep = False
        
        if keep:
            quartier = d["quartiers"][0]["nom"] if d.get("quartiers") else "Inconnu"
            nb_lines = STOP_COUNTS_MAP.get(d.get("nom"), 1)
            v_type = LINE_VEHICLE_MAP.get(d["id_ligne"], "Inconnu")

            formatted.append({
                "Arrêt": d.get("nom", "Inconnu"),
                "Quartier": quartier,
                "Type Véhicule": v_type,
                "Nb Lignes": nb_lines,
                "Latitude": d["location"]["coordinates"][1],
                "Longitude": d["location"]["coordinates"][0],
                "CO2 (ppm)": round(co2, 1),
                "Bruit (dB)": round(bruit, 1) if bruit else None,
                "Temp (°C)": round(temp, 1) if temp else None
            })
        
    return pd.DataFrame(formatted)

def get_trend_for_stops(stop_names=None):
    """Récupère la tendance CO2 uniquement pour une liste d'arrêts donnée."""
    if db is None: return pd.DataFrame()
    
    match_stage = {}
    if stop_names is not None:
        match_stage = {"nom": {"$in": stop_names}}

    pipeline = [
        {"$match": match_stage},
        {"$unwind": "$capteurs"},
        {"$match": {"capteurs.type_capteur": "CO2"}},
        {"$unwind": "$capteurs.mesures"},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$capteurs.mesures.horodatage"}}, 
            "avg_co2": {"$avg": "$capteurs.mesures.valeur"}
        }},
        {"$sort": {"_id": 1}}
    ]
    data = list(db.Arrets.aggregate(pipeline))
    return pd.DataFrame(data).rename(columns={"_id": "Date", "avg_co2": "Moyenne CO2"})

def create_interactive_map(df_line):
    if df_line.empty:
        return folium.Map(location=[48.8566, 2.3522], zoom_start=12)._repr_html_()
    
    center_lat = df_line["Latitude"].mean()
    center_lon = df_line["Longitude"].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")
    
    marker_cluster = MarkerCluster(name="Arrêts").add_to(m)

    for _, row in df_line.iterrows():
        val_co2 = row['CO2 (ppm)']
        color = "green"
        if val_co2 > 400: color = "orange"
        if val_co2 > 480: color = "red"
        if val_co2 > 550: color = "darkred"
        
        icon_type = "exchange" if row['Nb Lignes'] > 1 else "bus"
        
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="margin: 0; color: #2c3e50;">{row['Arrêt']}</h4>
            <span style="font-size: 0.8em; color: gray;">{row['Quartier']}</span>
            <hr style="margin: 5px 0;">
            <b>🚍 Type :</b> {row['Type Véhicule']}<br>
            <b>🔢 Lignes :</b> {row['Nb Lignes']}<br>
            <b>🔊 Bruit :</b> {row['Bruit (dB)'] or 'N/A'} dB<br>
            <br>
            <span style="color: {color};"><b>🌫️ CO₂ : {val_co2} ppm</b></span>
        </div>
        """
        
        folium.Marker(
            location=[row['Latitude'], row['Longitude']],
            popup=folium.Popup(popup_html, max_width=250),
            icon=folium.Icon(color=color, icon=icon_type, prefix="fa"),
            tooltip=f"{row['Arrêt']} ({val_co2} ppm)"
        ).add_to(marker_cluster)
        
    return m._repr_html_()

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

def get_co2_by_quartier():
    """Récupère le niveau moyen de CO₂ par quartier depuis MongoDB"""
    if db is None: 
        return pd.DataFrame()
    
    pipeline = [
        {"$unwind": "$quartiers"},
        {"$unwind": "$capteurs"},
        {"$match": {"capteurs.type_capteur": {"$regex": "CO2", "$options": "i"}}},
        {"$unwind": "$capteurs.mesures"},
        {"$group": {
            "_id": {
                "id_quartier": "$quartiers.id_quartier",
                "nom_quartier": "$quartiers.nom"
            },
            "avg_co2": {"$avg": "$capteurs.mesures.valeur"}
        }},
        {"$project": {
            "_id": 0,
            "id_quartier": "$_id.id_quartier",
            "nom_quartier": "$_id.nom_quartier",
            "avg_co2": 1
        }},
        {"$sort": {"avg_co2": -1}}
    ]
    
    data = list(db.Arrets.aggregate(pipeline))
    return pd.DataFrame(data)

def get_quartiers_geojson():
    """Récupère les données GeoJSON des quartiers depuis MongoDB"""
    if db is None:
        return {
            "type": "FeatureCollection",
            "features": []
        }
    
    # Récupérer tous les quartiers depuis MongoDB
    quartiers = list(db.Quartiers.find({}))
    
    features = []
    for quartier in quartiers:
        try:
            features.append({
                "type": "Feature",
                "id": int(quartier['id_quartier']),
                "properties": {
                    "id_quartier": int(quartier['id_quartier']),
                    "nom": quartier['nom']
                },
                "geometry": quartier['geometry']
            })
        except Exception as e:
            print(f"Erreur pour quartier {quartier.get('id_quartier', '?')}: {e}")
            continue
    
    return {
        "type": "FeatureCollection",
        "features": features
    }

def create_choropleth_map():
    """Crée une carte choroplèthe du CO₂ par quartier"""
    m = folium.Map(
        location=[48.8566, 2.3522], 
        zoom_start=12,
        tiles="cartodbpositron"
    )
    
    # Récupérer les données
    df_co2 = get_co2_by_quartier()
    geojson_data = get_quartiers_geojson()
    
    if df_co2.empty or not geojson_data['features']:
        print("⚠️ Pas de données CO₂ ou pas de quartiers trouvés")
        return m._repr_html_()
    
    print(f"✓ {len(df_co2)} quartiers avec données CO₂")
    print(f"✓ {len(geojson_data['features'])} quartiers GeoJSON chargés depuis MongoDB")
    
    # Enrichir les propriétés avec les données CO₂
    co2_dict = dict(zip(df_co2['id_quartier'], df_co2['avg_co2']))
    nom_dict = dict(zip(df_co2['id_quartier'], df_co2['nom_quartier']))
    
    for feature in geojson_data['features']:
        id_q = feature['id']
        avg_co2 = co2_dict.get(id_q, 0)
        feature['properties']['avg_co2'] = round(avg_co2, 2)
        feature['properties']['nom_complet'] = nom_dict.get(id_q, feature['properties']['nom'])
    
    # Ajouter la couche choroplèthe
    folium.Choropleth(
        geo_data=geojson_data,
        name='Niveau de CO₂',
        data=df_co2,
        columns=['id_quartier', 'avg_co2'],
        key_on='feature.id',
        fill_color='RdYlGn_r',
        fill_opacity=0.7,
        line_opacity=0.8,
        line_color='#2c3e50',
        line_weight=2,
        legend_name='CO₂ moyen (ppm)',
        nan_fill_color='lightgray',
        nan_fill_opacity=0.3,
        highlight=True
    ).add_to(m)
    
    # Ajouter des tooltips interactifs
    style_function = lambda x: {
        'fillColor': '#ffffff00',
        'color': '#2c3e50',
        'weight': 1,
        'fillOpacity': 0
    }
    
    highlight_function = lambda x: {
        'fillColor': '#3498db',
        'color': '#2c3e50',
        'fillOpacity': 0.4,
        'weight': 3
    }
    
    tooltip = folium.GeoJsonTooltip(
        fields=['nom', 'avg_co2'],
        aliases=['<b>Quartier</b>:', '<b>CO₂ moyen</b>:'],
        style="""
            background-color: white; 
            color: #333333; 
            font-family: 'Segoe UI', Arial, sans-serif; 
            font-size: 13px; 
            padding: 12px;
            border: 2px solid #2c3e50;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        """,
        localize=True
    )
    
    folium.GeoJson(
        geojson_data,
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=tooltip,
        name='Info quartiers'
    ).add_to(m)
    
    folium.LayerControl(collapsed=False).add_to(m)
    
    return m._repr_html_()

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

# --- CHARGEMENT DES DONNÉES ---
print("📂 Chargement des fichiers CSV...")
df_a = load("mongo_requete_a.csv")
df_b = load("mongo_requete_b.csv")
df_c = load("mongo_requete_c.csv")
df_d = load("mongo_requete_d.csv")
df_e = load("mongo_requete_e.csv")
df_f = load("mongo_requete_f.csv")
df_g = load("mongo_requete_g.csv")
df_h = load("mongo_requete_h.csv")
df_i = load("mongo_requete_i.csv")
df_j = load("mongo_requete_j.csv")
df_k = load("mongo_requete_k.csv")
df_l = load("mongo_requete_l.csv")
df_m = load("mongo_requete_m.csv")
df_n = load("mongo_requete_n.csv")

print(f"✓ Chargement terminé")



# Construction Figure E
fig_e = px.bar(
    df_e, 
    x="quartier_nom", 
    y="avg_bruit", 
    title="Top 5 Quartiers les Plus Bruyants (Niveau Sonore Moyen)"
)
if not df_e.empty:
    e_min = df_e["avg_bruit"].min()
    e_max = df_e["avg_bruit"].max()
    fig_e.update_layout(yaxis=dict(range=[e_min * 0.98, e_max * 1.01]))



# ---  fig_j_temp (Top/Bottom Températures) ---
if not df_j.empty:
    # On trie les données
    df_sorted = df_j.sort_values("avg_temperature", ascending=False)
    
    # On prend les 10 plus chaudes et les 10 plus froides
    top_10 = df_sorted.head(10)
    bottom_10 = df_sorted.tail(10)
    
    # On combine les deux
    df_j_filtered = pd.concat([top_10, bottom_10])
    
    # On crée le graphique
    fig_j_temp = px.bar(
        df_j_filtered, 
        x="nom_ligne", 
        y="avg_temperature", 
        title="Température : Top 10 Chaudes vs 10 Froides", 
        color="avg_temperature", 
        color_continuous_scale="RdBu_r",
        text_auto='.1f' # Affiche la valeur sur la barre
    )
    
    # Zoomer sur l'échelle Y pour voir les différences
    y_min = df_j_filtered["avg_temperature"].min() * 0.99
    y_max = df_j_filtered["avg_temperature"].max() * 1.01
    fig_j_temp.update_layout(yaxis=dict(range=[y_min, y_max]))
    fig_j_temp.update_traces(textposition='outside')
else:
    fig_j_temp = px.bar(title="Pas de données Température")



# --- Autres Graphiques ---
# MODIFICATION: Échelle logarithmique pour les retards
df_a_subset = df_a.sort_values("avg_retard", ascending=False).head(15) if not df_a.empty else df_a
fig_a_retard = px.bar(
    df_a_subset, 
    x="nom_ligne", 
    y="avg_retard", 
    title="a. Retards Moyens par Ligne de Bus (Échelle Logarithmique)",
    log_y=True,
    text_auto='.1f'
)
if not df_a_subset.empty:
    fig_a_retard.update_traces(textposition='outside')

df_k_subset = df_k.sort_values("avg_retard_minutes").head(10) if not df_k.empty else df_k
fig_k_chauffeurs = px.bar(
    df_k_subset, 
    x="chauffeur_nom", 
    y="avg_retard_minutes", 
    title="Top 10 Chauffeurs avec les Retards Moyens les Plus Élevés"
)
if not df_k_subset.empty:
    k_min = df_k_subset["avg_retard_minutes"].min()
    k_max = df_k_subset["avg_retard_minutes"].max()
    fig_k_chauffeurs.update_layout(yaxis=dict(range=[k_min * 0.90, k_max * 1.05]))

fig_d_co2 = px.histogram(
    df_d, 
    x="avg_co2", 
    color="type_vehicule", 
    title="Répartition des Émissions de CO2 par Type de Véhicule", 
    barmode="overlay"
)
fig_n_pie = px.pie(
    df_n, 
    names="classification_retard", 
    title="Répartition des Retards par Classification"
)
fig_c_incidents = px.scatter(
    df_c, 
    x="nom_ligne", 
    y="incident_taux", 
    size="incident_taux", 
    title="Taux d'Incidents par Ligne de Bus"
)

if not df_b.empty:
    fig_b_passagers = px.line(
        df_b.groupby("jour")["avg_passagers"].mean().reset_index(), 
        x="jour", 
        y="avg_passagers", 
        title="Évolution Moyenne du Nombre de Passagers par Jour"
    )
else:
    fig_b_passagers = px.line(title="Pas de Données sur les Passagers Disponibles")

fig_l_elec = px.bar(
    df_l.sort_values("taux_electrique", ascending=False), 
    x="nom_ligne", 
    y="taux_electrique", 
    title="Taux de Véhicules Électriques par Ligne"
)

fig_i_corr = px.bar(
    df_i.sort_values("correlation"), 
    x="nom_ligne", 
    y="correlation", 
    title="Corrélation entre les Retards et les Lignes de Bus"
)

# --- LAYOUT DASH ---
app = dash.Dash(__name__)

line_options = get_liste_lignes()
vehicle_options = get_vehicle_options()
co2_options = [
    {'label': 'Tous les niveaux', 'value': 'all'},
    {'label': '🟢 Faible (< 400 ppm)', 'value': 'low'},
    {'label': '🟠 Moyen (400 - 480 ppm)', 'value': 'medium'},
    {'label': '🔴 Élevé (> 480 ppm)', 'value': 'high'}
]

# On génère l'HTML de la carte une seule fois au lancement
map_html = create_combined_map()
choropleth_html = create_choropleth_map()
ponctualite = df_g.iloc[0,0]*100 if not df_g.empty else 0

app.layout = html.Div([
    html.H1("Paris 2055 - Dashboard Intégral (Folium & Dash)", style={'textAlign': 'center', 'margin': '30px'}),
    html.Div([
        html.H2(f"Taux de Ponctualité Global : {ponctualite:.2f}%", 
                style={'textAlign': 'center', 'color': 'white', 'backgroundColor': '#2c3e50', 'padding': '15px'})
    ], style={'margin': '20px'}),

    dcc.Tabs([
        # NOUVEL ONGLET: Supervision & Filtres
        dcc.Tab(label='🔍 Supervision & Filtres', children=[
            html.Div([
                # Filtres
                html.Div([
                    html.Div([html.Label("1. Choisir une Ligne"), dcc.Dropdown(id='line-selector', options=line_options, placeholder="Toutes les lignes...", clearable=True)], style={'width': '30%', 'display': 'inline-block'}),
                    html.Div([html.Label("2. Type de Véhicule"), dcc.Dropdown(id='vehicle-selector', options=vehicle_options, placeholder="Tous types...", clearable=True)], style={'width': '30%', 'display': 'inline-block', 'marginLeft': '2%'}),
                    html.Div([html.Label("3. Niveau de CO₂"), dcc.Dropdown(id='co2-selector', options=co2_options, value='all', clearable=False)], style={'width': '30%', 'display': 'inline-block', 'marginLeft': '2%'}),
                ], style={'padding': '15px', 'backgroundColor': '#ecf0f1', 'borderRadius': '5px', 'marginBottom': '20px'}),

                # HAUT : Carte + Pie Chart
                html.Div([
                    html.Div([
                        html.H4("Cartographie Temps Réel"),
                        html.Iframe(id='interactive-map', style={'width': '100%', 'height': '500px', 'border': '2px solid #ddd', 'borderRadius': '5px'})
                    ], style={'width': '68%', 'display': 'inline-block', 'verticalAlign': 'top'}),
                    
                    html.Div([
                        dcc.Graph(id='pie-vehicules', style={'height': '500px'})
                    ], style={'width': '30%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingLeft': '1%'})
                ]),

                # MILIEU : Tendance CO2
                html.Div([
                    html.Hr(),
                    dcc.Graph(id='co2-trend', style={'height': '350px'})
                ], style={'marginTop': '20px', 'marginBottom': '20px'}),

                # BAS : Tableau
                html.H4("📋 Détail des Arrêts Filtrés"),
                dash_table.DataTable(
                    id='filtered-table', page_size=10, 
                    style_header={'backgroundColor': '#2c3e50', 'color': 'white', 'fontWeight': 'bold'}, 
                    style_cell={'textAlign': 'center', 'fontFamily': 'Arial'},
                    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(248, 248, 248)'}]
                )
            ], style={'padding': '20px', 'backgroundColor': '#f9f9f9'})
        ]),
        
        dcc.Tab(label='🌍 Environnement', children=[
            html.Div([
                html.H3("Carte Choroplèthe - CO₂ Moyen par Quartier", style={'textAlign': 'center'}),
                html.Iframe(srcDoc=choropleth_html, style={'width': '100%', 'height': '600px', 'border': 'none'}),
                html.Hr(),
                html.H3("m. Carte de Chaleur Folium (Pollution CO2)", style={'textAlign': 'center'}),
                html.Iframe(srcDoc=map_html, style={'width': '100%', 'height': '600px', 'border': 'none'}),
                html.Div([
                    dcc.Graph(figure=fig_e, style={'width': '50%', 'display': 'inline-block'}),
                    dcc.Graph(figure=fig_j_temp, style={'width': '50%', 'display': 'inline-block'}),
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
                dcc.Graph(figure=fig_i_corr),
            ])
        ]),
    ])
])

# =============================================================================
# CALLBACKS
# =============================================================================
@app.callback(
    [Output('interactive-map', 'srcDoc'), 
     Output('filtered-table', 'data'), 
     Output('filtered-table', 'columns'),
     Output('pie-vehicules', 'figure'),
     Output('co2-trend', 'figure')],
    [Input('line-selector', 'value'), 
     Input('vehicle-selector', 'value'), 
     Input('co2-selector', 'value')]
)
def update_supervision(selected_line, selected_vehicle, selected_co2):
    df_filtered = get_filtered_data(id_ligne=selected_line, vehicle_type=selected_vehicle, co2_level=selected_co2)
    
    map_html = create_interactive_map(df_filtered)
    
    table_data, table_cols = [], []
    fig_pie = px.pie(title="Aucune donnée sélectionnée")
    fig_trend = px.line(title="Aucune donnée sélectionnée")
    
    if not df_filtered.empty:
        cols_to_show = ["Arrêt", "Quartier", "Type Véhicule", "Nb Lignes", "CO2 (ppm)", "Bruit (dB)"]
        table_cols = [{"name": i, "id": i} for i in cols_to_show if i in df_filtered.columns]
        table_data = df_filtered.to_dict('records')

        # Pie Chart Dynamique
        counts = df_filtered["Type Véhicule"].value_counts().reset_index()
        counts.columns = ["Type", "Nombre"]
        fig_pie = px.pie(counts, names="Type", values="Nombre", title="Répartition (Sur la sélection)", hole=0.4)
        
        # Tendance CO2 Dynamique
        target_stops = df_filtered["Arrêt"].tolist()
        if len(target_stops) > 2000:
             df_trend_filtered = get_trend_for_stops(None) 
             title_suffix = "(Tout le réseau)"
        else:
             df_trend_filtered = get_trend_for_stops(target_stops)
             title_suffix = "(Sélection filtrée)"

        if not df_trend_filtered.empty:
            fig_trend = px.line(df_trend_filtered, x="Date", y="Moyenne CO2", markers=True, title=f"Tendance Émissions CO₂ {title_suffix}")
            fig_trend.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    
    return map_html, table_data, table_cols, fig_pie, fig_trend

if __name__ == '__main__':
    app.run(debug=True, port=8051, use_reloader=False)