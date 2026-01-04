import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster, HeatMap
from pymongo import MongoClient

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
st.set_page_config(page_title="Dashboard Paris 2055", page_icon="🚌", layout="wide")

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "Paris2055"

@st.cache_resource
def init_connection():
    try:
        client = MongoClient(MONGO_URI)
        client.server_info() # Test de connexion
        return client
    except Exception as e:
        st.error(f"❌ Erreur connexion MongoDB: {e}")
        return None

client = init_connection()
# CORRECTION IMPORTANTE : Gestion propre de l'objet db
db = client[DB_NAME] if client else None

# =============================================================================
# 2. CHARGEMENT DES DONNÉES
# =============================================================================

@st.cache_data(ttl=600)
def get_global_kpi():
    """Récupère les chiffres clés"""
    if db is None: return 0, 0, 0
    
    nb_lignes = db.Lignes.count_documents({})
    nb_arrets = db.Arrets.count_documents({})
    
    # Retard moyen
    res_retard = list(db.Trafic.aggregate([
        {"$group": {"_id": None, "avg": {"$avg": "$retard_minutes"}}}
    ]))
    avg_delay = res_retard[0]["avg"] if res_retard else 0
    
    return nb_lignes, nb_arrets, avg_delay

@st.cache_data(ttl=600)
def get_lignes_list():
    """Liste simple pour le menu déroulant"""
    if db is None: return pd.DataFrame()
    return pd.DataFrame(list(db.Lignes.find({}, {"id_ligne": 1, "nom_ligne": 1, "_id": 0})))

@st.cache_data(ttl=600)
def get_heatmap_data():
    """
    Récupère uniquement les points (lat, lon, valeur_co2) pour la Heatmap.
    Ne dépend PAS de la collection Quartiers.
    """
    if db is None: return []

    pipeline = [
        # On ne garde que les arrêts avec une position
        {"$match": {"location": {"$ne": None}}},
        {"$unwind": "$capteurs"},
        # On ne garde que les capteurs de pollution/CO2
        {"$match": {"capteurs.type_capteur": {"$regex": "CO2|Pollution", "$options": "i"}}},
        {"$unwind": "$capteurs.mesures"},
        # On groupe par arrêt pour avoir une moyenne par point géographique
        {"$group": {
            "_id": "$_id",
            "lat": {"$first": {"$arrayElemAt": ["$location.coordinates", 1]}},
            "lon": {"$first": {"$arrayElemAt": ["$location.coordinates", 0]}},
            "valeur": {"$avg": "$capteurs.mesures.valeur"}
        }}
    ]
    
    data = list(db.Arrets.aggregate(pipeline))
    
    # Format attendu par Folium HeatMap : [lat, lon, weight]
    heatmap_points = [[d['lat'], d['lon'], d['valeur']] for d in data if d['valeur'] > 0]
    return heatmap_points

def get_arrets_data(selected_ligne=None):
    """Récupère les arrêts pour les marqueurs (filtré par ligne)"""
    if db is None: return pd.DataFrame()
    
    query = {}
    if selected_ligne:
        query["id_ligne"] = selected_ligne
        
    projection = {"_id": 0, "nom": 1, "location": 1, "id_ligne": 1, "capteurs": 1}
    cursor = db.Arrets.find(query, projection)
    
    data = []
    for arret in cursor:
        if "location" not in arret or not arret["location"]:
            continue

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
        
        data.append({
            "nom": arret.get("nom", "Inconnu"),
            "lat": arret["location"]["coordinates"][1],
            "lon": arret["location"]["coordinates"][0],
            "ligne": arret.get("id_ligne"),
            "co2": co2,
            "bruit": bruit,
            "temp": temp
        })
    return pd.DataFrame(data)

# =============================================================================
# 3. INTERFACE STREAMLIT
# =============================================================================

# Sidebar
st.sidebar.title("🎛️ Filtres")

if db is not None:
    df_lignes = get_lignes_list()
    choix_ligne = st.sidebar.selectbox(
        "Filtrer par Ligne", 
        [None] + df_lignes["id_ligne"].tolist() if not df_lignes.empty else [None], 
        format_func=lambda x: f"Ligne {x}" if x else "Toutes les lignes"
    )
else:
    choix_ligne = None
    st.sidebar.warning("Base de données non connectée")

# Header KPI
nb_l, nb_a, avg_d = get_global_kpi()
c1, c2, c3 = st.columns(3)
c1.metric("🚌 Lignes Totales", nb_l)
c2.metric("🚏 Arrêts Totaux", nb_a)
c3.metric("⏱️ Retard Moyen", f"{avg_d:.1f} min")

st.markdown("---")

# Carte
st.subheader("🗺️ Carte de Chaleur (Heatmap) : Pollution CO2")

# 1. Carte de base (Dark theme pour mieux voir la heatmap)
m = folium.Map(location=[48.8566, 2.3522], zoom_start=12, tiles="cartodbdark_matter")

# 2. COUCHE HEATMAP (Remplaçant les quartiers)
heat_data = get_heatmap_data()
if heat_data:
    HeatMap(
        heat_data,
        radius=15, 
        blur=10, 
        max_zoom=1,
        name="Densité Pollution"
    ).add_to(m)
else:
    st.info("Aucune donnée de pollution suffisante pour générer la Heatmap.")

# 3. COUCHE MARQUEURS (Arrêts)
df_arrets = get_arrets_data(choix_ligne)
if not df_arrets.empty:
    cluster = MarkerCluster(name="Arrêts de bus").add_to(m)
    
    for _, row in df_arrets.iterrows():
        # Couleur dynamique
        color = "green"
        val_co2 = row['co2'] if row['co2'] else 0
        if val_co2 > 400: color = "orange"
        if val_co2 > 500: color = "red"
        
        popup_html = f"""
        <div style="font-family: sans-serif; font-size: 12px;">
            <b>{row['nom']}</b> (Ligne {row['ligne']})<br>
            🌫️ CO2: <b>{f"{row['co2']:.1f}" if row['co2'] else 'N/A'}</b><br>
            🔊 Bruit: {f"{row['bruit']:.1f}" if row['bruit'] else 'N/A'} dB<br>
            🌡️ Temp: {f"{row['temp']:.1f}" if row['temp'] else 'N/A'} °C
        </div>
        """
        
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_html, max_width=200),
            icon=folium.Icon(color=color, icon="bus", prefix="fa")
        ).add_to(cluster)

# Affichage final
folium.LayerControl().add_to(m)
st_folium(m, width="100%", height=600)