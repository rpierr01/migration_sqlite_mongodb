"""
Script pour télécharger les vrais contours géographiques des quartiers de Paris
depuis les données ouvertes de la Ville de Paris
"""

import requests
import json

# URL des données ouvertes de Paris (quartiers administratifs)
# Alternative : arrondissements si quartiers pas disponibles
PARIS_QUARTIERS_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/quartier_paris/exports/geojson"

def download_paris_geojson():
    """Télécharge les contours réels des quartiers de Paris"""
    print("📥 Téléchargement des contours géographiques de Paris...")
    
    try:
        response = requests.get(PARIS_QUARTIERS_URL, timeout=30)
        response.raise_for_status()
        
        geojson_data = response.json()
        
        # Sauvegarder localement
        output_path = "data/paris_quartiers_real.geojson"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Données sauvegardées dans {output_path}")
        print(f"📊 Nombre de quartiers : {len(geojson_data.get('features', []))}")
        
        return geojson_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur de téléchargement : {e}")
        print("\n💡 Alternative : Utiliser les arrondissements de Paris")
        print("URL : https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/arrondissements/exports/geojson")
        return None

if __name__ == "__main__":
    download_paris_geojson()
