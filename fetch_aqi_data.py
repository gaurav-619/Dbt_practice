import requests
from datetime import datetime
import time
from google.cloud import bigquery

API_KEY = "d3690f5c07a35b0b9ef306266d4f3e03"  # Replace securely in prod
PROJECT_ID = "dbt-crop"
DATASET = "crop_weather_ds"
TABLE = "live_aqi_data"
BQ_TABLE_ID = "dbt-crop.crop_weather_ds.live_aqi_data"

def fetch_aqi_to_bq():
    state_city_coords = {
        "Delhi": (28.61, 77.20),
        "Maharashtra": (19.07, 72.87),
        "West Bengal": (22.57, 88.36),
        "Karnataka": (12.97, 77.59),
        "Tamil Nadu": (13.08, 80.27),
        "Telangana": (17.39, 78.49),
        "Uttar Pradesh": (26.85, 80.95),
        "Bihar": (25.60, 85.13),
        "Punjab": (30.91, 75.85),
        "Haryana": (28.47, 77.03),
        "Odisha": (20.27, 85.84),
        "Assam": (26.14, 91.76),
        "Madhya Pradesh": (23.25, 77.41),
        "Rajasthan": (26.91, 75.79),
        "Kerala": (8.52, 76.93),
        "Chhattisgarh": (21.25, 81.63),
        "Uttarakhand": (30.32, 78.03),
        "Jharkhand": (23.34, 85.31),
    }

    BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"
    rows = []

    for state, (lat, lon) in state_city_coords.items():
        try:
            res = requests.get(BASE_URL, params={
                "lat": lat, "lon": lon, "appid": API_KEY
            })
            res.raise_for_status()
            pollution = res.json()["list"][0]
            components = pollution["components"]
            rows.append({
                "State": state,
                "Latitude": lat,
                "Longitude": lon,
                "Timestamp": datetime.utcfromtimestamp(pollution["dt"]).isoformat(),  # ✅ F
                "PM2_5": components.get("pm2_5"),
                "PM10": components.get("pm10"),
                "NO2": components.get("no2"),
                "CO": components.get("co"),
                "O3": components.get("o3"),
                "SO2": components.get("so2"),
            })
        except Exception as e:
            print(f"❌ Error fetching for {state}: {e}")
        time.sleep(1)

    client = bigquery.Client()
    errors = client.insert_rows_json(BQ_TABLE_ID, rows)
    if errors:
        print(f"❌ BigQuery insert errors: {errors}")
    else:
        print("✅ Successfully inserted AQI data to BigQuery")
