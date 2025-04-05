import pandas as pd
import requests
from io import StringIO
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# === SETUP LOGGING ===
logging.basicConfig(
    filename="ingestion_log.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# === DB CONFIG ===
DB_USER = "postgres"
DB_PASS = "BGUCO2"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Co2ProjectDB"
TABLE_NAME = "co2_readings"

# === Station URLs ===
station_urls = {
    "Nizzana": "http://llvs:tmp.AceMaH@45.83.42.52:8080/data/il.ac.bgu.levintal_co2o2soils_860264050488908_readings.qo.csv",
    "Neot_Smadar": "http://llvs:tmp.AceMaH@45.83.42.52:8080/data/il.ac.bgu.levintal_co2o2soils_860264050706622_readings.qo.csv",
    "Avdat": "http://llvs:tmp.AceMaH@45.83.42.52:8080/data/il.ac.bgu.levintal_co2o2soils_860264050497271_readings.qo.csv",
    "Mashash": "http://llvs:tmp.AceMaH@45.83.42.52:8080/data/il.ac.bgu.levintal_co2o2soils_867648043598455_readings.qo.csv",
}

# === CONNECT TO DB ===
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_existing_timestamps():
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT DISTINCT \"Time\" FROM {TABLE_NAME}"))
            return set(row[0] for row in result.fetchall())
    except Exception as e:
        logging.warning(f"[!] Could not load existing timestamps: {e}")
        return set()

def fetch_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def process_and_insert(df, station_name, existing_timestamps):
    df['station'] = station_name
    df['Time'] = pd.to_datetime(df['Time'])  # ensure datetime format
    new_data = df[~df['Time'].isin(existing_timestamps)]
    
    if not new_data.empty:
        new_data.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
        logging.info(f"[✓] Inserted {len(new_data)} new rows for {station_name}")
    else:
        logging.info(f"[•] No new data for {station_name}")

def main():
    logging.info("=== Starting ingestion run ===")
    existing_timestamps = load_existing_timestamps()

    for station, url in station_urls.items():
        try:
            df = fetch_csv(url)
            process_and_insert(df, station, existing_timestamps)
        except Exception as e:
            logging.error(f"[✗] Failed for {station}: {e}")

    logging.info("=== Ingestion run complete ===")

if __name__ == "__main__":
    main()

import time

