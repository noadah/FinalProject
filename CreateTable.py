from sqlalchemy import create_engine, text

# === DB CONFIG ===
DB_USER = "postgres"
DB_PASS = "BGUCO2"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Co2ProjectDB"
TABLE_NAME = "co2_readings"

# === Connect to DB ===
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# === Create Table SQL ===
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    "Time" TIMESTAMP,
    "Batt_V" FLOAT,
    "BluesVoltage" FLOAT,
    "CO2SCD30A" FLOAT,
    "CO2SCD30B" FLOAT,
    "CO2SCD30C" FLOAT,
    "CO2SCD30D" FLOAT,
    "CO2SCD30E" FLOAT,
    "CO2SCD30F" FLOAT,
    "RHSCD30A" FLOAT,
    "RHSCD30B" FLOAT,
    "RHSCD30C" FLOAT,
    "RHSCD30D" FLOAT,
    "RHSCD30E" FLOAT,
    "RHSCD30F" FLOAT,
    "RTCtemp" FLOAT,
    "TemperatureSCD30A" FLOAT,
    "TemperatureSCD30B" FLOAT,
    "TemperatureSCD30C" FLOAT,
    "TemperatureSCD30D" FLOAT,
    "TemperatureSCD30E" FLOAT,
    "TemperatureSCD30F" FLOAT,
    "ms8607P" FLOAT,
    "ms8607RH" FLOAT,
    "ms8607T" FLOAT,
    "TDR_RAW" FLOAT,
    "TDR_degC" FLOAT,
    "TDR_bulkEC" FLOAT,
    "station" TEXT
);
"""

# === Run the CREATE TABLE ===
with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    print(f"[✓] Table '{TABLE_NAME}' created successfully (or already exists).")
