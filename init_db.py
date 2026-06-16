import os
import psycopg2

DB_URL = os.environ.get("DB_URL", "")

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS snapshots (
        ts                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        congestion_index    FLOAT,
        trams_muy_fluido    INT,
        trams_fluido        INT,
        trams_denso         INT,
        trams_muy_denso     INT,
        trams_congestion    INT,
        bikes_total         INT,
        bikes_electric      INT,
        slots_free          INT,
        no2_avg             FLOAT,
        pm10_avg            FLOAT,
        pm25_avg            FLOAT,
        o3_avg              FLOAT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trams_history (
        ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        tram_id     SMALLINT NOT NULL,
        estado      SMALLINT,
        prediccion  SMALLINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bicing_history (
        ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        station_id      INT NOT NULL,
        station_name    TEXT,
        lat             FLOAT,
        lon             FLOAT,
        bikes           SMALLINT,
        bikes_electric  SMALLINT,
        slots_free      SMALLINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS air_history (
        ts           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        station_id   TEXT NOT NULL,
        station_name TEXT,
        lat          FLOAT,
        lon          FLOAT,
        pollutant    TEXT,
        value        FLOAT,
        units        TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots (ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_trams_ts ON trams_history (ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_trams_id_ts ON trams_history (tram_id, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_bicing_ts ON bicing_history (ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_bicing_id_ts ON bicing_history (station_id, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_air_ts ON air_history (ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_air_pollutant ON air_history (pollutant, ts DESC)",
]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
for stmt in SCHEMA:
    cur.execute(stmt)
conn.commit()
cur.close()
conn.close()
print("Tablas e índices creados correctamente")
