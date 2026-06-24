import asyncio
import csv
import io
import json
import math
import os
import re
import random
from datetime import datetime, timezone
from typing import Optional

import httpx
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

DB_URL = os.environ.get("DB_URL", "")
MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN", "")
OPENSKY_USER = os.environ.get("OPENSKY_USER", "")
OPENSKY_PASS = os.environ.get("OPENSKY_PASS", "")
OPENSKY_CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

_opensky_token: str = ""
_opensky_token_expiry: float = 0.0

async def _get_opensky_token() -> str:
    """Obté o reutilitza el token OAuth2 d'OpenSky (expira cada 30min)."""
    import time
    global _opensky_token, _opensky_token_expiry
    if _opensky_token and time.time() < _opensky_token_expiry - 60:
        return _opensky_token
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(OPENSKY_TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": OPENSKY_CLIENT_ID,
            "client_secret": OPENSKY_CLIENT_SECRET,
        })
    if r.status_code != 200:
        print(f"[opensky-auth] error {r.status_code}: {r.text[:200]}")
        return ""
    d = r.json()
    _opensky_token = d.get("access_token", "")
    _opensky_token_expiry = time.time() + d.get("expires_in", 1800)
    print("[opensky-auth] token renovat OK")
    return _opensky_token
OCM_KEY = os.environ.get("OCM_KEY", "")

app = FastAPI(title="BCN Live Data")

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TMB_APP_ID = "9c8d3aaa"
TMB_APP_KEY = "fb5e81b620cfa5bb089fc9acc005cc93"
TMB_INT_APP_ID = "4c132798"
TMB_INT_APP_KEY = "a828910cef5a0376607986191db19d14"

# ── Cache en memoria ──────────────────────────────────────────────────────────
cache = {
    "trams": {"data": [], "updated": None},
    "bicing": {"data": [], "updated": None},
    "air": {"data": [], "updated": None},
    "incidents": {"data": "", "updated": None},
    "arbres": {"data": [], "updated": None},
    "accidents": {"data": [], "updated": None},
    "poblacio": {"data": None, "updated": None},
    "zones_verdes": {"data": None, "updated": None},
    "carrega": {"data": [], "updated": None},
    "fonts": {"data": None, "updated": None},
    "desfibril·ladors": {"data": None, "updated": None},
    "lavabos": {"data": None, "updated": None},
    "equipaments": {"data": None, "updated": None},
    "mercats": {"data": None, "updated": None},
    "airbnb": {"data": None, "updated": None},
    "camaras_live_snapshots": {},  # cam_id -> current image URL
}

# ── Càmeres Live ──────────────────────────────────────────────────────────────
CAMARAS_LIVE = [
    {
        "id": "bsm_llevant",
        "nom": "Port Olímpic — Marina Llevant",
        "lat": 41.3898, "lon": 2.2025,
        "tipus": "hls",
        "url": "/api/hls/llevant/index.m3u8",
        "font": "BSM",
    },
    {
        "id": "bsm_garbi",
        "nom": "Port Olímpic — Garbí (Hotel Arts)",
        "lat": 41.3876, "lon": 2.1978,
        "tipus": "hls",
        "url": "/api/hls/garbi/index.m3u8",
        "font": "BSM",
    },
    {
        "id": "3cat_portolimpic",
        "nom": "Port Olímpic (Panoràmica)",
        "lat": 41.3910, "lon": 2.2020,
        "tipus": "snapshot",
        "scrape_url": "https://www.3cat.cat/el-temps/port-olimpic-barcelona/camera/1/",
        "font": "3Cat Meteo",
    },
    {
        "id": "3cat_collserola",
        "nom": "Collserola (Vista aèria BCN)",
        "lat": 41.4178, "lon": 2.1195,
        "tipus": "snapshot",
        "scrape_url": "https://www.3cat.cat/el-temps/collserola-barcelona/camera/2/",
        "font": "3Cat Meteo",
    },
    {
        "id": "3cat_rambla",
        "nom": "La Rambla",
        "lat": 41.3827, "lon": 2.1736,
        "tipus": "snapshot",
        "scrape_url": "https://www.3cat.cat/el-temps/la-rambla-barcelona/camera/60/",
        "font": "3Cat Meteo",
    },
    {
        "id": "3cat_artssantamonica",
        "nom": "Arts Santa Mònica (Colom)",
        "lat": 41.3789, "lon": 2.1742,
        "tipus": "snapshot",
        "scrape_url": "https://www.3cat.cat/el-temps/arts-santa-monica-barcelona/camera/53/",
        "font": "3Cat Meteo",
    },
    {
        "id": "ipcam_sagradafamilia",
        "nom": "Sagrada Família",
        "lat": 41.4036, "lon": 2.1744,
        "tipus": "iframe",
        "url": "https://g2.ipcamlive.com/player/player.php?alias=sagradafamilia",
        "font": "ipcamlive",
    },
]


async def poll_3cat_snapshots():
    """Actualitza les URLs de les captures 3Cat cada 10 minuts."""
    import re as _re
    snapshots = cache["camaras_live_snapshots"]
    for cam in CAMARAS_LIVE:
        if cam["tipus"] != "snapshot":
            continue
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False) as client:
                r = await client.get(cam["scrape_url"], headers={"User-Agent": "Mozilla/5.0"})
            m = _re.search(r"beauties/v1/img/([0-9/]+\.jpg)", r.text)
            if m:
                snapshots[cam["id"]] = f"https://statics.3cat.cat/meteo/{m.group()}"
        except Exception as e:
            print(f"[3cat_snapshot] {cam['id']} error: {e}")

# GeoJSON estático de trams (se carga una vez al inicio)
trams_geo = {}  # id -> {description, coords: [[lon,lat],...]}


# ── Helpers ───────────────────────────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


TRAFFIC_LABELS = {
    0: "sin datos",
    1: "muy fluido",
    2: "fluido",
    3: "denso",
    4: "muy denso",
    5: "congestión",
}


# ── Loaders ───────────────────────────────────────────────────────────────────
async def load_trams_geo():
    """Carga una vez la geometría estática de los 527 trams."""
    url = (
        "https://opendata-ajuntament.barcelona.cat/data/dataset/"
        "1090983a-1c40-4609-8620-14ad49aae3ab/resource/"
        "1d6c814c-70ef-4147-aa16-a49ddb952f72/download/transit_relacio_trams.csv"
    )
    local_path = "static/transit_relacio_trams.csv"
    text = None
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(url)
        r.raise_for_status()
        text = r.text
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(text)
    except Exception as e:
        print(f"[load_trams_geo] error: {e}")
        if os.path.exists(local_path):
            print("[load_trams_geo] usant còpia local")
            with open(local_path, encoding="utf-8") as f:
                text = f.read()
        else:
            return
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        tid = int(row["Tram"])
        raw = [float(x) for x in row["Coordenades"].split(",")]
        coords = [[raw[i], raw[i + 1]] for i in range(0, len(raw) - 1, 2)]
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        trams_geo[tid] = {
            "description": row["Descripció"],
            "coords": coords,
            "centroid": [sum(lons) / len(lons), sum(lats) / len(lats)],
        }
    print(f"[geo] {len(trams_geo)} trams cargados")


async def poll_trams():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://www.bcn.cat/transit/dades/dadestrams.dat")
    rows = []
    for line in r.text.strip().splitlines():
        parts = line.split("#")
        if len(parts) < 4:
            continue
        tid = int(parts[0])
        estado = int(parts[2])
        prediccion = int(parts[3])
        geo = trams_geo.get(tid, {})
        rows.append({
            "id": tid,
            "description": geo.get("description", ""),
            "estado": estado,
            "estado_label": TRAFFIC_LABELS.get(estado, "?"),
            "prediccion": prediccion,
            "prediccion_label": TRAFFIC_LABELS.get(prediccion, "?"),
            "coords": geo.get("coords", []),
            "centroid": geo.get("centroid"),
        })
    cache["trams"]["data"] = rows
    cache["trams"]["updated"] = now_iso()


async def poll_bicing():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://api.citybik.es/v2/networks/bicing?fields=stations")
    stations = r.json()["network"]["stations"]
    cache["bicing"]["data"] = [
        {
            "id": s["extra"]["uid"],
            "name": s["name"].strip(),
            "lat": s["latitude"],
            "lon": s["longitude"],
            "bikes": s["free_bikes"],
            "bikes_mechanical": s["extra"].get("normal_bikes", 0),
            "bikes_electric": s["extra"].get("ebikes", 0),
            "slots_free": s["empty_slots"],
            "online": s["extra"].get("online", True),
        }
        for s in stations
    ]
    cache["bicing"]["updated"] = now_iso()


async def poll_air():
    url = (
        "https://analisi.transparenciacatalunya.cat/resource/tasf-thgu.json"
        "?$limit=100&municipi=Barcelona"
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
    raw = r.json()
    # Agrupar por estación y coger la última hora disponible
    stations: dict = {}
    for item in raw:
        sid = item["codi_eoi"]
        if sid not in stations:
            stations[sid] = {
                "id": sid,
                "name": item["nom_estacio"],
                "lat": float(item["latitud"]),
                "lon": float(item["longitud"]),
                "pollutants": {},
                "date": item["data"],
            }
        # Encontrar la última hora con dato
        last_val = None
        for h in range(24, 0, -1):
            key = f"h{h:02d}"
            if key in item and item[key] not in (None, ""):
                last_val = float(item[key])
                break
        if last_val is not None:
            stations[sid]["pollutants"][item["contaminant"]] = {
                "value": last_val,
                "units": item["unitats"],
            }
    cache["air"]["data"] = list(stations.values())
    cache["air"]["updated"] = now_iso()


async def poll_incidents():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://www.bcn.cat/transit/dades/dadespois.dat")
    # El .dat de incidencias tiene texto libre separado por puntos y coma
    cache["incidents"]["data"] = r.text.strip() if r.status_code == 200 else ""
    # Si no funciona, usar FRASE
    if not cache["incidents"]["data"]:
        async with httpx.AsyncClient(timeout=10) as client:
            r2 = await client.get(
                "https://opendata-ajuntament.barcelona.cat/data/dataset/"
                "425f3411-963d-4ccc-a8de-c9422d923378/resource/"
                "2f308e87-8bb1-4145-97f2-3dcce62623b9/download/FRASE_FRASE.txt"
            )
        cache["incidents"]["data"] = r2.text.strip()
    cache["incidents"]["updated"] = now_iso()


# ── Polling loop ──────────────────────────────────────────────────────────────
async def poll_weather():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=41.3851&longitude=2.1734"
        "&current=temperature_2m,wind_speed_10m,wind_direction_10m,"
        "precipitation,weather_code,relative_humidity_2m"
        "&timezone=Europe/Madrid"
    )
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
    c = r.json()["current"]
    cache["weather"]["data"] = {
        "temperature": c["temperature_2m"],
        "humidity": c["relative_humidity_2m"],
        "wind_speed_kmh": c["wind_speed_10m"],
        "wind_direction": c["wind_direction_10m"],
        "precipitation": c["precipitation"],
        "weather_code": c["weather_code"],
    }
    cache["weather"]["updated"] = now_iso()


BERTH_COORDS = {
    "01C": [2.1808, 41.3435], "01D": [2.1832, 41.3428],
    "02A": [2.1965, 41.3518], "02B": [2.1990, 41.3508], "02C": [2.2010, 41.3495], "02D": [2.2030, 41.3480],
    "03A": [2.2050, 41.3520], "03B": [2.2060, 41.3510], "03C": [2.2065, 41.3500],
    "04A": [2.2080, 41.3535],
    "05A": [2.1720, 41.3440], "05B": [2.1700, 41.3432],
    "06A": [2.1670, 41.3450], "06B": [2.1655, 41.3460], "06C": [2.1640, 41.3470], "06D": [2.1625, 41.3480],
    "07A": [2.1740, 41.3560], "07B": [2.1730, 41.3570], "07C": [2.1720, 41.3580],
    "08A": [2.1760, 41.3610],
    "09A": [2.1780, 41.3640], "09B": [2.1770, 41.3630], "09C": [2.1790, 41.3650],
    "10A": [2.1820, 41.3680], "10B": [2.1810, 41.3670], "10C": [2.1830, 41.3690],
    "11A": [2.1800, 41.3720], "11B": [2.1790, 41.3730], "11C": [2.1780, 41.3740],
    "12A": [2.1760, 41.3710], "13A": [2.1740, 41.3700],
    "14A": [2.1720, 41.3690],
    "15A": [2.1670, 41.3650], "15B": [2.1680, 41.3640], "15C": [2.1665, 41.3630], "15D": [2.1650, 41.3625], "15E": [2.1635, 41.3618],
    "16A": [2.1650, 41.3600],
    "17A": [2.1680, 41.3680], "17B": [2.1690, 41.3670],
    "18A": [2.1660, 41.3570], "18B": [2.1645, 41.3560], "18C": [2.1630, 41.3548],
    "19A": [2.1610, 41.3520], "19B": [2.1600, 41.3510],
    "20A": [2.1595, 41.3490], "20B": [2.1585, 41.3480], "20C": [2.1575, 41.3470], "20D": [2.1565, 41.3460],
    "21A": [2.1580, 41.3440],
    "22A": [2.1570, 41.3420], "22B": [2.1580, 41.3410], "22C": [2.1590, 41.3400],
    "23A": [2.1550, 41.3410],
    "24A": [2.1530, 41.3380], "24B": [2.1520, 41.3370], "24C": [2.1510, 41.3360], "24X": [2.1515, 41.3365],
    "26A": [2.1490, 41.3330],
    "27A": [2.1470, 41.3305], "27B": [2.1460, 41.3295], "27C": [2.1450, 41.3285],
    "28A": [2.1440, 41.3260], "28B": [2.1430, 41.3250],
    "29A": [2.1420, 41.3230],
    "30A": [2.1400, 41.3200], "30B": [2.1390, 41.3190], "30C": [2.1380, 41.3180],
    "31A": [2.1360, 41.3165], "31B": [2.1350, 41.3155], "31C": [2.1340, 41.3145],
    "32A": [2.1320, 41.3120], "32B": [2.1310, 41.3110], "32C": [2.1300, 41.3100],
    "32D": [2.1290, 41.3090], "32E": [2.1280, 41.3080], "32F": [2.1270, 41.3070], "32G": [2.1260, 41.3060], "32H": [2.1250, 41.3050],
    "33A": [2.1400, 41.3140], "33B": [2.1390, 41.3130], "33C": [2.1380, 41.3120],
    "34A": [2.1360, 41.3090], "34B": [2.1350, 41.3080],
    "35A": [2.1340, 41.3060],
    "36A": [2.1300, 41.3020],
    "90A": [2.2100, 41.3280], "90B": [2.2150, 41.3250], "99B": [2.1900, 41.3100],
}

async def poll_ships():
    url = (
        "https://opendata.portdebarcelona.cat/en/dataset/"
        "c6f3045b-8aee-476e-9ea3-7a46c453e04a/resource/"
        "7e75a37e-bafc-43fc-8b0a-02c0e051d8e5/download/portbcnvaixellsavui.csv"
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
    reader = csv.DictReader(io.StringIO(r.text))
    ships = []
    berth_count: dict = {}
    for row in reader:
        berth = row.get("CODALINEACIO", "").strip()
        base_coords = BERTH_COORDS.get(berth)
        if base_coords:
            idx = berth_count.get(berth, 0)
            berth_count[berth] = idx + 1
            angle = idx * 0.8
            r = 0.0004 * (idx // 4 + 1)
            coords = [base_coords[0] + r * math.cos(angle), base_coords[1] + r * math.sin(angle)]
        else:
            coords = None
        ships.append({
            "name": row.get("NOMVAIXELL", "").strip(),
            "eta": row.get("ETA", "").strip(),
            "etd": row.get("ETD", "").strip(),
            "country": row.get("NOMPAIS", "").strip(),
            "length": row.get("ESLORA", "").strip(),
            "operator": row.get("USUNOMRAOSOCIAL", "").strip(),
            "berth": berth,
            "berth_name": row.get("NOMALINEACIO", "").strip() if "NOMALINEACIO" in row else "",
            "coords": coords,
        })
    cache["ships"]["data"] = ships
    cache["ships"]["updated"] = now_iso()


async def poll_trains():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://gtfsrt.renfe.com/vehicle_positions.json")
    entities = r.json().get("entity", [])
    trains = []
    for e in entities:
        v = e.get("vehicle", {})
        pos = v.get("position", {})
        lat, lon = pos.get("latitude"), pos.get("longitude")
        if lat is None or lon is None:
            continue
        if not (41.0 <= lat <= 41.8 and 1.5 <= lon <= 2.5):
            continue
        trains.append({
            "id": v.get("vehicle", {}).get("id", ""),
            "label": v.get("vehicle", {}).get("label", ""),
            "lat": lat,
            "lon": lon,
            "status": v.get("currentStatus", ""),
            "stop_id": v.get("stopId", ""),
            "timestamp": v.get("timestamp", ""),
        })
    cache["trains"]["data"] = trains
    cache["trains"]["updated"] = now_iso()


BEACH_COORDS = {
    # Barcelona — OSM Nominatim
    "platja_barcelona.sant_sebasti":    [2.1893, 41.3713],
    "platja_barcelona.sant_miquel":     [2.1908, 41.3755],
    "platja_barcelona.barceloneta":     [2.1930, 41.3793],
    "platja_barcelona.somorrostro":     [2.1957, 41.3834],
    "platja_barcelona.nova_icria":      [2.2018, 41.3903],
    "platja_barcelona.bogatell":        [2.2067, 41.3940],
    "platja_barcelona.mar_bella":       [2.2118, 41.3980],
    "platja_barcelona.nova_mar_bella":  [2.2151, 41.4017],
    "platja_barcelona.llevant":         [2.2190, 41.4045],
    # Badalona — OSM
    "platja_badalona.mora":             [2.2377, 41.4272],
    "platja_badalona.l_estaci":         [2.2410, 41.4330],
    "platja_badalona.coco":             [2.2450, 41.4389],
    "platja_badalona.pescadors":        [2.2524, 41.4480],
    "platja_badalona.cristall":         [2.2603, 41.4548],
    # Montgat — estimated along coast
    "platja_montgat.platja_de_can_tano":   [2.2700, 41.4620],
    "platja_montgat.platja_de_les_roques": [2.2730, 41.4650],
    "platja_montgat.cala_taps":            [2.2760, 41.4680],
    "platja_montgat.platja_de_les_barques":[2.2790, 41.4710],
    "platja_montgat.platja_dels_toldos":   [2.2820, 41.4740],
    # El Prat — OSM (remolar) + estimated
    "platja_prat.el_prat":              [2.0900, 41.3020],
    "platja_prat.dels_militars":        [2.0850, 41.2960],
    "platja_prat.carrabiners":          [2.0810, 41.2910],
    "platja_prat.ca_larana":            [2.0795, 41.2845],
    "platja_prat.el_remolar":           [2.0780, 41.2780],
    "platja_prat.la_roberta":           [2.0760, 41.2730],
    "platja_prat.platja_naturista":     [2.0740, 41.2680],
    # Gavà — OSM
    "platja_gava.platja_de_gava":       [2.0299, 41.2676],
    "platja_gava.platja_de_lestany":    [2.0260, 41.2630],
    # Sant Adrià
    "platja_sadria.frum":               [2.2250, 41.4060],
    # Castelldefels
    "platja_castelldefels.castelldefels": [1.9660, 41.2610],
}

async def poll_beaches():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("http://opendata.amb.cat/dades_estat_platja/search")
    items = r.json().get("items", [])
    beaches = []
    for b in items:
        platja_id = b.get("platja", "")
        coords = BEACH_COORDS.get(platja_id)
        if not coords:
            continue
        beaches.append({
            "id": platja_id,
            "name": platja_id.split(".")[-1].replace("_", " ").title(),
            "municipality": b.get("municipi", ""),
            "flag": b.get("bandera", "DESCONEGUT"),
            "water_state": b.get("estat_aigua", ""),
            "water_aspect": b.get("aspecte_aigua", ""),
            "jellyfish": b.get("meduses", False),
            "occupancy": b.get("ocupacio", ""),
            "available": b.get("disponible", False),
            "updated": b.get("date_updated", ""),
            "coords": coords,
        })
    cache["beaches"]["data"] = beaches
    cache["beaches"]["updated"] = now_iso()
    print(f"[beaches] {len(beaches)} platges carregades")


async def poll_obras():
    """Obres a la via pública de Barcelona (actualitzat trimestralment)."""
    url = "https://opendata-ajuntament.barcelona.cat/data/dataset/fd9f355f-2160-4f89-96a1-6ece3924e3bd/resource/089bcf9e-140e-4ea3-bf93-03c6260ba0f5/download"
    local_path = "static/obras.json"
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        r = await client.get(url)
    if r.status_code == 200:
        data = r.json()
        with open(local_path, "w") as f:
            json.dump(data, f)
    elif os.path.exists(local_path):
        print(f"[obras] HTTP {r.status_code}, usant còpia local")
        with open(local_path) as f:
            data = json.load(f)
    else:
        print(f"[obras] HTTP {r.status_code} sense còpia local")
        return
    obras = []
    for o in data:
        if o.get("estat") == "Finalitzada":
            continue
        geo = o.get("geometria_wgs84") or ""
        coords = None
        nums = re.findall(r"([\d.]+)\s+([\d.]+)", geo)
        if nums:
            lons = [float(x[0]) for x in nums]
            lats = [float(x[1]) for x in nums]
            coords = [sum(lons)/len(lons), sum(lats)/len(lats)]
        def _date_to_ym(d):
            try:
                parts = (d or "")[:7].split("-")
                return int(parts[0]) * 100 + int(parts[1])
            except (ValueError, IndexError):
                return 0
        data_inici = (o.get("data_inici") or "")[:10]
        data_fi = (o.get("data_fi") or "")[:10]
        obras.append({
            "codi": o.get("codi", ""),
            "titol": o.get("titol", ""),
            "ubicacio": o.get("ubicacio", ""),
            "tipus": o.get("tipusobra", ""),
            "estat": o.get("estat", ""),
            "data_inici": data_inici,
            "data_fi": data_fi,
            "inici_ym": _date_to_ym(data_inici),
            "fi_ym": _date_to_ym(data_fi) or 209912,
            "promotor": o.get("promotor", ""),
            "barri": o.get("nom_barri", "").strip(),
            "districte": o.get("nom_districte", ""),
            "coords": coords,
        })
    if obras:
        cache["obras"]["data"] = obras
        cache["obras"]["updated"] = now_iso()
    print(f"[obras] {len(obras)} actives carregades")


async def poll_flights():
    """Aviones en tiempo real sobre Barcelona via OpenSky Network (OAuth2)."""
    url = "https://opensky-network.org/api/states/all?lamin=41.1&lamax=41.7&lomin=1.8&lomax=2.5"
    headers = {}
    if OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET:
        token = await _get_opensky_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
    if r.status_code == 401:
        # token caducat — forcem renovació al proper cicle
        global _opensky_token_expiry
        _opensky_token_expiry = 0.0
        print("[flights] 401 — token renovat al proper cicle")
        return
    if r.status_code == 429:
        print("[flights] 429 rate limit — esperant 5min")
        await asyncio.sleep(300)
        return
    if r.status_code != 200:
        print(f"[flights] HTTP {r.status_code}")
        return
    states = r.json().get("states") or []
    flights = []
    for s in states:
        if s[5] is None or s[6] is None:
            continue
        flights.append({
            "icao24": s[0],
            "callsign": (s[1] or "").strip() or s[0],
            "country": s[2],
            "lon": s[5],
            "lat": s[6],
            "altitude_m": s[7],
            "on_ground": s[8],
            "velocity_ms": s[9],
            "heading": s[10],
            "vertical_rate": s[11],
        })
    cache["flights"]["data"] = flights
    cache["flights"]["updated"] = now_iso()


async def flights_loop():
    """Actualiza aviones cada 60s (amb auth: ~1440 req/dia, sota el límit de 4000)."""
    while True:
        try:
            await poll_flights()
        except Exception as e:
            print(f"[flights] error: {e}")
        await asyncio.sleep(60)


async def poll_camaras_dgt():
    """Càmeres DGT a Catalunya via etraffic API (actualitza cada 10min — imatges estàtiques)."""
    import base64
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://etraffic.dgt.es/etrafficWEB/api/cache/getCamaras",
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                json={},
            )
        if r.status_code != 200:
            print(f"[camaras_dgt] HTTP {r.status_code}")
            return
        raw = r.text.strip().strip('"')
        decoded = bytes(b ^ 70 ^ 0x20 for b in base64.b64decode(raw))
        d = json.loads(decoded)
        url_base = d.get("urlBase", "https://etraffic.dgt.es/camarasEtraffic/")
        camaras_all = d.get("camaras", d) if isinstance(d, dict) else d
        # Filtra Catalunya (bbox: lon 0.15-3.33, lat 40.5-42.9)
        camaras = [
            {
                "id": c["idCamara"],
                "carretera": c.get("carretera", ""),
                "pk": c.get("pk", ""),
                "sentido": c.get("sentido", ""),
                "lon": float(c["coordX"]),
                "lat": float(c["coordY"]),
                "imagen_url": f"{url_base}{c['idCamara']}.jpg",
            }
            for c in camaras_all
            if isinstance(c, dict) and c.get("coordX") and c.get("coordY")
            and 0.15 < float(c["coordX"]) < 3.33
            and 40.5 < float(c["coordY"]) < 42.9
        ]
        cache["camaras_dgt"]["data"] = camaras
        cache["camaras_dgt"]["updated"] = now_iso()
        print(f"[camaras_dgt] {len(camaras)} càmeres carregades")
    except Exception as e:
        print(f"[camaras_dgt] error: {e}")


async def camaras_dgt_loop():
    while True:
        try:
            await poll_camaras_dgt()
        except Exception as e:
            print(f"[camaras_dgt] error: {e}")
        await asyncio.sleep(600)


async def polling_loop():
    while True:
        if not trams_geo:
            await load_trams_geo()
        try:
            await asyncio.gather(
                poll_trams(),
                poll_bicing(),
                poll_air(),
                poll_incidents(),
                poll_metro(),
                poll_weather(),
                poll_trains(),
                return_exceptions=True,
            )
        except Exception as e:
            print(f"[poll] error: {e}")
        await asyncio.sleep(60)


async def poll_arbres():
    """145k arbres de Barcelona — dataset estàtic CSV, carrega un cop."""
    url = "https://opendata-ajuntament.barcelona.cat/data/dataset/27b3f8a7-e536-4eea-b025-c64b4f83c021/resource/23124fd5-521f-40f8-85b8-efb1e71c2ec8/download/OD_Arbrat_Viari_BCN.csv"
    local_path = "static/arbres.csv"
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        r = await client.get(url)
    if r.status_code == 200:
        text = r.text
    elif os.path.exists(local_path):
        print(f"[arbres] HTTP {r.status_code}, usant còpia local")
        with open(local_path, encoding="utf-8") as f:
            text = f.read()
    else:
        print(f"[arbres] HTTP {r.status_code} i no hi ha còpia local")
        return
    reader = csv.DictReader(io.StringIO(text))
    arbres = []
    for a in reader:
        try:
            lat = float(a["latitud"])
            lon = float(a["longitud"])
        except (ValueError, TypeError, KeyError):
            continue
        arbres.append({
            "lon": lon, "lat": lat,
            "especie": a.get("cat_nom_catala") or a.get("cat_nom_castella") or a.get("cat_nom_cientific", ""),
            "cientific": a.get("cat_nom_cientific", ""),
            "barri": a.get("nom_barri", ""),
            "districte": a.get("nom_districte", ""),
            "data_plantacio": (a.get("data_plantacio") or "")[:4],
        })
    if arbres:
        cache["arbres"]["data"] = arbres
        cache["arbres"]["updated"] = now_iso()
    print(f"[arbres] {len(arbres)} arbres carregats")


async def poll_accidents():
    """Accidents de trànsit 2024 + 2025 — dataset semestral."""
    sources = [
        ("https://opendata-ajuntament.barcelona.cat/data/dataset/e769eb9d-d778-4cd7-9e3a-5858bba49b20/resource/066d46b1-25be-4f08-b0e0-5a233714bda2/download/2025_accidents_gu_bcn.csv", "static/accidents_2025.csv"),
        ("https://opendata-ajuntament.barcelona.cat/data/dataset/e769eb9d-d778-4cd7-9e3a-5858bba49b20/resource/66f5e7e3-045b-4d19-b649-2eaea622ae93/download/2024_accidents_gu_bcn.csv", "static/accidents_2024.csv"),
    ]
    accidents = []
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for url, local_path in sources:
            text = None
            r = await client.get(url)
            if r.status_code == 200:
                text = r.text
            elif os.path.exists(local_path):
                print(f"[accidents] HTTP {r.status_code}, usant còpia local {local_path}")
                with open(local_path, encoding="utf-8") as f:
                    text = f.read()
            else:
                print(f"[accidents] HTTP {r.status_code} i no hi ha còpia local")
                continue
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                try:
                    lat = float(row.get("Latitud_WGS84", ""))
                    lon = float(row.get("Longitud_WGS84", ""))
                except (ValueError, TypeError):
                    continue
                morts = int(row.get("Numero_morts", 0) or 0)
                greus = int(row.get("Numero_lesionats_greus", 0) or 0)
                lleus = int(row.get("Numero_lesionats_lleus", 0) or 0)
                any_ = int(row.get("NK_Any", 0) or 0)
                mes_ = int(row.get("Mes_any", 0) or 0)
                accidents.append({
                    "lon": lon, "lat": lat,
                    "any": any_,
                    "mes": mes_,
                    "nom_mes": row.get("Nom_mes", ""),
                    "ym": any_ * 100 + mes_,
                    "hora": row.get("Hora_dia", ""),
                    "torn": row.get("Descripcio_torn", ""),
                    "carrer": row.get("Nom_carrer", ""),
                    "barri": row.get("Nom_barri", ""),
                    "morts": morts,
                    "greus": greus,
                    "lleus": lleus,
                    "victimes": int(row.get("Numero_victimes", 0) or 0),
                    "gravetat": "mortal" if morts > 0 else "greu" if greus > 0 else "lleu",
                })
    if accidents:
        cache["accidents"]["data"] = accidents
        cache["accidents"]["updated"] = now_iso()
    print(f"[accidents] {len(accidents)} accidents carregats")


def _utm31n_to_wgs84(easting: float, northing: float):
    """Converteix ETRS89 UTM zona 31N (EPSG:25831) a WGS84 lat/lon."""
    a, f = 6378137.0, 1/298.257223563
    b = a*(1-f); e2 = 1-(b/a)**2; k0 = 0.9996
    x = easting - 500000; y = northing
    M = y/k0
    mu = M/(a*(1-e2/4-3*e2**2/64-5*e2**3/256))
    e1 = (1-math.sqrt(1-e2))/(1+math.sqrt(1-e2))
    phi1 = mu+(3*e1/2-27*e1**3/32)*math.sin(2*mu)+(21*e1**2/16-55*e1**4/32)*math.sin(4*mu)+(151*e1**3/96)*math.sin(6*mu)
    N1 = a/math.sqrt(1-e2*math.sin(phi1)**2)
    T1 = math.tan(phi1)**2; C1 = e2/(1-e2)*math.cos(phi1)**2
    R1 = a*(1-e2)/(1-e2*math.sin(phi1)**2)**1.5
    D = x/(N1*k0)
    lat = phi1-(N1*math.tan(phi1)/R1)*(D**2/2-(5+3*T1+10*C1-4*C1**2-9*e2/(1-e2))*D**4/24+(61+90*T1+298*C1+45*T1**2-252*e2/(1-e2)-3*C1**2)*D**6/720)
    lon0 = math.radians(3)  # zona 31: meridià central = 3°E
    lon = lon0+(D-(1+2*T1+C1)*D**3/6+(5-2*C1+28*T1-3*C1**2+8*e2/(1-e2)+24*T1**2)*D**5/120)/math.cos(phi1)
    return math.degrees(lat), math.degrees(lon)


def _parse_wkt_polygon(wkt: str):
    """Converteix WKT POLYGON de ETRS89 a llista de [lon,lat] WGS84."""
    import re
    coords_str = re.search(r'POLYGON\s*\(\((.+?)\)\)', wkt)
    if not coords_str:
        return None
    rings = []
    for ring_str in re.findall(r'\(([^()]+)\)', '(' + wkt.split('((', 1)[1]):
        pts = []
        for pair in ring_str.strip().split(','):
            parts = pair.strip().split()
            if len(parts) >= 2:
                lat, lon = _utm31n_to_wgs84(float(parts[0]), float(parts[1]))
                pts.append([round(lon, 6), round(lat, 6)])
        if pts:
            rings.append(pts)
    return rings if rings else None


async def poll_poblacio():
    """Densitat de població per secció censal — INE Padró 2025 + BCN geometries."""
    geo_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/resource/db90a207-d125-4f80-aac5-f9d5d6e648f5/download"
    pop_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/16c11ddf-a783-4b64-aa68-3dc83dc70379/resource/c3e2f76d-8397-4316-9353-deb41154b495/download"
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            geo_r, pop_r = await asyncio.gather(client.get(geo_url), client.get(pop_url))
    except Exception as e:
        print(f"[poll_poblacio] fetch error: {e}")
        return

    def _load(r, local_path):
        if r.status_code == 200:
            return r.json()
        if os.path.exists(local_path):
            print(f"[poblacio] HTTP {r.status_code}, usant còpia local {local_path}")
            with open(local_path) as f:
                return json.load(f)
        print(f"[poblacio] HTTP {r.status_code} i no hi ha còpia local {local_path}")
        return None

    geo_data = _load(geo_r, "static/seccions_censals.json")
    pop_data = _load(pop_r, "static/poblacio_sc.json")
    if not geo_data or not pop_data:
        return

    # Sumar homes+dones per secció censal — clau: "01001" (districte+seccio, 5 digits)
    pop_by_sc: dict[str, int] = {}
    for row in pop_data:
        sc_raw = str(row.get("Seccio_Censal", ""))  # ex: "1001" = districte1 + seccio001
        # Normalitzar a 5 dígits: districte 2 digits + seccio 3 digits
        sc_key = sc_raw.zfill(5) if len(sc_raw) <= 5 else sc_raw
        pop_by_sc[sc_key] = pop_by_sc.get(sc_key, 0) + int(row.get("Valor", 0) or 0)

    # Construir GeoJSON amb densitat
    features = []
    for sec in geo_data:
        d = str(sec.get("codi_districte", "")).zfill(2)
        s = str(sec.get("codi_seccio_censal", "")).zfill(3)
        sc_code = d + s  # ex: "01001"
        wkt = sec.get("geometria_etrs89", "")
        if not wkt:
            continue
        rings = _parse_wkt_polygon(wkt)
        if not rings:
            continue
        pop = pop_by_sc.get(sc_code, pop_by_sc.get(sc_code.lstrip("0"), 0))
        # Àrea aproximada en km² (bounding box heurístic per ràtio ràpid)
        lons = [p[0] for p in rings[0]]
        lats = [p[1] for p in rings[0]]
        dx = (max(lons)-min(lons))*111*math.cos(math.radians(sum(lats)/len(lats)))
        dy = (max(lats)-min(lats))*111
        area_km2 = max(dx*dy, 0.001)
        density = round(pop/area_km2)
        features.append({"type":"Feature","geometry":{"type":"Polygon","coordinates":rings},"properties":{
            "sc": sc_code,
            "barri": sec.get("nom_barri",""),
            "districte": sec.get("nom_districte",""),
            "poblacio": pop,
            "density": density,
        }})

    geojson = {"type":"FeatureCollection","features":features}
    cache["poblacio"]["data"] = geojson
    cache["poblacio"]["updated"] = now_iso()
    print(f"[poblacio] {len(features)} seccions censals, max densitat {max(f['properties']['density'] for f in features) if features else 0:.0f} hab/km²")


async def poll_carrega():
    """Punts de recàrrega EV — OpenChargeMap API (actualitzat contínuament)."""
    local_path = "static/carrega.json"
    raw = None
    if OCM_KEY:
        url = (f"https://api.openchargemap.io/v3/poi/?output=json&countrycode=ES"
               f"&latitude=41.3851&longitude=2.1734&distance=12&distanceunit=km"
               f"&maxresults=2000&compact=true&verbose=false&key={OCM_KEY}")
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url)
        if r.status_code == 200:
            raw = r.json()
            with open(local_path, "w") as f:
                json.dump(raw, f)
        else:
            print(f"[carrega] OCM HTTP {r.status_code}")

    if raw is None and os.path.exists(local_path):
        print("[carrega] usant còpia local")
        with open(local_path) as f:
            raw = json.load(f)
    if raw is None:
        return

    # StatusTypeID: 50=operatiu, 150=temporalment fora, 0/None=desconegut
    stations = []
    for s in raw:
        addr = s.get("AddressInfo", {})
        lat = addr.get("Latitude"); lon = addr.get("Longitude")
        if not lat or not lon: continue
        conns = s.get("Connections", [])
        max_kw = max((c.get("PowerKW") or 0 for c in conns), default=0)
        status_ids = set(c.get("StatusTypeID") for c in conns)
        operatiu = 50 in status_ids
        stations.append({
            "lat": lat, "lon": lon,
            "name": addr.get("Title", ""),
            "address": addr.get("AddressLine1", ""),
            "operatiu": operatiu,
            "n_sockets": sum(c.get("Quantity") or 1 for c in conns),
            "max_kw": max_kw,
            "cost": s.get("UsageCost", ""),
            "operator": (s.get("OperatorInfo") or {}).get("Title", "") if isinstance(s.get("OperatorInfo"), dict) else "",
        })

    if stations:
        cache["carrega"]["data"] = stations
        cache["carrega"]["updated"] = now_iso()
    print(f"[carrega] {len(stations)} estacions ({sum(1 for s in stations if s['operatiu'])} operatives)")


async def poll_zones_verdes():
    """Parcs i zones verdes — OSM Overpass, amb còpia local com a fallback."""
    local_path = "static/zones_verdes.geojson"
    query = '[out:json][timeout:50];(way["leisure"="park"](41.32,2.05,41.47,2.23);way["leisure"="garden"](41.32,2.05,41.47,2.23););out geom;'
    geojson = None
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post("https://overpass-api.de/api/interpreter", data={"data": query},
                                  headers={"Content-Type": "application/x-www-form-urlencoded",
                                           "User-Agent": "DATABARNA/1.0 (barcelona urban data dashboard)"})
        if r.status_code == 200:
            elements = r.json().get("elements", [])
            features = []
            for el in elements:
                geom = el.get("geometry", [])
                if not geom:
                    continue
                coords = [[g["lon"], g["lat"]] for g in geom]
                tags = el.get("tags", {})
                features.append({"type":"Feature","geometry":{"type":"Polygon","coordinates":[coords]},"properties":{"name":tags.get("name",""),"type":tags.get("leisure",tags.get("landuse",""))}})
            geojson = {"type":"FeatureCollection","features":features}
            with open(local_path, "w") as f:
                json.dump(geojson, f)
    except Exception as e:
        print(f"[zones_verdes] Overpass error: {e}")
    if not geojson and os.path.exists(local_path):
        print("[zones_verdes] usant còpia local")
        with open(local_path) as f:
            geojson = json.load(f)
    if geojson:
        cache["zones_verdes"]["data"] = geojson
        cache["zones_verdes"]["updated"] = now_iso()
        print(f"[zones_verdes] {len(geojson['features'])} parcs carregats")


async def _poll_osm_points(amenity_filter: str, cache_key: str, local_path: str, label: str):
    """Generic OSM Overpass fetch for point amenities. If local file exists, skip Overpass."""
    geojson = None
    if os.path.exists(local_path):
        with open(local_path) as f:
            geojson = json.load(f)
        print(f"[{label}] carregat des de fitxer estàtic ({len(geojson.get('features', []))} elements)")
    else:
        bbox = "41.32,2.05,41.47,2.23"
        query = f'[out:json][timeout:50];node[{amenity_filter}]({bbox});out body;'
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await client.post("https://overpass-api.de/api/interpreter", data={"data": query},
                                      headers={"Content-Type": "application/x-www-form-urlencoded",
                                               "User-Agent": "DATABARNA/1.0 (barcelona urban data dashboard)"})
            if r.status_code == 200:
                elements = r.json().get("elements", [])
                features = []
                for el in elements:
                    lat = el.get("lat"); lon = el.get("lon")
                    if not lat or not lon:
                        continue
                    tags = el.get("tags", {})
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [lon, lat]},
                        "properties": {
                            "name": tags.get("name", ""),
                            "description": tags.get("description", ""),
                            "wheelchair": tags.get("wheelchair", ""),
                            "fee": tags.get("fee", ""),
                            "opening_hours": tags.get("opening_hours", ""),
                            "operator": tags.get("operator", ""),
                        }
                    })
                geojson = {"type": "FeatureCollection", "features": features}
                with open(local_path, "w") as f:
                    json.dump(geojson, f)
                print(f"[{label}] {len(features)} elements descarregats i guardats")
            else:
                print(f"[{label}] Overpass HTTP {r.status_code}")
        except Exception as e:
            print(f"[{label}] Overpass error: {e}")
    if geojson:
        cache[cache_key]["data"] = geojson
        cache[cache_key]["updated"] = now_iso()


async def poll_fonts():
    await _poll_osm_points('"amenity"="drinking_water"', "fonts", "static/fonts.geojson", "fonts")


async def poll_desfibril():
    await _poll_osm_points('"emergency"="defibrillator"', "desfibril·ladors", "static/desfibril.geojson", "desfibril")


async def poll_lavabos():
    await _poll_osm_points('"amenity"="toilets"', "lavabos", "static/lavabos.geojson", "lavabos")


async def _poll_osm_mixed(osm_query: str, cache_key: str, local_path: str, label: str, extra_props_fn=None):
    """OSM Overpass fetch for nodes+ways (uses center for ways). Skip if local file exists."""
    geojson = None
    if os.path.exists(local_path):
        with open(local_path) as f:
            geojson = json.load(f)
        print(f"[{label}] carregat des de fitxer estàtic ({len(geojson.get('features', []))} elements)")
    else:
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await client.post("https://overpass-api.de/api/interpreter", data={"data": osm_query},
                                      headers={"Content-Type": "application/x-www-form-urlencoded",
                                               "User-Agent": "DATABARNA/1.0 (barcelona urban data dashboard)"})
            if r.status_code == 200:
                elements = r.json().get("elements", [])
                features = []
                for el in elements:
                    if el["type"] == "node":
                        lat, lon = el.get("lat"), el.get("lon")
                    else:
                        c = el.get("center", {})
                        lat, lon = c.get("lat"), c.get("lon")
                    if not lat or not lon:
                        continue
                    tags = el.get("tags", {})
                    props = {"name": tags.get("name", ""), "type": el["type"],
                             "amenity": tags.get("amenity", ""), "operator": tags.get("operator", ""),
                             "opening_hours": tags.get("opening_hours", ""), "phone": tags.get("phone", ""),
                             "website": tags.get("website", tags.get("contact:website", ""))}
                    if extra_props_fn:
                        props.update(extra_props_fn(tags))
                    features.append({"type": "Feature",
                                     "geometry": {"type": "Point", "coordinates": [lon, lat]},
                                     "properties": props})
                geojson = {"type": "FeatureCollection", "features": features}
                with open(local_path, "w") as f:
                    json.dump(geojson, f)
                print(f"[{label}] {len(features)} elements descarregats i guardats")
            else:
                print(f"[{label}] Overpass HTTP {r.status_code}")
        except Exception as e:
            print(f"[{label}] error: {e}")
    if geojson:
        cache[cache_key]["data"] = geojson
        cache[cache_key]["updated"] = now_iso()


async def poll_airbnb():
    """Pisos turístics — InsideAirbnb dataset públic. Estàtic: si existeix, no re-descarrega."""
    local_path = "static/airbnb.geojson"
    if os.path.exists(local_path):
        with open(local_path) as f:
            geojson = json.load(f)
        print(f"[airbnb] carregat des de fitxer estàtic ({len(geojson.get('features', []))} pisos)")
    else:
        url = "https://data.insideairbnb.com/spain/catalonia/barcelona/2025-12-14/visualisations/listings.csv"
        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True,
                                         headers={"User-Agent": "Mozilla/5.0"}) as client:
                r = await client.get(url)
            if r.status_code == 200:
                reader = csv.DictReader(io.StringIO(r.text))
                features = []
                for row in reader:
                    try:
                        lat, lon = float(row["latitude"]), float(row["longitude"])
                    except (ValueError, KeyError):
                        continue
                    room_type = row.get("room_type", "")
                    license_raw = row.get("license", "")
                    has_license = bool(license_raw and "HUTB" in license_raw)
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [lon, lat]},
                        "properties": {
                            "name": row.get("name", "")[:80],
                            "room_type": room_type,
                            "neighbourhood": row.get("neighbourhood", ""),
                            "price": row.get("price", ""),
                            "reviews": row.get("number_of_reviews", ""),
                            "host_listings": row.get("calculated_host_listings_count", ""),
                            "has_license": has_license,
                        }
                    })
                geojson = {"type": "FeatureCollection", "features": features}
                with open(local_path, "w") as f:
                    json.dump(geojson, f)
                print(f"[airbnb] {len(features)} pisos descarregats i guardats")
            else:
                print(f"[airbnb] HTTP {r.status_code}")
                return
        except Exception as e:
            print(f"[airbnb] error: {e}")
            return
    cache["airbnb"]["data"] = geojson
    cache["airbnb"]["updated"] = now_iso()


async def poll_equipaments():
    bbox = "41.32,2.05,41.47,2.23"
    q = (f'[out:json][timeout:50];'
         f'(node["amenity"~"^(library|community_centre|arts_centre)$"]({bbox});'
         f'way["amenity"~"^(library|community_centre|arts_centre)$"]({bbox}););'
         f'out center tags;')
    await _poll_osm_mixed(q, "equipaments", "static/equipaments.geojson", "equipaments")


async def poll_mercats():
    """Mercats municipals — BCN open data, amb fallback OSM."""
    local_path = "static/mercats.geojson"
    geojson = None
    if os.path.exists(local_path):
        with open(local_path) as f:
            geojson = json.load(f)
        print(f"[mercats] carregat des de fitxer estàtic ({len(geojson.get('features', []))} mercats)")
    else:
        # BCN open data — Institut Municipal de Mercats
        url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=b204171d-79e3-49d1-9a0a-0bdbb9d46bd2&limit=100"
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                r = await client.get(url)
            if r.status_code == 200:
                records = r.json().get("result", {}).get("records", [])
                features = []
                for rec in records:
                    lat = rec.get("latitud") or rec.get("Latitud") or rec.get("LATITUD")
                    lon = rec.get("longitud") or rec.get("Longitud") or rec.get("LONGITUD")
                    name = rec.get("nom_mercat") or rec.get("Nom_Mercat") or rec.get("NOM") or rec.get("nom") or ""
                    if not lat or not lon:
                        continue
                    try:
                        lat, lon = float(lat), float(lon)
                    except (ValueError, TypeError):
                        continue
                    features.append({"type": "Feature",
                                     "geometry": {"type": "Point", "coordinates": [lon, lat]},
                                     "properties": {"name": name, "address": rec.get("adreca", rec.get("Adreca", "")),
                                                    "districte": rec.get("nom_districte", rec.get("Districte", ""))}})
                if features:
                    geojson = {"type": "FeatureCollection", "features": features}
                    with open(local_path, "w") as f:
                        json.dump(geojson, f)
                    print(f"[mercats] {len(features)} mercats descarregats de BCN open data")
        except Exception as e:
            print(f"[mercats] BCN open data error: {e}")

        if not geojson:
            # Fallback OSM
            bbox = "41.32,2.05,41.47,2.23"
            q = (f'[out:json][timeout:30];'
                 f'(node["amenity"="marketplace"]({bbox});way["amenity"="marketplace"]({bbox}););out center tags;')
            await _poll_osm_mixed(q, "mercats", local_path, "mercats")
            return

    if geojson:
        cache["mercats"]["data"] = geojson
        cache["mercats"]["updated"] = now_iso()


async def snapshots_loop():
    """Refresca captures 3Cat cada 10 minuts."""
    while True:
        await poll_3cat_snapshots()
        await asyncio.sleep(600)


async def slow_poll_loop():
    """Datos que cambian poco: cada 6h."""
    while True:
        try:
            await asyncio.gather(poll_ships(), poll_beaches(), poll_obras(), poll_accidents(), return_exceptions=True)
        except Exception as e:
            print(f"[slow_poll] error: {e}")
        try:
            await poll_poblacio()
        except Exception as e:
            print(f"[slow_poll poblacio] error: {e}")
        await asyncio.sleep(21600)


async def persist_to_db():
    """Escribe snapshot + granular cada 5 minutos en Postgres."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _write_db)


def _write_db():
    trams = cache["trams"]["data"]
    bicing = cache["bicing"]["data"]
    air = cache["air"]["data"]

    if not trams:
        return

    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        ts = datetime.now(timezone.utc)

        # ── Snapshot global ──
        active = [t for t in trams if t["estado"] > 0]
        total = max(len(active), 1)
        congestion_index = round(sum(t["estado"] for t in active) / total, 2)

        from collections import defaultdict
        air_avgs: dict = defaultdict(list)
        for s in air:
            for p, info in s.get("pollutants", {}).items():
                air_avgs[p].append(info["value"])

        def avg(lst):
            return round(sum(lst) / len(lst), 1) if lst else None

        cur.execute("""
            INSERT INTO snapshots
            (ts, congestion_index, trams_muy_fluido, trams_fluido, trams_denso,
             trams_muy_denso, trams_congestion, bikes_total, bikes_electric,
             slots_free, no2_avg, pm10_avg, pm25_avg, o3_avg)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            ts, congestion_index,
            sum(1 for t in active if t["estado"] == 1),
            sum(1 for t in active if t["estado"] == 2),
            sum(1 for t in active if t["estado"] == 3),
            sum(1 for t in active if t["estado"] == 4),
            sum(1 for t in active if t["estado"] == 5),
            sum(s["bikes"] for s in bicing),
            sum(s["bikes_electric"] for s in bicing),
            sum(s["slots_free"] for s in bicing),
            avg(air_avgs.get("NO2", [])),
            avg(air_avgs.get("PM10", [])),
            avg(air_avgs.get("PM2.5", [])),
            avg(air_avgs.get("O3", [])),
        ))

        # ── Trams granular ──
        psycopg2.extras.execute_values(cur,
            "INSERT INTO trams_history (ts, tram_id, estado, prediccion) VALUES %s",
            [(ts, t["id"], t["estado"], t["prediccion"]) for t in trams]
        )

        # ── Bicing granular ──
        psycopg2.extras.execute_values(cur,
            """INSERT INTO bicing_history
               (ts, station_id, station_name, lat, lon, bikes, bikes_electric, slots_free)
               VALUES %s""",
            [(ts, s["id"], s["name"], s["lat"], s["lon"],
              s["bikes"], s["bikes_electric"], s["slots_free"]) for s in bicing]
        )

        # ── Aire granular ──
        air_rows = []
        for s in air:
            for pollutant, info in s.get("pollutants", {}).items():
                air_rows.append((
                    ts, s["id"], s["name"], s["lat"], s["lon"],
                    pollutant, info["value"], info["units"]
                ))
        if air_rows:
            psycopg2.extras.execute_values(cur,
                """INSERT INTO air_history
                   (ts, station_id, station_name, lat, lon, pollutant, value, units)
                   VALUES %s""",
                air_rows
            )

        conn.commit()
        cur.close()
        conn.close()
        print(f"[db] snapshot guardado — trams:{len(trams)} bicing:{len(bicing)} aire:{len(air_rows)}")

    except Exception as e:
        print(f"[db] error al escribir: {e}")


async def persist_loop():
    """Persiste en DB cada 5 minutos."""
    await asyncio.sleep(10)  # esperar primer poll
    while True:
        await persist_to_db()
        await asyncio.sleep(300)


async def load_bus_stops():
    """Carga todas las paradas de bus TMB una vez al inicio."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                f"https://api.tmb.cat/v1/transit/parades"
                f"?app_id={TMB_APP_ID}&app_key={TMB_APP_KEY}"
            )
        r.raise_for_status()
    except Exception as e:
        print(f"[load_bus_stops] error: {e}")
        return
    stops = []
    for f in r.json().get("features", []):
        p = f["properties"]
        if not f.get("geometry"):
            continue
        lon, lat = f["geometry"]["coordinates"]
        stops.append({
            "stop_id": p["CODI_PARADA"],
            "name": p["NOM_PARADA"],
            "address": p.get("DESC_PARADA", ""),
            "lat": lat,
            "lon": lon,
        })
    cache["bus_stops"]["data"] = stops
    cache["bus_stops"]["updated"] = now_iso()
    print(f"[bus] {len(stops)} parades carregades")


@app.on_event("startup")
async def startup():
    await load_trams_geo()
    await load_bus_stops()
    results = await asyncio.gather(poll_weather(), poll_ships(), poll_beaches(), poll_trains(), poll_obras(), poll_accidents(), return_exceptions=True)
    for name, r in zip(['weather','ships','beaches','trains','obras','accidents'], results):
        if isinstance(r, Exception):
            print(f"[startup] {name} error: {r}")
    asyncio.create_task(poll_arbres())
    asyncio.create_task(poll_poblacio())
    asyncio.create_task(poll_zones_verdes())
    asyncio.create_task(poll_carrega())
    asyncio.create_task(poll_fonts())
    asyncio.create_task(poll_desfibril())
    asyncio.create_task(poll_lavabos())
    asyncio.create_task(poll_equipaments())
    asyncio.create_task(poll_mercats())
    asyncio.create_task(poll_airbnb())
    asyncio.create_task(polling_loop())
    asyncio.create_task(flights_loop())
    asyncio.create_task(camaras_dgt_loop())
    asyncio.create_task(slow_poll_loop())
    asyncio.create_task(snapshots_loop())
    asyncio.create_task(persist_loop())


# ── Config ───────────────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    return {"mapbox_token": MAPBOX_TOKEN}

# ── Endpoints: Tráfico ────────────────────────────────────────────────────────
@app.get("/api/trams")
def get_trams(
    estado: Optional[int] = None,
    min_estado: Optional[int] = None,
    geojson: bool = False,
):
    """
    Todos los trams con estado de tráfico.
    ?estado=4         → solo trams con ese estado exacto
    ?min_estado=3     → trams con estado >= X (denso, muy denso, congestión)
    ?geojson=true     → devuelve FeatureCollection lista para Leaflet/MapboxGL
    """
    data = cache["trams"]["data"]
    if estado is not None:
        data = [t for t in data if t["estado"] == estado]
    if min_estado is not None:
        data = [t for t in data if t["estado"] >= min_estado]

    if geojson:
        features = []
        for t in data:
            if len(t["coords"]) < 2:
                continue
            features.append({
                "type": "Feature",
                "properties": {
                    "id": t["id"],
                    "description": t["description"],
                    "estado": t["estado"],
                    "estado_label": t["estado_label"],
                    "prediccion": t["prediccion"],
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": t["coords"],
                },
            })
        return {
            "type": "FeatureCollection",
            "updated": cache["trams"]["updated"],
            "features": features,
        }

    return {"updated": cache["trams"]["updated"], "trams": data}


@app.get("/api/trams/{tram_id}")
def get_tram(tram_id: int):
    """Estado de un tram específico."""
    for t in cache["trams"]["data"]:
        if t["id"] == tram_id:
            return t
    return JSONResponse(status_code=404, content={"error": "tram not found"})


@app.get("/api/trams/summary/stats")
def get_trams_stats():
    """Distribución de estados — útil para un heatmap global o gauge de ciudad."""
    from collections import Counter
    counts = Counter(t["estado"] for t in cache["trams"]["data"] if t["estado"] > 0)
    total = sum(counts.values())
    return {
        "updated": cache["trams"]["updated"],
        "total_active": total,
        "by_estado": {
            TRAFFIC_LABELS[k]: {"count": v, "pct": round(v / total * 100, 1)}
            for k, v in sorted(counts.items())
        },
        "congestion_index": round(
            sum(k * v for k, v in counts.items()) / max(total, 1), 2
        ),
    }


@app.get("/api/incidents")
def get_incidents():
    """Incidencias de tráfico en texto."""
    raw = cache["incidents"]["data"]
    # Limpiar y partir por puntos o separadores
    incidents = [i.strip() for i in raw.replace("             ", "\n").split("\n") if i.strip()]
    return {"updated": cache["incidents"]["updated"], "incidents": incidents}


# ── Endpoints: Bicing ─────────────────────────────────────────────────────────
@app.get("/api/bicing")
def get_bicing(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius: int = 500,
    min_bikes: Optional[int] = None,
    only_electric: bool = False,
):
    """
    Estaciones Bicing.
    ?lat=41.43&lon=2.14&radius=500  → dentro de N metros
    ?min_bikes=2                     → con al menos N bicis
    ?only_electric=true              → solo estaciones con ebikes
    """
    data = cache["bicing"]["data"]
    if lat is not None and lon is not None:
        data = [
            {**s, "distance_m": round(haversine(lat, lon, s["lat"], s["lon"]))}
            for s in data
            if haversine(lat, lon, s["lat"], s["lon"]) <= radius
        ]
        data.sort(key=lambda x: x["distance_m"])
    if min_bikes is not None:
        data = [s for s in data if s["bikes"] >= min_bikes]
    if only_electric:
        data = [s for s in data if s["bikes_electric"] > 0]
    return {"updated": cache["bicing"]["updated"], "stations": data}


@app.get("/api/bicing/summary/stats")
def get_bicing_stats():
    """Stats globales de Bicing — total bicis disponibles, ocupación, etc."""
    data = cache["bicing"]["data"]
    online = [s for s in data if s["online"]]
    total_bikes = sum(s["bikes"] for s in online)
    total_electric = sum(s["bikes_electric"] for s in online)
    total_slots = sum(s["slots_free"] for s in online)
    total_capacity = total_bikes + total_slots
    return {
        "updated": cache["bicing"]["updated"],
        "stations_total": len(data),
        "stations_online": len(online),
        "bikes_available": total_bikes,
        "bikes_mechanical": sum(s["bikes_mechanical"] for s in online),
        "bikes_electric": total_electric,
        "slots_free": total_slots,
        "occupancy_pct": round(total_bikes / max(total_capacity, 1) * 100, 1),
    }


# ── Endpoints: Calidad del Aire ───────────────────────────────────────────────
@app.get("/api/air")
def get_air(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius: int = 3000,
    pollutant: Optional[str] = None,
):
    """
    Calidad del aire por estación.
    ?lat=&lon=&radius=3000   → estaciones cercanas
    ?pollutant=NO2           → filtrar por contaminante (NO2, PM10, PM2.5, O3)
    """
    data = cache["air"]["data"]
    if lat is not None and lon is not None:
        data = [
            {**s, "distance_m": round(haversine(lat, lon, s["lat"], s["lon"]))}
            for s in data
            if haversine(lat, lon, s["lat"], s["lon"]) <= radius
        ]
        data.sort(key=lambda x: x["distance_m"])
    if pollutant:
        data = [
            {**s, "value": s["pollutants"].get(pollutant)}
            for s in data
            if pollutant in s.get("pollutants", {})
        ]
    return {"updated": cache["air"]["updated"], "stations": data}


@app.get("/api/air/summary/stats")
def get_air_stats():
    """Media de cada contaminante en Barcelona ahora mismo."""
    from collections import defaultdict
    totals: dict = defaultdict(list)
    for s in cache["air"]["data"]:
        for p, info in s.get("pollutants", {}).items():
            totals[p].append(info["value"])
    averages = {
        p: {"avg": round(sum(v) / len(v), 1), "max": max(v), "min": min(v), "n_stations": len(v)}
        for p, v in totals.items()
    }
    return {"updated": cache["air"]["updated"], "pollutants": averages}


# ── Endpoints: TMB ────────────────────────────────────────────────────────────
@app.get("/api/buses/stops")
def get_bus_stops(geojson: bool = False):
    """Todas las paradas de bus TMB."""
    data = cache["bus_stops"]["data"]
    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["bus_stops"]["updated"],
            "features": [
                {
                    "type": "Feature",
                    "properties": {"stop_id": s["stop_id"], "name": s["name"], "address": s["address"]},
                    "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
                }
                for s in data
            ],
        }
    return {"updated": cache["bus_stops"]["updated"], "stops": data}


@app.get("/api/buses/{stop_id}")
async def get_buses(stop_id: int):
    """Próximos buses en tiempo real para una parada TMB."""
    url = f"https://api.tmb.cat/v1/ibus/stops/{stop_id}?app_id={TMB_APP_ID}&app_key={TMB_APP_KEY}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
    if r.status_code != 200:
        return JSONResponse(status_code=r.status_code, content={"error": "TMB error"})
    data = r.json()
    return {
        "stop_id": stop_id,
        "updated": now_iso(),
        "buses": data.get("data", {}).get("ibus", []),
    }


@app.get("/api/buses/stops/nearby")
async def get_stops_nearby(
    lat: float = Query(...),
    lon: float = Query(...),
    radius: int = 400,
):
    """Paradas de bus TMB cercanas a unas coordenadas."""
    url = f"https://api.tmb.cat/v1/transit/parades?app_id={TMB_APP_ID}&app_key={TMB_APP_KEY}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
    features = r.json().get("features", [])
    nearby = []
    for f in features:
        slon, slat = f["geometry"]["coordinates"]
        d = haversine(lat, lon, slat, slon)
        if d <= radius:
            p = f["properties"]
            nearby.append({
                "stop_id": p["CODI_PARADA"],
                "name": p["NOM_PARADA"],
                "address": p["DESC_PARADA"],
                "lat": slat,
                "lon": slon,
                "distance_m": round(d),
            })
    nearby.sort(key=lambda x: x["distance_m"])
    return {"lat": lat, "lon": lon, "radius_m": radius, "stops": nearby}


# ── Endpoint: Todo cerca de un punto ─────────────────────────────────────────
@app.get("/api/nearby")
async def get_nearby(
    lat: float = Query(..., description="Latitud"),
    lon: float = Query(..., description="Longitud"),
    radius: int = Query(500, description="Radio en metros"),
):
    """
    Todo lo relevante cerca de un punto: bicing, tráfico, aire.
    Útil para el display o para calcular rutas.
    """
    # Bicing
    bicing_nearby = [
        {**s, "distance_m": round(haversine(lat, lon, s["lat"], s["lon"]))}
        for s in cache["bicing"]["data"]
        if haversine(lat, lon, s["lat"], s["lon"]) <= radius
    ]
    bicing_nearby.sort(key=lambda x: x["distance_m"])

    # Trams con centroide cercano
    trams_nearby = []
    for t in cache["trams"]["data"]:
        if not t.get("centroid"):
            continue
        tlon, tlat = t["centroid"]
        d = haversine(lat, lon, tlat, tlon)
        if d <= radius * 2:  # trams tienen más spread
            trams_nearby.append({**t, "distance_m": round(d)})
    trams_nearby.sort(key=lambda x: x["distance_m"])

    # Aire (radio mayor, las estaciones están más separadas)
    air_nearby = [
        {**s, "distance_m": round(haversine(lat, lon, s["lat"], s["lon"]))}
        for s in cache["air"]["data"]
        if haversine(lat, lon, s["lat"], s["lon"]) <= 5000
    ]
    air_nearby.sort(key=lambda x: x["distance_m"])

    return {
        "lat": lat,
        "lon": lon,
        "radius_m": radius,
        "updated": now_iso(),
        "bicing": bicing_nearby[:5],
        "trams": trams_nearby[:10],
        "air": air_nearby[:3],
    }


# ── Endpoint: Snapshot global ─────────────────────────────────────────────────
@app.get("/api/snapshot")
def get_snapshot():
    """Estado completo de la ciudad en un solo objeto. Para dashboards."""
    trams = cache["trams"]["data"]
    active = [t for t in trams if t["estado"] > 0]
    return {
        "updated": now_iso(),
        "traffic": {
            "updated": cache["trams"]["updated"],
            "congestion_index": round(
                sum(t["estado"] for t in active) / max(len(active), 1), 2
            ),
            "by_estado": {
                TRAFFIC_LABELS[i]: sum(1 for t in active if t["estado"] == i)
                for i in range(1, 6)
            },
        },
        "bicing": {
            "updated": cache["bicing"]["updated"],
            "bikes_available": sum(s["bikes"] for s in cache["bicing"]["data"]),
            "bikes_electric": sum(s["bikes_electric"] for s in cache["bicing"]["data"]),
        },
        "air": {
            "updated": cache["air"]["updated"],
            "stations": len(cache["air"]["data"]),
        },
    }


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "cache": {k: {"updated": v["updated"]} for k, v in cache.items()},
    }


# ── Metro ─────────────────────────────────────────────────────────────────────
METRO_LINE_COLORS = {
    "L1": "#E3000B", "L2": "#9B2D8E", "L3": "#007B43", "L4": "#FFCC00",
    "L5": "#0033A0", "L9N": "#F77F00", "L9S": "#F77F00", "L10N": "#00A6D6",
    "L10S": "#00A6D6", "L11": "#9BC73A", "L12": "#B47F3B",
}

cache["metro_stations"] = {"data": [], "updated": None}
cache["metro_lines"] = {"data": [], "updated": None}
cache["bus_stops"] = {"data": [], "updated": None}
cache["flights"] = {"data": [], "updated": None}
cache["weather"] = {"data": {}, "updated": None}
cache["ships"] = {"data": [], "updated": None}
cache["trains"] = {"data": [], "updated": None}
cache["beaches"] = {"data": [], "updated": None}
cache["obras"] = {"data": [], "updated": None}
cache["camaras_dgt"] = {"data": [], "updated": None}

# Càmeres SCT (Generalitat) — coords exactes via Cercalia API (bbox Barcelona metropolitana)
CAMARAS_SCT = [
    # (sct_id, carretera, nom, lon, lat)
    ("sc60.jpg", "A-2", "A-2 (Pk 560.58)", 1.68214927, 41.59419814),
    ("sc23.jpg", "C-32", "C-32 (Pk 43.46)", 1.95275776, 41.26731322),
    ("sc73.jpg", "C-32", "C-32 (Pk 46.9)", 1.98966056, 41.27929528),
    ("sc44.jpg", "B-23", "B-23 (Pk 15.2)", 1.99412518, 41.44633828),
    ("sc18.jpg", "C-31", "C-31 (Pk 182.65)", 1.99965881, 41.2699904),
    ("sc43.jpg", "B-23", "B-23 (Pk 13.57)", 2.00438394, 41.43493475),
    ("rc3.jpg", "C-16", "C-16 (Pk 22.35)", 2.006486, 41.5504178),
    ("sc40.jpg", "B-23", "B-23 (Pk 11.1)", 2.01125606, 41.41351471),
    ("nc29.jpg", "C-58", "C-58 (Pk 20.06)", 2.01770596, 41.54313617),
    ("sc72.jpg", "C-32", "C-32 (Pk 50.37)", 2.02120041, 41.29896464),
    ("sc37.jpg", "B-23", "B-23 (Pk 7.38)", 2.02896185, 41.38129689),
    ("sc67.jpg", "B-23", "B-23 (Pk 6.14)", 2.03784619, 41.37396227),
    ("sc59.jpg", "A-2", "A-2 (Pk 606.09)", 2.04238268, 41.36891415),
    ("nc28.jpg", "C-58", "C-58 (Pk 17.73)", 2.04398168, 41.54401351),
    ("rc2.jpg", "C-16", "C-16 (Pk 14.85)", 2.04971293, 41.49748317),
    ("sc33.jpg", "B-23", "B-23 (Pk 3.11)", 2.05939555, 41.37536978),
    ("sc65.jpg", "B-20", "B-20 (Pk 0.83)", 2.05966469, 41.32984026),
    ("sc71.jpg", "C-32", "C-32 (Pk 54.3)", 2.06175637, 41.33534905),
    ("sc64.jpg", "B-20", "B-20 (Pk 1.65)", 2.07222286, 41.32781536),
    ("sc12.jpg", "C-31", "C-31 (Pk 188.94)", 2.07473413, 41.30817566),
    ("nc80.jpg", "AP-7", "AP-7 (Pk 154)", 2.07616609, 41.47571856),
    ("sc70.jpg", "C-31", "C-31 (Pk 192.18)", 2.08051002, 41.29907063),
    ("rc1.jpg", "C-16", "C-16 (Pk 4.76)", 2.08441508, 41.42286085),
    ("nc20.jpg", "C-58", "C-58 (Pk 15.24)", 2.09152622, 41.53490494),
    ("sc69.jpg", "C-31", "C-31 (Pk 195.03)", 2.09797941, 41.33593607),
    ("sc5.jpg", "C-31", "C-31 (Pk 195.6)", 2.10125826, 41.33995092),
    ("sc29.jpg", "B-23", "B-23 (Pk 0)", 2.10716918, 41.38256887),
    ("sc74.jpg", "C-31", "C-31 (Pk 196.93)", 2.11398739, 41.34789866),
    ("nc15.jpg", "C-58", "C-58 (Pk 7.08)", 2.14170042, 41.50420355),
    ("nc78.jpg", "AP-7", "AP-7 (Pk 146.5)", 2.14493435, 41.51579061),
    ("nc89.jpg", "C-58", "C-58 (Pk 6.15)", 2.14807846, 41.49834845),
    ("nc32.jpg", "C-58", "C-58 (Pk 4.59)", 2.16045724, 41.48727161),
    ("nc88.jpg", "C-58", "C-58 (Pk 4)", 2.16076267, 41.48289438),
    ("nc85.jpg", "C-58", "C-58 (Pk 1.82)", 2.17447096, 41.46672712),
    ("nc87.jpg", "C-58", "C-58 (Pk 0.5)", 2.1849543, 41.45989551),
    ("nc86.jpg", "Meridiana", "Meridiana (Pk 0)", 2.18923926, 41.45271175),
    ("nc27.jpg", "C-33", "C-33 (Pk 84.02)", 2.2029206, 41.50742464),
    ("nc62.jpg", "B-20", "B-20 (Pk 18.41)", 2.20780164, 41.45580745),
    ("nc2.jpg", "C-31", "C-31 (Pk 209.13)", 2.21762603, 41.42452988),
    ("nc91.gif", "C-17", "C-17 (Pk 12.05)", 2.22468678, 41.5425895),
    ("nc3.jpg", "C-31", "C-31 (Pk 210.15)", 2.22474967, 41.43189256),
    ("nc81.jpg", "B-20", "B-20 (Pk 20)", 2.2315589, 41.46109836),
    ("nc82.jpg", "C-31", "C-31 (Pk 211.64)", 2.23201704, 41.44322228),
    ("nc64.jpg", "B-20", "B-20 (Pk 21.2)", 2.2427519, 41.46599061),
    ("nc83.jpg", "C-31", "C-31 (Pk 213.55)", 2.2487257, 41.45612465),
    ("nc92.gif", "C-17", "C-17 (Pk 20.5)", 2.26414079, 41.60799388),
    ("nc8.jpg", "C-31", "C-31 (Pk 215.81)", 2.27239631, 41.46464595),
    ("nc67.jpg", "B-20", "B-20 (Pk 24.65)", 2.2796547, 41.47105815),
    ("nc10.jpg", "C-32", "C-32 (Pk 85.05)", 2.29594115, 41.48348883),
    ("sc58.jpg", "A-2", "A-2 (Pk 608.13)", 2.05, 41.368),
]
cache["camaras_sct"] = {
    "data": [
        {
            "id": sct_id,
            "carretera": carretera,
            "nom": nom,
            "lon": lon,
            "lat": lat,
            "imagen_url": f"https://mct.gencat.cat/mct2bo/RenderService?sctidcam={sct_id}",
        }
        for sct_id, carretera, nom, lon, lat in CAMARAS_SCT
    ],
    "updated": now_iso(),
}

# Càmeres Ajuntament de Barcelona — GIFs de bcn.cat/transit, coords+noms exactes via Cercalia API
CAMARAS_BCN = [
    # (img_filename, nom, lon, lat)
    ("RondadeDaltCrtaEsplugues.gif", "Ronda de Dalt - Ctra. Esplugues", 2.10813038, 41.3892869),
    ("DiagonalMCristina.gif", "Diagonal - Maria Cristina", 2.12690517, 41.38859686),
    ("PlCerda.gif", "Plaça Cerdà", 2.13499, 41.3647116),
    ("RondadeDaltSantGervasi.gif", "Ronda de Dalt - Sant Gervasi", 2.13689443, 41.41544876),
    ("BalmesMitre.gif", "Balmes - Mitre", 2.14059549, 41.40446586),
    ("PlPaissosCatalans.gif", "Plaça Països Catalans", 2.14270653, 41.38016698),
    ("RondaLitoralZonaFranca.gif", "Ronda Litoral - Zona Franca", 2.14686573, 41.3494556),
    ("PlMolina.gif", "Plaça Molina", 2.14771913, 41.40115845),
    ("PlEspanya.gif", "Plaça Espanya", 2.14948881, 41.37467276),
    ("RondadeDaltVelodrom.gif", "Ronda de Dalt - Velòdrom", 2.14971339, 41.43707771),
    ("AragoPgGracia.gif", "Aragó - Passeig de Gràcia", 2.16502967, 41.39226345),
    ("BalmesGranVia.gif", "Balmes - Gran Via", 2.16551476, 41.38710852),
    ("TunelRovira.gif", "Túnel Rovira", 2.16617951, 41.41456966),
    ("PlCatalunya.gif", "Plaça Catalunya", 2.17014108, 41.38575545),
    ("PlUrquinaona.gif", "Plaça Urquinaona", 2.17215331, 41.38884717),
    ("RondaLitoralMollFusta.gif", "Ronda Litoral - Moll de la Fusta", 2.17896254, 41.37657414),
    ("GranViaMarina.gif", "Gran Via - Marina", 2.18120832, 41.39917663),
    ("PlAntonioLopez.gif", "Plaça Antonio López", 2.18172935, 41.38176374),
    ("PlPauVila.gif", "Plaça Pau Vila", 2.18522379, 41.38162165),
    ("MeridianaFelipII.gif", "Meridiana - Felip II", 2.18688568, 41.42045266),
    ("MeridianaRioJaneiro.gif", "Meridiana - Rio de Janeiro", 2.18695754, 41.44332367),
    ("RondadeDaltMeridiana.gif", "Ronda de Dalt - Meridiana", 2.18749653, 41.44696687),
    ("MarinaPujades.gif", "Marina - Pujades", 2.1891135, 41.39314962),
    ("DiagonalCiutatdeGranada.gif", "Diagonal - Ciutat de Granada", 2.19057775, 41.40410063),
    ("GranViaBacRoda.gif", "Gran Via - Bac de Roda", 2.19843801, 41.4117091),
    ("RondaLitoralBadajoz.gif", "Ronda Litoral - Badajoz", 2.20289366, 41.39293992),
    ("RondaLitoralBonPastor.gif", "Ronda Litoral - Bon Pastor", 2.21237986, 41.43028353),
]
cache["camaras_bcn"] = {
    "data": [
        {
            "id": img,
            "nom": nom,
            "lon": lon,
            "lat": lat,
            "imagen_url": f"https://www.bcn.cat/transit/imatges/{img}",
        }
        for img, nom, lon, lat in CAMARAS_BCN
    ],
    "updated": now_iso(),
}


async def poll_metro():
    async with httpx.AsyncClient(timeout=15) as client:
        # Estaciones con coordenadas reales y CODI_ESTACIO para tiempos en tiempo real
        r_est = await client.get(
            f"https://api.tmb.cat/v1/transit/linies/metro/estacions"
            f"?app_id={TMB_INT_APP_ID}&app_key={TMB_INT_APP_KEY}"
            f"&maxFeatures=500&srsName=EPSG:4326"
        )
        # Líneas
        r_lin = await client.get(
            f"https://api.tmb.cat/v1/transit/linies/metro"
            f"?app_id={TMB_INT_APP_ID}&app_key={TMB_INT_APP_KEY}"
        )

    # Agrupar estaciones por CODI_ESTACIO (pueden aparecer en varias líneas)
    station_map: dict = {}
    for f in r_est.json().get("features", []):
        p = f["properties"]
        codi = p["CODI_ESTACIO"]
        coords = f.get("geometry") and f["geometry"].get("coordinates")
        if not coords:
            continue
        lon, lat = coords
        if codi not in station_map:
            station_map[codi] = {
                "codi_estacio": codi,
                "codi_grup": p["CODI_GRUP_ESTACIO"],
                "name": p["NOM_ESTACIO"],
                "lines": [],
                "lat": lat,
                "lon": lon,
            }
        nom_linia = p.get("NOM_LINIA", "")
        if nom_linia and nom_linia not in station_map[codi]["lines"]:
            station_map[codi]["lines"].append(nom_linia)

    stations = []
    for s in station_map.values():
        first_line = s["lines"][0] if s["lines"] else ""
        stations.append({
            **s,
            "color": METRO_LINE_COLORS.get(first_line, "#888888"),
        })

    lines = []
    for f in r_lin.json().get("features", []):
        p = f["properties"]
        lines.append({
            "id": p["ID_LINIA"],
            "code": p["CODI_LINIA"],
            "name": p["NOM_LINIA"],
            "origin": p["ORIGEN_LINIA"],
            "destination": p["DESTI_LINIA"],
            "color": "#" + p.get("COLOR_LINIA", "888888"),
        })

    cache["metro_stations"]["data"] = stations
    cache["metro_stations"]["updated"] = now_iso()
    cache["metro_lines"]["data"] = lines
    cache["metro_lines"]["updated"] = now_iso()


@app.get("/api/metro/stations")
def get_metro_stations(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius: int = 1000,
    geojson: bool = False,
):
    """
    Estaciones de metro con líneas y colores.
    ?geojson=true          → FeatureCollection para Mapbox
    ?lat=&lon=&radius=1000 → filtrar por proximidad
    """
    data = cache["metro_stations"]["data"]
    if lat is not None and lon is not None:
        data = [
            {**s, "distance_m": round(haversine(lat, lon, s["lat"], s["lon"]))}
            for s in data
            if haversine(lat, lon, s["lat"], s["lon"]) <= radius
        ]
        data.sort(key=lambda x: x["distance_m"])

    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["metro_stations"]["updated"],
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "id": s["codi_estacio"],
                        "name": s["name"],
                        "lines": ", ".join(s["lines"]),
                        "color": s["color"],
                    },
                    "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
                }
                for s in data
            ],
        }

    return {"updated": cache["metro_stations"]["updated"], "stations": data}


@app.get("/api/metro/lines")
def get_metro_lines():
    """Estado y info de las líneas de metro."""
    return {"updated": cache["metro_lines"]["updated"], "lines": cache["metro_lines"]["data"]}


@app.get("/api/metro/arrivals/{codi_estacio}")
async def get_metro_arrivals(codi_estacio: int, codi_linia: Optional[int] = None):
    """
    Próximos trenes en tiempo real para una estación de metro.
    codi_estacio: código de estación (e.g. 328 para Diagonal)
    ?codi_linia=3  → filtrar por línea (opcional)
    """
    url = (
        f"https://api.tmb.cat/v1/itransit/metro/estacions/{codi_estacio}"
        f"?app_id={TMB_INT_APP_ID}&app_key={TMB_INT_APP_KEY}&temps_teoric=true"
    )
    if codi_linia:
        url += f"&codi_linia={codi_linia}"

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url)
    if r.status_code != 200:
        return JSONResponse(status_code=r.status_code, content={"error": "TMB error"})

    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    result = []
    for linia in r.json().get("linies", []):
        for via in linia.get("estacions", []):
            for traj in via.get("linies_trajectes", []):
                trens = []
                for t in traj.get("propers_trens", []):
                    wait_s = max(0, round((t["temps_arribada"] - now_ms) / 1000))
                    trens.append({
                        "wait_s": wait_s,
                        "wait_min": round(wait_s / 60, 1),
                        "is_real": not t.get("temps_teoric", False),
                    })
                result.append({
                    "linia": traj["nom_linia"],
                    "color": "#" + traj["color_linia"],
                    "desti": traj["desti_trajecte"],
                    "via": via["codi_via"],
                    "propers_trens": trens[:4],
                })

    return {"codi_estacio": codi_estacio, "updated": now_iso(), "arrivals": result}


# ── Metro train positions ─────────────────────────────────────────────────────

_METRO_LINES: dict = {}

def _load_metro_lines():
    global _METRO_LINES
    path = os.path.join(os.path.dirname(__file__), "static", "metro_lines.json")
    try:
        with open(path) as f:
            _METRO_LINES = json.load(f)
    except Exception as e:
        print(f"[metro_trains] no s'ha pogut carregar metro_lines.json: {e}")

_load_metro_lines()


def _bearing_at_t(coords: list, t: float) -> float:
    """Bearing en graus clockwise des de N a la fracció t de la polilínia."""
    if not coords or len(coords) < 2:
        return 0.0
    segs, total = [], 0.0
    for i in range(len(coords) - 1):
        d = math.hypot(coords[i+1][0]-coords[i][0], coords[i+1][1]-coords[i][1])
        segs.append(d)
        total += d
    if total == 0:
        return 0.0
    target = t * total
    acc = 0.0
    for i, d in enumerate(segs):
        if acc + d >= target or i == len(segs) - 1:
            dx = coords[i+1][0] - coords[i][0]
            dy = coords[i+1][1] - coords[i][1]
            return (math.degrees(math.atan2(dx, dy)) + 360) % 360
        acc += d
    return 0.0


def _point_on_polyline(coords: list, t: float) -> tuple[float, float]:
    """Interpola un punt a la fracció t [0,1] al llarg d'una polilínia de coords [[lon,lat],...]."""
    if not coords or len(coords) < 2:
        return coords[0] if coords else (0, 0)
    # Calcular longitud total
    segs = []
    total = 0.0
    for i in range(len(coords) - 1):
        dx = coords[i+1][0] - coords[i][0]
        dy = coords[i+1][1] - coords[i][1]
        d = math.hypot(dx, dy)
        segs.append(d)
        total += d
    if total == 0:
        return tuple(coords[0])
    target = t * total
    acc = 0.0
    for i, d in enumerate(segs):
        if acc + d >= target or i == len(segs) - 1:
            frac = (target - acc) / d if d > 0 else 0
            lon = coords[i][0] + frac * (coords[i+1][0] - coords[i][0])
            lat = coords[i][1] + frac * (coords[i+1][1] - coords[i][1])
            return lon, lat
        acc += d
    return tuple(coords[-1])


def _nearest_t_for_station(coords: list, slon: float, slat: float) -> float:
    """Troba la fracció t de la polilínia més propera a les coords d'una estació."""
    if not coords:
        return 0.0
    segs_len = []
    total = 0.0
    for i in range(len(coords) - 1):
        d = math.hypot(coords[i+1][0]-coords[i][0], coords[i+1][1]-coords[i][1])
        segs_len.append(d)
        total += d
    if total == 0:
        return 0.0
    best_t = 0.0
    best_dist = float("inf")
    acc = 0.0
    for i, d in enumerate(segs_len):
        # Projecció del punt sobre el segment
        ax, ay = coords[i]
        bx, by = coords[i+1]
        px, py = slon, slat
        abx, aby = bx - ax, by - ay
        apx, apy = px - ax, py - ay
        ab2 = abx*abx + aby*aby
        frac = max(0.0, min(1.0, (apx*abx + apy*aby) / ab2)) if ab2 > 0 else 0.0
        cx, cy = ax + frac*abx, ay + frac*aby
        dist = math.hypot(px-cx, py-cy)
        if dist < best_dist:
            best_dist = dist
            best_t = (acc + frac * d) / total
        acc += d
    return best_t


@app.get("/api/metro/trains")
async def get_metro_trains():
    """
    Posicions aproximades dels trens de metro en temps real.
    Interpola entre estacions basant-se en ETAs i geometria real de les vies.
    """
    if not _METRO_LINES:
        return {"trains": [], "updated": now_iso(), "error": "metro_lines.json no carregat"}

    stations = cache["metro_stations"]["data"]
    now_ms = datetime.now(timezone.utc).timestamp() * 1000

    # Buscar ETAs de totes les estacions en paral·lel
    async def fetch_station(codi: int):
        url = (f"https://api.tmb.cat/v1/itransit/metro/estacions/{codi}"
               f"?app_id={TMB_INT_APP_ID}&app_key={TMB_INT_APP_KEY}&temps_teoric=true")
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(url)
            if r.status_code != 200:
                return codi, []
            arrivals = []
            for linia in r.json().get("linies", []):
                for via in linia.get("estacions", []):
                    for traj in via.get("linies_trajectes", []):
                        for t in traj.get("propers_trens", [])[:2]:
                            wait_s = (t["temps_arribada"] - now_ms) / 1000
                            if -30 < wait_s < 1200:
                                arrivals.append({
                                    "linia": traj["nom_linia"],
                                    "desti": traj["desti_trajecte"],
                                    "servei": t.get("codi_servei"),
                                    "eta_s": wait_s,
                                    "is_real": not t.get("temps_teoric", False),
                                })
            return codi, arrivals
        except Exception:
            return codi, []

    tasks = [fetch_station(s["codi_estacio"]) for s in stations]
    results = await asyncio.gather(*tasks)
    station_etas: dict[int, list] = {codi: arr for codi, arr in results}

    # Per cada línia, calcular posicions de trens
    trains = []
    for nom_linia, line_data in _METRO_LINES.items():
        coords = line_data.get("geometry_coords", [])
        ordered_stations = line_data.get("stations", [])
        color = line_data.get("color", "#888")
        if not coords or not ordered_stations:
            continue

        # Mapeig codi_estacio → t (fracció a la polilínia)
        st_t = {}
        for st in ordered_stations:
            t = _nearest_t_for_station(coords, st["lon"], st["lat"])
            st_t[st["codi"]] = t

        # t de la primera i última estació per determinar sentit de la geometria
        t_first = st_t.get(ordered_stations[0]["codi"], 0.0)
        t_last = st_t.get(ordered_stations[-1]["codi"], 1.0)
        nom_desti = ordered_stations[-1]["nom"]   # última estació = terminus "desti"
        nom_origen = ordered_stations[0]["nom"]   # primera estació = terminus "origen"

        # temps inter-estació basat en distància real de la geometria
        n_st = len(ordered_stations)
        avg_inter_s = 90.0  # ~90s entre estacions (aprox. 40 km/h)
        total_s = n_st * avg_inter_s

        tren_positions: dict[str, dict] = {}
        for st in ordered_stations:
            codi = st["codi"]
            t_st = st_t.get(codi, 0.0)
            etas = station_etas.get(codi, [])
            for arr in etas:
                if arr["linia"] != nom_linia:
                    continue
                eta_s = arr["eta_s"]
                servei = arr.get("servei") or arr["desti"]
                key = f"{nom_linia}_{servei}_{arr['desti']}"
                delta_t = (eta_s / total_s) * abs(t_last - t_first)

                # Sentit: si el destí del tren és el terminus final de la línia,
                # el tren va cap a t_last (creixent si t_last>t_first)
                desti = arr["desti"]
                toward_end = any(d in desti for d in [nom_desti, line_data.get("desti","")]) \
                             and not any(d in desti for d in [nom_origen, line_data.get("origen","")])

                if t_last > t_first:
                    # geometria: t creix d'origen a desti
                    t_tren = max(0.0, min(1.0, t_st - delta_t)) if toward_end \
                             else max(0.0, min(1.0, t_st + delta_t))
                else:
                    # geometria inversa (no hauria de passar, però per seguretat)
                    t_tren = max(0.0, min(1.0, t_st + delta_t)) if toward_end \
                             else max(0.0, min(1.0, t_st - delta_t))

                if key not in tren_positions or tren_positions[key]["is_real"] < arr["is_real"]:
                    lon, lat = _point_on_polyline(coords, t_tren)
                    tren_positions[key] = {
                        "linia": nom_linia,
                        "color": color,
                        "desti": desti,
                        "lon": round(lon, 6),
                        "lat": round(lat, 6),
                        "eta_s": round(eta_s),
                        "is_real": arr["is_real"],
                        "t": round(t_tren, 4),
                        "bearing": round((_bearing_at_t(coords, t_tren) + (0 if toward_end else 180)) % 360, 1),
                    }

        trains.extend(tren_positions.values())

    return {"trains": trains, "count": len(trains), "updated": now_iso()}


WMO_CODES = {
    0:"Cel clar",1:"Principalment clar",2:"Parcialment ennuvolat",3:"Ennuvolat",
    45:"Boira",48:"Boira amb gebre",51:"Plugim lleuger",53:"Plugim moderat",55:"Plugim dens",
    61:"Pluja lleugera",63:"Pluja moderada",65:"Pluja forta",
    71:"Neu lleugera",73:"Neu moderada",75:"Neu forta",
    80:"Ruixats lleugers",81:"Ruixats moderats",82:"Ruixats violents",
    95:"Tempesta",96:"Tempesta amb pedra",99:"Tempesta forta amb pedra",
}

@app.get("/api/weather")
def get_weather():
    """Temps actual a Barcelona (Open-Meteo)."""
    d = cache["weather"]["data"]
    return {
        "updated": cache["weather"]["updated"],
        **d,
        "description": WMO_CODES.get(d.get("weather_code", 0), "Desconegut"),
    }


@app.get("/api/ships")
def get_ships(geojson: bool = False):
    """Vaixells al Port de Barcelona avui."""
    data = cache["ships"]["data"]
    if geojson:
        features = []
        for s in data:
            if not s.get("coords"):
                continue
            features.append({
                "type": "Feature",
                "properties": {
                    "name": s["name"], "country": s["country"],
                    "berth": s["berth"], "etd": s["etd"],
                    "operator": s["operator"], "length": s["length"],
                },
                "geometry": {"type": "Point", "coordinates": s["coords"]},
            })
        return {"type": "FeatureCollection", "updated": cache["ships"]["updated"], "features": features}
    return {"updated": cache["ships"]["updated"], "ships": data, "total": len(data)}


@app.get("/api/trains")
def get_trains(geojson: bool = False):
    """Trens Renfe Rodalies en temps real a la zona BCN."""
    data = cache["trains"]["data"]
    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["trains"]["updated"],
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "id": t["id"],
                        "label": t["label"],
                        "status": t["status"],
                        "stop_id": t["stop_id"],
                    },
                    "geometry": {"type": "Point", "coordinates": [t["lon"], t["lat"]]},
                }
                for t in data
            ],
        }
    return {"updated": cache["trains"]["updated"], "trains": data, "total": len(data)}


@app.get("/api/obras")
def get_obras(geojson: bool = False, estat: Optional[str] = None):
    """Obres a la via pública de Barcelona."""
    data = cache["obras"]["data"]
    if estat:
        data = [o for o in data if o["estat"] == estat]
    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["obras"]["updated"],
            "features": [
                {
                    "type": "Feature",
                    "properties": {k: v for k, v in o.items() if k != "coords"},
                    "geometry": {"type": "Point", "coordinates": o["coords"]},
                }
                for o in data if o.get("coords")
            ],
        }
    return {"updated": cache["obras"]["updated"], "obras": data, "total": len(data)}


@app.get("/api/beaches")
def get_beaches():
    """Estat de les platges metropolitanes de Barcelona."""
    return {"updated": cache["beaches"]["updated"], "beaches": cache["beaches"]["data"]}


@app.get("/api/arbres")
def get_arbres(geojson: bool = True, especie: Optional[str] = None):
    """145k arbres de Barcelona amb espècie i ubicació."""
    data = cache["arbres"]["data"]
    if especie:
        data = [a for a in data if especie.lower() in (a.get("especie") or "").lower()]
    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["arbres"]["updated"],
            "features": [{
                "type": "Feature",
                "properties": {k: v for k, v in a.items() if k not in ("lon", "lat")},
                "geometry": {"type": "Point", "coordinates": [a["lon"], a["lat"]]}
            } for a in data]
        }
    return {"updated": cache["arbres"]["updated"], "count": len(data), "arbres": data}


@app.get("/api/accidents")
def get_accidents(geojson: bool = True, gravetat: Optional[str] = None):
    """Accidents de trànsit 2024-2025 a Barcelona."""
    data = cache["accidents"]["data"]
    if gravetat:
        data = [a for a in data if a.get("gravetat") == gravetat]
    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["accidents"]["updated"],
            "features": [{
                "type": "Feature",
                "properties": {k: v for k, v in a.items() if k not in ("lon", "lat")},
                "geometry": {"type": "Point", "coordinates": [a["lon"], a["lat"]]}
            } for a in data]
        }
    return {"updated": cache["accidents"]["updated"], "count": len(data)}


@app.get("/api/carrega")
def get_carrega(geojson: bool = True):
    data = cache["carrega"]["data"]
    if not geojson:
        return {"updated": cache["carrega"]["updated"], "count": len(data)}
    return {"type":"FeatureCollection","features":[{
        "type":"Feature",
        "geometry":{"type":"Point","coordinates":[s["lon"],s["lat"]]},
        "properties":{k:v for k,v in s.items() if k not in ("lat","lon")}
    } for s in data]}


@app.get("/api/calor")
def get_calor():
    path = "static/calor.geojson"
    if not os.path.exists(path):
        return {"type":"FeatureCollection","features":[]}
    with open(path) as f:
        data = json.load(f)
    # Injectar temperatura actual del cache (Open-Meteo)
    weather = cache.get("weather", {}).get("data") or {}
    base_temp = weather.get("temperature") if weather else None
    if base_temp is not None:
        for feat in data["features"]:
            offset = feat["properties"].get("offset", 0)
            feat["properties"]["temp"] = round(base_temp + offset, 1)
    return JSONResponse(content=data)


@app.get("/api/soroll")
def get_soroll():
    path = "static/soroll.geojson"
    if not os.path.exists(path):
        return {"type":"FeatureCollection","features":[]}
    with open(path) as f:
        return JSONResponse(content=json.load(f))


@app.get("/api/zones_verdes")
def get_zones_verdes():
    data = cache["zones_verdes"]["data"]
    if not data:
        return {"type":"FeatureCollection","features":[]}
    return JSONResponse(content=data)


@app.get("/api/poblacio")
def get_poblacio():
    data = cache["poblacio"]["data"]
    if not data:
        return {"type":"FeatureCollection","features":[]}
    return JSONResponse(content=data)


@app.get("/api/fonts")
def get_fonts():
    data = cache["fonts"]["data"]
    return JSONResponse(content=data if data else {"type":"FeatureCollection","features":[]})


@app.get("/api/desfibril")
def get_desfibril():
    data = cache["desfibril·ladors"]["data"]
    return JSONResponse(content=data if data else {"type":"FeatureCollection","features":[]})


@app.get("/api/lavabos")
def get_lavabos():
    data = cache["lavabos"]["data"]
    return JSONResponse(content=data if data else {"type":"FeatureCollection","features":[]})


@app.get("/api/equipaments")
def get_equipaments():
    data = cache["equipaments"]["data"]
    return JSONResponse(content=data if data else {"type":"FeatureCollection","features":[]})


@app.get("/api/mercats")
def get_mercats():
    data = cache["mercats"]["data"]
    return JSONResponse(content=data if data else {"type":"FeatureCollection","features":[]})


@app.get("/api/airbnb")
def get_airbnb():
    data = cache["airbnb"]["data"]
    return JSONResponse(content=data if data else {"type":"FeatureCollection","features":[]})


@app.get("/api/flights")
def get_flights(geojson: bool = False):
    """Aviones en tiempo real sobre Barcelona (OpenSky Network)."""
    data = [f for f in cache["flights"]["data"] if not f["on_ground"]]
    if geojson:
        return {
            "type": "FeatureCollection",
            "updated": cache["flights"]["updated"],
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "callsign": f["callsign"],
                        "country": f["country"],
                        "altitude_m": f["altitude_m"],
                        "altitude_ft": round(f["altitude_m"] * 3.281) if f["altitude_m"] else None,
                        "velocity_kmh": round(f["velocity_ms"] * 3.6) if f["velocity_ms"] else None,
                        "heading": f["heading"],
                        "vertical_rate": f["vertical_rate"],
                    },
                    "geometry": {"type": "Point", "coordinates": [f["lon"], f["lat"]]},
                }
                for f in data
            ],
        }
    return {"updated": cache["flights"]["updated"], "flights": data}


@app.get("/api/camaras_sct")
def get_camaras_sct(geojson: bool = False):
    """Càmeres SCT (Generalitat) a carreteres catalanes."""
    data = cache["camaras_sct"]["data"]
    if geojson:
        features = [
            {
                "type": "Feature",
                "properties": {
                    "id": c["id"],
                    "nom": c["nom"],
                    "carretera": c["carretera"],
                    "imagen_url": c["imagen_url"],
                },
                "geometry": {"type": "Point", "coordinates": [c["lon"], c["lat"]]},
            }
            for c in data
        ]
        return {"type": "FeatureCollection", "updated": cache["camaras_sct"]["updated"], "features": features}
    return {"updated": cache["camaras_sct"]["updated"], "count": len(data), "camaras": data}


@app.get("/api/camaras_dgt")
def get_camaras_dgt(geojson: bool = False):
    """Càmeres DGT a Catalunya."""
    data = cache["camaras_dgt"]["data"]
    if geojson:
        features = [
            {
                "type": "Feature",
                "properties": {
                    "id": c["id"],
                    "carretera": c["carretera"],
                    "pk": c["pk"],
                    "sentido": c["sentido"],
                    "imagen_url": c["imagen_url"],
                },
                "geometry": {"type": "Point", "coordinates": [c["lon"], c["lat"]]},
            }
            for c in data
        ]
        return {"type": "FeatureCollection", "updated": cache["camaras_dgt"]["updated"], "features": features}
    return {"updated": cache["camaras_dgt"]["updated"], "count": len(data), "camaras": data}


@app.get("/api/camaras_bcn")
def get_camaras_bcn(geojson: bool = False):
    """Càmeres de trànsit de l'Ajuntament de Barcelona."""
    data = cache["camaras_bcn"]["data"]
    if geojson:
        features = [
            {
                "type": "Feature",
                "properties": {
                    "id": c["id"],
                    "nom": c["nom"],
                    "imagen_url": c["imagen_url"],
                    "source": "Ajuntament de Barcelona",
                },
                "geometry": {"type": "Point", "coordinates": [c["lon"], c["lat"]]},
            }
            for c in data
        ]
        return {"type": "FeatureCollection", "updated": cache["camaras_bcn"]["updated"], "features": features}
    return {"updated": cache["camaras_bcn"]["updated"], "count": len(data), "camaras": data}


BSM_STREAMS = {"llevant", "garbi"}
BSM_BASE = "https://streamingcameresport.bsmsa.eu/videos"


@app.get("/api/hls/{name}/index.m3u8")
async def hls_proxy_playlist(name: str):
    """Proxy m3u8 de BSM reescrivint els segments per evitar CORS."""
    if name not in BSM_STREAMS:
        return JSONResponse(status_code=404, content={"error": "stream not found"})
    try:
        async with httpx.AsyncClient(timeout=10, verify=False) as client:
            r = await client.get(f"{BSM_BASE}/{name}.m3u8")
        lines = []
        for line in r.text.splitlines():
            if line.endswith(".ts"):
                lines.append(f"/api/hls/{name}/{line}")
            else:
                lines.append(line)
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("\n".join(lines), media_type="application/vnd.apple.mpegurl",
                                 headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "no-cache"})
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@app.get("/api/hls/{name}/{segment}")
async def hls_proxy_segment(name: str, segment: str):
    """Proxy segments .ts de BSM."""
    if name not in BSM_STREAMS or not segment.endswith(".ts"):
        return JSONResponse(status_code=404, content={"error": "not found"})
    try:
        async with httpx.AsyncClient(timeout=10, verify=False) as client:
            r = await client.get(f"{BSM_BASE}/{segment}")
        from fastapi.responses import Response
        return Response(content=r.content, media_type="video/mp2t",
                        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "max-age=60"})
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@app.get("/api/camaras_live")
def get_camaras_live():
    """Càmeres live: BSM HLS, 3Cat snapshots, ipcamlive."""
    snapshots = cache["camaras_live_snapshots"]
    features = []
    for cam in CAMARAS_LIVE:
        props = {
            "id": cam["id"],
            "nom": cam["nom"],
            "tipus": cam["tipus"],
            "font": cam["font"],
        }
        if cam["tipus"] == "hls":
            props["url"] = cam["url"]
        elif cam["tipus"] == "snapshot":
            props["url"] = snapshots.get(cam["id"], "")
        elif cam["tipus"] == "iframe":
            props["url"] = cam["url"]
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [cam["lon"], cam["lat"]]},
        })
    return {"type": "FeatureCollection", "features": features}


def _compute_hexbins(points_lonlat, size_deg=0.004):
    """
    Hexagonal binning en coordenades geogràfiques (flat-top).
    Retorna GeoJSON FeatureCollection de polígons hexagonals.
    """
    # Escala lon per la latitud de BCN (~41°) per fer hexàgons equilibrats
    LAT_SCALE = math.cos(math.radians(41.39))
    SW = size_deg  # mida en graus lat
    SL = size_deg / LAT_SCALE  # mida en graus lon

    def _to_hex(lon, lat):
        # Coordenades axials flat-top
        q = (2/3 * lon / SL)
        r = (-1/3 * lon / SL + math.sqrt(3)/3 * lat / SW)
        # Cube round
        s = -q - r
        rq, rr, rs = round(q), round(r), round(s)
        dq, dr, ds = abs(rq-q), abs(rr-r), abs(rs-s)
        if dq > dr and dq > ds: rq = -rr - rs
        elif dr > ds: rr = -rq - rs
        return (rq, rr)

    def _hex_polygon(q, r):
        cx = SL * 3/2 * q
        cy = SW * math.sqrt(3) * (r + q/2)
        verts = []
        for i in range(7):
            a = math.radians(60 * i)
            verts.append([round(cx + SL * math.cos(a), 6), round(cy + SW * math.sin(a), 6)])
        return verts

    bins = {}
    for item in points_lonlat:
        lon, lat = item["lon"], item["lat"]
        key = _to_hex(lon, lat)
        if key not in bins:
            bins[key] = {"count": 0, "vals": []}
        bins[key]["count"] += 1
        if "val" in item:
            bins[key]["vals"].append(item["val"])

    features = []
    for (q, r), b in bins.items():
        poly = _hex_polygon(q, r)
        avg_val = sum(b["vals"]) / len(b["vals"]) if b["vals"] else None
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [poly]},
            "properties": {"count": b["count"], "avg_val": avg_val}
        })
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/hexbins/{layer}")
def get_hexbins(layer: str):
    """Hexagonal binning per Airbnb i Accidents."""
    if layer == "airbnb":
        data = cache["airbnb"]["data"]
        if not data:
            return {"type":"FeatureCollection","features":[]}
        pts = [{"lon": f["geometry"]["coordinates"][0],
                "lat": f["geometry"]["coordinates"][1],
                "val": 1 if f["properties"].get("has_license") else 0}
               for f in data.get("features", [])]
        return JSONResponse(content=_compute_hexbins(pts, size_deg=0.004))
    elif layer == "accidents":
        data = cache["accidents"]["data"]
        if not data:
            return {"type":"FeatureCollection","features":[]}
        pts = [{"lon": a["lon"], "lat": a["lat"],
                "val": 3 if a["gravetat"] == "mortal" else 2 if a["gravetat"] == "greu" else 1}
               for a in data]
        return JSONResponse(content=_compute_hexbins(pts, size_deg=0.003))
    return {"error": "layer must be airbnb or accidents"}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
