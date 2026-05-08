import os
import time
from contextlib import asynccontextmanager

import httpx
import numpy as np
import pandas as pd
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.responses import FileResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Config (injected via environment / docker-compose)
# ---------------------------------------------------------------------------
REDIS_URL: str = os.environ.get("REDIS_URL", "redis://redis:6379")
API_KEY: str = os.environ.get("API_KEY", "changeme")
STREAM_NAME: str = "whoop_raw_stream"
STREAM_MAXLEN: int = 100_000  # approximate trim (~) to keep Redis memory bounded
CLICKHOUSE_URL: str = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123")
CLICKHOUSE_USER: str = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = os.environ.get("CLICKHOUSE_PASSWORD", "")

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def require_api_key(key: str = Security(api_key_header)) -> str:
    if key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key.",
        )
    return key

# ---------------------------------------------------------------------------
# App lifespan – single shared async Redis & HTTP client
# ---------------------------------------------------------------------------
redis_client: aioredis.Redis | None = None
http_client: httpx.AsyncClient | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, http_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    print(f"[startup] Connected to Redis at {REDIS_URL}")
    
    http_client = httpx.AsyncClient(
        base_url=CLICKHOUSE_URL,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    )
    print(f"[startup] HTTP Client initialized for ClickHouse at {CLICKHOUSE_URL}")
    
    yield
    
    await redis_client.aclose()
    print("[shutdown] Redis connection closed.")
    await http_client.aclose()
    print("[shutdown] HTTP Client closed.")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Whoop BLE Ingest Gateway",
    description="Receives raw BLE hex payloads and appends them to a Redis Stream.",
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class BLEPayload(BaseModel):
    timestamp: str = Field(..., description="ISO-8601 or Unix epoch timestamp from the BLE capture device.")
    hex_payload: str = Field(..., description="Raw BLE advertisement or characteristic data as a hex string.")

class PingPayload(BaseModel):
    status: str = Field(..., description="Status string, e.g. 'connected'")

class BatteryPayload(BaseModel):
    battery_level: int = Field(..., ge=0, le=100, description="Battery percentage 0-100")

class IngestResponse(BaseModel):
    stream_id: str = Field(..., description="Redis Stream entry ID assigned by XADD.")
    stream: str = Field(..., description="Name of the Redis Stream written to.")

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health", tags=["ops"])
async def health():
    """Liveness probe – does not require authentication."""
    return {"status": "ok"}

@app.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["ingest"],
    dependencies=[Depends(require_api_key)],
)
async def ingest(payload: BLEPayload):
    """
    Accept a raw BLE hex payload and append it to the Redis Stream
    ``whoop_raw_stream`` using XADD with an approximate MAXLEN cap.
    """
    current_time = int(time.time())
    await redis_client.set("iphone_last_seen", current_time)
    
    entry_id: str = await redis_client.xadd(
        name=STREAM_NAME,
        fields={
            "timestamp": payload.timestamp,
            "hex_payload": payload.hex_payload,
        },
        maxlen=STREAM_MAXLEN,
        approximate=True,
    )
    return IngestResponse(stream_id=entry_id, stream=STREAM_NAME)

@app.post(
    "/ping",
    status_code=status.HTTP_200_OK,
    tags=["ingest"],
    dependencies=[Depends(require_api_key)],
)
async def ping(payload: PingPayload):
    """
    Explicit heartbeat endpoint to update the connection status.
    """
    current_time = int(time.time())
    await redis_client.set("iphone_last_seen", current_time)
    return {"status": "ok", "last_seen": current_time}

@app.post(
    "/ingest/battery",
    status_code=status.HTTP_200_OK,
    tags=["ingest"],
    dependencies=[Depends(require_api_key)],
)
async def ingest_battery(payload: BatteryPayload):
    """
    Stores the latest Whoop battery level in Redis.
    """
    await redis_client.set("iphone_battery_level", payload.battery_level)
    return {"status": "ok", "battery_level": payload.battery_level}

# ---------------------------------------------------------------------------
# Dashboard & Live API
# ---------------------------------------------------------------------------
@app.get("/api/status", tags=["dashboard"])
async def connection_status():
    """
    Returns the live connection status based on the iphone_last_seen key.
    """
    last_seen_str = await redis_client.get("iphone_last_seen")
    battery_str   = await redis_client.get("iphone_battery_level")

    battery_level = int(battery_str) if battery_str is not None else None

    if not last_seen_str:
        return {"connected": False, "last_seen_seconds_ago": None, "battery_level": battery_level}

    try:
        last_seen = int(last_seen_str)
        seconds_ago = int(time.time()) - last_seen
        connected = seconds_ago <= 5
        return {"connected": connected, "last_seen_seconds_ago": seconds_ago, "battery_level": battery_level}
    except ValueError:
        return {"connected": False, "last_seen_seconds_ago": None, "battery_level": battery_level}
@app.get("/api/latest", tags=["dashboard"])
async def get_latest_data():
    """
    Fetches the latest 50 rows from whoop_hr and whoop_accelerometer 
    from ClickHouse via HTTP interface.
    """
    hr_query = "SELECT timestamp, hr AS heart_rate FROM dhoop.whoop_hr ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    accel_query = "SELECT timestamp, acc0 AS accel_x, acc1 AS accel_y, acc2 AS accel_z FROM dhoop.whoop_accelerometer ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    raw_query = "SELECT timestamp, data AS hex_data FROM dhoop.whoop_raw_data ORDER BY timestamp DESC LIMIT 20 FORMAT JSON"
    
    try:
        hr_resp = await http_client.post("/", params={"query": hr_query})
        hr_resp.raise_for_status()
        hr_data = hr_resp.json().get("data", [])
        
        accel_resp = await http_client.post("/", params={"query": accel_query})
        accel_resp.raise_for_status()
        accel_data = accel_resp.json().get("data", [])

        raw_resp = await http_client.post("/", params={"query": raw_query})
        raw_resp.raise_for_status()
        raw_data = raw_resp.json().get("data", [])
        
        return {
            "hr": hr_data,
            "accelerometer": accel_data,
            "raw": raw_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sleep", tags=["dashboard"])
async def get_sleep_analysis():
    """
    Analyses the last 12 hours of accelerometer + HR data from ClickHouse
    and returns estimated sleep onset, wake time, duration, and average HR.
    """
    # ── 1. Fetch raw data from ClickHouse ─────────────────────────────────────
    accel_query = (
        "SELECT timestamp, acc0 AS x, acc1 AS y, acc2 AS z "
        "FROM dhoop.whoop_accelerometer "
        "WHERE timestamp >= now() - INTERVAL 12 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )
    hr_query = (
        "SELECT timestamp, hr "
        "FROM dhoop.whoop_hr "
        "WHERE timestamp >= now() - INTERVAL 12 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )

    try:
        accel_resp = await http_client.post("/", params={"query": accel_query})
        accel_resp.raise_for_status()
        hr_resp = await http_client.post("/", params={"query": hr_query})
        hr_resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ClickHouse fetch error: {e}")

    accel_rows = accel_resp.json().get("data", [])
    hr_rows    = hr_resp.json().get("data", [])

    if len(accel_rows) < 180:  # need at least 15 min of data at 1 Hz
        return {
            "status": "insufficient_data",
            "detail": f"Only {len(accel_rows)} accelerometer samples available (need ≥180).",
            "sleep_onset": None,
            "wake_time": None,
            "total_sleep_duration_minutes": None,
            "average_sleeping_hr": None,
        }

    # ── 2. Build Accel DataFrame + magnitude ──────────────────────────────────
    accel_df = pd.DataFrame(accel_rows)
    accel_df["timestamp"] = pd.to_datetime(accel_df["timestamp"], utc=True)
    accel_df = accel_df.set_index("timestamp").sort_index()
    accel_df[["x", "y", "z"]] = accel_df[["x", "y", "z"]].apply(pd.to_numeric, errors="coerce")
    accel_df["magnitude"] = np.sqrt(
        accel_df["x"] ** 2 + accel_df["y"] ** 2 + accel_df["z"] ** 2
    )

    # ── 3. 5-minute rolling variance of magnitude ─────────────────────────────
    accel_df["variance"] = accel_df["magnitude"].rolling("5min").var()
    accel_df = accel_df.dropna(subset=["variance"])

    # ── 4. Sleep onset: first 15-consecutive-minute window below threshold ────
    VARIANCE_THRESHOLD = 0.01
    MIN_SLEEP_WINDOW   = pd.Timedelta(minutes=15)

    sleep_onset  = None
    wake_time    = None
    asleep_since = None

    for ts, row in accel_df.iterrows():
        if row["variance"] < VARIANCE_THRESHOLD:
            if asleep_since is None:
                asleep_since = ts
            elif (ts - asleep_since) >= MIN_SLEEP_WINDOW and sleep_onset is None:
                sleep_onset = asleep_since
        else:
            if sleep_onset is not None and wake_time is None:
                wake_time = ts
                break
            asleep_since = None  # reset — movement restarted before 15-min threshold

    if sleep_onset is None:
        return {
            "status": "no_sleep_detected",
            "detail": "No sustained low-movement window found in the last 12 hours.",
            "sleep_onset": None,
            "wake_time": None,
            "total_sleep_duration_minutes": None,
            "average_sleeping_hr": None,
            "data_points_analyzed": len(accel_df),
        }

    # If still asleep at end of window, wake_time = last sample
    if wake_time is None:
        wake_time = accel_df.index[-1]

    duration_minutes = (wake_time - sleep_onset).total_seconds() / 60

    # ── 5. Average sleeping HR within the detected sleep window ───────────────
    avg_sleeping_hr = None
    if hr_rows:
        hr_df = pd.DataFrame(hr_rows)
        hr_df["timestamp"] = pd.to_datetime(hr_df["timestamp"], utc=True)
        hr_df["hr"] = pd.to_numeric(hr_df["hr"], errors="coerce")
        sleeping_hr = hr_df[
            (hr_df["timestamp"] >= sleep_onset) & (hr_df["timestamp"] <= wake_time)
        ]["hr"]
        if not sleeping_hr.empty:
            avg_sleeping_hr = round(float(sleeping_hr.mean()), 1)

    return {
        "status": "ok",
        "sleep_onset": sleep_onset.isoformat(),
        "wake_time": wake_time.isoformat(),
        "total_sleep_duration_minutes": round(duration_minutes, 1),
        "average_sleeping_hr": avg_sleeping_hr,
        "data_points_analyzed": len(accel_df),
    }

@app.get("/dashboard", response_class=FileResponse, tags=["dashboard"])
async def dashboard():
    """Returns the live scrolling telemetry dashboard."""
    # Assuming FastAPI process is run with CWD at ingest/
    return FileResponse("templates/dashboard.html")
