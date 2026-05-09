import os
import time
from contextlib import asynccontextmanager
from datetime import date

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

# User demographics — used for physiological metric calculations
_dob_str        = os.environ.get("USER_BIRTHDATE", "2000-01-01")
_dob            = date.fromisoformat(_dob_str)
_today          = date.today()
USER_AGE:       int   = _today.year - _dob.year - ((_today.month, _today.day) < (_dob.month, _dob.day))
USER_WEIGHT_KG: float = float(os.environ.get("USER_WEIGHT_KG", "75"))
USER_HEIGHT_CM: float = float(os.environ.get("USER_HEIGHT_CM", "180"))
MAX_HR:         int   = 220 - USER_AGE   # age-predicted maximum heart rate


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
    hr_query        = "SELECT timestamp, hr AS heart_rate FROM dhoop.whoop_hr ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    accel_query     = "SELECT timestamp, acc0 AS accel_x, acc1 AS accel_y, acc2 AS accel_z FROM dhoop.whoop_accelerometer ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    raw_query       = "SELECT timestamp, data AS hex_data FROM dhoop.whoop_raw_data ORDER BY timestamp DESC LIMIT 20 FORMAT JSON"
    skin_temp_query = "SELECT timestamp, temp_c FROM dhoop.whoop_skin_temp ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    spo2_query      = "SELECT timestamp, spo2 FROM dhoop.whoop_spo2 ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    gyro_query      = "SELECT timestamp, gx, gy, gz FROM dhoop.whoop_gyro ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    tap_query       = "SELECT timestamp FROM dhoop.whoop_double_tap ORDER BY timestamp DESC LIMIT 10 FORMAT JSON"
    wrist_query     = "SELECT timestamp, on_wrist FROM dhoop.whoop_wrist_state ORDER BY timestamp DESC LIMIT 10 FORMAT JSON"

    # Each query is isolated — a missing/new table won't kill the whole endpoint.
    async def _query(q: str) -> list:
        try:
            r = await http_client.post("/", params={"query": q})
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception:
            return []

    hr_data, accel_data, raw_data, skin_temp_data, spo2_data, gyro_data, tap_data, wrist_data = (
        await _query(hr_query),
        await _query(accel_query),
        await _query(raw_query),
        await _query(skin_temp_query),
        await _query(spo2_query),
        await _query(gyro_query),
        await _query(tap_query),
        await _query(wrist_query),
    )

    return {
        "hr": hr_data,
        "accelerometer": accel_data,
        "raw": raw_data,
        "skin_temp": skin_temp_data,
        "spo2": spo2_data,
        "gyro": gyro_data,
        "double_tap": tap_data,
        "wrist_state": wrist_data,
    }

# ---------------------------------------------------------------------------
# Internal helper — read-merge-write to whoop_daily_summary

# ---------------------------------------------------------------------------
async def _save_daily_summary(date: str, updates: dict) -> None:
    """
    Read-merge-write pattern for whoop_daily_summary.
    Fetches any existing row for `date`, merges `updates` on top,
    then INSERTs the complete row. ReplacingMergeTree deduplicates by date.
    """
    # Step A — read existing row (if any)
    fetch_q = (
        f"SELECT sleep_score, daily_strain, resting_hr, hrv_rmssd, "
        f"sleep_duration_min, time_in_bed_min, disturbances "
        f"FROM dhoop.whoop_daily_summary WHERE date = '{date}' LIMIT 1 FORMAT JSON"
    )
    row: dict = {
        "date": date,
        "sleep_score": 0.0,
        "daily_strain": 0.0,
        "resting_hr": 0.0,
        "hrv_rmssd": 0.0,
        "sleep_duration_min": 0.0,
        "time_in_bed_min": 0.0,
        "disturbances": 0,
    }
    try:
        resp = await http_client.post("/", params={"query": fetch_q})
        resp.raise_for_status()
        existing = resp.json().get("data", [])
        if existing:
            row.update(existing[0])   # overlay DB values
    except Exception:
        pass  # first write of the day — no existing row is fine

    # Step B — overlay new values
    row.update(updates)

    # Step C — INSERT (ReplacingMergeTree keeps the newest by insertion order)
    insert_q = (
        f"INSERT INTO dhoop.whoop_daily_summary "
        f"(date, sleep_score, daily_strain, resting_hr, hrv_rmssd, "
        f"sleep_duration_min, time_in_bed_min, disturbances) VALUES "
        f"('{row['date']}', {float(row['sleep_score'])}, {float(row['daily_strain'])}, "
        f"{float(row['resting_hr'])}, {float(row['hrv_rmssd'])}, "
        f"{float(row['sleep_duration_min'])}, {float(row['time_in_bed_min'])}, "
        f"{int(row['disturbances'])})"
    )
    await http_client.post("/", params={"query": insert_q})


# ---------------------------------------------------------------------------
# Daily Strain  (/api/strain)
# ---------------------------------------------------------------------------
@app.get("/api/strain", tags=["metrics"])
async def get_daily_strain():
    """
    Calculates a Whoop-style Daily Strain (0–21) using the TRIMP exponential
    weighting model applied to the last 24 hours of heart-rate data.
    """
    hr_q = (
        "SELECT timestamp, hr FROM dhoop.whoop_hr "
        "WHERE timestamp >= now() - INTERVAL 24 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )
    try:
        resp = await http_client.post("/", params={"query": hr_q})
        resp.raise_for_status()
        rows = resp.json().get("data", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    if len(rows) < 2:
        return {"status": "insufficient_data", "strain": None, "trimp": None}

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["hr"] = pd.to_numeric(df["hr"], errors="coerce")
    df = df.dropna(subset=["hr"]).sort_values("timestamp").reset_index(drop=True)

    # Resting HR = lowest 5-minute rolling mean
    df = df.set_index("timestamp")
    rolling_min = df["hr"].rolling("5min").mean().min()
    resting_hr = float(rolling_min) if not np.isnan(rolling_min) else 50.0
    df = df.reset_index()

    # TRIMP: Σ ΔT(min) × HR_ratio × e^(1.92 × HR_ratio)
    hr_range = MAX_HR - resting_hr
    total_trimp = 0.0
    for i in range(1, len(df)):
        dt_min = (df.loc[i, "timestamp"] - df.loc[i - 1, "timestamp"]).total_seconds() / 60
        hr_val = float(df.loc[i, "hr"])
        hr_ratio = max(0.0, (hr_val - resting_hr) / hr_range) if hr_range > 0 else 0.0
        total_trimp += dt_min * hr_ratio * np.exp(1.92 * hr_ratio)

    # Map TRIMP → 0–21 log scale (reference ceiling: TRIMP ~600 = strain 21)
    TRIMP_CEIL = 600.0
    strain = round(21.0 * np.log1p(total_trimp) / np.log1p(TRIMP_CEIL), 2)
    strain = min(21.0, max(0.0, strain))

    today = pd.Timestamp.now(tz="UTC").date().isoformat()
    await _save_daily_summary(today, {"daily_strain": strain, "resting_hr": round(resting_hr, 1)})

    return {
        "status": "ok",
        "strain": strain,
        "trimp": round(total_trimp, 2),
        "resting_hr": round(resting_hr, 1),
        "max_hr": MAX_HR,
        "samples_analyzed": len(df),
    }


# ---------------------------------------------------------------------------
# Sleep Score — enhanced with efficiency, RHR, HRV (RMSSD)
# ---------------------------------------------------------------------------
@app.get("/api/sleep", tags=["metrics"])
async def get_sleep_analysis():
    """
    Analyses the last 12 hours of accel + HR + RR data.
    Returns sleep onset/wake, efficiency, resting HR, and HRV (RMSSD).
    """
    accel_q = (
        "SELECT timestamp, acc0 AS x, acc1 AS y, acc2 AS z "
        "FROM dhoop.whoop_accelerometer "
        "WHERE timestamp >= now() - INTERVAL 12 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )
    hr_q = (
        "SELECT timestamp, hr FROM dhoop.whoop_hr "
        "WHERE timestamp >= now() - INTERVAL 12 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )
    rr_q = (
        "SELECT timestamp, rr_ms FROM dhoop.whoop_rr_intervals "
        "WHERE timestamp >= now() - INTERVAL 12 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )

    try:
        accel_resp = await http_client.post("/", params={"query": accel_q})
        accel_resp.raise_for_status()
        hr_resp = await http_client.post("/", params={"query": hr_q})
        hr_resp.raise_for_status()
        rr_resp = await http_client.post("/", params={"query": rr_q})
        rr_resp.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ClickHouse error: {e}")

    accel_rows = accel_resp.json().get("data", [])
    hr_rows    = hr_resp.json().get("data", [])
    rr_rows    = rr_resp.json().get("data", [])

    if len(accel_rows) < 180:
        return {
            "status": "insufficient_data",
            "detail": f"Only {len(accel_rows)} accel samples (need ≥180).",
            "sleep_onset": None, "wake_time": None,
            "total_sleep_duration_minutes": None, "average_sleeping_hr": None,
            "sleep_efficiency": None, "resting_hr": None, "hrv_rmssd": None,
        }

    # ── Accel → rolling variance → sleep onset ────────────────────────────
    accel_df = pd.DataFrame(accel_rows)
    accel_df["timestamp"] = pd.to_datetime(accel_df["timestamp"], utc=True)
    accel_df = accel_df.set_index("timestamp").sort_index()
    accel_df[["x", "y", "z"]] = accel_df[["x", "y", "z"]].apply(pd.to_numeric, errors="coerce")
    accel_df["magnitude"] = np.sqrt(accel_df["x"]**2 + accel_df["y"]**2 + accel_df["z"]**2)
    accel_df["variance"]  = accel_df["magnitude"].rolling("5min").var()
    accel_df = accel_df.dropna(subset=["variance"])

    VARIANCE_THRESHOLD = 0.01
    MIN_SLEEP_WINDOW   = pd.Timedelta(minutes=15)
    sleep_onset = wake_time = asleep_since = None

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
            asleep_since = None

    if sleep_onset is None:
        return {
            "status": "no_sleep_detected",
            "sleep_onset": None, "wake_time": None,
            "total_sleep_duration_minutes": None, "average_sleeping_hr": None,
            "sleep_efficiency": None, "resting_hr": None, "hrv_rmssd": None,
            "data_points_analyzed": len(accel_df),
        }

    if wake_time is None:
        wake_time = accel_df.index[-1]

    duration_minutes = (wake_time - sleep_onset).total_seconds() / 60
    # time_in_bed = first accel sample to wake (conservative proxy)
    time_in_bed_min  = (wake_time - accel_df.index[0]).total_seconds() / 60
    efficiency = round(duration_minutes / time_in_bed_min, 3) if time_in_bed_min > 0 else None

    # ── HR within sleep window ────────────────────────────────────────────
    avg_sleeping_hr = resting_hr_sleep = None
    if hr_rows:
        hr_df = pd.DataFrame(hr_rows)
        hr_df["timestamp"] = pd.to_datetime(hr_df["timestamp"], utc=True)
        hr_df["hr"] = pd.to_numeric(hr_df["hr"], errors="coerce")
        sleeping_hr_series = hr_df[
            (hr_df["timestamp"] >= sleep_onset) & (hr_df["timestamp"] <= wake_time)
        ]["hr"]
        if not sleeping_hr_series.empty:
            avg_sleeping_hr = round(float(sleeping_hr_series.mean()), 1)
            # Resting HR = lowest 5-min rolling mean in the sleep window
            hr_sleep = hr_df.set_index("timestamp").sort_index()["hr"]
            rolling_rhr = hr_sleep.rolling("5min").mean().min()
            if not np.isnan(rolling_rhr):
                resting_hr_sleep = round(float(rolling_rhr), 1)

    # ── HRV (RMSSD) from RR intervals in sleep window ────────────────────
    hrv_rmssd = None
    if rr_rows:
        rr_df = pd.DataFrame(rr_rows)
        rr_df["timestamp"] = pd.to_datetime(rr_df["timestamp"], utc=True)
        rr_df["rr_ms"] = pd.to_numeric(rr_df["rr_ms"], errors="coerce")
        sleep_rr = rr_df[
            (rr_df["timestamp"] >= sleep_onset) & (rr_df["timestamp"] <= wake_time)
        ]["rr_ms"].dropna().values
        if len(sleep_rr) >= 2:
            diffs = np.diff(sleep_rr.astype(float))
            hrv_rmssd = round(float(np.sqrt(np.mean(diffs**2))), 1)

    # Sleep score: composite of efficiency (60%), HRV presence (20%), RHR dip (20%)
    sleep_score = round(min(100.0, (efficiency or 0) * 100 * 0.6
                        + (20.0 if hrv_rmssd and hrv_rmssd > 20 else 0)
                        + (20.0 if resting_hr_sleep and resting_hr_sleep < 65 else 0)), 1)

    today = pd.Timestamp.now(tz="UTC").date().isoformat()
    await _save_daily_summary(today, {
        "sleep_score": sleep_score,
        "resting_hr": resting_hr_sleep or 0.0,
        "hrv_rmssd": hrv_rmssd or 0.0,
        "sleep_duration_min": round(duration_minutes, 1),
        "time_in_bed_min": round(time_in_bed_min, 1),
    })

    return {
        "status": "ok",
        "sleep_onset": sleep_onset.isoformat(),
        "wake_time": wake_time.isoformat(),
        "total_sleep_duration_minutes": round(duration_minutes, 1),
        "time_in_bed_minutes": round(time_in_bed_min, 1),
        "sleep_efficiency": efficiency,
        "sleep_score": sleep_score,
        "average_sleeping_hr": avg_sleeping_hr,
        "resting_hr": resting_hr_sleep,
        "hrv_rmssd": hrv_rmssd,
        "data_points_analyzed": len(accel_df),
    }


# ---------------------------------------------------------------------------
# Caloric Burn  (/api/calories)
# ---------------------------------------------------------------------------
@app.get("/api/calories", tags=["metrics"])
async def get_caloric_burn():
    """
    Estimates daily caloric burn using Mifflin-St Jeor BMR + activity multiplier
    derived from 24h accelerometer variance.
    """
    # BMR — Mifflin-St Jeor (male baseline; extend with sex env var later)
    bmr = (10 * USER_WEIGHT_KG) + (6.25 * USER_HEIGHT_CM) - (5 * USER_AGE) + 5

    accel_q = (
        "SELECT acc0, acc1, acc2 FROM dhoop.whoop_accelerometer "
        "WHERE timestamp >= now() - INTERVAL 24 HOUR FORMAT JSON"
    )
    try:
        resp = await http_client.post("/", params={"query": accel_q})
        resp.raise_for_status()
        rows = resp.json().get("data", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    activity_multiplier = 1.2  # sedentary default
    daily_variance = None

    if len(rows) >= 10:
        df = pd.DataFrame(rows).apply(pd.to_numeric, errors="coerce").dropna()
        magnitude = np.sqrt(df["acc0"]**2 + df["acc1"]**2 + df["acc2"]**2)
        daily_variance = float(magnitude.var())
        # Map variance → multiplier: 0 → 1.2 (sedentary), ≥1.0 → 1.9 (very active)
        activity_multiplier = round(1.2 + min(0.7, daily_variance * 0.7), 2)

    total_calories = round(bmr * activity_multiplier, 1)
    active_calories = round(total_calories - bmr, 1)

    return {
        "status": "ok",
        "bmr": round(bmr, 1),
        "activity_multiplier": activity_multiplier,
        "estimated_active_calories": active_calories,
        "total_estimated_calories": total_calories,
        "daily_accel_variance": round(daily_variance, 4) if daily_variance is not None else None,
    }


# ---------------------------------------------------------------------------
# History  (/api/history)
# ---------------------------------------------------------------------------
@app.get("/api/history", tags=["metrics"])
async def get_history():
    """Returns the last 14 days of daily summaries from whoop_daily_summary."""
    q = (
        "SELECT date, "
        "toInt32(sleep_score) AS sleep_score, "
        "daily_strain AS strain, "
        "resting_hr, hrv_rmssd "
        "FROM dhoop.whoop_daily_summary "
        "WHERE date >= today() - 14 "
        "ORDER BY date ASC FORMAT JSON"
    )
    try:
        resp = await http_client.post("/", params={"query": q})
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ---------------------------------------------------------------------------
# Baselines  (/api/baselines)
# ---------------------------------------------------------------------------
@app.get("/api/baselines", tags=["metrics"])
async def get_baselines():
    """
    Queries 30 days of whoop_daily_summary and returns mean ± 1 SD bands
    for RHR and HRV — used by the iOS app to contextualise live readings.
    """
    q = (
        "SELECT resting_hr, hrv_rmssd FROM dhoop.whoop_daily_summary "
        "WHERE date >= today() - 30 AND resting_hr > 0 AND hrv_rmssd > 0 FORMAT JSON"
    )
    try:
        resp = await http_client.post("/", params={"query": q})
        resp.raise_for_status()
        rows = resp.json().get("data", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    if len(rows) < 3:
        # Return zero-filled defaults so iOS decoder doesn't fail on null fields
        return {
            "status": "insufficient_data",
            "detail": f"Only {len(rows)} days with valid data (need ≥3).",
            "hrv_low": 0.0, "hrv_high": 0.0,
            "rhr_low": 0.0, "rhr_high": 0.0,
        }

    df = pd.DataFrame(rows).apply(pd.to_numeric, errors="coerce").dropna()

    hrv_mean, hrv_std = df["hrv_rmssd"].mean(), df["hrv_rmssd"].std()
    rhr_mean, rhr_std = df["resting_hr"].mean(), df["resting_hr"].std()

    return {
        "status": "ok",
        "days_analyzed": len(df),
        "hrv_mean": round(hrv_mean, 1),
        "hrv_low":  round(hrv_mean - hrv_std, 1),
        "hrv_high": round(hrv_mean + hrv_std, 1),
        "rhr_mean": round(rhr_mean, 1),
        "rhr_low":  round(rhr_mean - rhr_std, 1),
        "rhr_high": round(rhr_mean + rhr_std, 1),
    }


@app.get("/dashboard", response_class=FileResponse, tags=["dashboard"])
async def dashboard():
    """Returns the live scrolling telemetry dashboard."""
    # Assuming FastAPI process is run with CWD at ingest/
    return FileResponse("templates/dashboard.html")
