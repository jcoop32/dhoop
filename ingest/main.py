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
# MAX_HR: dynamically resolved at query-time — see _get_max_hr().
# Falls back to age-predicted (220 - age) if < 10 HR samples exist.
_AGE_PREDICTED_MAX_HR: int = 220 - USER_AGE


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
# Internal helper — dynamic Max HR (all-time peak, never decreases)
# ---------------------------------------------------------------------------
async def _get_max_hr() -> int:
    """
    Returns the all-time peak heart rate ever recorded in ClickHouse.
    WHOOP uses your real observed max — not an age formula — because
    the age formula (220-age) is a population average with ±10-20 bpm error.

    Falls back to age-predicted (220 - age) until enough data exists.
    The recorded peak can only ever go UP as you do harder workouts over time.
    """
    q = "SELECT max(hr) AS peak_hr FROM dhoop.whoop_hr FORMAT JSON"
    try:
        resp = await http_client.post("/", params={"query": q})
        resp.raise_for_status()
        rows = resp.json().get("data", [])
        if rows and rows[0].get("peak_hr"):
            peak = int(float(rows[0]["peak_hr"]))
            # Only trust peaks above a physiologically plausible floor
            if peak >= 140:
                return peak
    except Exception:
        pass
    return _AGE_PREDICTED_MAX_HR


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
        f"sleep_duration_min, time_in_bed_min, disturbances, recovery_score "
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
        "recovery_score": 0.0,
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
        f"sleep_duration_min, time_in_bed_min, disturbances, recovery_score) VALUES "
        f"('{row['date']}', {float(row['sleep_score'])}, {float(row['daily_strain'])}, "
        f"{float(row['resting_hr'])}, {float(row['hrv_rmssd'])}, "
        f"{float(row['sleep_duration_min'])}, {float(row['time_in_bed_min'])}, "
        f"{int(row['disturbances'])}, {float(row['recovery_score'])})"
    )
    await http_client.post("/", params={"query": insert_q})


# ---------------------------------------------------------------------------
# Daily Strain  (/api/strain)
# ---------------------------------------------------------------------------
# WHOOP 5-zone HR model multipliers (matches Borg RPE zones)
# Zone 1: 50-60% HRR = 0.1x  (very light, barely counts)
# Zone 2: 60-70% HRR = 0.5x  (aerobic base, Zone 2 training)
# Zone 3: 70-80% HRR = 1.0x  (aerobic threshold)
# Zone 4: 80-90% HRR = 2.0x  (lactate threshold, high intensity)
# Zone 5: 90-100% HRR = 4.0x (VO2max, red zone)
HR_ZONE_MULTIPLIERS = [0.0, 0.1, 0.5, 1.0, 2.0, 4.0]

@app.get("/api/strain", tags=["metrics"])
async def get_daily_strain():
    """
    Calculates WHOOP-style Daily Strain (0-21) using a 5-zone HR model.
    WHOOP uses time-in-zone weighted by zone multipliers, normalized to a log scale.
    Zones are defined as % of Heart Rate Reserve (HRR = MaxHR - RHR).
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
        return {"status": "insufficient_data", "strain": None}

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["hr"] = pd.to_numeric(df["hr"], errors="coerce")
    df = df.dropna(subset=["hr"]).sort_values("timestamp").reset_index(drop=True)

    # Resting HR = 10th percentile of all readings (more robust than rolling min)
    resting_hr = float(np.percentile(df["hr"].values, 10))
    max_hr = await _get_max_hr()
    hr_reserve = max(1.0, max_hr - resting_hr)

    # Zone boundaries as % of HRR (Karvonen method)
    # Zone 0: below 50% HRR, Zone 1: 50-60%, ..., Zone 5: 90%+
    zone_thresholds = [0.50, 0.60, 0.70, 0.80, 0.90, 1.01]

    zone_minutes = [0.0] * 6
    total_active_minutes = 0.0

    for i in range(1, len(df)):
        dt_sec = (df.loc[i, "timestamp"] - df.loc[i-1, "timestamp"]).total_seconds()
        # Skip gaps > 5 min (app disconnected, band off)
        if dt_sec > 300:
            continue
        dt_min = dt_sec / 60.0
        hr_val = float(df.loc[i, "hr"])
        hrr_pct = (hr_val - resting_hr) / hr_reserve

        zone = 0
        for z, thresh in enumerate(zone_thresholds):
            if hrr_pct >= thresh:
                zone = z + 1
        zone = min(zone, 5)
        zone_minutes[zone] += dt_min
        total_active_minutes += dt_min

    # Weighted score = Σ (zone_minutes × zone_multiplier)
    weighted_score = sum(zone_minutes[z] * HR_ZONE_MULTIPLIERS[z] for z in range(6))

    # Map weighted score → 0-21 log scale
    # Calibration: ~480 weighted minutes (e.g. 2h Zone 4) → strain 21
    WEIGHTED_CEIL = 480.0
    strain = round(21.0 * np.log1p(weighted_score) / np.log1p(WEIGHTED_CEIL), 2)
    strain = min(21.0, max(0.0, strain))

    today = pd.Timestamp.now(tz="UTC").date().isoformat()
    await _save_daily_summary(today, {"daily_strain": strain, "resting_hr": round(resting_hr, 1)})

    return {
        "status": "ok",
        "strain": strain,
        "resting_hr": round(resting_hr, 1),
        "max_hr": max_hr,
        "max_hr_source": "observed_peak" if max_hr >= 140 else "age_predicted",
        "zone_minutes": {f"zone_{z}": round(zone_minutes[z], 1) for z in range(6)},
        "total_active_minutes": round(total_active_minutes, 1),
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
    sleep_onset = wake_time = None

    # 1. Find sleep onset: the start of the first 15-minute block of stillness
    asleep_since = None
    for ts, row in accel_df.iterrows():
        if row["variance"] < VARIANCE_THRESHOLD:
            if asleep_since is None:
                asleep_since = ts
            elif (ts - asleep_since) >= MIN_SLEEP_WINDOW:
                sleep_onset = asleep_since
                break
        else:
            asleep_since = None

    # 2. Find wake time: the end of the last 15-minute block of stillness
    awake_since = None
    for ts, row in accel_df.iloc[::-1].iterrows():
        if row["variance"] < VARIANCE_THRESHOLD:
            if awake_since is None:
                awake_since = ts
            elif (awake_since - ts) >= MIN_SLEEP_WINDOW:
                wake_time = awake_since
                break
        else:
            awake_since = None

    if sleep_onset is None or wake_time is None or wake_time <= sleep_onset:
        return {
            "status": "no_sleep_detected",
            "sleep_onset": None, "wake_time": None,
            "total_sleep_duration_minutes": None, "average_sleeping_hr": None,
            "sleep_efficiency": None, "resting_hr": None, "hrv_rmssd": None,
            "data_points_analyzed": len(accel_df),
        }

    # time_in_bed is the full window from onset to wake
    time_in_bed_min = (wake_time - sleep_onset).total_seconds() / 60
    
    # Calculate efficiency by measuring how many periods inside the window had high variance (restlessness)
    sleep_window = accel_df[(accel_df.index >= sleep_onset) & (accel_df.index <= wake_time)]
    low_var_count = len(sleep_window[sleep_window["variance"] < 0.05])
    efficiency = round(low_var_count / len(sleep_window), 3) if len(sleep_window) > 0 else 0.85
    
    # Actual sleep is time in bed minus restlessness
    duration_minutes = time_in_bed_min * efficiency

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

    # ── WHOOP-style Sleep Performance Score ──────────────────────────────
    # Component 1: Sleep Sufficiency — hours slept vs. 8h target (40%)
    # WHOOP uses your personal sleep need; we default to 8h (480 min)
    SLEEP_NEEDED_MIN = 480.0
    sufficiency = min(1.0, duration_minutes / SLEEP_NEEDED_MIN)
    sufficiency_score = sufficiency * 40.0

    # Component 2: Sleep Efficiency — % of time in bed actually asleep (30%)
    efficiency_score = (efficiency or 0.0) * 30.0

    # Component 3: HRV Quality — above 20ms is meaningful (20%)
    # Scale: 20ms=10pts, 60ms=20pts (linear)
    hrv_score = 0.0
    if hrv_rmssd and hrv_rmssd > 0:
        hrv_score = min(20.0, max(0.0, (hrv_rmssd - 20.0) / 40.0 * 20.0))

    # Component 4: Nocturnal RHR dip — good sleep = HR drops below 65 (10%)
    # Scale: 55bpm or below=10pts, 70bpm+=0pts
    rhr_score = 0.0
    if resting_hr_sleep and resting_hr_sleep > 0:
        rhr_score = min(10.0, max(0.0, (70.0 - resting_hr_sleep) / 15.0 * 10.0))

    sleep_score = round(min(100.0, sufficiency_score + efficiency_score + hrv_score + rhr_score), 1)

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
        "score_breakdown": {
            "sufficiency_pts": round(sufficiency_score, 1),
            "efficiency_pts": round(efficiency_score, 1),
            "hrv_pts": round(hrv_score, 1),
            "rhr_pts": round(rhr_score, 1),
        },
        "average_sleeping_hr": avg_sleeping_hr,
        "resting_hr": resting_hr_sleep,
        "hrv_rmssd": hrv_rmssd,
        "data_points_analyzed": len(accel_df),
    }


# ---------------------------------------------------------------------------
# Recovery Score  (/api/recovery)
# ---------------------------------------------------------------------------
@app.get("/api/recovery", tags=["metrics"])
async def get_recovery_score():
    """
    WHOOP-style Recovery (1-100%) composed of four factors:
      - HRV (RMSSD) vs. personal 30-day baseline  → 50% weight
      - Resting HR vs. personal 30-day baseline    → 25% weight
      - Sleep Performance score                    → 15% weight
      - HR Consistency overnight (proxy for resp rate stability) → 10% weight

    Recovery zones: Green 67-100%, Yellow 34-66%, Red 1-33%
    """
    today = pd.Timestamp.now(tz="UTC").date().isoformat()

    # Fetch today's summary for sleep score, resting HR, and HRV
    today_q = (
        f"SELECT sleep_score, resting_hr, hrv_rmssd "
        f"FROM dhoop.whoop_daily_summary FINAL "
        f"WHERE date = '{today}' FORMAT JSON"
    )
    # Fetch 30-day baselines for personalisation
    baseline_q = (
        "SELECT avg(hrv_rmssd) AS hrv_avg, stddev(hrv_rmssd) AS hrv_std, "
        "avg(resting_hr) AS rhr_avg, stddev(resting_hr) AS rhr_std "
        "FROM dhoop.whoop_daily_summary FINAL "
        "WHERE date >= today() - 30 AND hrv_rmssd > 0 AND resting_hr > 0 FORMAT JSON"
    )
    try:
        t_resp = await http_client.post("/", params={"query": today_q})
        b_resp = await http_client.post("/", params={"query": baseline_q})
        t_resp.raise_for_status()
        b_resp.raise_for_status()
        today_rows = t_resp.json().get("data", [])
        base_rows  = b_resp.json().get("data", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    if not today_rows:
        return {"status": "no_data", "recovery": None, "zone": None}

    td  = today_rows[0]
    bas = base_rows[0] if base_rows else {}

    hrv_today  = float(td.get("hrv_rmssd") or 0)
    rhr_today  = float(td.get("resting_hr") or 0)
    sleep_perf = float(td.get("sleep_score") or 0) / 100.0

    hrv_avg = float(bas.get("hrv_avg") or hrv_today or 50.0)
    hrv_std = float(bas.get("hrv_std") or 10.0)
    rhr_avg = float(bas.get("rhr_avg") or rhr_today or 60.0)
    rhr_std = float(bas.get("rhr_std") or 5.0)

    # ── Component 1: HRV z-score → 0-1 (50% weight) ─────────────────────
    # z-score tells how many std devs above/below your personal average
    # +2 SD above baseline = 100%, -2 SD = 0%
    if hrv_today > 0 and hrv_std > 0:
        hrv_z = (hrv_today - hrv_avg) / hrv_std
        hrv_pct = min(1.0, max(0.0, (hrv_z + 2.0) / 4.0))
    elif hrv_today > 0:
        # No baseline yet: scale 20-80ms → 0-100%
        hrv_pct = min(1.0, max(0.0, (hrv_today - 20.0) / 60.0))
    else:
        hrv_pct = 0.5  # neutral if no data

    # ── Component 2: RHR z-score → 0-1 (25% weight, inverted) ───────────
    # Lower RHR = better recovery, so invert the z-score
    if rhr_today > 0 and rhr_std > 0:
        rhr_z = (rhr_today - rhr_avg) / rhr_std
        rhr_pct = min(1.0, max(0.0, (-rhr_z + 2.0) / 4.0))  # inverted
    elif rhr_today > 0:
        # No baseline: scale 45-75bpm → 100-0% (lower is better)
        rhr_pct = min(1.0, max(0.0, (75.0 - rhr_today) / 30.0))
    else:
        rhr_pct = 0.5

    # ── Component 3: Sleep Performance (15% weight) ───────────────────────
    sleep_pct = sleep_perf  # already 0-1

    # ── Component 4: HR Overnight Consistency (10% weight) ────────────────
    # Proxy: standard deviation of HR during sleep — lower = better
    hr_q = (
        "SELECT hr FROM dhoop.whoop_hr "
        "WHERE timestamp >= now() - INTERVAL 10 HOUR FORMAT JSON"
    )
    hr_std_pct = 0.5  # neutral default
    try:
        hr_resp = await http_client.post("/", params={"query": hr_q})
        hr_rows = hr_resp.json().get("data", [])
        if len(hr_rows) > 10:
            hrs = np.array([float(r["hr"]) for r in hr_rows if r.get("hr")])
            hr_std = float(np.std(hrs))
            # Low std dev (< 5bpm) = consistent = good. High (> 20bpm) = bad.
            hr_std_pct = min(1.0, max(0.0, (20.0 - hr_std) / 15.0))
    except Exception:
        pass

    # ── Composite score ───────────────────────────────────────────────────
    recovery_raw = (hrv_pct * 0.50) + (rhr_pct * 0.25) + (sleep_pct * 0.15) + (hr_std_pct * 0.10)
    recovery = round(max(1, min(100, recovery_raw * 100)))

    zone = "green" if recovery >= 67 else ("yellow" if recovery >= 34 else "red")

    # Persist to daily summary
    await _save_daily_summary(today, {"recovery_score": float(recovery)})

    return {
        "status": "ok",
        "recovery": recovery,
        "zone": zone,
        "components": {
            "hrv_pct": round(hrv_pct * 100, 1),
            "rhr_pct": round(rhr_pct * 100, 1),
            "sleep_pct": round(sleep_pct * 100, 1),
            "hr_consistency_pct": round(hr_std_pct * 100, 1),
        },
        "hrv_rmssd": hrv_today or None,
        "resting_hr": rhr_today or None,
        "sleep_score": float(td.get("sleep_score") or 0) or None,
    }


# ---------------------------------------------------------------------------
# Daily Outlook  (/api/daily-outlook)
# ---------------------------------------------------------------------------
@app.get("/api/daily-outlook", tags=["metrics"])
async def get_daily_outlook():
    """
    WHOOP-style Daily Outlook — analyzes overnight recovery data (HRV, RHR,
    sleep quality, and yesterday's strain) and returns a personalized
    coaching summary with zone, optimal strain target range, and advice sentence.
    """
    # Re-use recovery endpoint logic
    rec_data = await get_recovery_score()
    recovery = rec_data.get("recovery") or 50
    zone     = rec_data.get("zone", "yellow")

    # Yesterday's strain for context
    yesterday = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).date().isoformat()
    yesterday_q = (
        f"SELECT daily_strain FROM dhoop.whoop_daily_summary FINAL "
        f"WHERE date = '{yesterday}' FORMAT JSON"
    )
    prev_strain = None
    try:
        y_resp = await http_client.post("/", params={"query": yesterday_q})
        y_rows = y_resp.json().get("data", [])
        if y_rows:
            prev_strain = float(y_rows[0].get("daily_strain") or 0) or None
    except Exception:
        pass

    # Strain target based on recovery zone (matches WHOOP's Strain Target feature)
    if zone == "green":
        strain_min, strain_max = 14.0, 18.0
        zone_label = "Green"
        advice = (
            "Your body is well recovered and primed to perform. "
            "Push for a high-strain day — aim for a workout in Zone 3-4 to build fitness."
        )
    elif zone == "yellow":
        strain_min, strain_max = 10.0, 14.0
        zone_label = "Yellow"
        advice = (
            "Your body is maintaining and ready for moderate effort. "
            "Stick to Zone 2 cardio or a moderate strength session today."
        )
    else:
        strain_min, strain_max = 7.0, 10.0
        zone_label = "Red"
        advice = (
            "Your body is working hard to recover. Prioritize rest or active recovery — "
            "a walk, yoga, or light stretching will help without adding to your load."
        )

    if prev_strain and prev_strain > 16:
        advice += f" Yesterday's high strain ({prev_strain:.1f}) means your body especially needs today's recovery."

    return {
        "status": "ok",
        "recovery": recovery,
        "zone": zone_label,
        "strain_target": {"min": strain_min, "max": strain_max},
        "advice": advice,
        "previous_strain": prev_strain,
    }


# ---------------------------------------------------------------------------
# Health Monitor  (/api/health)
# ---------------------------------------------------------------------------
@app.get("/api/health", tags=["metrics"])
async def get_health_monitor():
    """
    Returns the latest real SpO2 and skin temperature readings alongside
    the cached resting HR and HRV from today's daily summary.
    All values default to null (not hardcoded) if no data is available.
    """
    today = pd.Timestamp.now(tz="UTC").date().isoformat()

    # Latest SpO2 — most recent sample within the last 24 hours
    spo2_q = (
        "SELECT spo2 FROM dhoop.whoop_spo2 "
        "WHERE timestamp >= now() - INTERVAL 24 HOUR "
        "ORDER BY timestamp DESC LIMIT 1 FORMAT JSON"
    )
    # Latest skin temperature
    temp_q = (
        "SELECT temp_c FROM dhoop.whoop_skin_temp "
        "WHERE timestamp >= now() - INTERVAL 24 HOUR "
        "ORDER BY timestamp DESC LIMIT 1 FORMAT JSON"
    )
    # Today's cached RHR + HRV
    summary_q = (
        f"SELECT resting_hr, hrv_rmssd FROM dhoop.whoop_daily_summary FINAL "
        f"WHERE date = '{today}' FORMAT JSON"
    )

    spo2 = rhr = hrv = temp_c = None
    try:
        spo2_r = await http_client.post("/", params={"query": spo2_q})
        temp_r = await http_client.post("/", params={"query": temp_q})
        summ_r = await http_client.post("/", params={"query": summary_q})

        spo2_rows = spo2_r.json().get("data", [])
        temp_rows = temp_r.json().get("data", [])
        summ_rows = summ_r.json().get("data", [])

        if spo2_rows:
            raw = float(spo2_rows[0]["spo2"])
            # Our AC/DC ratio gives a value in 0-1 range; convert to 0-100%
            spo2 = round(raw * 100.0, 1) if raw <= 1.0 else round(raw, 1)
            # Clamp to physiological range 70-100%
            spo2 = max(70.0, min(100.0, spo2)) if spo2 else None

        if temp_rows:
            temp_c = round(float(temp_rows[0]["temp_c"]), 1)

        if summ_rows:
            rhr_val = float(summ_rows[0].get("resting_hr") or 0)
            hrv_val = float(summ_rows[0].get("hrv_rmssd") or 0)
            rhr = round(rhr_val, 1) if rhr_val > 0 else None
            hrv = round(hrv_val, 1) if hrv_val > 0 else None
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # Skin temp status: normal range is ±0.5°C from typical 33-35°C wrist temp
    temp_status = None
    if temp_c is not None:
        if 32.0 <= temp_c <= 36.0:
            temp_status = "Within Range"
        elif temp_c < 32.0:
            temp_status = "Low"
        else:
            temp_status = "Elevated"

    return {
        "status": "ok",
        "resting_hr": rhr,
        "hrv_rmssd": hrv,
        "spo2_pct": spo2,
        "skin_temp_c": temp_c,
        "skin_temp_status": temp_status,
    }


# ---------------------------------------------------------------------------
# Stress Monitor  (/api/stress)
# ---------------------------------------------------------------------------
@app.get("/api/stress", tags=["metrics"])
async def get_stress_monitor():
    """
    Real-time stress score using short-term HRV variance (RMSSD coefficient
    of variation over rolling 15-minute windows).

    WHOOP's Stress Monitor works by:
      1. Computing HRV (RMSSD) in 15-minute sliding windows throughout the day
      2. Comparing each window's HRV to your personal overnight baseline
      3. High stress = HRV is LOW relative to your baseline (sympathetic dominance)
      4. Low stress = HRV is HIGH, close to or above baseline (parasympathetic)

    Stress scale: 0.0 (no stress) → 3.0 (high stress)
    """
    rr_q = (
        "SELECT timestamp, rr_ms FROM dhoop.whoop_rr_intervals "
        "WHERE timestamp >= now() - INTERVAL 2 HOUR "
        "ORDER BY timestamp ASC FORMAT JSON"
    )
    # Overnight baseline for comparison
    today = pd.Timestamp.now(tz="UTC").date().isoformat()
    baseline_q = (
        f"SELECT hrv_rmssd FROM dhoop.whoop_daily_summary FINAL "
        f"WHERE date = '{today}' AND hrv_rmssd > 0 FORMAT JSON"
    )

    try:
        rr_resp   = await http_client.post("/", params={"query": rr_q})
        base_resp = await http_client.post("/", params={"query": baseline_q})
        rr_rows   = rr_resp.json().get("data", [])
        base_rows = base_resp.json().get("data", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    overnight_hrv = None
    if base_rows:
        overnight_hrv = float(base_rows[0].get("hrv_rmssd") or 0) or None

    if len(rr_rows) < 10:
        return {
            "status": "insufficient_data",
            "stress_score": None,
            "stress_level": None,
            "current_hrv": None,
            "baseline_hrv": overnight_hrv,
            "detail": f"Only {len(rr_rows)} RR samples (need ≥10). Wear band longer.",
        }

    rr_df = pd.DataFrame(rr_rows)
    rr_df["timestamp"] = pd.to_datetime(rr_df["timestamp"], utc=True)
    rr_df["rr_ms"] = pd.to_numeric(rr_df["rr_ms"], errors="coerce")
    rr_df = rr_df.dropna(subset=["rr_ms"]).sort_values("timestamp")

    # Current HRV = RMSSD of the most recent 15-minute window
    cutoff = rr_df["timestamp"].max() - pd.Timedelta(minutes=15)
    recent = rr_df[rr_df["timestamp"] >= cutoff]["rr_ms"].values

    current_hrv = None
    if len(recent) >= 2:
        diffs = np.diff(recent.astype(float))
        current_hrv = round(float(np.sqrt(np.mean(diffs**2))), 1)

    # Stress score: how suppressed is current HRV vs. overnight baseline?
    stress_score = None
    if current_hrv is not None:
        baseline = overnight_hrv or 50.0  # default if no overnight data yet
        # Ratio < 1 = HRV is suppressed = stressed
        # Ratio > 1 = HRV elevated = recovered/relaxed
        ratio = current_hrv / max(1.0, baseline)
        # Map: ratio 0 → stress 3.0, ratio 1.0 → stress 1.0, ratio 1.5+ → stress 0.0
        stress_score = round(max(0.0, min(3.0, 3.0 - (ratio * 2.0))), 2)

    # Stress level labels (matches WHOOP's UI)
    stress_level = None
    if stress_score is not None:
        if stress_score < 0.5:
            stress_level = "Recovered"
        elif stress_score < 1.5:
            stress_level = "Low"
        elif stress_score < 2.2:
            stress_level = "Medium"
        else:
            stress_level = "High"

    return {
        "status": "ok",
        "stress_score": stress_score,
        "stress_level": stress_level,
        "current_hrv": current_hrv,
        "baseline_hrv": overnight_hrv,
        "rr_samples_analyzed": len(rr_df),
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
        "resting_hr, hrv_rmssd, "
        "sleep_duration_min, time_in_bed_min, disturbances, "
        "recovery_score "
        "FROM dhoop.whoop_daily_summary FINAL "
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
