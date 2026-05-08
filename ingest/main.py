import os
import time
from contextlib import asynccontextmanager

import httpx
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

# ---------------------------------------------------------------------------
# Dashboard & Live API
# ---------------------------------------------------------------------------
@app.get("/api/status", tags=["dashboard"])
async def connection_status():
    """
    Returns the live connection status based on the iphone_last_seen key.
    """
    last_seen_str = await redis_client.get("iphone_last_seen")
    
    if not last_seen_str:
        return {"connected": False, "last_seen_seconds_ago": None}
        
    try:
        last_seen = int(last_seen_str)
        seconds_ago = int(time.time()) - last_seen
        connected = seconds_ago <= 5
        return {"connected": connected, "last_seen_seconds_ago": seconds_ago}
    except ValueError:
        return {"connected": False, "last_seen_seconds_ago": None}
@app.get("/api/latest", tags=["dashboard"])
async def get_latest_data():
    """
    Fetches the latest 50 rows from whoop_hr and whoop_accelerometer 
    from ClickHouse via HTTP interface.
    """
    hr_query = "SELECT timestamp, heart_rate FROM whoop_hr ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    accel_query = "SELECT timestamp, accel_x, accel_y, accel_z FROM whoop_accelerometer ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    
    try:
        hr_resp = await http_client.post("/", params={"query": hr_query})
        hr_resp.raise_for_status()
        hr_data = hr_resp.json().get("data", [])
        
        accel_resp = await http_client.post("/", params={"query": accel_query})
        accel_resp.raise_for_status()
        accel_data = accel_resp.json().get("data", [])
        
        return {
            "hr": hr_data,
            "accelerometer": accel_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard", response_class=FileResponse, tags=["dashboard"])
async def dashboard():
    """Returns the live scrolling telemetry dashboard."""
    # Assuming FastAPI process is run with CWD at ingest/
    return FileResponse("templates/dashboard.html")
