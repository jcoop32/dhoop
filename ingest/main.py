import os
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.responses import HTMLResponse
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
# App lifespan – single shared async Redis client
# ---------------------------------------------------------------------------
redis_client: aioredis.Redis | None = None
http_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, http_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    print(f"[startup] Connected to Redis at {REDIS_URL}")
    
    http_client = httpx.AsyncClient(base_url=CLICKHOUSE_URL)
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
    timestamp: str = Field(
        ...,
        description="ISO-8601 or Unix epoch timestamp from the BLE capture device.",
        examples=["2026-05-07T18:00:00Z"],
    )
    hex_payload: str = Field(
        ...,
        description="Raw BLE advertisement or characteristic data as a hex string.",
        examples=["0a1b2c3d4e5f"],
    )


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
    entry_id: str = await redis_client.xadd(
        name=STREAM_NAME,
        fields={
            "timestamp": payload.timestamp,
            "hex_payload": payload.hex_payload,
        },
        maxlen=STREAM_MAXLEN,
        approximate=True,  # MAXLEN ~ 100000 — efficient trimming
    )
    return IngestResponse(stream_id=entry_id, stream=STREAM_NAME)


# ---------------------------------------------------------------------------
# Dashboard & Live API
# ---------------------------------------------------------------------------
@app.get("/api/latest", tags=["dashboard"])
async def get_latest_data():
    """
    Fetches the latest 50 rows from whoop_hr and whoop_accelerometer 
    from ClickHouse via HTTP interface.
    """
    hr_query = "SELECT timestamp, heart_rate FROM whoop_hr ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    accel_query = "SELECT timestamp, accel_x, accel_y, accel_z FROM whoop_accelerometer ORDER BY timestamp DESC LIMIT 50 FORMAT JSON"
    
    try:
        hr_resp = await http_client.get("/", params={"query": hr_query})
        hr_resp.raise_for_status()
        hr_data = hr_resp.json().get("data", [])
        
        accel_resp = await http_client.get("/", params={"query": accel_query})
        accel_resp.raise_for_status()
        accel_data = accel_resp.json().get("data", [])
        
        return {
            "hr": hr_data,
            "accelerometer": accel_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Whoop BLE Live Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            background-color: #09090b; /* Zinc dark palette from previous NBA watcher projects */
            color: #ffffff;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            margin: 0;
            padding: 40px 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        h1 {
            font-weight: 700;
            margin-bottom: 30px;
            color: #fafafa;
            letter-spacing: -0.05em;
        }
        .charts-container {
            width: 100%;
            max-width: 1200px;
            display: flex;
            flex-direction: column;
            gap: 30px;
        }
        .chart-box {
            background: rgba(24, 24, 27, 0.7);
            backdrop-filter: blur(10px);
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
            border: 1px solid rgba(255, 255, 255, 0.05);
        }
        canvas {
            width: 100% !important;
            max-height: 350px;
        }
    </style>
</head>
<body>
    <h1>Live Telemetry</h1>
    <div class="charts-container">
        <div class="chart-box">
            <canvas id="hrChart"></canvas>
        </div>
        <div class="chart-box">
            <canvas id="accelChart"></canvas>
        </div>
    </div>

    <script>
        // Set default Chart.js text color for dark theme
        Chart.defaults.color = '#a1a1aa';
        Chart.defaults.borderColor = 'rgba(255,255,255,0.05)';
        Chart.defaults.font.family = "'Inter', sans-serif";

        // Initialize Heart Rate Chart
        const hrCtx = document.getElementById('hrChart').getContext('2d');
        const hrChart = new Chart(hrCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Heart Rate (bpm)',
                    data: [],
                    borderColor: '#ef4444', // Red-500
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 0 }, // Disable animation for live scrolling effect
                scales: {
                    x: { display: false },
                    y: {
                        min: 40,
                        max: 200,
                        grid: { color: 'rgba(255,255,255,0.05)' }
                    }
                },
                plugins: {
                    legend: { labels: { color: '#fafafa', font: { weight: 600 } } }
                }
            }
        });

        // Initialize Accelerometer Chart
        const accelCtx = document.getElementById('accelChart').getContext('2d');
        const accelChart = new Chart(accelCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'X Axis',
                        data: [],
                        borderColor: '#3b82f6', // Blue-500
                        borderWidth: 2,
                        pointRadius: 0,
                        tension: 0.1
                    },
                    {
                        label: 'Y Axis',
                        data: [],
                        borderColor: '#10b981', // Emerald-500
                        borderWidth: 2,
                        pointRadius: 0,
                        tension: 0.1
                    },
                    {
                        label: 'Z Axis',
                        data: [],
                        borderColor: '#f59e0b', // Amber-500
                        borderWidth: 2,
                        pointRadius: 0,
                        tension: 0.1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 0 },
                scales: {
                    x: { display: false },
                    y: {
                        min: -2.5,
                        max: 2.5,
                        grid: { color: 'rgba(255,255,255,0.05)' }
                    }
                },
                plugins: {
                    legend: { labels: { color: '#fafafa', font: { weight: 600 } } }
                }
            }
        });

        async function updateCharts() {
            try {
                const response = await fetch('/api/latest');
                const data = await response.json();
                
                // Reverse the descending arrays back into chronological (ASC) order for left-to-right rendering
                const hrData = data.hr.reverse();
                const accelData = data.accelerometer.reverse();

                // Update HR
                hrChart.data.labels = hrData.map(d => d.timestamp);
                hrChart.data.datasets[0].data = hrData.map(d => d.heart_rate);
                hrChart.update();

                // Update Accelerometer
                accelChart.data.labels = accelData.map(d => d.timestamp);
                accelChart.data.datasets[0].data = accelData.map(d => d.accel_x);
                accelChart.data.datasets[1].data = accelData.map(d => d.accel_y);
                accelChart.data.datasets[2].data = accelData.map(d => d.accel_z);
                accelChart.update();
                
            } catch (error) {
                console.error("Error fetching latest data:", error);
            }
        }

        // Fetch every 500ms
        setInterval(updateCharts, 500);
        updateCharts(); // Initial fetch
    </script>
</body>
</html>
"""


@app.get("/dashboard", response_class=HTMLResponse, tags=["dashboard"])
async def dashboard():
    """Returns the live scrolling telemetry dashboard."""
    return DASHBOARD_HTML
