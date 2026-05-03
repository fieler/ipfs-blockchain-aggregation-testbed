"""
IoT Multi-Core Load Simulator — FastAPI service that spawns parallel worker processes
to simulate configurable numbers of IoT sensors sending data to the middleware.

Each worker process runs an async loop and distributes requests across its assigned
sensor subset. The total request rate is (num_sensors * messages_per_second).

Configuration via environment variables:
  GATEWAY_URL            - Target middleware ingest endpoint (default: http://10.0.0.3:8000/ingest)
  NUM_SENSORS            - Number of simulated sensors (default: 50)
  MESSAGES_PER_SECOND    - Send rate per sensor in msg/s (default: 1.0)
"""
import asyncio
import httpx
import time
import os
import random
import uuid
import logging
import multiprocessing as mp
from fastapi import FastAPI, BackgroundTasks, HTTPException, Response
from pydantic import BaseModel, Field
from typing import Optional, List
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("simulator-master")

app = FastAPI(title="IoT Multi-Core Load Simulator")

# --- SHARED STATE (cross-process) ---
# Shared values allow worker processes to update counters visible to the master process
total_sent_shared = mp.Value('q', 0)  # long long
total_errors_shared = mp.Value('q', 0)
stop_event_shared = mp.Event()

class SimulatorConfig(BaseModel):
    gateway_url: str = Field(..., example="http://10.0.0.3:8000/ingest")
    num_sensors: int = Field(..., gt=0)
    messages_per_second: float = Field(..., gt=0)

# Global state for the master process
class MasterState:
    def __init__(self):
        self.processes: List[mp.Process] = []
        self.is_running = False
        self.config = SimulatorConfig(
            gateway_url=os.getenv("GATEWAY_URL", "http://10.0.0.3:8000/ingest"),
            num_sensors=int(os.getenv("NUM_SENSORS", "50")),
            messages_per_second=float(os.getenv("MESSAGES_PER_SECOND", "1.0"))
        )
        self.last_sent_val = 0
        self.last_errors_val = 0

state = MasterState()

# Prometheus metrics (master process only).
# Counter is the correct type for monotonically increasing values and supports rate() queries.
SIM_REQ_SENT = Counter("simulator_requests_sent_total", "Total number of successfully sent requests")
SIM_REQ_ERROR = Counter("simulator_request_errors_total", "Total number of failed requests")

# --- WORKER LOGIC ---

def run_worker(worker_id: int, sensors_subset: List[str], config: SimulatorConfig):
    """
    Entry point for a worker process.
    Runs a single async loop that sends requests at a rate derived from
    (num_sensors_in_subset * messages_per_second). This is more efficient than
    spawning one task per sensor.
    """
    logger.info(f"Worker-{worker_id}: starting with {len(sensors_subset)} sensors.")

    async def _start_async_worker():
        num_worker_sensors = len(sensors_subset)
        if num_worker_sensors == 0:
            return

        if config.messages_per_second <= 0:
            logger.warning(f"Worker-{worker_id}: messages_per_second is 0 or negative, worker will not run.")
            return

        # Total rate for this worker; interval is the sleep time between requests
        worker_rate = num_worker_sensors * config.messages_per_second
        interval = 1.0 / worker_rate

        limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
        async with httpx.AsyncClient(limits=limits) as client:
            while not stop_event_shared.is_set():
                start_loop = time.perf_counter()

                # Pick a random sensor from this worker's assigned subset
                sensor_id = random.choice(sensors_subset)

                payload = {
                    "sensor_id": sensor_id,
                    "reading_id": str(uuid.uuid4()),
                    "timestamp": time.time(),
                    "data": {"temp": round(random.uniform(20, 30), 2)}
                }

                try:
                    resp = await client.post(config.gateway_url, json=payload, timeout=5.0)
                    if resp.status_code == 200:
                        with total_sent_shared.get_lock():
                            total_sent_shared.value += 1
                    else:
                        with total_errors_shared.get_lock():
                            total_errors_shared.value += 1
                except Exception:
                    with total_errors_shared.get_lock():
                        total_errors_shared.value += 1

                # Precise pacing for the worker loop
                elapsed = time.perf_counter() - start_loop
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

    try:
        asyncio.run(_start_async_worker())
    except Exception as e:
        logger.error(f"Worker-{worker_id} stopped unexpectedly: {e}", exc_info=True)

# --- API ENDPOINTS ---

@app.get("/status")
async def get_status():
    # This endpoint serves UI status display; /metrics is the Prometheus source of truth
    return {
        "is_running": state.is_running,
        "sent_ok": total_sent_shared.value,
        "errors": total_errors_shared.value,
        "config": state.config,
        "active_processes": len([p for p in state.processes if p.is_alive()])
    }

@app.post("/config")
async def update_config(new_config: SimulatorConfig):
    if state.is_running:
        raise HTTPException(status_code=400, detail="Stop the simulator first")
    state.config = new_config
    return {"status": "updated", "config": state.config}

@app.post("/start")
async def start_simulation():
    if state.is_running:
        return {"status": "already running"}

    # Reset stats
    total_sent_shared.value = 0
    total_errors_shared.value = 0
    state.last_sent_val = 0
    state.last_errors_val = 0
    stop_event_shared.clear()

    # Distribute sensors evenly across available CPU cores
    num_procs = mp.cpu_count()
    all_sensors = [f"SN-{i:03d}" for i in range(state.config.num_sensors)]
    chunks = [all_sensors[i::num_procs] for i in range(num_procs)]

    state.processes = []
    for i in range(num_procs):
        if not chunks[i]: continue
        p = mp.Process(target=run_worker, args=(i, chunks[i], state.config))
        p.start()
        state.processes.append(p)

    state.is_running = True
    return {"status": "started", "cores_used": len(state.processes)}

@app.post("/stop")
async def stop_simulation():
    stop_event_shared.set()
    for p in state.processes:
        p.join(timeout=2)
        if p.is_alive(): p.terminate()

    state.processes = []
    state.is_running = False
    return {"status": "stopped"}

@app.get("/metrics")
async def metrics():
    # Compute delta since last scrape and increment Prometheus counters accordingly.
    # This bridges the shared-memory counters (written by worker processes) into
    # the Prometheus client (which lives only in the master process).
    current_sent = total_sent_shared.value
    current_errors = total_errors_shared.value

    sent_delta = current_sent - state.last_sent_val
    errors_delta = current_errors - state.last_errors_val

    # Only increment on positive delta to guard against counter resets
    if sent_delta > 0:
        SIM_REQ_SENT.inc(sent_delta)
    if errors_delta > 0:
        SIM_REQ_ERROR.inc(errors_delta)

    state.last_sent_val = current_sent
    state.last_errors_val = current_errors

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
