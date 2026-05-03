"""
IoT Aggregation Middleware — FastAPI service for batching IoT sensor messages,
uploading them to IPFS, and anchoring the content hash on a Hyperledger Besu PoA chain.

Supports three batching strategies:
  - volume: flush when the buffer reaches BATCH_SIZE_LIMIT messages
  - time:   flush when BATCH_TIME_LIMIT seconds have elapsed
  - none:   process each message individually without buffering

Configuration is done entirely via environment variables (see CONFIGURATION section below).
"""
import asyncio
import time
import json
import os
import csv
import httpx
import random
from pathlib import Path
from fastapi import FastAPI, Response, Request, UploadFile, File
from fastapi.responses import FileResponse
import base58
from pydantic import BaseModel, Field
from web3 import AsyncWeb3, AsyncHTTPProvider, Web3
from web3.middleware import async_geth_poa_middleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from enum import Enum

# --- CONFIGURATION (via environment variables) ---
BESU_NODES = os.getenv("BESU_NODES", "http://localhost:8545").split(",")
IPFS_NODES = os.getenv("IPFS_NODES", "http://localhost:5001").split(",")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS", "0xa5676dBaCDc4388013727E17161c25aC53656CE2")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
SENDER_ADDRESS = os.getenv("SENDER_ADDRESS", "")

# Aggregation parameters
class BatchingStrategy(str, Enum):
    TIME = "time"
    VOLUME = "volume"
    NONE = "none"

BATCH_SIZE_LIMIT = int(os.getenv("BATCH_SIZE_LIMIT", "50"))
BATCH_TIME_LIMIT = int(os.getenv("BATCH_TIME_LIMIT", "30"))
BATCHING_STRATEGY = BatchingStrategy(os.getenv("BATCHING_STRATEGY", "volume"))
USE_ROUND_ROBIN = os.getenv("USE_ROUND_ROBIN", "false").lower() == "true"
CADVISOR_URL = os.getenv("CADVISOR_URL", "http://localhost:8888")

# --- METRICS ---
REQ_TOTAL = Counter("middleware_requests_total", "Total number of ingest requests")
BATCHES_PROCESSED_TOTAL = Counter("middleware_batches_processed_total", "Number of successfully processed batches")
BATCHES_FAILED_TOTAL = Counter("middleware_batches_failed_total", "Number of failed batches that will be retried")
BUFFER_GAUGE = Gauge("middleware_buffer_size", "Current number of items in the buffer for time/volume strategy")
INGEST_QUEUE_GAUGE = Gauge("middleware_ingest_queue_size", "Current number of items in the ingest queue for 'none' strategy")
BATCH_DURATION = Histogram("middleware_batch_processing_seconds", "Duration of a complete batch processing cycle")
IPFS_DURATION = Histogram("middleware_ipfs_upload_seconds", "Duration of the IPFS upload")
CHAIN_DURATION = Histogram("middleware_chain_anchor_seconds", "Duration of the blockchain transaction")
PROPAGATION_DELAY = Histogram("middleware_global_propagation_seconds", "Time until data is visible on all nodes")
E2E_FINALITY = Histogram("middleware_e2e_finality_seconds", "End-to-end latency from ingest to global propagation")

app = FastAPI(title="IoT-Aggregation Middleware")

# Global state
data_buffer = []
ingest_queue = asyncio.Queue()
last_flush_time = time.time()
# The lock ensures that only one batch is processed at a time (nonce safety)
lock = asyncio.Lock()

# --- CSV LOGGING ---
CSV_DIR = Path(os.getenv("CSV_DIR", "/app/data"))
EXPERIMENT_LABEL = "unlabeled"

BATCH_CSV_FIELDS = [
    "timestamp", "scenario_label", "batching_strategy", "batch_param",
    "batch_size", "ipfs_duration_s", "chain_duration_s", "batch_duration_s",
    "tx_hash", "ipfs_cid",
    "cpu_percent", "memory_usage_bytes", "memory_limit_bytes", "throughput_msg_per_s",
]
E2E_CSV_FIELDS = [
    "timestamp", "scenario_label", "tx_hash", "batch_size",
    "propagation_delay_s", "e2e_finality_min_s", "e2e_finality_max_s",
    "e2e_finality_mean_s", "nodes_total", "nodes_synced",
]

def _parse_cadvisor_ts(ts_str: str):
    """Parse cAdvisor timestamp like '2024-01-01T12:00:00.123456789Z' into datetime."""
    from datetime import datetime, timezone
    ts = ts_str.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".", 1)
        ts = f"{base}.{frac[:6]}"  # truncate to microseconds (Python limit)
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)


async def get_container_metrics():
    """Query cAdvisor for current middleware container CPU and memory usage.
    Returns dict with cpu_percent, memory_usage_bytes, memory_limit_bytes or None on failure."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{CADVISOR_URL}/api/v1.3/docker/middleware")
            response.raise_for_status()
            data = response.json()

        container_data = next(iter(data.values()))
        stats = container_data.get("stats", [])
        if len(stats) < 2:
            return None

        s1, s2 = stats[-2], stats[-1]

        # CPU: delta of cumulative nanoseconds / time delta / allocated CPUs
        cpu_delta_ns = s2["cpu"]["usage"]["total"] - s1["cpu"]["usage"]["total"]
        t1 = _parse_cadvisor_ts(s1["timestamp"])
        t2 = _parse_cadvisor_ts(s2["timestamp"])
        time_delta_ns = (t2 - t1).total_seconds() * 1e9

        # CPU shares: 1024 per CPU (3 CPUs = 3072 shares)
        cpu_shares = container_data.get("spec", {}).get("cpu", {}).get("limit", 3072)
        num_cpus = cpu_shares / 1024.0
        cpu_percent = (cpu_delta_ns / time_delta_ns / num_cpus * 100) if time_delta_ns > 0 else 0.0

        # Memory
        memory_usage = s2.get("memory", {}).get("usage", 0)
        memory_limit = container_data.get("spec", {}).get("memory", {}).get("limit", 0)

        return {
            "cpu_percent": round(cpu_percent, 2),
            "memory_usage_bytes": memory_usage,
            "memory_limit_bytes": memory_limit,
        }
    except Exception as e:
        print(f"WARNING: cAdvisor query failed: {e}")
        return None


def _ensure_csv(filepath, fieldnames):
    """Create CSV file with header if it does not exist."""
    if not filepath.exists():
        CSV_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

def _append_csv(filepath, fieldnames, row):
    """Append a single row to a CSV file. Creates file+header if missing."""
    _ensure_csv(filepath, fieldnames)
    with open(filepath, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow(row)

from typing import Optional

# Pydantic model for POST /config
class UpdateConfigRequest(BaseModel):
    batching_strategy: Optional[BatchingStrategy] = None
    batch_size_limit: Optional[int] = Field(None, gt=0, description="Must be greater than 0")
    batch_time_limit: Optional[int] = Field(None, gt=0, description="Must be greater than 0")

class ExperimentLabelRequest(BaseModel):
    label: str = Field(..., min_length=1, max_length=50, pattern=r'^[A-Za-z0-9_-]+$')

# Minimal ABI for the anchor contract
CONTRACT_ABI = json.loads('''
[
    {
        "inputs": [
            {"name": "_docId", "type": "bytes32"},
            {"name": "_ipfsDigest", "type": "bytes32"}
        ],
        "name": "anchorData",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "docId", "type": "bytes32"},
            {"indexed": true, "name": "ipfsDigest", "type": "bytes32"},
            {"indexed": false, "name": "timestamp", "type": "uint256"}
        ],
        "name": "DataAnchored",
        "type": "event"
    }
]
''')

@app.get("/metrics")
async def metrics():
    """Explicit metrics endpoint for Prometheus."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/config", tags=["Configuration"])
async def get_config():
    """Returns the current aggregation configuration."""
    return {
        "batching_strategy": BATCHING_STRATEGY,
        "batch_size_limit": BATCH_SIZE_LIMIT,
        "batch_time_limit": BATCH_TIME_LIMIT
    }

@app.post("/config", tags=["Configuration"])
async def update_config(config_request: UpdateConfigRequest):
    """Updates aggregation parameters at runtime."""
    global BATCHING_STRATEGY, BATCH_SIZE_LIMIT, BATCH_TIME_LIMIT

    if config_request.batching_strategy is not None:
        BATCHING_STRATEGY = config_request.batching_strategy
    if config_request.batch_size_limit is not None:
        BATCH_SIZE_LIMIT = config_request.batch_size_limit
    if config_request.batch_time_limit is not None:
        BATCH_TIME_LIMIT = config_request.batch_time_limit

    print(f"Configuration updated: strategy={BATCHING_STRATEGY}, batch_size={BATCH_SIZE_LIMIT}, batch_time={BATCH_TIME_LIMIT}")

    return {"status": "success", "new_config": {
        "batching_strategy": BATCHING_STRATEGY,
        "batch_size_limit": BATCH_SIZE_LIMIT,
        "batch_time_limit": BATCH_TIME_LIMIT
    }}

@app.get("/experiment-label", tags=["Experiment"])
async def get_experiment_label():
    """Returns the current experiment label."""
    return {"label": EXPERIMENT_LABEL}

@app.post("/experiment-label", tags=["Experiment"])
async def set_experiment_label(req: ExperimentLabelRequest):
    """Sets the experiment label used for CSV recording."""
    global EXPERIMENT_LABEL
    EXPERIMENT_LABEL = req.label
    print(f"Experiment label set: {EXPERIMENT_LABEL}")
    return {"status": "success", "label": EXPERIMENT_LABEL}

@app.post("/generate-plots", tags=["Experiment"])
async def generate_plots():
    """Triggers boxplot PDF generation from collected CSV data."""
    import subprocess
    plots_dir = CSV_DIR / "plots"
    batch_csv = CSV_DIR / "batch_metrics.csv"
    e2e_csv = CSV_DIR / "e2e_metrics.csv"

    if not batch_csv.exists() and not e2e_csv.exists():
        return Response(status_code=400, content="No CSV data available yet. Run experiments first.")

    result = subprocess.run(
        ["python", "/app/generate_boxplots.py",
         "--data-dir", str(CSV_DIR),
         "--output-dir", str(plots_dir)],
        capture_output=True, text=True, timeout=120,
    )

    if result.returncode != 0:
        print(f"ERROR: Plot generation failed:\n{result.stderr}")
        return {"status": "error", "stderr": result.stderr, "stdout": result.stdout}

    # List generated files
    generated = [f.name for f in plots_dir.iterdir() if f.suffix == ".pdf"] if plots_dir.exists() else []
    print(f"{len(generated)} plots generated: {generated}")
    return {
        "status": "success",
        "files": generated,
        "stdout": result.stdout,
    }

@app.get("/experiment-data/{filename}", tags=["Experiment"])
async def download_experiment_file(filename: str):
    """Downloads a CSV or PDF experiment data file."""
    allowed_csv = {"batch_metrics.csv", "e2e_metrics.csv"}
    allowed_pdf = {
        "ff1_batch_duration.pdf", "ff1_chain_duration.pdf", "ff1_ipfs_duration.pdf",
        "ff2_e2e_finality.pdf", "ff2_propagation.pdf",
        "ff1_cpu_vs_load.pdf", "ff1_ram_vs_load.pdf", "ff1_tx_comparison.pdf",
    }

    if filename in allowed_csv:
        path = CSV_DIR / filename
        media_type = "text/csv"
    elif filename in allowed_pdf:
        path = CSV_DIR / "plots" / filename
        media_type = "application/pdf"
    else:
        return Response(status_code=404, content="Not found")

    if not path.exists():
        return Response(status_code=404, content="File not available. Generate plots first." if filename.endswith(".pdf") else "No data yet")
    return FileResponse(str(path), media_type=media_type, filename=filename)

@app.post("/upload-experiment-data/{filename}", tags=["Experiment"])
async def upload_experiment_file(filename: str, file: UploadFile = File(...)):
    """Restores a CSV data file after infrastructure recreation so experiments can continue."""
    allowed = {"batch_metrics.csv", "e2e_metrics.csv"}
    if filename not in allowed:
        return Response(status_code=400, content=f"Only {', '.join(allowed)} are accepted")
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    (CSV_DIR / filename).write_bytes(content)
    print(f"Restored {filename} ({len(content):,} bytes)")
    return {"status": "success", "filename": filename, "bytes": len(content)}

def get_node(node_list):
    """Selects the target node based on the configured strategy (round-robin or first)."""
    if USE_ROUND_ROBIN:
        return random.choice(node_list)
    return node_list[0]

async def check_propagation(tx_hash, ingest_timestamps):
    """
    Polls all Besu nodes until the transaction receipt is visible on each one,
    then records propagation delay and end-to-end finality metrics.
    Times out after 30 seconds (60 polls at 0.5s intervals).
    """
    start_check = time.perf_counter()
    nodes_synced = set()
    total_nodes = len(BESU_NODES)

    print(f"Starting propagation check for tx {tx_hash.hex()} across {total_nodes} nodes...")

    for _ in range(60):
        if len(nodes_synced) >= total_nodes:
            break

        for node_url in BESU_NODES:
            if node_url in nodes_synced:
                continue

            try:
                w3_check = AsyncWeb3(AsyncHTTPProvider(node_url, request_kwargs={'timeout': 5}))
                w3_check.middleware_onion.inject(async_geth_poa_middleware, layer=0)

                receipt = await w3_check.eth.get_transaction_receipt(tx_hash)

                if receipt is not None:
                    nodes_synced.add(node_url)
                    print(f"  - Tx confirmed on node {node_url} (block: {receipt.blockNumber})")

            except Exception:
                # Errors are expected while the tx is still propagating
                pass

        await asyncio.sleep(0.5)

    duration = time.perf_counter() - start_check

    if len(nodes_synced) == total_nodes:
        now = time.time()
        e2e_values = [now - ts for ts in ingest_timestamps]
        for val in e2e_values:
            E2E_FINALITY.observe(val)
        print(f"All {total_nodes} nodes synced in {duration:.2f}s.")
        PROPAGATION_DELAY.observe(duration)

        _append_csv(CSV_DIR / "e2e_metrics.csv", E2E_CSV_FIELDS, {
            "timestamp": now,
            "scenario_label": EXPERIMENT_LABEL,
            "tx_hash": tx_hash.hex(),
            "batch_size": len(ingest_timestamps),
            "propagation_delay_s": f"{duration:.6f}",
            "e2e_finality_min_s": f"{min(e2e_values):.6f}",
            "e2e_finality_max_s": f"{max(e2e_values):.6f}",
            "e2e_finality_mean_s": f"{sum(e2e_values)/len(e2e_values):.6f}",
            "nodes_total": total_nodes,
            "nodes_synced": len(nodes_synced),
        })
    else:
        print(f"WARNING: Timeout — only {len(nodes_synced)} of {total_nodes} nodes synced after {duration:.2f}s.")

async def process_batch(items):
    """
    Core pipeline: buffer items -> IPFS upload -> blockchain anchor.
    Returns True on success, False on error (caller should retry).
    """
    if not items:
        return True

    # The lock ensures transactions are sent sequentially to avoid "nonce too low" errors
    async with lock:
        start_time = time.perf_counter()

        current_batch = items
        print(f"Processing batch of {len(current_batch)} items...")

        try:
            # 1. IPFS Upload
            target_ipfs = get_node(IPFS_NODES)
            ipfs_start = time.perf_counter()
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(f"{target_ipfs}/api/v0/add", files={'file': json.dumps(current_batch)}, timeout=30)
                response.raise_for_status()
                ipfs_cid_str = response.json()["Hash"]
            ipfs_dur = time.perf_counter() - ipfs_start
            IPFS_DURATION.observe(ipfs_dur)

            # Workaround for 'CID' object has no attribute 'multihash' error.
            # Manually decode the CIDv0 string to extract the raw digest.
            # A CIDv0 (starting with 'Qm') is a base58-encoded multihash.
            # The multihash format is: <hash_code><hash_length><hash_digest>
            # For the default sha256, this is 0x12, 0x20, followed by 32 bytes of digest.
            # We slice off the first two bytes to get the raw digest for the contract.
            multihash_bytes = base58.b58decode(ipfs_cid_str)
            raw_ipfs_digest = multihash_bytes[2:]

            # 2. Blockchain Anchor (AsyncWeb3)
            target_besu = get_node(BESU_NODES)
            chain_start = time.perf_counter()
            w3 = AsyncWeb3(AsyncHTTPProvider(target_besu, request_kwargs={'timeout': 30}))
            w3.middleware_onion.inject(async_geth_poa_middleware, layer=0)

            contract = w3.eth.contract(address=w3.to_checksum_address(CONTRACT_ADDRESS), abi=CONTRACT_ABI)
            doc_id_str = f"BATCH-{int(time.time())}-{random.randint(1000, 9999)}"

            doc_id_hex = w3.to_hex(Web3.keccak(text=doc_id_str))
            ipfs_digest_hex = w3.to_hex(raw_ipfs_digest)

            # 'pending' is important under load to get the correct next nonce
            nonce = await w3.eth.get_transaction_count(w3.to_checksum_address(SENDER_ADDRESS), 'pending')
            current_gas_price = await w3.eth.gas_price

            tx = await contract.functions.anchorData(doc_id_hex, ipfs_digest_hex).build_transaction({
                "from": w3.to_checksum_address(SENDER_ADDRESS),
                "nonce": nonce,
                "gas": 200000,
                "gasPrice": current_gas_price
            })

            signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash = await w3.eth.send_raw_transaction(signed_tx.rawTransaction)

            chain_dur = time.perf_counter() - chain_start
            batch_dur = time.perf_counter() - start_time
            CHAIN_DURATION.observe(chain_dur)
            BATCH_DURATION.observe(batch_dur)
            BATCHES_PROCESSED_TOTAL.inc()
            print(f"Batch transaction sent: {tx_hash.hex()}")

            batch_param = BATCH_SIZE_LIMIT if BATCHING_STRATEGY == BatchingStrategy.VOLUME else (
                BATCH_TIME_LIMIT if BATCHING_STRATEGY == BatchingStrategy.TIME else 0
            )
            throughput = len(current_batch) / batch_dur if batch_dur > 0 else 0.0
            container_metrics = await get_container_metrics()
            _append_csv(CSV_DIR / "batch_metrics.csv", BATCH_CSV_FIELDS, {
                "timestamp": time.time(),
                "scenario_label": EXPERIMENT_LABEL,
                "batching_strategy": BATCHING_STRATEGY.value,
                "batch_param": batch_param,
                "batch_size": len(current_batch),
                "ipfs_duration_s": f"{ipfs_dur:.6f}",
                "chain_duration_s": f"{chain_dur:.6f}",
                "batch_duration_s": f"{batch_dur:.6f}",
                "tx_hash": tx_hash.hex(),
                "ipfs_cid": ipfs_cid_str,
                "cpu_percent": f"{container_metrics['cpu_percent']:.2f}" if container_metrics else "",
                "memory_usage_bytes": container_metrics["memory_usage_bytes"] if container_metrics else "",
                "memory_limit_bytes": container_metrics["memory_limit_bytes"] if container_metrics else "",
                "throughput_msg_per_s": f"{throughput:.4f}",
            })

            ingest_timestamps = [item["ts"] for item in current_batch]
            asyncio.create_task(check_propagation(tx_hash, ingest_timestamps))
            return True

        except Exception as e:
            print(f"ERROR: Batch processing failed: {e}")
            BATCHES_FAILED_TOTAL.inc()
            return False

async def aggregation_worker():
    """Background worker for time- and volume-based aggregation strategies."""
    global last_flush_time
    while True:
        # Short sleep interval for fast response to a full buffer
        await asyncio.sleep(0.2)

        items_to_process = None
        async with lock:
            # Volume trigger is checked first to strictly enforce batch size
            if BATCHING_STRATEGY == BatchingStrategy.VOLUME and len(data_buffer) >= BATCH_SIZE_LIMIT:
                print(f"Volume trigger fired. Buffer size: {len(data_buffer)}, batch limit: {BATCH_SIZE_LIMIT}")

                # Take exactly BATCH_SIZE_LIMIT items from the front of the buffer
                items_to_process = data_buffer[:BATCH_SIZE_LIMIT]
                del data_buffer[:BATCH_SIZE_LIMIT]

                last_flush_time = time.time()

            # Time trigger is only checked when the volume trigger did not fire
            elif BATCHING_STRATEGY == BatchingStrategy.TIME and time.time() - last_flush_time >= BATCH_TIME_LIMIT and len(data_buffer) > 0:
                print(f"Time trigger fired. Buffer size: {len(data_buffer)}")

                # Flush the entire buffer on a time-based flush
                items_to_process = list(data_buffer)
                data_buffer.clear()

                last_flush_time = time.time()

            if items_to_process:
                BUFFER_GAUGE.set(len(data_buffer))

        if items_to_process:
            # Processing is sequential to avoid nonce conflicts
            success = await process_batch(items_to_process)
            if not success:
                print(f"Batch processing failed. Re-buffering {len(items_to_process)} items.")
                await asyncio.sleep(1)
                async with lock:
                    # Prepend failed items to preserve ordering
                    data_buffer[0:0] = items_to_process
                    BUFFER_GAUGE.set(len(data_buffer))

async def none_strategy_worker():
    """Background worker that drains the ingest queue for the 'none' (no-buffering) strategy."""
    while True:
        item = await ingest_queue.get()
        INGEST_QUEUE_GAUGE.dec()

        # Await the result to retry on failure and to serialize requests (prevents nonce conflicts)
        success = await process_batch([item])
        if not success:
            print(f"Single-item processing failed. Re-queueing item.")
            await asyncio.sleep(1)
            await ingest_queue.put(item)
            INGEST_QUEUE_GAUGE.inc()

        ingest_queue.task_done()

@app.on_event("startup")
async def startup_event():
    """Starts background workers for aggregation and none-strategy processing."""
    global last_flush_time
    last_flush_time = time.time()
    asyncio.create_task(aggregation_worker())
    asyncio.create_task(none_strategy_worker())

@app.post("/ingest")
async def ingest(payload: dict):
    """Accepts incoming sensor data and routes it according to the active batching strategy."""
    REQ_TOTAL.inc()
    item = {"ts": time.time(), "data": payload}

    # Strategy routing

    # 1. No aggregation: enqueue for immediate individual processing
    if BATCHING_STRATEGY == BatchingStrategy.NONE:
        await ingest_queue.put(item)
        INGEST_QUEUE_GAUGE.inc()
        return {"status": "queued_for_processing", "queue_size": ingest_queue.qsize()}

    # 2. Time/volume strategies: append to buffer; the aggregation_worker handles flushing
    async with lock:
        data_buffer.append(item)
        current_size = len(data_buffer)
        BUFFER_GAUGE.set(current_size)

    return {"status": "buffered", "count": current_size}
