# Bachelorarbeit — IoT-IPFS-Blockchain Aggregation Testbed

Automated infrastructure and experiment platform for a Bachelor's thesis on aggregation strategies in a hybrid IPFS-Blockchain storage system for agricultural supply-chain traceability.

**Research question:** How do time-based and volume-based aggregation strategies on an API gateway affect system latency, throughput, and resource efficiency of a hybrid IPFS-Blockchain storage solution?

---

## Architecture

The system is deployed on Hetzner Cloud and managed entirely via Ansible. Two servers are provisioned:

| Host role | Services |
|-----------|----------|
| **Gateway** | Middleware (FastAPI), cAdvisor |
| **Control** | Load Simulator, Web UI, Monitoring Stack (Prometheus, Grafana, Node Exporter, cAdvisor) |
| **Storage** (× N) | Hyperledger Besu (PoA), IPFS |

All services run in Docker containers. The smart contract `ThesisTraceability.sol` is deployed to Besu during `node_setup.yml` and its address is written to `.tmp_artifacts/deployed_addresses.json` for consumption by downstream playbooks.

### Component overview

```
IoT Simulator  ──►  Middleware (Gateway)  ──►  IPFS Node
                        │                       │
                        │ anchorData()           │ CID
                        ▼                        ▼
                    Besu Node  ◄──────────  Smart Contract
                        │
                    Propagation check (all Besu nodes)
                        │
                    CSV logging  ──►  generate_boxplots.py  ──►  PNG plots

Web UI (port 3333)  ──►  proxies all control APIs
                     ──►  WebSocket log stream
                     ──►  Grafana dashboards
```

**Middleware** (`roles/middleware/files/main.py`): FastAPI service that buffers incoming IoT messages and flushes them to IPFS + Besu according to the active aggregation strategy. Records per-batch timing and container metrics (CPU, RAM) to CSV. After each batch, asynchronously checks propagation across all Besu nodes and records end-to-end finality to a second CSV.

**Web UI** (`roles/web_ui/files/app/`): Node.js + React single-page application that proxies all control APIs, streams server logs over WebSocket, and provides a 4-page interface for running and monitoring experiments.

---

## Experiment Scenarios

Seven aggregation strategies are tested at three load levels (1 / 5 / 10 msg/s), each repeated 5 times:

| Scenario | Strategy | Parameter |
|----------|----------|-----------|
| **N** | Naive 1:1 (no buffering) | — |
| **V1** | Volume-based batching | 50 messages |
| **V2** | Volume-based batching | 100 messages |
| **V3** | Volume-based batching | 500 messages |
| **Z1** | Time-based batching | 10 seconds |
| **Z2** | Time-based batching | 20 seconds |
| **Z3** | Time-based batching | 60 seconds |

All scenarios use 50 simulated sensors. The full matrix is 7 scenarios × 3 rates × 5 repetitions = **105 runs**, automated via the scheduler.

---

## Prerequisites

- Ansible with the Hetzner Cloud collection (`hcloud`)
- A Hetzner Cloud API token in `group_vars/all/hetzner.yml`
- SSH key configured for Ansible
- Python 3 on the control machine (for the plot-fetch script)

---

## Required Configuration Before Deployment

The following values must be set before running any playbooks. All secrets are stored as placeholders in this repository — fill them in locally and never commit the real values.

| File | Variable / Key | Description |
|------|---------------|-------------|
| `group_vars/all/hetzner.yml` | `hcloud_token` | Hetzner Cloud API token — create one at console.hetzner.cloud |
| `group_vars/all/hetzner.yml` | `hcloud_ssh_key.name` | Name of the SSH key in your Hetzner Cloud account |
| `group_vars/all/hetzner.yml` | `hcloud_ssh_key.key` | SSH public key that Ansible will use to access provisioned servers |
| `group_vars/all/defaults.yml` | `grafana_admin_password` | Grafana web UI admin password |
| `group_vars/all/defaults.yml` | `besu_enode_key` | 64-char hex private key for the Besu PoA authority node and transaction signer |
| `roles/middleware/templates/docker_compose_middleware.yml.j2` | `SENDER_ADDRESS` | Ethereum address corresponding to the private key above |
| `roles/middleware/templates/docker_compose_middleware.yml.j2` | `PRIVATE_KEY` | Same private key as `besu_enode_key`, prefixed with `0x` |
| `ansible.cfg` | `private_key_file` | Local path to the SSH private key Ansible uses to connect to servers |

The `SENDER_ADDRESS` and `PRIVATE_KEY` in the middleware template must match `besu_enode_key` and its corresponding Ethereum address. This network has no real monetary value, but treat these keys as secrets regardless.

---

## Deployment

### 1. Deploy infrastructure

```bash
./run_deploy_environment.sh
```

This script runs all four playbooks in sequence:
1. `hetzer_environment.yml` — provisions Hetzner Cloud servers
2. `basic_config.yml` — installs Docker, hardens SSH, creates users
3. `node_setup.yml` — deploys Besu + IPFS, deploys the smart contract
4. `middleware_setup.yml` — deploys middleware, load simulator, monitoring stack, and web UI

If new servers are created the script waits 3 minutes for them to initialize before continuing.

### 2. Re-deploy only the application layer

After the initial infrastructure deployment, individual plays can be run directly:

```bash
# Middleware only (e.g. after code changes)
ansible-playbook plays/middleware_setup.yml

# Monitoring + web UI only
ansible-playbook plays/monitoring_setup.yml
```

### 3. Tear down

```bash
./run_cleanup_environment.sh
```

Destroys all Hetzner Cloud resources. **Save your CSV data first** (see below).

---

## Running Experiments

Open the Web UI in a browser at `http://<control-host-ip>:3333`.

The UI has four pages accessible via the tab bar:

| Page | Purpose |
|------|---------|
| **Scheduler** | Build and run the automated experiment queue |
| **Dashboard** | Manual simulator/middleware controls, live metrics charts |
| **Records** | Browse on-chain records, fetch IPFS content, download data |
| **Logs** | Full server log stream |

### Option A — Automated scheduler (recommended)

1. Go to the **Scheduler** page.
2. Click **Generate Full Matrix** to populate the queue with all 21 scenario-rate combinations (each with 5 repetitions, 2 min warmup, 5 min experiment, 2 min cooldown).
3. Optionally edit individual items (duration, repetitions, cooldown) or delete entries.
4. Click **Start**. The scheduler runs entirely server-side — you can close the browser and come back.

Each run is labelled automatically (e.g. `V1_5rps_r3`) so warmup data is excluded from analysis and per-run aggregation works correctly in the plot scripts.

Live progress shows the current experiment, run number, phase (warmup / experiment / cooldown), a countdown timer, and an estimated completion time.

Use **Skip Current** to abort the current experiment and move to the next, or **Stop** to cancel the entire queue.

### Option B — Manual single experiment

1. Go to the **Dashboard** page.
2. Set the middleware configuration (strategy + parameter) under **Middleware Config**.
3. Set the simulator load (sensors, msg/s) under **Simulator Config**.
4. Set a label under **Experiment Label** (format: `{Scenario}_{Rate}rps`, e.g. `V1_5rps`).
5. Click **Start Simulator**. Run for the desired duration, then **Stop Simulator**.

---

## Fetching Data and Generating Plots

The `run_fetch_plots.sh` script manages experiment data between your local machine and the server.

### Download plots (after generating them via the UI)

```bash
./run_fetch_plots.sh
```

Downloads all PNG plots and both CSVs into `.tmp_plots/`.

### Generate plots on the server, then download

```bash
./run_fetch_plots.sh --generate
```

Triggers `generate_boxplots.py` on the middleware server, then downloads everything.

### Save CSVs before tearing down infrastructure

```bash
./run_fetch_plots.sh --fetch-csv
```

Downloads only `batch_metrics.csv` and `e2e_metrics.csv` to `.tmp_plots/`. Run this before `run_cleanup_environment.sh` if you want to keep your data.

### Restore CSVs to a freshly deployed server

```bash
./run_fetch_plots.sh --upload-csv
```

Uploads locally saved CSVs back to a new middleware instance so experiments can continue from where they left off. Requires `--fetch-csv` to have been run first.

---

## Output Data

### CSV files (on the middleware server at `/opt/bacc/middleware/data/`)

**`batch_metrics.csv`** — one row per processed batch:

| Column | Description |
|--------|-------------|
| `scenario_label` | Experiment label (e.g. `V1_5rps_r2`) |
| `batching_strategy` | `volume`, `time`, or `none` |
| `batch_param` | Size limit (volume) or time limit (time) |
| `batch_size` | Number of messages in this batch |
| `ipfs_duration_s` | IPFS upload time |
| `chain_duration_s` | Blockchain transaction time |
| `batch_duration_s` | Total batch processing time |
| `cpu_percent` | Middleware container CPU at batch time |
| `memory_usage_bytes` | Middleware container RAM at batch time |
| `throughput_msg_per_s` | `batch_size / batch_duration_s` |
| `tx_hash` / `ipfs_cid` | On-chain and IPFS references |

**`e2e_metrics.csv`** — one row per propagation check:

| Column | Description |
|--------|-------------|
| `scenario_label` | Experiment label |
| `propagation_delay_s` | Time until all Besu nodes confirmed the TX |
| `e2e_finality_mean_s` | Mean time from message ingest to global confirmation |
| `e2e_finality_min_s` / `e2e_finality_max_s` | Per-batch min/max TTF |

Rows labelled `unlabeled` are warmup data and are automatically excluded during plot generation.

### Generated PNG plots

Boxplot metrics produce one PNG per scenario (e.g. `ff1_batch_duration_N.png` … `ff1_batch_duration_Z3.png`). Line and bar charts are single files.

| File pattern | Content | Research question |
|---|---|---|
| `ff1_batch_duration_{scenario}.png` | Boxplots: batch processing time per rate | FF1 |
| `ff1_chain_duration_{scenario}.png` | Boxplots: blockchain TX time per rate | FF1 |
| `ff1_ipfs_duration_{scenario}.png` | Boxplots: IPFS upload time per rate | FF1 |
| `ff1_cpu_vs_load.png` | Line chart: CPU% vs load, one line per scenario | FF1 |
| `ff1_ram_vs_load.png` | Line chart: RAM (MB) vs load, one line per scenario | FF1 |
| `ff1_tx_comparison.png` | Bar chart: blockchain TXs per run by scenario/rate | FF1 |
| `ff2_e2e_finality_{scenario}.png` | Boxplots: end-to-end Time-to-Finality per rate | FF2 |
| `ff2_propagation_{scenario}.png` | Boxplots: global propagation delay per rate | FF2 |

FF1 boxplots use per-run aggregation (N=5 values per box, one mean per repetition) and show the arithmetic mean as a red diamond. FF2 boxplots pool all individual values from all repetitions to show the full TTF distribution.

---

## Monitoring

Grafana is available at `http://<control-host-ip>:3000` (admin / `<grafana_admin_password from defaults.yml>`).

The **Thesis Master Analytics** dashboard (uid: `thesis-hcloud-master`) shows:
- Live ingest rate vs batch rate
- Write reduction factor (`requests_total / batches_processed_total`)
- Batch / IPFS / chain duration histograms
- Time-to-Finality percentiles
- Besu transaction pool depth
- Container CPU and RAM (via cAdvisor)

---

## Key Configuration

All defaults are in `group_vars/all/defaults.yml`. The middleware supports live configuration changes via the API (no restart required):

```bash
# Change to time-based batching, 20s window
curl -X POST http://<gateway>:8000/config \
  -H 'Content-Type: application/json' \
  -d '{"batching_strategy": "time", "batch_time_limit": 20}'

# Set experiment label manually
curl -X POST http://<gateway>:8000/experiment-label \
  -H 'Content-Type: application/json' \
  -d '{"label": "Z2_5rps"}'
```
