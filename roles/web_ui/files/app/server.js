import express from 'express';
import morgan from 'morgan';
import cors from 'cors';
import axios from 'axios';
import path from 'path';
import { fileURLToPath } from 'url';
import { ethers } from 'ethers';
import { WebSocketServer } from 'ws';
import http from 'http';
import { CID } from 'multiformats/cid';
import * as digest from 'multiformats/hashes/digest';
import { sha256 } from 'multiformats/hashes/sha2';
import abi from './abi.json' with { type: "json" };

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


// --- CONFIGURATION ---
const PORT = process.env.PORT || 3000;
const SIMULATOR_API_URL = process.env.SIMULATOR_API_URL || 'http://localhost:9011';
const MIDDLEWARE_API_URL = process.env.MIDDLEWARE_API_URL; // e.g., 'http://localhost:8000'
const RPC_PROVIDER_URL = process.env.RPC_PROVIDER_URL || 'http://localhost:8545';
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS; // This MUST be provided
const IPFS_GATEWAY_URL = process.env.IPFS_GATEWAY_URL || 'http://localhost:8080/ipfs/';

// --- INITIALIZATION ---
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(cors());
app.use(morgan('dev'));

// --- WEBSOCKET LOGGING ---
const clients = new Set();
wss.on('connection', (ws) => {
    clients.add(ws);
    logToUI('INFO', 'Log stream connected.');
    ws.on('close', () => {
        clients.delete(ws);
    });
});

function logToUI(level, message, data) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    data,
  };
  const formattedMessage = JSON.stringify(logEntry);
  console.log(`[${level}] ${message}`, data || '');
  clients.forEach(client => {
    if (client.readyState === 1) { // 1 means OPEN
      client.send(formattedMessage);
    }
  });
}

// --- STATIC FRONTEND ---
app.use(express.static(path.join(__dirname, 'public')));

// --- SCHEDULER STATE ---
let scheduler = {
  status: 'idle',             // 'idle' | 'running' | 'completed' | 'error'
  queue: [],                  // Array of experiment definitions
  currentExperimentIndex: -1,
  currentRunIndex: -1,
  currentPhase: null,         // 'warmup' | 'experiment' | 'cooldown' | null
  phaseStartedAt: null,
  phaseDurationMinutes: null,
  startedAt: null,
  timers: [],                 // [{ timer, resolve, reject }]
  transitions: [],
  error: null,
  skipCurrent: false,
};

function resetScheduler() {
  cancelAllTimers('cancelled');
  scheduler = {
    status: 'idle',
    queue: [],
    currentExperimentIndex: -1,
    currentRunIndex: -1,
    currentPhase: null,
    phaseStartedAt: null,
    phaseDurationMinutes: null,
    startedAt: null,
    timers: [],
    transitions: [],
    error: null,
    skipCurrent: false,
  };
}

function sleep(ms) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    scheduler.timers.push({ timer: t, resolve, reject });
  });
}

function cancelAllTimers(reason = 'cancelled') {
  scheduler.timers.forEach(({ timer, reject }) => {
    clearTimeout(timer);
    reject(new Error(reason));
  });
  scheduler.timers = [];
}

function resolveAllTimers() {
  scheduler.timers.forEach(({ timer, resolve }) => {
    clearTimeout(timer);
    resolve();
  });
  scheduler.timers = [];
}

async function emergencyStop(reason) {
  if (scheduler.status === 'error') return;
  cancelAllTimers('cancelled');
  const now = new Date().toISOString();
  scheduler.transitions.push({ event: 'emergency_stop', at: now, reason });
  scheduler.error = { reason, at: now };
  scheduler.status = 'error';
  scheduler.currentPhase = null;
  if (scheduler.currentExperimentIndex >= 0) {
    const exp = scheduler.queue[scheduler.currentExperimentIndex];
    if (exp && exp.status === 'running') exp.status = 'error';
  }
  logToUI('ERROR', `SCHEDULER ERROR: ${reason}. Emergency stop.`);
  try {
    await axios.post(`${SIMULATOR_API_URL}/stop`);
  } catch (e) {
    logToUI('ERROR', `Failed to stop simulator during emergency stop: ${e.message}`);
  }
}

function makeQueueItem(exp) {
  return {
    id: exp.id || `${exp.label}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
    label: exp.label,
    middleware_config: exp.middleware_config || {},
    simulator_config: exp.simulator_config || {},
    warmup_duration_minutes: exp.warmup_duration_minutes ?? 2,
    experiment_duration_minutes: exp.experiment_duration_minutes ?? 5,
    repetitions: exp.repetitions ?? 5,
    cooldown_minutes: exp.cooldown_minutes ?? 2,
    status: 'pending',
    completedRuns: 0,
  };
}

async function runQueue() {
  try {
    for (let i = 0; i < scheduler.queue.length; i++) {
      if (scheduler.status !== 'running') break;

      const exp = scheduler.queue[i];
      if (exp.status !== 'pending') continue;

      scheduler.currentExperimentIndex = i;
      exp.status = 'running';
      scheduler.skipCurrent = false;

      logToUI('INFO', `Starting experiment ${i + 1}/${scheduler.queue.length}: ${exp.label}`);

      // Configure middleware
      try {
        const mwUrl = await getMiddlewareUrl();
        await axios.post(`${mwUrl}/config`, exp.middleware_config);
        logToUI('INFO', `Middleware configured for ${exp.label}: ${JSON.stringify(exp.middleware_config)}`);
      } catch (e) {
        throw new Error(`Failed to configure middleware for ${exp.label}: ${e.message}`);
      }

      // Configure simulator
      try {
        const simStatus = await axios.get(`${SIMULATOR_API_URL}/status`);
        const gatewayUrl = simStatus.data.config.gateway_url;
        await axios.post(`${SIMULATOR_API_URL}/config`, {
          gateway_url: gatewayUrl,
          ...exp.simulator_config,
        });
        logToUI('INFO', `Simulator configured for ${exp.label}`);
      } catch (e) {
        throw new Error(`Failed to configure simulator for ${exp.label}: ${e.message}`);
      }

      // Runs loop
      for (let r = 1; r <= exp.repetitions; r++) {
        if (scheduler.status !== 'running' || scheduler.skipCurrent) break;

        scheduler.currentRunIndex = r;
        logToUI('INFO', `${exp.label} — Run ${r}/${exp.repetitions}`);

        // Set unlabeled for warmup
        try {
          const mwUrl = await getMiddlewareUrl();
          await axios.post(`${mwUrl}/experiment-label`, { label: 'unlabeled' });
        } catch (e) {
          logToUI('WARN', `Failed to set warmup label: ${e.message}`);
        }

        // Start simulator (warmup)
        try {
          await axios.post(`${SIMULATOR_API_URL}/start`);
        } catch (e) {
          throw new Error(`Failed to start simulator for ${exp.label} run ${r} warmup: ${e.message}`);
        }

        scheduler.currentPhase = 'warmup';
        scheduler.phaseStartedAt = new Date().toISOString();
        scheduler.phaseDurationMinutes = exp.warmup_duration_minutes;
        logToUI('INFO', `[Warmup] ${exp.label} run ${r} — ${exp.warmup_duration_minutes} min`);

        await sleep(exp.warmup_duration_minutes * 60 * 1000);
        if (scheduler.status !== 'running' || scheduler.skipCurrent) {
          try { await axios.post(`${SIMULATOR_API_URL}/stop`); } catch {}
          break;
        }

        // Stop between warmup and experiment
        try { await axios.post(`${SIMULATOR_API_URL}/stop`); } catch (e) {
          logToUI('WARN', `Failed to stop simulator after warmup: ${e.message}`);
        }

        // Set experiment label
        const runLabel = `${exp.label}_r${r}`;
        try {
          const mwUrl = await getMiddlewareUrl();
          await axios.post(`${mwUrl}/experiment-label`, { label: runLabel });
        } catch (e) {
          logToUI('WARN', `Failed to set experiment label: ${e.message}`);
        }

        // Start simulator (experiment)
        try {
          await axios.post(`${SIMULATOR_API_URL}/start`);
        } catch (e) {
          throw new Error(`Failed to start simulator for ${exp.label} run ${r}: ${e.message}`);
        }

        scheduler.currentPhase = 'experiment';
        scheduler.phaseStartedAt = new Date().toISOString();
        scheduler.phaseDurationMinutes = exp.experiment_duration_minutes;
        logToUI('SUCCESS', `[Experiment] ${runLabel} — ${exp.experiment_duration_minutes} min`);

        await sleep(exp.experiment_duration_minutes * 60 * 1000);
        if (scheduler.status !== 'running' || scheduler.skipCurrent) {
          try { await axios.post(`${SIMULATOR_API_URL}/stop`); } catch {}
          break;
        }

        // Stop after experiment
        try { await axios.post(`${SIMULATOR_API_URL}/stop`); } catch (e) {
          logToUI('WARN', `Failed to stop simulator after experiment: ${e.message}`);
        }

        // Reset label
        try {
          const mwUrl = await getMiddlewareUrl();
          await axios.post(`${mwUrl}/experiment-label`, { label: 'unlabeled' });
        } catch (e) {
          logToUI('WARN', `Failed to reset label: ${e.message}`);
        }

        exp.completedRuns = r;
        logToUI('INFO', `Completed run ${r}/${exp.repetitions} for ${exp.label}`);

        // Cooldown between runs (not after last)
        if (r < exp.repetitions && exp.cooldown_minutes > 0 && !scheduler.skipCurrent) {
          scheduler.currentPhase = 'cooldown';
          scheduler.phaseStartedAt = new Date().toISOString();
          scheduler.phaseDurationMinutes = exp.cooldown_minutes;
          logToUI('INFO', `[Cooldown] ${exp.cooldown_minutes} min between runs`);
          await sleep(exp.cooldown_minutes * 60 * 1000);
        }
      }

      // Safety stop after all runs
      try { await axios.post(`${SIMULATOR_API_URL}/stop`); } catch {}

      if (scheduler.skipCurrent) {
        logToUI('WARN', `Skipped ${exp.label} (${exp.completedRuns} runs completed)`);
        exp.status = 'skipped';
        scheduler.skipCurrent = false;
      } else if (scheduler.status !== 'running') {
        if (exp.status === 'running') exp.status = 'error';
        break;
      } else {
        exp.status = 'completed';
        logToUI('SUCCESS', `Experiment ${exp.label} completed (${exp.completedRuns}/${exp.repetitions} runs)`);

        // Cooldown between experiments (not after last)
        if (i < scheduler.queue.length - 1 && exp.cooldown_minutes > 0) {
          scheduler.currentPhase = 'cooldown';
          scheduler.phaseStartedAt = new Date().toISOString();
          scheduler.phaseDurationMinutes = exp.cooldown_minutes;
          logToUI('INFO', `[Cooldown] ${exp.cooldown_minutes} min between experiments`);
          await sleep(exp.cooldown_minutes * 60 * 1000);
          if (scheduler.status !== 'running') break;
        }
      }
    }

    if (scheduler.status === 'running') {
      scheduler.status = 'completed';
      scheduler.currentPhase = null;
      scheduler.currentExperimentIndex = -1;
      scheduler.currentRunIndex = -1;
      scheduler.transitions.push({ event: 'all_completed', at: new Date().toISOString() });
      logToUI('SUCCESS', 'All experiments completed!');
    }
  } catch (e) {
    if (e.message !== 'cancelled') {
      await emergencyStop(e.message);
    }
  }
}

// --- PROMETHEUS TEXT PARSER ---
function parsePrometheusText(text) {
  const result = {};
  for (const line of text.split('\n')) {
    if (line.startsWith('#') || !line.trim()) continue;
    // Handle histogram _sum and _count
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*?)(?:\{[^}]*\})?\s+([\d.eE+-]+|NaN|Inf|\+Inf|-Inf)$/);
    if (match) {
      const [, name, value] = match;
      const num = parseFloat(value);
      if (!isNaN(num)) result[name] = num;
    }
  }
  return result;
}

// --- HELPERS ---
let discoveredMiddlewareUrl = null; // Cache for discovered URL

async function getMiddlewareUrl() {
    // 1. Prefer explicitly configured URL, unless it's localhost
    if (MIDDLEWARE_API_URL && !MIDDLEWARE_API_URL.includes('localhost')) {
        return MIDDLEWARE_API_URL;
    }

    // 2. Use cached discovered URL if available
    if (discoveredMiddlewareUrl) {
        return discoveredMiddlewareUrl;
    }

    // 3. Discover and cache the URL from the simulator
    logToUI('WARN', `MIDDLEWARE_API_URL is '${MIDDLEWARE_API_URL || 'not set'}'. Attempting to discover from simulator...`);
    try {
        const simResponse = await axios.get(`${SIMULATOR_API_URL}/status`);
        if (simResponse.data?.config?.gateway_url) {
            const gatewayUrl = new URL(simResponse.data.config.gateway_url);
            const discoveredUrl = `${gatewayUrl.protocol}//${gatewayUrl.host}`;
            logToUI('INFO', `Discovered and cached middleware URL: ${discoveredUrl}`);
            discoveredMiddlewareUrl = discoveredUrl; // Cache it
            return discoveredUrl;
        }
    } catch (simError) {
        logToUI('ERROR', 'Could not discover middleware URL from simulator.', { message: simError.message });
    }
    
    // 4. Fallback to the original (potentially misconfigured) URL
    return MIDDLEWARE_API_URL;
}

// --- MIDDLEWARE API PROXY ---

app.get('/api/middleware/config', async (req, res, next) => {
    const targetMiddlewareUrl = await getMiddlewareUrl();
    if (!targetMiddlewareUrl) return next(new Error('MIDDLEWARE_API_URL is not configured and could not be discovered.'));
    logToUI('INFO', 'Fetching middleware config...');
    try {
        const response = await axios.get(`${targetMiddlewareUrl}/config`);
        logToUI('SUCCESS', 'Successfully fetched middleware config.', response.data);
        res.json(response.data);
    } catch (error) {
        const errorInfo = {
            message: error.message,
            url: error.config?.url,
            method: error.config?.method,
            status: error.response?.status,
            data: error.response?.data
        };
        logToUI('ERROR', 'Failed to fetch middleware config.', errorInfo);
        next(error);
    }
});

app.post('/api/middleware/config', async (req, res, next) => {
    const targetMiddlewareUrl = await getMiddlewareUrl();
    if (!targetMiddlewareUrl) return next(new Error('MIDDLEWARE_API_URL is not configured and could not be discovered.'));
    logToUI('INFO', 'Updating middleware config...', req.body);
    try {
        // Updated validation and payload construction
        const { batching_strategy, batch_size_limit, batch_time_limit } = req.body;
        
        const payload = {};
        if (batching_strategy) {
            payload.batching_strategy = batching_strategy;
        }
        if (typeof batch_size_limit === 'number') {
            payload.batch_size_limit = batch_size_limit;
        }
        if (typeof batch_time_limit === 'number') {
            payload.batch_time_limit = batch_time_limit;
        }

        const response = await axios.post(`${targetMiddlewareUrl}/config`, payload);
        logToUI('SUCCESS', 'Middleware config updated.', response.data);
        res.json(response.data);
    } catch (error) {
        // Improved error logging
        const errorInfo = {
            message: error.message,
            url: error.config?.url,
            method: error.config?.method,
            status: error.response?.status,
            data: error.response?.data
        };
        logToUI('ERROR', 'Failed to update middleware config.', errorInfo);
        next(error); // Pass the original error to the error handler
    }
});

// --- EXPERIMENT LABEL PROXY ---

app.get('/api/middleware/experiment-label', async (req, res, next) => {
    const targetMiddlewareUrl = await getMiddlewareUrl();
    if (!targetMiddlewareUrl) return next(new Error('Middleware URL not available'));
    try {
        const response = await axios.get(`${targetMiddlewareUrl}/experiment-label`);
        res.json(response.data);
    } catch (error) {
        next(error);
    }
});

app.post('/api/middleware/experiment-label', async (req, res, next) => {
    const targetMiddlewareUrl = await getMiddlewareUrl();
    if (!targetMiddlewareUrl) return next(new Error('Middleware URL not available'));
    try {
        const response = await axios.post(`${targetMiddlewareUrl}/experiment-label`, req.body);
        logToUI('SUCCESS', `Experiment label set to: ${req.body.label}`);
        res.json(response.data);
    } catch (error) {
        next(error);
    }
});

// --- EXPERIMENT DATA PROXY ---

app.post('/api/middleware/generate-plots', async (req, res, next) => {
    const targetMiddlewareUrl = await getMiddlewareUrl();
    if (!targetMiddlewareUrl) return next(new Error('Middleware URL not available'));
    try {
        logToUI('INFO', 'Generating boxplot PDFs...');
        const response = await axios.post(`${targetMiddlewareUrl}/generate-plots`, {}, { timeout: 120000 });
        logToUI('SUCCESS', `Plot generation complete: ${response.data.files?.length || 0} files`);
        res.json(response.data);
    } catch (error) {
        logToUI('ERROR', `Plot generation failed: ${error.message}`);
        next(error);
    }
});

app.get('/api/middleware/experiment-data/:filename', async (req, res, next) => {
    const targetMiddlewareUrl = await getMiddlewareUrl();
    if (!targetMiddlewareUrl) return next(new Error('Middleware URL not available'));
    try {
        const response = await axios.get(
            `${targetMiddlewareUrl}/experiment-data/${req.params.filename}`,
            { responseType: 'stream' }
        );
        res.set('Content-Type', response.headers['content-type']);
        res.set('Content-Disposition', response.headers['content-disposition'] || `attachment; filename="${req.params.filename}"`);
        response.data.pipe(res);
    } catch (error) {
        if (error.response?.status === 404) {
            return res.status(404).json({ error: 'File not found' });
        }
        next(error);
    }
});

// --- SIMULATOR API PROXY ---

app.get('/api/simulator/status', async (req, res, next) => {
  logToUI('INFO', 'Fetching simulator status...');
  try {
    const response = await axios.get(`${SIMULATOR_API_URL}/status`);
    const fullStatus = {
        ...response.data,
        config: {
            ...response.data.config,
            rpc_provider_url: RPC_PROVIDER_URL,
            ipfs_gateway_url: IPFS_GATEWAY_URL,
            contract_address: CONTRACT_ADDRESS
        }
    };
    logToUI('SUCCESS', 'Successfully fetched simulator status.', fullStatus);
    res.json(fullStatus);
  } catch (error) {
    const errorInfo = {
        message: error.message,
        url: error.config?.url,
        method: error.config?.method,
        status: error.response?.status,
        data: error.response?.data
    };
    logToUI('ERROR', 'Failed to fetch simulator status.', errorInfo);
    next(error);
  }
});

app.post('/api/simulator/config', async (req, res, next) => {
  logToUI('INFO', 'Updating simulator config...', req.body);
  try {
    const { num_sensors, messages_per_second } = req.body;
    if (typeof num_sensors !== 'number' || typeof messages_per_second !== 'number') {
      logToUI('ERROR', 'Invalid config update payload.');
      return res.status(400).json({ error: 'Invalid input types for sensors or messages_per_second.' });
    }
    
    const statusRes = await axios.get(`${SIMULATOR_API_URL}/status`);
    const currentGatewayUrl = statusRes.data.config.gateway_url;

    const config = {
        gateway_url: currentGatewayUrl,
        num_sensors,
        messages_per_second
    };

    const response = await axios.post(`${SIMULATOR_API_URL}/config`, config);
    logToUI('SUCCESS', 'Simulator config updated.', response.data);
    res.json(response.data);
  } catch (error) {
    const errorInfo = {
        message: error.message,
        url: error.config?.url,
        method: error.config?.method,
        status: error.response?.status,
        data: error.response?.data
    };
    logToUI('ERROR', 'Failed to update simulator config.', errorInfo);
    next(error);
  }
});

app.post('/api/simulator/start', async (req, res, next) => {
  logToUI('INFO', 'Starting simulator...');
  try {
    const response = await axios.post(`${SIMULATOR_API_URL}/start`);
    logToUI('SUCCESS', 'Simulator started successfully.', response.data);
    res.json(response.data);
  } catch (error) {
    const errorInfo = {
        message: error.message,
        url: error.config?.url,
        method: error.config?.method,
        status: error.response?.status,
        data: error.response?.data
    };
    logToUI('ERROR', 'Failed to start simulator.', errorInfo);
    next(error);
  }
});

app.post('/api/simulator/stop', async (req, res, next) => {
  logToUI('INFO', 'Stopping simulator...');
  try {
    const response = await axios.post(`${SIMULATOR_API_URL}/stop`);
    logToUI('SUCCESS', 'Simulator stopped successfully.', response.data);
    res.json(response.data);
  } catch (error) {
    const errorInfo = {
        message: error.message,
        url: error.config?.url,
        method: error.config?.method,
        status: error.response?.status,
        data: error.response?.data
    };
    logToUI('ERROR', 'Failed to stop simulator.', errorInfo);
    next(error);
  }
});

// --- HEALTH ENDPOINT ---

app.get('/api/health', async (req, res) => {
  const results = { simulator: false, middleware: false, blockchain: false };
  try { await axios.get(`${SIMULATOR_API_URL}/status`, { timeout: 3000 }); results.simulator = true; } catch {}
  try {
    const mwUrl = await getMiddlewareUrl();
    if (mwUrl) { await axios.get(`${mwUrl}/config`, { timeout: 3000 }); results.middleware = true; }
  } catch {}
  try {
    const p = new ethers.JsonRpcProvider(RPC_PROVIDER_URL);
    await p.getBlockNumber();
    results.blockchain = true;
  } catch {}
  res.json(results);
});

// --- METRICS ENDPOINTS ---

app.get('/api/metrics/simulator', async (req, res, next) => {
  try {
    const response = await axios.get(`${SIMULATOR_API_URL}/metrics`, { timeout: 3000, responseType: 'text' });
    const parsed = parsePrometheusText(response.data);
    res.json({
      requests_sent_total: parsed['simulator_requests_sent_total'] || 0,
      request_errors_total: parsed['simulator_request_errors_total'] || 0,
    });
  } catch (error) {
    res.json({ requests_sent_total: 0, request_errors_total: 0, error: error.message });
  }
});

app.get('/api/metrics/middleware', async (req, res, next) => {
  try {
    const mwUrl = await getMiddlewareUrl();
    if (!mwUrl) return res.json({ error: 'Middleware URL not available' });
    const response = await axios.get(`${mwUrl}/metrics`, { timeout: 3000, responseType: 'text' });
    const p = parsePrometheusText(response.data);
    res.json({
      requests_total: p['middleware_requests_total'] || 0,
      batches_processed_total: p['middleware_batches_processed_total'] || 0,
      batches_failed_total: p['middleware_batches_failed_total'] || 0,
      buffer_size: p['middleware_buffer_size'] || 0,
      ingest_queue_size: p['middleware_ingest_queue_size'] || 0,
      batch_processing_seconds: { sum: p['middleware_batch_processing_seconds_sum'] || 0, count: p['middleware_batch_processing_seconds_count'] || 0 },
      ipfs_upload_seconds: { sum: p['middleware_ipfs_upload_seconds_sum'] || 0, count: p['middleware_ipfs_upload_seconds_count'] || 0 },
      chain_anchor_seconds: { sum: p['middleware_chain_anchor_seconds_sum'] || 0, count: p['middleware_chain_anchor_seconds_count'] || 0 },
      e2e_finality_seconds: { sum: p['middleware_e2e_finality_seconds_sum'] || 0, count: p['middleware_e2e_finality_seconds_count'] || 0 },
    });
  } catch (error) {
    res.json({ error: error.message });
  }
});

// --- SCHEDULER ENDPOINTS ---

app.get('/api/scheduler/status', (req, res) => {
  const now = Date.now();
  const result = {
    status: scheduler.status,
    queue: scheduler.queue,
    currentExperimentIndex: scheduler.currentExperimentIndex,
    currentRunIndex: scheduler.currentRunIndex,
    currentPhase: scheduler.currentPhase,
    phaseStartedAt: scheduler.phaseStartedAt,
    phaseDurationMinutes: scheduler.phaseDurationMinutes,
    startedAt: scheduler.startedAt,
    transitions: scheduler.transitions,
    error: scheduler.error,
    elapsed_seconds: scheduler.startedAt ? (now - new Date(scheduler.startedAt).getTime()) / 1000 : 0,
  };

  if (scheduler.currentPhase && scheduler.phaseStartedAt && scheduler.phaseDurationMinutes) {
    const elapsed = (now - new Date(scheduler.phaseStartedAt).getTime()) / 1000;
    const total = scheduler.phaseDurationMinutes * 60;
    result.phaseProgress = {
      elapsed_seconds: Math.min(elapsed, total),
      remaining_seconds: Math.max(0, total - elapsed),
      percent: Math.min(100, (elapsed / total) * 100),
    };
  }

  res.json(result);
});

app.post('/api/scheduler/queue', (req, res) => {
  if (scheduler.status === 'running') {
    return res.status(400).json({ error: 'Cannot set queue while scheduler is running.' });
  }
  const { experiments } = req.body;
  if (!Array.isArray(experiments) || experiments.length === 0) {
    return res.status(400).json({ error: 'experiments must be a non-empty array.' });
  }
  scheduler.queue = experiments.map(makeQueueItem);
  res.json({ queue: scheduler.queue });
});

app.post('/api/scheduler/queue/add', (req, res) => {
  if (scheduler.status === 'running') {
    return res.status(400).json({ error: 'Cannot modify queue while scheduler is running.' });
  }
  const { experiments } = req.body;
  if (!Array.isArray(experiments) || experiments.length === 0) {
    return res.status(400).json({ error: 'experiments must be a non-empty array.' });
  }
  scheduler.queue.push(...experiments.map(makeQueueItem));
  res.json({ queue: scheduler.queue });
});

app.delete('/api/scheduler/queue/:id', (req, res) => {
  if (scheduler.status === 'running') {
    return res.status(400).json({ error: 'Cannot modify queue while scheduler is running.' });
  }
  const idx = scheduler.queue.findIndex(e => e.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Experiment not found in queue.' });
  scheduler.queue.splice(idx, 1);
  res.json({ queue: scheduler.queue });
});

app.post('/api/scheduler/start', (req, res) => {
  if (scheduler.status === 'running') {
    return res.status(400).json({ error: 'Scheduler is already running.' });
  }
  const pendingCount = scheduler.queue.filter(e => e.status === 'pending').length;
  if (pendingCount === 0) {
    return res.status(400).json({ error: 'No pending experiments in queue. Add experiments first.' });
  }

  scheduler.status = 'running';
  scheduler.startedAt = new Date().toISOString();
  scheduler.error = null;
  scheduler.transitions = [{ event: 'started', at: scheduler.startedAt }];
  scheduler.currentExperimentIndex = -1;
  scheduler.currentRunIndex = -1;
  scheduler.currentPhase = null;
  scheduler.skipCurrent = false;

  logToUI('SUCCESS', `Scheduler started with ${pendingCount} experiments.`);
  runQueue(); // fire-and-forget, runs server-side
  res.json({ status: 'running', startedAt: scheduler.startedAt, queueLength: pendingCount });
});

app.post('/api/scheduler/stop', async (req, res) => {
  if (scheduler.status !== 'running') {
    return res.json({ status: scheduler.status, message: 'Scheduler is not running.' });
  }

  scheduler.status = 'idle';
  cancelAllTimers('cancelled');

  const now = new Date().toISOString();
  scheduler.transitions.push({ event: 'cancelled', at: now });
  logToUI('WARN', 'Scheduler cancelled by user.');

  // Mark current experiment as errored, remaining as skipped
  if (scheduler.currentExperimentIndex >= 0) {
    const cur = scheduler.queue[scheduler.currentExperimentIndex];
    if (cur && cur.status === 'running') cur.status = 'error';
    for (let j = scheduler.currentExperimentIndex + 1; j < scheduler.queue.length; j++) {
      if (scheduler.queue[j].status === 'pending') scheduler.queue[j].status = 'skipped';
    }
  }

  scheduler.currentPhase = null;
  scheduler.currentExperimentIndex = -1;
  scheduler.currentRunIndex = -1;

  try {
    await axios.post(`${SIMULATOR_API_URL}/stop`);
    logToUI('INFO', 'Simulator stopped.');
  } catch (e) {
    logToUI('ERROR', `Failed to stop simulator: ${e.message}`);
  }

  try {
    const mwUrl = await getMiddlewareUrl();
    await axios.post(`${mwUrl}/experiment-label`, { label: 'unlabeled' });
  } catch (e) {
    logToUI('WARN', `Failed to reset experiment label: ${e.message}`);
  }

  res.json({ status: 'idle', message: 'Scheduler stopped.' });
});

app.post('/api/scheduler/reset', (req, res) => {
  if (scheduler.status === 'running') {
    return res.status(400).json({ error: 'Cannot reset while running. Stop first.' });
  }
  resetScheduler();
  res.json({ status: 'idle' });
});

app.post('/api/scheduler/skip-current', (req, res) => {
  if (scheduler.status !== 'running') {
    return res.status(400).json({ error: 'Scheduler is not running.' });
  }
  logToUI('WARN', 'Skip requested for current experiment/run.');
  scheduler.skipCurrent = true;
  resolveAllTimers(); // wake up any sleeping phase immediately
  res.json({ status: 'ok', message: 'Skip requested.' });
});

// --- BLOCKCHAIN & IPFS API ---

const MAX_RPC_RANGE = 4999; // Max range for a single eth_getLogs query

app.get('/api/records', async (req, res, next) => {
    if (!CONTRACT_ADDRESS) {
        logToUI('ERROR', 'CONTRACT_ADDRESS environment variable is not set.');
        return res.status(500).json({ error: "CONTRACT_ADDRESS environment variable is not set." });
    }

    logToUI('INFO', 'New request for blockchain records.');

    try {
        const provider = new ethers.JsonRpcProvider(RPC_PROVIDER_URL);
        logToUI('INFO', `Connecting to RPC provider at ${RPC_PROVIDER_URL}...`);
        
        await provider.getNetwork();
        logToUI('SUCCESS', 'RPC provider connection successful.');

        const contract = new ethers.Contract(CONTRACT_ADDRESS, abi, provider);
        const latestBlock = await provider.getBlockNumber();
        logToUI('INFO', `Current latest block: ${latestBlock}`);

        let allEvents = [];
        const totalBlocksToScan = 20000; // Scan the last 20,000 blocks as requested
        let fromBlock = Math.max(0, latestBlock - totalBlocksToScan + 1);
        let toBlock = latestBlock;

        logToUI('INFO', `Scanning for events in ${totalBlocksToScan} blocks (from ${fromBlock} to ${toBlock})...`);

        // Paginate through the block range to avoid RPC limits
        for (let currentBlock = fromBlock; currentBlock <= toBlock; currentBlock += MAX_RPC_RANGE) {
            const endBlock = Math.min(currentBlock + MAX_RPC_RANGE - 1, toBlock);
            logToUI('INFO', `Querying block range: ${currentBlock} - ${endBlock}`);
            try {
                const events = await contract.queryFilter('DataAnchored', currentBlock, endBlock);
                allEvents = allEvents.concat(events);
                logToUI('INFO', `Found ${events.length} events in this range.`);
            } catch (e) {
                 logToUI('ERROR', `Error querying range ${currentBlock}-${endBlock}. The range might be too large or the node is overloaded. Skipping.`, { message: e.message });
                 // Continue to the next chunk
            }
        }
        
        logToUI('SUCCESS', `Total events found: ${allEvents.length}`);

        if (allEvents.length === 0) {
            logToUI('INFO', 'No new events found in the scanned range.');
            return res.json([]);
        }

        const records = allEvents.map(event => {
            // The contract event returns a bytes32 digest. We need to reconstruct
            // the full IPFS CID to make it fetchable by the client.
            // The digest from the event is a hex string like '0x...'.
            const digestBytes = ethers.getBytes(event.args.ipfsDigest);

            // Create a multihash digest object.
            const multihash = digest.create(sha256.code, digestBytes);

            // Create a CIDv0 with the 'dag-pb' codec (0x70).
            const cid = CID.create(0, 0x70, multihash);

            return {
                docId: event.args.docId,
                ipfsDigest: cid.toString(), // e.g., "Qm..."
                timestamp: Number(event.args.timestamp),
                blockNumber: event.blockNumber,
            };
        });

        const sortedRecords = records.sort((a, b) => b.blockNumber - a.blockNumber);
        logToUI('SUCCESS', 'Returning sorted records to client.');
        res.json(sortedRecords);

    } catch (error) {
        logToUI('ERROR', 'A critical error occurred while fetching blockchain records.', { message: error.message, stack: error.stack });
        next(error);
    }
});


app.get('/api/ipfs/:hash', async (req, res, next) => {
    const { hash } = req.params;
    logToUI('INFO', `Fetching content from IPFS for hash: ${hash}`);
    try {
        const response = await axios.get(`${IPFS_GATEWAY_URL}${hash}`, { responseType: 'text' });
        logToUI('SUCCESS', `Successfully fetched IPFS content for hash: ${hash}`);
        res.send(response.data);
    } catch (error) {
        if (error.response && error.response.status === 404) {
            logToUI('WARN', `IPFS content not found for hash: ${hash}`);
            return res.status(404).send('Content not found on IPFS gateway.');
        }
        const errorInfo = {
            message: error.message,
            url: error.config?.url,
            method: error.config?.method,
            status: error.response?.status,
            data: error.response?.data
        };
        logToUI('ERROR', `Failed to fetch IPFS content for hash: ${hash}`, errorInfo);
        next(error);
    }
});

// --- FALLBACK FOR SPA ---
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// --- ERROR HANDLING ---
app.use((err, req, res, next) => {
  const status = err.response?.status || 500;
  const message = err.response?.data?.detail || err.message || 'Something went wrong!';
  // Don't log the full error to the UI, just the console.
  console.error(err.stack);
  res.status(status).json({ error: message });
});


// --- SERVER START ---
server.listen(PORT, () => {
  console.log(`UI Backend and WebSocket server running on http://localhost:${PORT}`);
  logToUI('INFO', 'Backend server started.');
  if (!CONTRACT_ADDRESS) {
    const warningMsg = 'WARNING: CONTRACT_ADDRESS environment variable is not set. The /api/records endpoint will not work.';
    console.warn(warningMsg);
    logToUI('WARN', warningMsg);
  }
  if (!MIDDLEWARE_API_URL) {
    const warningMsg = 'WARNING: MIDDLEWARE_API_URL environment variable is not set. The middleware config endpoints will not work.';
    console.warn(warningMsg);
    logToUI('WARN', warningMsg);
  }
});
