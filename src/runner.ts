/**
 * runner.ts — spawns run-pipeline.ts as a child process and supervises it.
 *
 * Responsibilities:
 *   1. Acquire Redis lock for the source before spawning
 *   2. Spawn `npx tsx job-filter/scripts/run-pipeline.ts` with correct env
 *   3. Pipe child stdout/stderr to both the terminal and a per-run log file
 *   4. Send heartbeat UPDATE to Postgres every 60s while child is alive
 *   5. On child exit: clear heartbeat, write exit_code, call monitor, release lock
 *   6. On SIGTERM/SIGINT: forward to child, wait 30s for clean exit, then SIGKILL
 *
 * What it does NOT do:
 *   - Touch run-pipeline.ts internals
 *   - Manage BullMQ queues or per-job state
 *   - Handle retries (the pipeline retries internally)
 */

import { spawn, ChildProcess } from "child_process";
import fs from "fs";
import path from "path";
import pg from "pg";

import { acquireLock, releaseLock } from "./lock.js";
import { checkRun, appendOrchestratorLog, runLogPath, ensureLogDirs } from "./monitor.js";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL ?? "postgresql://postgres:postgres@localhost:5432/jobhunter";
const PROJECT_ROOT  = path.resolve(process.env.PROJECT_ROOT ?? path.join(import.meta.dirname, "..", ".."));

// Lazy pool — only for heartbeat and exit_code writes
let _pool: InstanceType<typeof Pool> | null = null;

function getPool(): InstanceType<typeof Pool> {
  if (!_pool) {
    _pool = new Pool({ connectionString: DATABASE_URL, max: 2 });
    _pool.on("error", () => { /* silent */ });
  }
  return _pool;
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

async function updateHeartbeat(runId: string): Promise<void> {
  try {
    await getPool().query(
      `UPDATE runs SET last_heartbeat = NOW() WHERE run_id = $1`,
      [runId],
    );
  } catch { /* non-throwing — missed heartbeat is acceptable */ }
}

async function markExitCode(runId: string, code: number): Promise<void> {
  try {
    await getPool().query(
      `UPDATE runs SET exit_code = $2 WHERE run_id = $1`,
      [runId, code],
    );
  } catch (e) {
    console.error("[runner] markExitCode failed:", (e as Error).message);
  }
}

// ---------------------------------------------------------------------------
// Run config
// ---------------------------------------------------------------------------

export interface RunConfig {
  source:       string;
  postedWithin: string;    // ONE | THREE | SEVEN | "" (LinkedIn doesn't use it)
  max:          number;
  runId:        string;
  lockTtlSecs:  number;    // 14400 (4h) daily, 21600 (6h) backfill
}

// ---------------------------------------------------------------------------
// Main spawn function
// ---------------------------------------------------------------------------

/**
 * spawnRun — acquires lock, spawns run-pipeline.ts, supervises it.
 *
 * Returns the child's exit code, or -1 if the lock was not acquired.
 * Never throws — all errors are caught and logged.
 */
export async function spawnRun(config: RunConfig): Promise<number> {
  const { source, postedWithin, max, runId, lockTtlSecs } = config;

  // Ensure log directories exist
  ensureLogDirs();

  // Acquire lock — skip this tick if already held or Redis is down
  const acquired = await acquireLock(source, runId, lockTtlSecs);
  if (!acquired) return -1;

  appendOrchestratorLog(
    `[${source}] run ${runId} starting — max=${max} postedWithin=${postedWithin || "n/a"}`,
  );

  // Build env for child process
  const childEnv: Record<string, string> = {
    ...process.env as Record<string, string>,
    SOURCE:       source,
    MAX:          String(max),
    EXTRACT:      "1",
    SCORE:        "1",
    JUDGE:        "1",
    COVER:        "1",
    // RUN_ID is set so the pipeline uses the same ID the orchestrator registered
    // run-pipeline.ts must respect this env var if set, rather than generating its own
    RUN_ID:       runId,
  };

  if (postedWithin) {
    childEnv.POSTED_WITHIN = postedWithin;
  }

  // Open per-run log file
  const logPath = runLogPath(runId);
  const logStream = fs.createWriteStream(logPath, { flags: "a" });

  // Spawn child
  const child: ChildProcess = spawn(
    "npx",
    ["tsx", "job-filter/scripts/run-pipeline.ts"],
    {
      cwd:   PROJECT_ROOT,
      env:   childEnv,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  // Pipe stdout + stderr to log file AND process stdout/stderr
  child.stdout?.on("data", (chunk: Buffer) => {
    process.stdout.write(chunk);
    logStream.write(chunk);
  });

  child.stderr?.on("data", (chunk: Buffer) => {
    process.stderr.write(chunk);
    logStream.write(chunk);
  });

  // Heartbeat — every 60s while child is running
  const heartbeatInterval = setInterval(() => {
    updateHeartbeat(runId);
  }, 60_000);

  // SIGTERM/SIGINT forwarding — registered per-run, cleaned up on exit
  let exited = false;

  const forwardSignal = (signal: NodeJS.Signals) => {
    if (exited) return;
    appendOrchestratorLog(
      `[${source}] run ${runId} received ${signal} — forwarding to child`,
    );
    child.kill(signal);

    // Give child up to 30s to finish in-flight jobs cleanly
    const forceKill = setTimeout(() => {
      if (!exited) {
        appendOrchestratorLog(
          `[${source}] run ${runId} did not exit within 30s — SIGKILL`,
        );
        child.kill("SIGKILL");
      }
    }, 30_000);

    // Don't hold the event loop open waiting for force-kill
    forceKill.unref();
  };

  process.once("SIGTERM", () => forwardSignal("SIGTERM"));
  process.once("SIGINT",  () => forwardSignal("SIGINT"));

  // Wait for child to exit
  const exitCode = await new Promise<number>((resolve) => {
    child.on("close", (code) => {
      exited = true;
      resolve(code ?? 1);
    });

    child.on("error", (err) => {
      appendOrchestratorLog(
        `[${source}] run ${runId} child spawn error: ${err.message}`,
      );
      exited = true;
      resolve(1);
    });
  });

  // Cleanup
  clearInterval(heartbeatInterval);
  logStream.end();

  // Write exit code to DB
  await markExitCode(runId, exitCode);

  // Post-run monitor check
  await checkRun(source, runId, exitCode);

  // Release lock — always, regardless of exit code
  await releaseLock(source);

  appendOrchestratorLog(
    `[${source}] run ${runId} finished — exit=${exitCode} log=${logPath}`,
  );

  return exitCode;
}

export async function closeRunnerPool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}