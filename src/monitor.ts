/**
 * monitor.ts — post-run health check for the orchestrator.
 *
 * Called by runner.ts after a child process exits cleanly (exit code 0).
 * Fetches RunStats from Postgres and checks three warning conditions that
 * represent degraded-but-not-crashed pipeline states.
 *
 * Warning conditions:
 *   1. jobs_total === 0        — scraper produced nothing (blocked IP, broken
 *                                selectors, expired cookie)
 *   2. extractRate < 0.5       — extraction succeeding on fewer than half of
 *      && attempted > 5          attempted jobs (OpenRouter credits, rate limit)
 *   3. jobs_passed > 10        — jobs made it through the filter but nothing
 *      && jobs_covered === 0     came out the end (scoring/judge/cover degraded)
 *
 * All warnings are written to output/logs/orchestrator.log (rolling).
 * A success line is also written on clean runs so you can see the pipeline
 * ran without needing to check psql.
 *
 * The `> 5` guard on condition 2 handles EXTRACT=0 runs where every job has
 * extract_status="skipped" → attempted=0 → no false warning.
 */

import fs from "fs";
import path from "path";
import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL ?? "postgresql://postgres:postgres@localhost:5432/jobhunter";

// Lazy pool — only used for the stats query, not for pipeline persistence
let _pool: InstanceType<typeof Pool> | null = null;

function getPool(): InstanceType<typeof Pool> {
  if (!_pool) {
    _pool = new Pool({ connectionString: DATABASE_URL, max: 2 });
    _pool.on("error", () => { /* silent — monitor is best-effort */ });
  }
  return _pool;
}

// ---------------------------------------------------------------------------
// Log helpers
// ---------------------------------------------------------------------------

const OUTPUT_LOGS_DIR = path.resolve(process.env.OUTPUT_DIR ?? "output", "logs");

export function ensureLogDirs(): void {
  fs.mkdirSync(path.join(OUTPUT_LOGS_DIR, "runs"), { recursive: true });
}

function orchestratorLogPath(): string {
  return path.join(OUTPUT_LOGS_DIR, "orchestrator.log");
}

export function appendOrchestratorLog(line: string): void {
  const ts = new Date().toISOString();
  const entry = `[${ts}] ${line}\n`;
  try {
    fs.appendFileSync(orchestratorLogPath(), entry, "utf-8");
  } catch {
    // If we can't write the log, print to stderr at minimum
    process.stderr.write(entry);
  }
  // Also print to stdout so the terminal shows it when running interactively
  process.stdout.write(entry);
}

export function reaperLogPath(): string {
  return path.join(OUTPUT_LOGS_DIR, "reaper.log");
}

export function appendReaperLog(line: string): void {
  const ts = new Date().toISOString();
  const entry = `[${ts}] ${line}\n`;
  try {
    fs.appendFileSync(reaperLogPath(), entry, "utf-8");
  } catch {
    process.stderr.write(entry);
  }
  process.stdout.write(entry);
}

export function runLogPath(runId: string): string {
  return path.join(OUTPUT_LOGS_DIR, "runs", `${runId}.log`);
}

// ---------------------------------------------------------------------------
// Stats query
// ---------------------------------------------------------------------------

interface RunStats {
  jobs_total:            number;
  jobs_passed:           number;
  jobs_gated:            number;
  jobs_covered:          number;
  extractions_attempted: number;
  extractions_succeeded: number;
}

async function fetchRunStats(runId: string): Promise<RunStats | null> {
  try {
    const result = await getPool().query<RunStats>(
      `SELECT jobs_total, jobs_passed, jobs_gated, jobs_covered,
              extractions_attempted, extractions_succeeded
         FROM runs
        WHERE run_id = $1`,
      [runId],
    );
    return result.rows[0] ?? null;
  } catch (e) {
    console.error("[monitor] fetchRunStats failed:", (e as Error).message);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Main check
// ---------------------------------------------------------------------------

/**
 * checkRun — fetches stats for a completed run and emits warnings to the
 * orchestrator log for any degraded conditions. Always emits a summary line.
 *
 * @param source  — pipeline source name (for log context)
 * @param runId   — run_id of the just-completed run
 * @param exitCode — child process exit code (0 = clean, non-zero = error)
 */
export async function checkRun(
  source: string,
  runId: string,
  exitCode: number,
): Promise<void> {
  // Non-zero exit gets a simple failure line — no stats to check
  if (exitCode !== 0) {
    appendOrchestratorLog(
      `[${source}] run ${runId} FAILED exit=${exitCode}`,
    );
    return;
  }

  const stats = await fetchRunStats(runId);

  if (!stats) {
    // Can't fetch stats — still log that the run finished
    appendOrchestratorLog(
      `[${source}] run ${runId} exit=0 (stats unavailable)`,
    );
    return;
  }

  const warnings: string[] = [];

  // Condition 1 — scraper produced nothing
  if (stats.jobs_total === 0) {
    warnings.push("⚠ 0 jobs scraped — selectors broken or auth expired");
  }

  // Condition 2 — extraction degraded
  const extractRate =
    stats.extractions_succeeded / Math.max(stats.extractions_attempted, 1);
  const pct = (r: number): string => `${(r * 100).toFixed(0)}%`;

  if (extractRate < 0.5 && stats.extractions_attempted > 5) {
    warnings.push(
      `⚠ extract success rate ${pct(extractRate)} (${stats.extractions_succeeded}/${stats.extractions_attempted}) — credits? rate limit?`,
    );
  }

  // Condition 3 — pipeline passed jobs but produced nothing at the end
  if (stats.jobs_passed > 10 && stats.jobs_covered === 0) {
    warnings.push(
      `⚠ 0 cover letters from ${stats.jobs_passed} passed — scoring/judge/cover degraded`,
    );
  }

  // Summary line (always written)
  const extractSummary = stats.extractions_attempted > 0
    ? ` extract=${pct(extractRate)}(${stats.extractions_succeeded}/${stats.extractions_attempted})`
    : "";

  const status = warnings.length > 0 ? "OK with warnings" : "OK";
  appendOrchestratorLog(
    `[${source}] run ${runId} ${status} — ` +
    `total=${stats.jobs_total} passed=${stats.jobs_passed} ` +
    `gated=${stats.jobs_gated} covered=${stats.jobs_covered}${extractSummary}`,
  );

  for (const w of warnings) {
    appendOrchestratorLog(`[${source}] run ${runId} ${w}`);
  }
}

export async function closeMonitorPool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}