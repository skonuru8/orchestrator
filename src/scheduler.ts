/**
 * scheduler.ts — cron schedule definitions for the job-hunter pipeline.
 *
 * Schedule (all times local to the machine running the orchestrator):
 *
 *   Dice daily (Mon–Sat):   0 9,13,17,21 * * 1-6   POSTED_WITHIN=ONE   MAX=50
 *   Dice backfill (Sun):    0 9 * * 0               POSTED_WITHIN=SEVEN MAX=100  TTL=6h
 *   Dice Sun afternoons:    0 13,17,21 * * 0        POSTED_WITHIN=ONE   MAX=50
 *   Jobright API (Mon–Sat): 0 9,13,17,21 * * 1-6    MAX=40
 *   Jobright API (Sun):     0 13,17,21 * * 0        MAX=40
 *   LinkedIn (daily):       0 14 * * *              (no POSTED_WITHIN)  MAX=30
 *   Ghost reaper:           *\/10 * * * *           sweeps stale runs (every 10 minutes)
 *
 * Jobright and LinkedIn are offset from Dice by 1h to avoid hitting
 * OpenRouter simultaneously from three sources.
 *
 * Sunday 9am uses the backfill config (POSTED_WITHIN=SEVEN) rather than the
 * normal daily config. Sunday 13/17/21 use normal daily config so Sunday
 * afternoons aren't dark.
 *
 * The ghost reaper runs every 10 minutes. It finds runs where finished_at IS
 * NULL and last_heartbeat has gone stale (> 5 minutes ago), marks them as
 * exit_code=-1, sets finished_at=NOW(), and releases their Redis lock.
 */

import cron from "node-cron";
import pg from "pg";
import { randomUUID } from "crypto";

import { spawnRun } from "./runner.js";
import { releaseLock } from "./lock.js";
import { appendReaperLog, appendOrchestratorLog } from "./monitor.js";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL ?? "postgresql://postgres:postgres@localhost:5432/jobhunter";

// Lazy pool for ghost reaper DB queries
let _reaperPool: InstanceType<typeof Pool> | null = null;

function getRealPool(): InstanceType<typeof Pool> {
  if (!_reaperPool) {
    _reaperPool = new Pool({ connectionString: DATABASE_URL, max: 2 });
    _reaperPool.on("error", () => { /* silent */ });
  }
  return _reaperPool;
}

// ---------------------------------------------------------------------------
// ID generation — uses crypto.randomUUID (built-in Node 16+)
// ---------------------------------------------------------------------------

function newRunId(): string {
  return randomUUID();
}

// ---------------------------------------------------------------------------
// Ghost reaper
// ---------------------------------------------------------------------------

/**
 * runReaper — finds stale runs (heartbeat > 5min ago, finished_at IS NULL)
 * and marks them as ghost exits. Releases the Redis lock for each.
 */
async function runReaper(): Promise<void> {
  const STALE_MINUTES = 5;

  try {
    const result = await getRealPool().query<{ run_id: string; source: string }>(
      `SELECT run_id, source
         FROM runs
        WHERE finished_at    IS NULL
          AND last_heartbeat IS NOT NULL
          AND last_heartbeat < NOW() - ($1 || ' minutes')::INTERVAL`,
      [STALE_MINUTES],
    );

    if (result.rows.length === 0) return;

    for (const ghost of result.rows) {
      appendReaperLog(
        `[reaper] ghost run detected — run_id=${ghost.run_id} source=${ghost.source} — marking exit_code=-1`,
      );

      // Mark as ghost exit
      await getRealPool().query(
        `UPDATE runs
            SET exit_code   = -1,
                finished_at = NOW()
          WHERE run_id = $1`,
        [ghost.run_id],
      );

      // Release the Redis lock unconditionally (DEL is idempotent)
      await releaseLock(ghost.source);

      appendReaperLog(
        `[reaper] ghost run ${ghost.run_id} cleaned up — lock released for source=${ghost.source}`,
      );
    }
  } catch (e) {
    appendReaperLog(`[reaper] error during sweep: ${(e as Error).message}`);
  }
}

// ---------------------------------------------------------------------------
// Schedule registration
// ---------------------------------------------------------------------------

export interface ScheduledTask {
  stop: () => void;
}

export function registerSchedules(): ScheduledTask[] {
  const tasks: cron.ScheduledTask[] = [];

  // Helper that guards against concurrent ticks for the same expression.
  // node-cron won't overlap by default if the callback is synchronous, but
  // since spawnRun is async we add a per-schedule "running" flag.
  function schedule(
    expression: string,
    label: string,
    fn: () => Promise<void>,
  ): cron.ScheduledTask {
    let running = false;

    const task = cron.schedule(expression, async () => {
      if (running) {
        appendOrchestratorLog(
          `[scheduler] ${label} tick skipped — previous tick still running`,
        );
        return;
      }
      running = true;
      try {
        await fn();
      } catch (e) {
        appendOrchestratorLog(
          `[scheduler] ${label} tick error: ${(e as Error).message}`,
        );
      } finally {
        running = false;
      }
    });

    tasks.push(task);
    return task;
  }

  // ── Dice daily (Mon–Sat) ─────────────────────────────────────────────────
  schedule("0 9,13,17,21 * * 1-6", "dice-daily", async () => {
    await spawnRun({
      source:       "dice",
      postedWithin: "ONE",
      max:          50,
      runId:        newRunId(),
      lockTtlSecs:  14_400,   // 4h
    });
  });

  // ── Dice backfill (Sunday 9am only) ─────────────────────────────────────
  schedule("0 9 * * 0", "dice-backfill", async () => {
    await spawnRun({
      source:       "dice",
      postedWithin: "SEVEN",
      max:          100,
      runId:        newRunId(),
      lockTtlSecs:  21_600,   // 6h — backfill can take longer
    });
  });

  // ── Dice Sunday afternoons (not dark after backfill) ─────────────────────
  schedule("0 13,17,21 * * 0", "dice-sunday", async () => {
    await spawnRun({
      source:       "dice",
      postedWithin: "ONE",
      max:          50,
      runId:        newRunId(),
      lockTtlSecs:  14_400,
    });
  });

  // ── Jobright API (Mon–Sat, 4×/day matching Dice cadence) ─────────────────
  // Replaces the Playwright HTML scraper. The API path:
  // - Fetches structured JSON instead of scraping JS-rendered SPA
  // - Synthesizes description_raw from API fields (no separate fetch needed)
  // - Eliminates ATS 403/empty-body failure class for Jobright source
  // - Capped at MAX=40 to stay well under Jobright's ~30-40 rate limit
  schedule("0 9,13,17,21 * * 1-6", "jobright-api-daily", async () => {
    await spawnRun({
      source:       "jobright_api",
      postedWithin: "",      // Jobright API doesn't take posted_within
      max:          40,
      runId:        newRunId(),
      lockTtlSecs:  14_400,
    });
  });

  // ── Jobright API Sunday afternoons ───────────────────────────────────────
  schedule("0 13,17,21 * * 0", "jobright-api-sunday", async () => {
    await spawnRun({
      source:       "jobright_api",
      postedWithin: "",
      max:          40,
      runId:        newRunId(),
      lockTtlSecs:  14_400,
    });
  });

  // ── Jobright HTML scraper (DEPRECATED — kept for fallback) ───────────────
  // The HTML scraper is preserved as a fallback in case the API session
  // expires or rate-limits hard. To re-enable: comment the API schedules
  // above and uncomment this block.
  //
  // schedule("0 10 * * *", "jobright-html-daily", async () => {
  //   await spawnRun({
  //     source:       "jobright",
  //     postedWithin: "ONE",
  //     max:          50,
  //     runId:        newRunId(),
  //     lockTtlSecs:  14_400,
  //   });
  // });

  // ── LinkedIn (daily, offset by 1h from Jobright) ─────────────────────────
  // LinkedIn does not support POSTED_WITHIN — JobSpy doesn't expose it
  schedule("0 14 * * *", "linkedin-daily", async () => {
    await spawnRun({
      source:       "linkedin",
      postedWithin: "",        // no recency filter for LinkedIn
      max:          30,
      runId:        newRunId(),
      lockTtlSecs:  14_400,
    });
  });

  // ── Ghost reaper (every 10 minutes) ─────────────────────────────────────
  schedule("*/10 * * * *", "reaper", async () => {
    await runReaper();
  });

  appendOrchestratorLog(
    `[scheduler] registered ${tasks.length} schedules — orchestrator running`,
  );

  return tasks.map(t => ({ stop: () => t.stop() }));
}

export async function closeSchedulerPool(): Promise<void> {
  if (_reaperPool) {
    await _reaperPool.end();
    _reaperPool = null;
  }
}