/**
 * index.ts — orchestrator entry point.
 *
 * Boots the cron scheduler and runs indefinitely. Handles SIGTERM/SIGINT
 * gracefully: stops new ticks from firing, allows in-flight runs to finish
 * (runner.ts handles SIGTERM forwarding to child processes), then exits.
 *
 * Usage:
 *   cd orchestrator && npm start
 *
 * Environment variables (all have defaults for local dev):
 *   DATABASE_URL     postgresql://postgres:postgres@localhost:5432/jobhunter
 *   REDIS_URL        redis://localhost:6379
 *   OUTPUT_DIR       output   (relative to project root — logs go here)
 *   PROJECT_ROOT     ..       (path to the job-hunter repo root)
 */

import { registerSchedules, closeSchedulerPool } from "./scheduler.js";
import { closeRedis } from "./lock.js";
import { closeMonitorPool, appendOrchestratorLog, ensureLogDirs } from "./monitor.js";
import { closeRunnerPool } from "./runner.js";

async function shutdown(reason: string): Promise<void> {
  appendOrchestratorLog(`[orchestrator] shutting down — reason: ${reason}`);

  // Stop all scheduled ticks — in-flight runs are NOT killed here;
  // runner.ts handles SIGTERM forwarding to child processes independently.
  tasks.forEach(t => t.stop());

  // Close DB connections and Redis
  await Promise.allSettled([
    closeSchedulerPool(),
    closeMonitorPool(),
    closeRunnerPool(),
    closeRedis(),
  ]);

  appendOrchestratorLog("[orchestrator] shutdown complete");
  process.exit(0);
}

// Boot
ensureLogDirs();
appendOrchestratorLog("[orchestrator] starting up");

const tasks = registerSchedules();

// Graceful shutdown on SIGTERM (systemd / docker stop) and SIGINT (Ctrl+C)
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));

// Unhandled rejection safety net — log and keep running (don't crash the
// scheduler because one cron tick threw)
process.on("unhandledRejection", (reason) => {
  appendOrchestratorLog(
    `[orchestrator] unhandledRejection: ${reason instanceof Error ? reason.message : String(reason)}`,
  );
});

appendOrchestratorLog("[orchestrator] all schedules active — waiting for next tick");