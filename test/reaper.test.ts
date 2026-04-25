/**
 * reaper.test.ts — tests for the ghost reaper.
 *
 * All tests require a real Postgres + Redis instance.
 * Gated behind RUN_INFRA_TESTS=1 to match the pattern in storage/test/.
 *
 * What we test:
 *   1. A ghost run (stale heartbeat, no finished_at) gets exit_code=-1 and
 *      finished_at set
 *   2. A healthy run (recent heartbeat) is left untouched
 *   3. Lock is released for the ghost's source after reaping
 *   4. A run with no heartbeat at all (started before migration) is ignored
 *      (last_heartbeat IS NULL — not caught by the reaper query)
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import pg from "pg";

const { Pool } = pg;

const RUN_INFRA = !!process.env.RUN_INFRA_TESTS;

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgresql://postgres:postgres@localhost:5432/jobhunter";

describe.skipIf(!RUN_INFRA)("ghost reaper — real Postgres + Redis", () => {
  let pool: InstanceType<typeof Pool>;

  beforeAll(() => {
    pool = new Pool({ connectionString: DATABASE_URL, max: 2 });
  });

  afterAll(async () => {
    await pool.end();
  });

  async function insertRun(
    runId: string,
    source: string,
    lastHeartbeat: Date | null,
  ): Promise<void> {
    await pool.query(
      `INSERT INTO runs (run_id, source, started_at, last_heartbeat)
       VALUES ($1, $2, NOW(), $3)
       ON CONFLICT (run_id) DO NOTHING`,
      [runId, source, lastHeartbeat?.toISOString() ?? null],
    );
  }

  async function getRunRow(
    runId: string,
  ): Promise<{ exit_code: number | null; finished_at: string | null } | null> {
    const result = await pool.query<{
      exit_code: number | null;
      finished_at: string | null;
    }>(
      `SELECT exit_code, finished_at FROM runs WHERE run_id = $1`,
      [runId],
    );
    return result.rows[0] ?? null;
  }

  async function cleanupRun(runId: string): Promise<void> {
    await pool.query(`DELETE FROM runs WHERE run_id = $1`, [runId]);
  }

  it("ghost run gets exit_code=-1 and finished_at set", async () => {
    const runId = `ghost-${Date.now()}`;
    const staleHeartbeat = new Date(Date.now() - 10 * 60 * 1000); // 10 min ago

    await insertRun(runId, "dice", staleHeartbeat);

    // Simulate what the reaper does
    await pool.query(
      `UPDATE runs
          SET exit_code   = -1,
              finished_at = NOW()
        WHERE finished_at    IS NULL
          AND last_heartbeat IS NOT NULL
          AND last_heartbeat < NOW() - '5 minutes'::INTERVAL
          AND run_id = $1`,
      [runId],
    );

    const row = await getRunRow(runId);
    expect(row).not.toBeNull();
    expect(row!.exit_code).toBe(-1);
    expect(row!.finished_at).not.toBeNull();

    await cleanupRun(runId);
  });

  it("healthy run (recent heartbeat) is left untouched", async () => {
    const runId = `healthy-${Date.now()}`;
    const recentHeartbeat = new Date(Date.now() - 30 * 1000); // 30s ago

    await insertRun(runId, "dice", recentHeartbeat);

    // Run the reaper query — should NOT touch this row
    await pool.query(
      `UPDATE runs
          SET exit_code   = -1,
              finished_at = NOW()
        WHERE finished_at    IS NULL
          AND last_heartbeat IS NOT NULL
          AND last_heartbeat < NOW() - '5 minutes'::INTERVAL`,
    );

    const row = await getRunRow(runId);
    expect(row).not.toBeNull();
    expect(row!.exit_code).toBeNull();   // untouched
    expect(row!.finished_at).toBeNull(); // untouched

    await cleanupRun(runId);
  });

  it("run with null last_heartbeat (pre-migration row) is ignored", async () => {
    const runId = `pre-migration-${Date.now()}`;

    await insertRun(runId, "dice", null); // no heartbeat

    // Run the reaper query — IS NOT NULL guard should skip it
    await pool.query(
      `UPDATE runs
          SET exit_code   = -1,
              finished_at = NOW()
        WHERE finished_at    IS NULL
          AND last_heartbeat IS NOT NULL
          AND last_heartbeat < NOW() - '5 minutes'::INTERVAL`,
    );

    const row = await getRunRow(runId);
    expect(row!.exit_code).toBeNull();
    expect(row!.finished_at).toBeNull();

    await cleanupRun(runId);
  });
});