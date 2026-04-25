/**
 * lock.ts — Redis-based run lock for the orchestrator.
 *
 * Prevents two pipeline runs for the same source from overlapping.
 * Each source gets its own lock key. The lock is acquired before spawning
 * run-pipeline.ts and released when the child exits.
 *
 * Key shape:   orchestrator:lock:{source}
 * Value:       run_id (string) — stored for audit only, not used for logic
 * TTL:         passed by caller — 4h for daily runs, 6h for Sunday backfill
 *
 * Design decisions:
 * - Value is run_id only. No PID — PID reuse would make stale lock checks
 *   misleading. TTL is the safety net, not PID inspection.
 * - acquireLock returns null (not throws) when Redis is down. Caller decides
 *   whether to skip or proceed — caller skips (safe default).
 * - releaseLock uses DEL which is idempotent. Calling on an expired or
 *   already-deleted key is a no-op. Ghost reaper calls this unconditionally.
 * - Module owns its own Redis connection; does not share with dedup module
 *   (dedup lives inside the child process, not the orchestrator).
 */

import Redis from "ioredis";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

let _client: Redis | null = null;
let _connectionFailed = false;

function getClient(): Redis | null {
  if (_connectionFailed) return null;
  if (_client) return _client;

  _client = new Redis(REDIS_URL, {
    lazyConnect:         true,
    enableReadyCheck:    false,
    maxRetriesPerRequest: 1,
  });

  _client.on("error", (err) => {
    if (!_connectionFailed) {
      console.error("[lock] Redis connection error:", err.message);
      _connectionFailed = true;
    }
  });

  return _client;
}

function lockKey(source: string): string {
  return `orchestrator:lock:${source}`;
}

/**
 * acquireLock — attempts to set the lock for a source.
 *
 * Returns the run_id on success (lock acquired), null if the lock is already
 * held or Redis is unavailable.
 *
 * @param source    — pipeline source (dice, jobright, linkedin)
 * @param runId     — run_id to store as the lock value
 * @param ttlSecs   — lock TTL in seconds (4h = 14400, 6h = 21600)
 */
export async function acquireLock(
  source: string,
  runId: string,
  ttlSecs: number,
): Promise<string | null> {
  const client = getClient();
  if (!client) {
    console.warn(`[lock] Redis unavailable — skipping ${source} run`);
    return null;
  }

  try {
    // SET key value NX EX ttl — atomic: only sets if key does not exist
    const result = await client.set(lockKey(source), runId, "EX", ttlSecs, "NX");
    if (result === "OK") return runId;

    // Lock is held — log who holds it
    const holder = await client.get(lockKey(source));
    console.log(`[lock] ${source} lock held by run ${holder ?? "unknown"} — skipping tick`);
    return null;
  } catch (e) {
    console.error("[lock] acquireLock error:", (e as Error).message);
    return null;
  }
}

/**
 * releaseLock — deletes the lock key. Idempotent (DEL on missing key = 0, not error).
 * Called both by runner (on clean exit) and ghost reaper (on dead run cleanup).
 */
export async function releaseLock(source: string): Promise<void> {
  const client = getClient();
  if (!client) return;

  try {
    await client.del(lockKey(source));
  } catch (e) {
    console.error("[lock] releaseLock error:", (e as Error).message);
  }
}

/**
 * isLockHeld — returns true if a lock exists for this source.
 * Used by the ghost reaper to detect stale locks on dead runs.
 */
export async function isLockHeld(source: string): Promise<boolean> {
  const client = getClient();
  if (!client) return false;

  try {
    const val = await client.exists(lockKey(source));
    return val === 1;
  } catch (e) {
    console.error("[lock] isLockHeld error:", (e as Error).message);
    return false;
  }
}

/**
 * closeRedis — graceful shutdown. Called on SIGTERM/SIGINT.
 */
export async function closeRedis(): Promise<void> {
  if (_client) {
    try { await _client.quit(); } catch { /* ignore */ }
    _client = null;
  }
}

/** Test helper — reset module state between tests */
export function _resetLockClientForTesting(): void {
  _client = null;
  _connectionFailed = false;
}