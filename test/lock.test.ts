/**
 * lock.test.ts — tests for the Redis-based run lock.
 *
 * Tests that require a real Redis instance are gated behind RUN_INFRA_TESTS=1,
 * matching the pattern used in storage/test/persist.test.ts.
 *
 * Pure contract tests (Redis-down behavior) run in all environments.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { acquireLock, releaseLock, isLockHeld, _resetLockClientForTesting } from "../src/lock.js";

const RUN_INFRA = !!process.env.RUN_INFRA_TESTS;

// ---------------------------------------------------------------------------
// Redis-down contract — runs always (no real Redis needed)
// ---------------------------------------------------------------------------

describe("lock — Redis-down contract", () => {
  beforeEach(() => {
    // Point at a port nothing is listening on to simulate Redis being down
    process.env.REDIS_URL = "redis://localhost:19999";
    _resetLockClientForTesting();
  });

  afterEach(() => {
    delete process.env.REDIS_URL;
    _resetLockClientForTesting();
  });

  it("acquireLock returns null when Redis is unreachable", async () => {
    const result = await acquireLock("dice", "run-abc", 14400);
    expect(result).toBeNull();
  });

  it("releaseLock does not throw when Redis is unreachable", async () => {
    await expect(releaseLock("dice")).resolves.toBeUndefined();
  });

  it("isLockHeld returns false when Redis is unreachable", async () => {
    const held = await isLockHeld("dice");
    expect(held).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Real Redis tests — gated behind RUN_INFRA_TESTS=1
// ---------------------------------------------------------------------------

describe.skipIf(!RUN_INFRA)("lock — real Redis", () => {
  const TEST_SOURCE = `test-source-${Date.now()}`;
  const TEST_RUN_ID = `test-run-${Date.now()}`;

  beforeEach(async () => {
    // Clean up any leftover key from a previous failed test
    await releaseLock(TEST_SOURCE);
  });

  afterEach(async () => {
    await releaseLock(TEST_SOURCE);
    _resetLockClientForTesting();
  });

  it("acquireLock returns run_id when lock is free", async () => {
    const result = await acquireLock(TEST_SOURCE, TEST_RUN_ID, 60);
    expect(result).toBe(TEST_RUN_ID);
  });

  it("acquireLock returns null when lock is already held", async () => {
    // First acquire
    const first = await acquireLock(TEST_SOURCE, TEST_RUN_ID, 60);
    expect(first).toBe(TEST_RUN_ID);

    // Second acquire on same source — should fail
    const second = await acquireLock(TEST_SOURCE, "other-run-id", 60);
    expect(second).toBeNull();
  });

  it("releaseLock allows subsequent acquire to succeed", async () => {
    await acquireLock(TEST_SOURCE, TEST_RUN_ID, 60);
    await releaseLock(TEST_SOURCE);

    const result = await acquireLock(TEST_SOURCE, "new-run-id", 60);
    expect(result).toBe("new-run-id");
  });

  it("releaseLock on expired/missing key does not throw (idempotent)", async () => {
    // Key was never set — DEL is a no-op
    await expect(releaseLock(`nonexistent-${Date.now()}`)).resolves.toBeUndefined();
  });

  it("isLockHeld returns true when lock exists, false after release", async () => {
    await acquireLock(TEST_SOURCE, TEST_RUN_ID, 60);
    expect(await isLockHeld(TEST_SOURCE)).toBe(true);

    await releaseLock(TEST_SOURCE);
    expect(await isLockHeld(TEST_SOURCE)).toBe(false);
  });
});