import { spawnRun } from "./src/runner.ts";

const code = await spawnRun({
  source:       "dice",
  postedWithin: "ONE",
  max:          10,
  runId:        `e2e-test-${Date.now()}`,
  lockTtlSecs:  3600,
});

console.log("exit code:", code);
process.exit(0);