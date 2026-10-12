import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { stripTypeScriptTypes } from "node:module";
import test from "node:test";
import vm from "node:vm";

// Exercise the real fixture helpers with explicitly ordered external events.
// No services, clocks, sockets or polling delays are needed for these races.
async function fixtures(overrides) {
  const expect = (actual) => ({
    toBe: expected => assert.equal(actual, expected),
    toHaveLength: expected => assert.equal(actual.length, expected),
    toBeGreaterThan: expected => assert.ok(actual > expected),
  });
  expect.poll = callback => ({ toBe: async expected => assert.equal(await callback(), expected) });
  const modules = {
    "node:fs": { default: fs },
    "node:path": { default: path },
    "../helpers": { expect, graphql: overrides.graphql, postProbeArticle: async () => {},
      nntpBodyMetrics: undefined, submitProbeNzb: undefined, test: undefined },
    "./datastore": { literal: String, waitRows: overrides.waitRows },
    "./downloads": { startDownload: undefined, waitTerminal: undefined },
    "./e2e-clock": { clockFile: undefined, readClock: () => new Date("2032-06-13T10:30:00Z") },
    "./script-settings": { nzbDocument: () => "<nzb/>", nzbgetRpc: overrides.nzbgetRpc,
      withControlKey: async (_request, use) => use("fixture-key") },
  };
  const source = fs.readFileSync(new URL("../tests/support/schedules.ts", import.meta.url), "utf8");
  const module = new vm.SourceTextModule(stripTypeScriptTypes(source));
  await module.link(specifier => {
    assert.ok(Object.hasOwn(modules, specifier), `unexpected import ${specifier}`);
    const exports = modules[specifier];
    return new vm.SyntheticModule(Object.keys(exports), function () {
      for (const [name, value] of Object.entries(exports)) this.setExport(name, value);
    });
  });
  await module.evaluate();
  return module.namespace;
}

test("cleanup drains a pause already queued between rule deletions before the operator resumes", async () => {
  const rows = new Map();
  const pending = [];
  let serial = 0;
  let paused = false;
  let speed = 0;
  let replayed = false;
  const { Rules, resumeAll } = await fixtures({
    graphql: async (_request, query, variables) => {
      if (query.includes("createSchedule")) {
        const row = { ...variables.input, id: String(++serial) };
        rows.set(row.id, row);
        if (row.actionType === "speed_limit") pending.push(() => { speed = row.speedLimitBytes; });
        return { createSchedule: [...rows.values()] };
      }
      if (query.includes("deleteSchedule")) {
        const deleted = rows.get(variables.id);
        rows.delete(variables.id);
        if (deleted.actionType === "resume" && [...rows.values()].some(row => row.actionType === "pause")) {
          // A tick captured the old pause before the next deletion; its
          // pipeline command may arrive even after both rows are gone.
          pending.push(() => { paused = true; replayed = true; });
        }
        return { deleteSchedule: [...rows.values()] };
      }
      if (query.includes("updateSchedule")) {
        Object.assign(rows.get(variables.id), variables.input);
        pending.push(() => { speed = variables.input.speedLimits[0].bytesPerSec; });
        return { updateSchedule: [...rows.values()] };
      }
      if (query.includes("globalQueueState")) {
        for (const apply of pending.splice(0)) apply();
        return { globalQueueState: { downloadBlock: { scheduledSpeedLimit: speed } } };
      }
      assert.equal(query, "mutation { resumeAll }");
      assert.equal(replayed, true, "the evaluator must drain before the operator reset");
      paused = false;
      return { resumeAll: true };
    },
  });
  const rules = new Rules({}, "cleanup");
  await rules.create({ actionType: "resume", time: "10:30" });
  await rules.create({ actionType: "pause", time: "10:00" });
  await rules.clearAndSettle();
  await resumeAll({});
  for (const apply of pending.splice(0)) apply();
  assert.equal(paused, false);
  assert.equal(speed, 0);
  assert.equal(rows.size, 0);
});

for (const scheduledResume of [false, true]) {
  test(`cancel fixture preserves a per-job pause and global state (scheduled resume: ${scheduledResume})`, async () => {
    let globalPaused = true;
    let jobPaused = false;
    let cancelled = false;
    const id = 10000;
    const { cancelledJob } = await fixtures({
      nzbgetRpc: async (_request, _key, method, params) => {
        assert.equal(method, "append");
        jobPaused = params[5] === true;
        if (scheduledResume) globalPaused = false;
        assert.ok(jobPaused || globalPaused, "an unpaused job would finish before cancellation");
        return id;
      },
      graphql: async (_request, query, variables) => {
        assert.equal(variables.id, id);
        if (query.includes("queueItem")) return { queueItem: { state: jobPaused ? "PAUSED" : "MOVING" } };
        assert.ok(query.includes("cancelJob"), "the fixture must not mutate global pause state");
        assert.equal(jobPaused, true);
        cancelled = true;
        return { cancelJob: true };
      },
      waitRows: async (query, predicate) => {
        assert.ok(query.endsWith(`job_id = ${id}`));
        assert.equal(cancelled, true);
        assert.equal(predicate([{ status: "cancelled" }]), true);
      },
    });
    assert.equal(await cancelledJob({}, "cancel-fixture"), id);
    assert.equal(globalPaused, !scheduledResume);
    assert.equal(cancelled, true);
  });
}
