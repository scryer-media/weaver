import { expect, graphql, postProbeArticle, submitProbeNzb, test } from "./helpers";
import { waitTerminal } from "./support/downloads";
import { stage } from "./support/network-flow";
import { gateWaiting, removeFixtureScripts, removeStaleGate, writeFixtureScript } from "./support/script-fixtures";
import {
  deleteScriptInstance, instanceIds, loadStageState, queueRows, saveStageState, scriptSettings, useScripts, waitQueueRows,
} from "./support/script-settings";

/**
 * Q12, in a file of its own so it is the last test of the event-scripts
 * stage: its initial stage leaves a queue run started for the restart, and
 * that run holds the event script slot every scan, feed and scheduler script
 * also waits on.
 */

const token = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;

test("Q12 a restart marks a started queue run interrupted and the job carries on", async ({ request }) => {
  const script = "q12-recovery";
  if (stage() === "initial") {
    writeFixtureScript(script, { kinds: ["QUEUE"], queueEvents: ["NZB_ADDED"], gate: true });
    const current = (await scriptSettings(request)).lists;
    // Deliberately not restored: the run must still be started when Weaver restarts.
    await useScripts(request, { global: [...current.global.filter(entry => entry.script !== script), { script }], categories: current.categories });
    // A queue run whose job has already finished is skipped, so the one-file
    // job is held until its NZB_ADDED run has started.
    await graphql(request, "mutation { pauseAll }");
    try {
      const name = `q12-${token()}`;
      const messageId = `${name}-0@e2e.invalid`;
      await postProbeArticle(messageId, 4096);
      const submitted = await submitProbeNzb(request, name, [{ messageId, bytes: 4096 }]);
      expect(submitted, `submit ${name}`).toMatchObject({ accepted: true });
      const jobId = submitted.jobId!;
      await expect.poll(() => gateWaiting(script, jobId), { message: "NZB_ADDED run at its gate", timeout: 0 }).toBe(true);
      await waitQueueRows(jobId, rows => rows.some(row => row.event === "NZB_ADDED" && row.state === "started"), "NZB_ADDED run started");
      saveStageState("q12", { jobId });
    } finally {
      await graphql(request, "mutation { resumeAll }");
    }
    return;
  }
  const { jobId } = loadStageState<{ jobId: number }>("q12");
  for (const id of await instanceIds(request, script)) await deleteScriptInstance(request, id);
  // The restart killed the run that read this gate.
  removeStaleGate(script, jobId);
  removeFixtureScripts([script]);
  expect((await queueRows(jobId, "NZB_ADDED")).map(row => row.state)).toEqual(["interrupted"]);
  expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
});
