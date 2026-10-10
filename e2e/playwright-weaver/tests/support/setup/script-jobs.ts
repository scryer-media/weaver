import type { APIRequestContext } from "@playwright/test";

import { expect, graphql, postProbeArticle, submitProbeNzb } from "../../helpers";
import { waitTerminal } from "../downloads";
import { removeFixtureScripts, writeBareScript, writeFixtureScript } from "../script-fixtures";
import { UPDATE_SCRIPT_INSTANCE_MUTATION } from "../web-script-documents";
import {
  type ScriptInstance, type ScriptResult, createScriptInstance, deleteScriptInstance, scriptInstances, scriptResults, useScripts, waitResults,
} from "../script-settings";

/**
 * Sanctioned setup for the browser-owned scripts spec: script files in the
 * shared scripts directory, the directory and execution switched on, jobs
 * submitted and waited for, and the instances a test made removed again.
 * Everything the spec asserts is read from the screens; what this reads back
 * through the API is only what a wait has to be tied to.
 */

export { removeFixtureScripts, writeBareScript, writeFixtureScript };
export type { ScriptResult };

/** Point Weaver at the shared scripts directory with execution on and no jobs; returns the restore. */
export async function useScriptDirectory(request: APIRequestContext): Promise<() => Promise<void>> {
  return useScripts(request, {});
}

/** A one-article job that finishes and runs whatever script jobs are set up. Returns its id. */
export async function submitScriptJob(request: APIRequestContext, name: string): Promise<number> {
  const messageId = `${name}@post-processing.e2e.invalid`;
  await postProbeArticle(messageId, 1024);
  const submission = await submitProbeNzb(request, name, [{ messageId, bytes: 1024 }]);
  expect(submission.accepted, JSON.stringify(submission)).toBeTruthy();
  return submission.jobId!;
}

/** Wait until the job is finished and its post-processing results number `count`; returns them in run order. */
export async function waitScriptJob(request: APIRequestContext, jobId: number, count: number): Promise<{ state: string; results: ScriptResult[] }> {
  const state = await waitTerminal(request, jobId);
  const results = await waitResults(request, jobId, all => all.filter(result => result.event === "post_processing").length >= count,
    `${count} post-processing results of job ${jobId}`);
  return { state, results: results.filter(result => result.event === "post_processing") };
}

/** Post-processing results recorded on a job so far. */
export async function postProcessingResults(request: APIRequestContext, jobId: number): Promise<ScriptResult[]> {
  return (await scriptResults(request, jobId)).filter(result => result.event === "post_processing");
}

/** Remove every instance of these scripts, whoever made it. */
export async function removeScriptJobs(request: APIRequestContext, scripts: string[]): Promise<void> {
  for (const instance of await scriptInstances(request)) {
    if (scripts.includes(instance.script)) await deleteScriptInstance(request, instance.id);
  }
}

/**
 * How many post-processing runs are already recorded, all and failed, so a
 * spec that adds its own can say what the Runs screen must then count.
 */
export async function postProcessingRunCounts(request: APIRequestContext): Promise<{ all: number; failed: number }> {
  const page = (await graphql<{ scriptRuns: { total: number; statusCounts: Array<{ status: string; count: number }> } }>(request,
    `query { scriptRuns(limit: 1, kind: POST_PROCESSING) { total statusCounts { status count } } }`)).scriptRuns;
  return { all: page.total, failed: page.statusCounts.find(entry => entry.status === "FAILED")?.count ?? 0 };
}

/**
 * `count` finished post-processing runs of one script, all on one job, so
 * the Runs screen has more than a page of them. Returns the job id. The jobs
 * that ran are switched off afterwards, not deleted: deleting a job deletes
 * its runs with it. The caller removes them with `removeScriptJobs` once the
 * screen has been read.
 */
export async function seedScriptRuns(request: APIRequestContext, script: string, count: number): Promise<number> {
  const instances: ScriptInstance[] = [];
  try {
    for (let index = 0; index < count; index++) {
      instances.push(await createScriptInstance(request, {
        name: `${script}-${String(index + 1).padStart(2, "0")}`, script, trigger: "POST_PROCESSING",
      }));
    }
    const jobId = await submitScriptJob(request, `${script}-job`);
    await waitScriptJob(request, jobId, count);
    return jobId;
  } finally {
    for (const instance of instances) {
      await graphql(request, UPDATE_SCRIPT_INSTANCE_MUTATION, { id: instance.id, input: {
        name: instance.name, script: instance.script, trigger: instance.trigger, categories: instance.categories,
        enabled: false, blocking: instance.blocking, timeoutSeconds: instance.timeoutSeconds,
        inputs: instance.inputs.map(({ name, value }) => ({ name, value })),
      } });
    }
  }
}
