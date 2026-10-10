import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, weaverRoute } from "../helpers";
import { type Row, literal, query, waitRows } from "./datastore";

/**
 * Script settings, instances and results through the public API, for the
 * script specs. Every spec takes the settings it needs with `useScripts` and
 * gives them back with the returned restore, so specs never inherit each
 * other's instances or limits.
 *
 * The specs still describe what runs as lists of script names, one for every
 * download and one per category. Weaver runs instances, so a list is set up as
 * the instances its scripts' headers ask for: one per declared trigger, filled
 * from the header, a schedule job keeping each declared run time other than
 * start-up. A category's entries become instances narrowed to it, for the
 * triggers a download raises, and a category with instances of its own does
 * not also run the ones for every category.
 */
export const WEAVER_SCRIPTS_DIR = "/data/scripts";

export type ScriptSettings = {
  eventScriptConcurrency: number;
  eventScriptTimeoutSeconds: number;
  fileDownloadedEventInterval: number;
  scriptOutputRunsPerJob: number;
  scriptOutputFailedRunsPerJob: number;
  scriptDirectory: string;
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  strictSecurityRefusesExecution: boolean;
  globalScriptsRun: "ALWAYS" | "ONLY_WITHOUT_CATEGORY_SCRIPTS";
  /** Read back and sent again on every save: the settings input clears one it omits. */
  pythonInterpreter: string | null;
  powershellInterpreter: string | null;
  batchInterpreter: string | null;
  goInterpreter: string | null;
};
export type ListEntry = { script: string; enabled?: boolean; timeoutSeconds?: number | null };
export type ScriptLists = { global: ListEntry[]; categories: Array<{ category: string; entries: ListEntry[] }> };

export type ScriptTrigger = "POST_PROCESSING" | "QUEUE" | "SCAN" | "SCHEDULER" | "FEED";
/** When a schedule job runs: `HH:MM` or `*:MM` times, `mon` to `sun` days (none is every day). */
export type ScriptSchedule = { days: string[]; times: string[]; runAtStartup: boolean };
export type ScriptInstance = {
  id: string; name: string; script: string; trigger: ScriptTrigger; queueEvent: string | null;
  /** A secret input shows the secret it links, or that it is sealed, never a value. */
  inputs: Array<{ name: string; value: string; secret: { id: string; name: string } | null; sealed: boolean }>;
  categories: string[]; enabled: boolean; blocking: boolean; timeoutSeconds: number | null; runOrder: number;
  schedule: ScriptSchedule;
  scriptProblem: string | null; headerDrift: boolean;
};
export type ScriptInstanceInput = {
  name?: string; script: string; trigger: ScriptTrigger; queueEvent?: string | null;
  /**
   * Each input is a plain `value`, the `secretId` of a named secret, or with
   * `secret` a value sealed into the instance.
   */
  inputs?: Array<{ name: string; value?: string; secretId?: string; secret?: boolean }>;
  categories?: string[]; enabled?: boolean; blocking?: boolean; timeoutSeconds?: number | null;
  /** Only a schedule job keeps one. */
  schedule?: ScriptSchedule;
};

const SETTINGS_FIELDS = `eventScriptConcurrency eventScriptTimeoutSeconds fileDownloadedEventInterval
  scriptOutputRunsPerJob scriptOutputFailedRunsPerJob
  scriptDirectory executionEnabled concurrency terminationGraceSeconds strictSecurityRefusesExecution globalScriptsRun
  pythonInterpreter powershellInterpreter batchInterpreter goInterpreter`;
const INSTANCE_FIELDS = `id name script trigger queueEvent inputs { name value secret { id name } sealed } categories enabled blocking
  timeoutSeconds runOrder schedule { days times runAtStartup } scriptProblem headerDrift`;

/** Every saved instance, in run order. */
export async function scriptInstances(request: APIRequestContext): Promise<ScriptInstance[]> {
  return (await graphql<{ scriptInstances: ScriptInstance[] }>(request,
    `query { scriptInstances { ${INSTANCE_FIELDS} } }`)).scriptInstances;
}

export async function createScriptInstance(request: APIRequestContext, input: ScriptInstanceInput): Promise<ScriptInstance> {
  return (await graphql<{ createScriptInstance: ScriptInstance }>(request,
    `mutation($input: ScriptInstanceInput!) { createScriptInstance(input: $input) { ${INSTANCE_FIELDS} } }`, { input })).createScriptInstance;
}

export type Secret = { id: string; name: string; usedBy: Array<{ id: string; name: string }> };

/** Keep `value` under `name`, for a secret input to link by id. The value is never read back. */
export async function createSecret(request: APIRequestContext, name: string, value: string): Promise<Secret> {
  return (await graphql<{ createSecret: Secret }>(request,
    "mutation($name: String!, $value: String!) { createSecret(name: $name, value: $value) { id name usedBy { id name } } }",
    { name, value })).createSecret;
}

/** Remove a secret no instance links any more. */
export async function deleteSecret(request: APIRequestContext, id: string): Promise<void> {
  await graphql(request, "mutation($id: String!) { deleteSecret(id: $id) }", { id });
}

/** Remove an instance, with the run times saved on it. */
export async function deleteScriptInstance(request: APIRequestContext, id: string): Promise<void> {
  await graphql(request, "mutation($id: String!) { deleteScriptInstance(id: $id) }", { id });
}

/** The ids of the instances of `script`, optionally only those `trigger` starts. */
export async function instanceIds(request: APIRequestContext, script: string, trigger?: ScriptTrigger): Promise<string[]> {
  return (await scriptInstances(request))
    .filter(instance => instance.script === script && (trigger === undefined || instance.trigger === trigger))
    .map(instance => instance.id);
}

/**
 * An instance the lists below own: one named after its script, as an
 * instance created without a name is. A test that creates a named instance
 * of its own keeps it across list changes and removes it itself.
 */
const listed = (instance: ScriptInstance) => instance.name === instance.script;

/** The listed instances read back as lists of script names. */
function listsOf(saved: ScriptInstance[]): ScriptLists {
  const instances = saved.filter(listed);
  const entries = (of: ScriptInstance[]): ListEntry[] => {
    const byScript = new Map<string, ListEntry>();
    for (const instance of of) {
      const entry = byScript.get(instance.script);
      if (entry) entry.enabled = entry.enabled || instance.enabled;
      else byScript.set(instance.script, { script: instance.script, enabled: instance.enabled, timeoutSeconds: instance.timeoutSeconds });
    }
    return [...byScript.values()];
  };
  const ordered = [...instances].sort((left, right) => left.runOrder - right.runOrder);
  const categories = [...new Set(ordered.flatMap(instance => instance.categories))];
  return {
    global: entries(ordered.filter(instance => instance.categories.length === 0)),
    categories: categories.map(category => ({
      category, entries: entries(ordered.filter(instance => instance.categories.includes(category))),
    })),
  };
}

export async function scriptSettings(request: APIRequestContext): Promise<ScriptSettings & { lists: ScriptLists }> {
  const data = await graphql<{ postProcessingSettings: ScriptSettings; scriptInstances: ScriptInstance[] }>(request,
    `query { postProcessingSettings { ${SETTINGS_FIELDS} } scriptInstances { ${INSTANCE_FIELDS} } }`);
  return { ...data.postProcessingSettings, lists: listsOf(data.scriptInstances) };
}

function settingsInput(settings: ScriptSettings & { lists?: ScriptLists }): Record<string, unknown> {
  // The query also carries the read-only directory and policy flag and the
  // script lists, none of which the settings input accepts.
  const { scriptDirectory: _directory, strictSecurityRefusesExecution: _strict, lists: _lists, ...input } = settings;
  return input;
}

export async function setScriptSettings(request: APIRequestContext, patch: Partial<ScriptSettings>): Promise<ScriptSettings> {
  const current = await scriptSettings(request);
  return (await graphql<{ setPostProcessingSettings: ScriptSettings }>(request,
    `mutation($input: PostProcessingSettingsInput!) { setPostProcessingSettings(input: $input) { ${SETTINGS_FIELDS} } }`,
    { input: settingsInput({ ...current, ...patch }) })).setPostProcessingSettings;
}

type Preset = {
  triggers: Array<{ trigger: ScriptTrigger; queueEvent: string | null }>;
  taskTimes: string[];
  inputs: Array<{ name: string; value: string; secret: boolean }>;
};

/** What each listed script's header asks for, by script name. */
async function presets(request: APIRequestContext): Promise<Map<string, Preset>> {
  const listing = (await graphql<{ discoveredScripts: { scripts: Array<{ name: string; preset: Preset }> } }>(request,
    "query { discoveredScripts { scripts { name preset { triggers { trigger queueEvent } taskTimes inputs { name value secret } } } } }",
  )).discoveredScripts;
  return new Map(listing.scripts.map(script => [script.name, script.preset]));
}

/** The triggers a download raises, the only ones a category can narrow. */
const CATEGORY_TRIGGERS = new Set<ScriptTrigger>(["POST_PROCESSING", "QUEUE"]);

/**
 * Replace every listed instance with the ones `lists` describes, in list
 * order. A script whose header declares no trigger, such as a bare SABnzbd
 * script, runs as post-processing.
 */
export async function setScriptLists(request: APIRequestContext, lists: Partial<ScriptLists>): Promise<ScriptLists> {
  for (const instance of (await scriptInstances(request)).filter(listed)) await deleteScriptInstance(request, instance.id);
  const known = await presets(request);
  const add = async (entry: ListEntry, category: string | null) => {
    const preset = known.get(entry.script);
    const triggers = preset?.triggers.length ? preset.triggers : [{ trigger: "POST_PROCESSING" as const, queueEvent: null }];
    const enabled = entry.enabled ?? true;
    for (const { trigger, queueEvent } of triggers) {
      if (category !== null && !CATEGORY_TRIGGERS.has(trigger)) continue;
      // A start-up time is left off: a spec that wants one asks for it on its own job.
      const schedule = trigger === "SCHEDULER"
        ? { days: [], times: (preset?.taskTimes ?? []).filter(time => time !== "*"), runAtStartup: false }
        : undefined;
      await createScriptInstance(request, {
        script: entry.script, trigger, queueEvent, enabled, timeoutSeconds: entry.timeoutSeconds ?? null,
        categories: category === null ? [] : [category],
        inputs: (preset?.inputs ?? []).filter(input => !input.secret).map(({ name, value }) => ({ name, value })),
        schedule,
      });
    }
  };
  for (const entry of lists.global ?? []) await add(entry, null);
  for (const list of lists.categories ?? []) for (const entry of list.entries) await add(entry, list.category);
  // A category with a list of its own runs that list instead.
  await setScriptSettings(request, { globalScriptsRun: "ONLY_WITHOUT_CATEGORY_SCRIPTS" });
  return listsOf(await scriptInstances(request));
}

/**
 * Point Weaver at the shared scripts directory, turn execution on and apply
 * `patch` and `lists`. Returns a restore that puts the previous settings and
 * lists back.
 */
export async function useScripts(
  request: APIRequestContext, lists: Partial<ScriptLists>, patch: Partial<ScriptSettings> = {},
): Promise<() => Promise<void>> {
  const before = await scriptSettings(request);
  if (before.scriptDirectory !== WEAVER_SCRIPTS_DIR) {
    await graphql(request, "mutation($directory: String!) { setPostProcessingScriptDirectory(directory: $directory) { scriptDirectory } }",
      { directory: WEAVER_SCRIPTS_DIR });
  }
  await setScriptSettings(request, { executionEnabled: true, ...patch });
  await setScriptLists(request, lists);
  return async () => {
    await setScriptLists(request, before.lists);
    await setScriptSettings(request, before);
  };
}

export type ScriptResult = {
  outputId: string | null; outputRetained: boolean; script: string; instanceId: string | null; instanceName: string | null;
  event: string; background: boolean; adapter: string;
  status: string; exitCode: number | null; durationMs: number; outputTail: string; outputTruncated: boolean;
  errorMessage: string | null; finishedAtEpochMs: number;
};
const RESULT_FIELDS = `outputId outputRetained script instanceId instanceName event background adapter status exitCode durationMs
  outputTail outputTruncated errorMessage finishedAtEpochMs`;

export async function scriptResults(request: APIRequestContext, jobId: number): Promise<ScriptResult[]> {
  return (await graphql<{ postProcessingResults: ScriptResult[] }>(request,
    `query($jobId: Int!) { postProcessingResults(jobId: $jobId) { ${RESULT_FIELDS} } }`, { jobId })).postProcessingResults;
}

/** Poll a job's results until `predicate` holds; returns them. */
export async function waitResults(
  request: APIRequestContext, jobId: number, predicate: (results: ScriptResult[]) => boolean, describe: string,
): Promise<ScriptResult[]> {
  let results: ScriptResult[] = [];
  await expect.poll(async () => predicate(results = await scriptResults(request, jobId)), { message: describe, timeout: 0 }).toBe(true);
  return results;
}

export async function scriptOutput(request: APIRequestContext, outputId: string): Promise<string | null> {
  return (await graphql<{ scriptOutput: string | null }>(request,
    "query($id: String!) { scriptOutput(outputId: $id) }", { id: outputId })).scriptOutput;
}

/** Results Weaver retained for runs with no job (URL, scan, feed, scheduler), newest last. */
export async function jobLessResults(script: string): Promise<Array<ScriptResult & { id: string }>> {
  const rows = await query(`SELECT id, result_json FROM script_outputs WHERE script = ${literal(script)} AND job_id IS NULL ORDER BY seq`);
  return rows.map(row => ({ ...JSON.parse(row.result_json!) as ScriptResult, id: row.id! }));
}

export async function waitJobLessResults(script: string, count: number): Promise<Array<ScriptResult & { id: string }>> {
  await waitRows(`SELECT id FROM script_outputs WHERE script = ${literal(script)} AND job_id IS NULL`,
    rows => rows.length >= count, `${count} retained runs of ${script}`);
  return jobLessResults(script);
}

/** The durable queue-event rows for a job, in sequence order. */
export async function queueRows(jobId: number, event?: string): Promise<Row[]> {
  return query(`SELECT run_id, event, state, seq, created_at FROM script_event_queue WHERE job_id = ${literal(jobId)}${
    event ? ` AND event = ${literal(event)}` : ""} ORDER BY seq`);
}

export async function waitQueueRows(jobId: number, predicate: (rows: Row[]) => boolean, describe: string, event?: string): Promise<Row[]> {
  let rows: Row[] = [];
  await expect.poll(async () => predicate(rows = await queueRows(jobId, event)), { message: describe, timeout: 0 }).toBe(true);
  return rows;
}

export type SubmissionResult = {
  accepted: boolean; status: string; jobId: number | null; errorCode: string | null; message: string | null;
  item: { id: number; name: string; category: string | null; state: string; attributes: Array<{ key: string; value: string }> } | null;
};
const SUBMISSION_FIELDS = "accepted status jobId errorCode message item { id name category state attributes { key value } }";

/** `submitNzb` without asserting acceptance; GraphQL errors come back as `errors`. */
export async function submitNzb(request: APIRequestContext, input: Record<string, unknown>): Promise<{ result: SubmissionResult | null; errors: string[] }> {
  await graphql(request, "query { __typename }"); // opens the API session
  const response = await request.post(weaverRoute("/graphql"), {
    data: { query: `mutation($input: SubmitNzbInput!) { submitNzb(input: $input) { ${SUBMISSION_FIELDS} } }`, variables: { input } },
  });
  return submissionPayload(await response.text(), response.ok());
}

function submissionPayload(text: string, ok: boolean): { result: SubmissionResult | null; errors: string[] } {
  expect(ok, text).toBeTruthy();
  const payload = JSON.parse(text) as { data?: { submitNzb: SubmissionResult } | null; errors?: Array<{ message: string }> };
  return { result: payload.data?.submitNzb ?? null, errors: (payload.errors ?? []).map(error => error.message) };
}

/** Upload an NZB through the GraphQL multipart `nzbUpload` field. */
export async function uploadNzb(
  request: APIRequestContext, filename: string, body: Buffer, mimeType: string, input: Record<string, unknown> = {},
): Promise<{ result: SubmissionResult | null; errors: string[] }> {
  await graphql(request, "query { __typename }"); // opens the API session
  const operations = JSON.stringify({
    query: `mutation($input: SubmitNzbInput!) { submitNzb(input: $input) { ${SUBMISSION_FIELDS} } }`,
    variables: { input: { ...input, filename, nzbUpload: null } },
  });
  const response = await request.post(weaverRoute("/graphql"), {
    headers: { "x-apollo-operation-name": "WeaverE2EUpload", "apollo-require-preflight": "true" },
    multipart: {
      operations,
      map: JSON.stringify({ 0: ["variables.input.nzbUpload"] }),
      0: { name: filename, mimeType, buffer: body },
    },
  });
  return submissionPayload(await response.text(), response.ok());
}

/** An NZB document for `articles`, one file per article. */
export function nzbDocument(name: string, articles: Array<{ messageId: string; bytes: number }>): string {
  const files = articles.map(({ messageId, bytes }, index) => `
    <file poster="weaver-e2e" date="1700000000" subject="${name}.${index + 1}.bin">
      <groups><group>alt.binaries.test</group></groups>
      <segments><segment bytes="${bytes}" number="1">${messageId.replace(/^<|>$/g, "")}</segment></segments>
    </file>`).join("");
  return `<?xml version="1.0" encoding="UTF-8"?>\n<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files}</nzb>\n`;
}

/** Create a CONTROL API key for the NZBGet facade, use it, and delete it again. */
export async function withControlKey<T>(request: APIRequestContext, use: (key: string) => Promise<T>): Promise<T> {
  const created = (await graphql<{ createApiKey: { key: { id: number }; rawKey: string } }>(request,
    "mutation($name: String!) { createApiKey(name: $name, scope: CONTROL) { key { id } rawKey } }",
    { name: `e2e-scripts-${Date.now()}` })).createApiKey;
  try {
    return await use(created.rawKey);
  } finally {
    await graphql(request, "mutation($id: Int!) { deleteApiKey(id: $id) { id } }", { id: created.key.id });
  }
}

/** One NZBGet JSON-RPC call with the key as the Basic password, as NZBGet clients send it. */
export async function nzbgetRpc(request: APIRequestContext, key: string, method: string, params: unknown[]): Promise<unknown> {
  const response = await request.post(weaverRoute("/jsonrpc"), {
    headers: { authorization: `Basic ${Buffer.from(`user:${key}`).toString("base64")}` },
    data: { method, params, id: 1 },
  });
  const text = await response.text();
  expect(response.ok(), text).toBeTruthy();
  const payload = JSON.parse(text) as { result?: unknown; error?: unknown };
  expect(payload.error ?? null, text).toBeNull();
  return payload.result;
}

/**
 * State a staged scenario carries from one stage to the next. Only Weaver
 * restarts between stages, so the shared data volume keeps it.
 */
const STAGE_STATE_DIR = "/weaver-data/e2e-stage-state";

export function saveStageState(name: string, value: unknown): void {
  fs.mkdirSync(STAGE_STATE_DIR, { recursive: true });
  fs.writeFileSync(path.join(STAGE_STATE_DIR, `${name}.json`), JSON.stringify(value));
}

export function loadStageState<T>(name: string): T {
  const file = path.join(STAGE_STATE_DIR, `${name}.json`);
  expect(fs.existsSync(file), `state ${name} saved by the previous stage`).toBe(true);
  return JSON.parse(fs.readFileSync(file, "utf8")) as T;
}
