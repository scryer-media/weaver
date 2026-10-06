import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, weaverRoute } from "../helpers";
import { type Row, literal, query, waitRows } from "./datastore";

/**
 * Script settings, lists and results through the public API, for the script
 * specs. Every spec takes the settings it needs with `useScripts` and gives
 * them back with the returned restore, so specs never inherit each other's
 * lists or limits.
 */
export const WEAVER_SCRIPTS_DIR = "/data/scripts";

export type ScriptSettings = {
  eventScriptConcurrency: number;
  eventScriptTimeoutSeconds: number;
  fileDownloadedEventInterval: number;
  scriptOutputCeilingBytes: number;
  scriptOutputRunsPerJob: number;
  scriptOutputRingBytes: number;
  scriptOutputRunCapBytes: number;
  scriptDirectory: string;
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  strictSecurityRefusesExecution: boolean;
};
export type ListEntry = { script: string; enabled?: boolean; timeoutSeconds?: number | null };
export type ScriptLists = { global: ListEntry[]; categories: Array<{ category: string; entries: ListEntry[] }> };

const SETTINGS_FIELDS = `eventScriptConcurrency eventScriptTimeoutSeconds fileDownloadedEventInterval
  scriptOutputCeilingBytes scriptOutputRunsPerJob scriptOutputRingBytes scriptOutputRunCapBytes
  scriptDirectory executionEnabled concurrency terminationGraceSeconds strictSecurityRefusesExecution`;
const LIST_FIELDS = "global { script enabled timeoutSeconds } categories { category entries { script enabled timeoutSeconds } }";

export async function scriptSettings(request: APIRequestContext): Promise<ScriptSettings & { lists: ScriptLists }> {
  return (await graphql<{ postProcessingSettings: ScriptSettings & { lists: ScriptLists } }>(request,
    `query { postProcessingSettings { ${SETTINGS_FIELDS} lists { ${LIST_FIELDS} } } }`)).postProcessingSettings;
}

function settingsInput(settings: ScriptSettings): Record<string, unknown> {
  const { scriptDirectory: _directory, strictSecurityRefusesExecution: _strict, ...input } = settings;
  return input;
}

export async function setScriptSettings(request: APIRequestContext, patch: Partial<ScriptSettings>): Promise<ScriptSettings> {
  const current = await scriptSettings(request);
  return (await graphql<{ setPostProcessingSettings: ScriptSettings }>(request,
    `mutation($input: PostProcessingSettingsInput!) { setPostProcessingSettings(input: $input) { ${SETTINGS_FIELDS} } }`,
    { input: settingsInput({ ...current, ...patch }) })).setPostProcessingSettings;
}

export async function setScriptLists(request: APIRequestContext, lists: Partial<ScriptLists>): Promise<ScriptLists> {
  const input = {
    global: (lists.global ?? []).map(entry => ({ enabled: true, ...entry })),
    categories: (lists.categories ?? []).map(list => ({ category: list.category, entries: list.entries.map(entry => ({ enabled: true, ...entry })) })),
  };
  return (await graphql<{ setScriptLists: ScriptLists }>(request,
    `mutation($input: ScriptListsInput!) { setScriptLists(input: $input) { ${LIST_FIELDS} } }`, { input })).setScriptLists;
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
  outputId: string | null; outputRetained: boolean; script: string; event: string; adapter: string;
  status: string; exitCode: number | null; durationMs: number; outputTail: string; outputTruncated: boolean;
  errorMessage: string | null; finishedAtEpochMs: number;
};
const RESULT_FIELDS = "outputId outputRetained script event adapter status exitCode durationMs outputTail outputTruncated errorMessage finishedAtEpochMs";

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
