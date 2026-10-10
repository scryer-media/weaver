import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, metricValue, metrics, postProbeArticle, postYencFileArticle, submitProbeNzb, test, weaverRoute } from "./helpers";
import { waitTerminal } from "./support/downloads";
import { graphqlErrors, stage } from "./support/network-flow";
import { fixtureState, setFixtureNzb } from "./support/proxy-fixture";
import {
  SCRIPTS_DIR, type ScriptRecord, gateWaiting, releaseGate, removeFixtureScripts, removeStaleGate, scriptBodies,
  scriptRecords, waitingGates, writeBareScript, writeFixturePackage, writeFixtureScript,
} from "./support/script-fixtures";
import {
  type ScriptInstance, type ScriptResult, createScriptInstance, deleteScriptInstance, loadStageState, nzbDocument, nzbgetRpc,
  saveStageState, scriptInstances, scriptOutput, scriptResults, scriptSettings, setScriptLists, setScriptSettings, submitNzb,
  useScripts, waitResults, withControlKey,
} from "./support/script-settings";
import {
  CANCEL_JOB_POST_PROCESSING_MUTATION, CANCEL_SCRIPT_TEST_MUTATION, CREATE_SCRIPT_INSTANCE_MUTATION, CREATE_SECRET_MUTATION, DELETE_SCRIPT_INSTANCE_MUTATION,
  DELETE_SECRET_MUTATION, DISCOVERED_SCRIPTS_QUERY, POST_PROCESSING_RESULTS_QUERY, POST_PROCESSING_SETTINGS_QUERY,
  REAPPLY_SCRIPT_HEADER_MUTATION, REORDER_SCRIPT_INSTANCES_MUTATION, RERUN_POST_PROCESSING_MUTATION, SCRIPT_INSTANCES_QUERY, SCRIPT_RUNS_QUERY,
  SCRIPT_RUN_OUTPUT_QUERY, SCRIPT_TEST_RUN_QUERY, SECRETS_QUERY, SET_UP_SCRIPT_FROM_HEADER_MUTATION,
  TEST_SCRIPT_INSTANCE_MUTATION, UPDATE_SCRIPT_INSTANCE_MUTATION, UPDATE_SECRET_MUTATION, type ScriptRunPage,
  type ScriptTestRun,
} from "./support/web-script-documents";

/**
 * Post-processing scripts, end to end: what each adapter hands a script
 * (asserted from records the scripts write themselves), how exit codes end a
 * run and a job, what the API, the Runs records, the job events, the logs
 * and the NZBGet facade show for it, and how instances, inputs, secrets,
 * interpreters, ordering, categories, retention and concurrency behave.
 *
 * Every script name and message id carries a token unique to the run, so
 * records and articles left by an earlier stage never satisfy a wait. The
 * concurrency test is staged: its initial half lowers the limit, which
 * applies at start-up, and its @restart half checks it after the restart.
 * The restarted stage runs only @restart tests.
 */

const token = () => `${Date.now().toString(36)}${randomBytes(2).toString("hex")}`;

function note(type: string, description: string): void {
  test.info().annotations.push({ type, description });
}

/** A one-file probe job; returns its id. */
async function job(request: APIRequestContext, name: string, extraInput: Record<string, unknown> = {}): Promise<number> {
  const messageId = `${name}@e2e.invalid`;
  await postProbeArticle(messageId, 4096);
  const result = await submitProbeNzb(request, name, [{ messageId, bytes: 4096 }], extraInput);
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  return result.jobId!;
}

/** A job of one real file `filename` holding `data`. */
async function fileJob(request: APIRequestContext, name: string, filename: string, data: Buffer, extraInput: Record<string, unknown> = {}): Promise<number> {
  const messageId = `${name}@e2e.invalid`;
  await postYencFileArticle(messageId, filename, data);
  const nzb = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="weaver-e2e" date="1700000000" subject="&quot;${filename}&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="${data.length}" number="1">${messageId}</segment></segments>
  </file>
</nzb>
`;
  const { result, errors } = await submitNzb(request, { nzbBase64: Buffer.from(nzb).toString("base64"), filename: `${name}.nzb`, ...extraInput });
  expect(errors).toEqual([]);
  expect(result?.accepted, JSON.stringify(result)).toBe(true);
  return result!.jobId!;
}

type History = {
  id: number; name: string; state: string; error: string | null; outputDir: string | null; category: string | null;
  hasPassword: boolean; health: number; totalBytes: number; downloadedBytes: number; attributes: Array<{ key: string; value: string }>;
};

async function history(request: APIRequestContext, id: number): Promise<History> {
  const item = (await graphql<{ historyItem: History | null }>(request,
    `query($id: Int!) { historyItem(id: $id) { id name state error outputDir category hasPassword health totalBytes downloadedBytes
      attributes { key value } } }`, { id })).historyItem;
  expect(item, `job ${id} in history`).toBeTruthy();
  return item!;
}

async function version(request: APIRequestContext): Promise<string> {
  return (await graphql<{ version: string }>(request, "query { version }")).version;
}

async function addCategory(request: APIRequestContext, name: string): Promise<number> {
  return (await graphql<{ addCategory: { id: number } }>(request,
    "mutation($input: CategoryInput!) { addCategory(input: $input) { id } }", { input: { name } })).addCategory.id;
}

async function removeCategory(request: APIRequestContext, id: number): Promise<void> {
  await graphql(request, "mutation($id: Int!) { removeCategory(id: $id) { id } }", { id });
}

async function jobEvents(request: APIRequestContext, jobId: number): Promise<Array<{ kind: string; message: string }>> {
  return (await graphql<{ jobEvents: Array<{ kind: string; message: string }> }>(request,
    "query($jobId: Int!) { jobEvents(jobId: $jobId) { kind message } }", { jobId })).jobEvents;
}

async function serviceLogs(request: APIRequestContext): Promise<string[]> {
  return (await graphql<{ serviceLogs: { lines: string[] } }>(request, "query { serviceLogs(limit: 5000) { lines } }")).serviceLogs.lines;
}

async function scriptRuns(request: APIRequestContext, variables: Record<string, unknown>): Promise<ScriptRunPage> {
  return (await graphql<{ scriptRuns: ScriptRunPage }>(request, SCRIPT_RUNS_QUERY, variables)).scriptRuns;
}

type FacadeHistory = {
  NZBID: number; Status: string; ParStatus: string; UnpackStatus: string; DeleteStatus: string;
  ScriptStatus: string; ScriptStatuses: Array<{ Name: string; Status: string }>;
};

/**
 * The job's row in the NZBGet facade's `history`. The facade reports every
 * success as SUCCESS/ALL and every failure as FAILURE/HEALTH with
 * DeleteStatus HEALTH and no stage claimed; the stage-exact status reaches
 * scripts through NZBPP_STATUS instead.
 */
async function facadeHistory(request: APIRequestContext, jobId: number): Promise<FacadeHistory> {
  const rows = await withControlKey(request, key => nzbgetRpc(request, key, "history", [false])) as FacadeHistory[];
  const row = rows.find(candidate => candidate.NZBID === jobId);
  expect(row, `job ${jobId} in the NZBGet facade's history`).toBeTruthy();
  return row!;
}

const forJob = (jobId: number) => (record: ScriptRecord) => record.env.NZBPP_NZBID === String(jobId);

async function waitRecords(script: string, count: number, filter: (record: ScriptRecord) => boolean = () => true): Promise<ScriptRecord[]> {
  let records: ScriptRecord[] = [];
  await expect.poll(() => (records = scriptRecords(script).filter(filter)).length >= count,
    { message: `${count} records of ${script}`, timeout: 0 }).toBe(true);
  return records;
}

/** Wait for the post-processing results of every listed script, in run order. */
async function waitPostProcessing(request: APIRequestContext, jobId: number, scripts: string[]): Promise<ScriptResult[]> {
  const ofPass = (all: ScriptResult[]) => all.filter(result => result.event === "post_processing" && scripts.includes(result.script));
  return ofPass(await waitResults(request, jobId, all => ofPass(all).length >= scripts.length, `job ${jobId} results of ${scripts.join(", ")}`));
}

const PLATFORM_ENV = new Set(["PATH", "HOME", "USERPROFILE", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL", "TZ"]);
const SHELL_ENV = new Set(["PWD", "SHLVL", "OLDPWD", "_"]);
const CONTRACT_FAMILY = /^(NZB(PP|PO|OP|PR)_|SAB_|WEAVER_)/;

/** Variables outside the platform allow-list and the three script families. */
const outsideAllowList = (env: Record<string, string>, extra: string[] = []) =>
  Object.keys(env).filter(key => !PLATFORM_ENV.has(key) && !SHELL_ENV.has(key) && !CONTRACT_FAMILY.test(key) && !extra.includes(key));

type Expected = {
  jobId: number; name: string; category: string; password: string; script: string; instance: ScriptInstance;
  sabStatus: string; nzbStatus: string; par: string; unpack: string; scriptStatus: string; version: string;
  item: History; directory?: string;
};

/**
 * Every argument and variable the adapters promise a post-processing run,
 * as one record shows them. Weaver hands every family to every script.
 */
function expectContract(record: ScriptRecord, expected: Expected): void {
  const env = record.env;
  const directory = expected.directory ?? env.SAB_COMPLETE_DIR!;
  const label = `${expected.script} for job ${expected.jobId}`;
  expect(directory, `${label}: directory`).toBeTruthy();
  expect(record.argv, `${label}: SABnzbd arguments`).toEqual([
    directory, `${expected.name}.nzb`, expected.name, "", expected.category, "", expected.sabStatus, "",
  ]);
  expect(record.cwd, `${label}: working directory`).toBe(directory);
  const total = expected.nzbStatus.split("/")[0]!;
  expect(env, `${label}: SABnzbd variables`).toMatchObject({
    SAB_VERSION: expected.version, SAB_NZO_ID: String(expected.jobId), SAB_FINAL_NAME: expected.name,
    SAB_FILENAME: `${expected.name}.nzb`, SAB_CAT: expected.category, SAB_GROUP: "", SAB_COMPLETE_DIR: directory,
    SAB_STATUS: "Running", SAB_PP_STATUS: expected.sabStatus, SAB_URL: "", SAB_FAILURE_URL: "",
    SAB_BYTES: String(expected.item.totalBytes), SAB_BYTES_DOWNLOADED: String(expected.item.downloadedBytes),
    SAB_BYTES_TRIED: String(expected.item.downloadedBytes), SAB_PASSWORD: expected.password,
    SAB_REPAIR: expected.par === "0" ? "0" : "1", SAB_UNPACK: expected.unpack === "0" ? "0" : "1", SAB_SCRIPT: expected.script,
    SAB_CORRECT_PASSWORD: "", SAB_DUPLICATE: "", SAB_DUPLICATE_KEY: "", SAB_ENCRYPTED: "", SAB_OVERSIZED: "", SAB_PP: "",
    SAB_PRIORITY: "", SAB_UNWANTED_EXT: "",
  });
  expect(env.SAB_PROGRAM_DIR, `${label}: SAB_PROGRAM_DIR`).toBeTruthy();
  expect(env, `${label}: NZBGet variables`).toMatchObject({
    NZBPP_NZBID: String(expected.jobId), NZBPP_NZBNAME: expected.name, NZBPP_DIRECTORY: directory,
    NZBPP_NZBFILENAME: `${expected.name}.nzb`, NZBPP_QUEUEDFILE: `${expected.name}.nzb`, NZBPP_URL: "",
    NZBPP_FINALDIR: directory, NZBPP_CATEGORY: expected.category, NZBPP_STATUS: expected.nzbStatus, NZBPP_TOTALSTATUS: total,
    NZBPP_SCRIPTSTATUS: expected.scriptStatus, NZBPP_PARSTATUS: expected.par, NZBPP_UNPACKSTATUS: expected.unpack,
    NZBPP_HEALTH: String(expected.item.health), NZBPP_CRITICALHEALTH: "850",
  });
  expect(env, `${label}: NZBGet global options with their upper-case aliases`).toMatchObject({
    NZBOP_Version: expected.version, NZBOP_VERSION: expected.version,
    NZBOP_AppDir: env.SAB_PROGRAM_DIR, NZBOP_APPDIR: env.SAB_PROGRAM_DIR,
    NZBOP_MainDir: env.WEAVER_DATA_DIR, NZBOP_MAINDIR: env.WEAVER_DATA_DIR,
    NZBOP_DestDir: env.WEAVER_COMPLETE_DIR, NZBOP_DESTDIR: env.WEAVER_COMPLETE_DIR,
    NZBOP_InterDir: env.NZBOP_INTERDIR, NZBOP_TempDir: env.NZBOP_TEMPDIR,
  });
  for (const key of ["NZBOP_InterDir", "NZBOP_TempDir", "WEAVER_DATA_DIR", "WEAVER_COMPLETE_DIR"]) expect(env[key], `${label}: ${key}`).toBeTruthy();
  expect(env, `${label}: weaver variables`).toMatchObject({
    WEAVER_JOB_ID: String(expected.jobId), WEAVER_JOB_NAME: expected.name, WEAVER_CATEGORY: expected.category,
    WEAVER_DIRECTORY: directory, WEAVER_FINAL_DIRECTORY: directory, WEAVER_STATUS: total, WEAVER_VERSION: expected.version,
    WEAVER_INSTANCE_ID: expected.instance.id, WEAVER_INSTANCE_NAME: expected.instance.name, WEAVER_TRIGGER: "post_processing",
  });
  expect(env.WEAVER_RUN_ID, `${label}: WEAVER_RUN_ID`).toBeTruthy();
  expect(env.WEAVER_RUN_TOKEN, `${label}: WEAVER_RUN_TOKEN`).toBeTruthy();
  expect(env.WEAVER_API_URL, `${label}: WEAVER_API_URL`).toMatch(/^http:\/\/[^/]+\/graphql$/);
  expect(outsideAllowList(env), `${label}: nothing outside the allow-list`).toEqual([]);
}

/** The single listed instance of `script`. */
async function instanceOf(request: APIRequestContext, script: string): Promise<ScriptInstance> {
  const found = (await scriptInstances(request)).filter(instance => instance.script === script && instance.trigger === "POST_PROCESSING");
  expect(found, `one post-processing instance of ${script}`).toHaveLength(1);
  return found[0]!;
}

const echoSecrets = "printf 'password=%s token=%s\\n' \"$SAB_PASSWORD\" \"$WEAVER_RUN_TOKEN\"\n";

test("PP01 a successful job hands every adapter its full contract, with a category, a password and attributes", async ({ request }) => {
  const tag = `pp01-${token()}`;
  const password = `pp01-pass-${token()}`;
  const sab = writeBareScript(`${tag}-sab`, { body: echoSecrets });
  const bare = writeFixtureScript(`${tag}-nzb`, { kinds: ["POST-PROCESSING"], headerOptions: ["Label=bare-label"], exitCode: 93, body: echoSecrets });
  const pkg = writeFixturePackage(`${tag}-pkg`, {
    kinds: ["POST-PROCESSING"], exitCode: 93,
    scriptOptions: [{ name: "Label", value: "pkg-label" }, { name: "Count", value: 7 }, { name: "Ratio", value: 0.25 }, { name: "Enabled", value: true }],
  });
  const scripts = [sab, bare, pkg];
  const categoryId = await addCategory(request, tag);
  const restore = await useScripts(request, { categories: [{ category: tag, entries: scripts.map(script => ({ script })) }] });
  try {
    const instances = await Promise.all(scripts.map(script => instanceOf(request, script)));
    const jobId = await job(request, tag, { category: tag, password, attributes: [{ key: "Origin", value: "e2e" }] });
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const item = await history(request, jobId);
    expect(item).toMatchObject({ category: tag, health: 1000 });
    expect.soft(item.hasPassword, "history hasPassword of a job submitted with a password").toBe(true);
    const results = await waitPostProcessing(request, jobId, scripts);
    expect(results.map(result => [result.script, result.adapter, result.status, result.exitCode, result.background, result.instanceId, result.instanceName]))
      .toEqual([
        [sab, "SABNZBD", "SUCCEEDED", 0, false, instances[0]!.id, sab],
        [bare, "NZBGET", "SUCCEEDED", 93, false, instances[1]!.id, bare],
        [pkg, "NZBGET", "SUCCEEDED", 93, false, instances[2]!.id, pkg],
      ]);

    const current = await version(request);
    const records = await Promise.all(scripts.map(script => waitRecords(script, 1, forJob(jobId))));
    const order = scriptRecords().filter(forJob(jobId)).map(record => record.script);
    expect(order, "scripts ran in list order").toEqual(scripts);
    const previous = ["NONE", "SUCCESS", "SUCCESS"];
    for (const [index, script] of scripts.entries()) {
      expectContract(records[index]![0]!, {
        jobId, name: item.name, category: tag, password, script, instance: instances[index]!, sabStatus: "0",
        nzbStatus: "SUCCESS/HEALTH", par: "0", unpack: "0", scriptStatus: previous[index]!, version: current,
        item, directory: item.outputDir!,
      });
      expect(records[index]![0]!.env, `${script}: the job's attribute as a parameter`).toMatchObject({ NZBPR_Origin: "e2e", NZBPR_ORIGIN: "e2e" });
    }
    const [sabEnv, bareEnv, pkgEnv] = records.map(list => list[0]!.env);
    expect(Object.keys(sabEnv!).filter(key => /^(NZBPO_|SAB_OPTION_|WEAVER_INPUT_)/.test(key)), "the SABnzbd script has no inputs").toEqual([]);
    expect(bareEnv).toMatchObject({ NZBPO_Label: "bare-label", NZBPO_LABEL: "bare-label", SAB_OPTION_LABEL: "bare-label", WEAVER_INPUT_LABEL: "bare-label" });
    expect(pkgEnv).toMatchObject({
      NZBPO_Label: "pkg-label", NZBPO_Count: "7", NZBPO_COUNT: "7", NZBPO_Ratio: "0.25", NZBPO_Enabled: "yes",
      SAB_OPTION_COUNT: "7", SAB_OPTION_RATIO: "0.25", SAB_OPTION_ENABLED: "yes", WEAVER_INPUT_LABEL: "pkg-label",
    });
    expect(new Set(records.map(list => list[0]!.env.WEAVER_RUN_ID)).size, "a run id per run").toBe(3);

    // What the scripts printed reaches the API with the password and run token redacted.
    for (const result of results.filter(candidate => candidate.script !== pkg)) {
      const output = await scriptOutput(request, result.outputId!);
      for (const text of [result.outputTail, output ?? ""]) {
        expect(text).toContain("password=[REDACTED] token=[REDACTED]");
        expect(text).not.toContain(password);
      }
    }
    for (const record of records.flat()) {
      const runToken = record.env.WEAVER_RUN_TOKEN!;
      const everything = JSON.stringify([results, await jobEvents(request, jobId), await scriptRuns(request, { jobId })]);
      expect(everything, "no run token in results, events or runs").not.toContain(runToken);
    }
    expect(JSON.stringify(await serviceLogs(request)), "no password in the service log").not.toContain(password);

    const facade = await facadeHistory(request, jobId);
    expect(facade).toMatchObject({ Status: "SUCCESS/ALL", DeleteStatus: "NONE", ScriptStatus: "SUCCESS" });
    expect(facade.ScriptStatuses).toEqual(scripts.map(script => ({ Name: script, Status: "SUCCESS" })));

    // One event per script on the job, which a client must be able to tell
    // from the job's creation.
    const events = await jobEvents(request, jobId);
    for (const result of results) {
      const event = events.find(candidate => candidate.message.startsWith(`${result.script} SUCCEEDED (exit ${result.exitCode}) in `));
      expect(event, `${result.script} job event`).toBeTruthy();
      expect.soft(event!.kind, `${result.script} job event kind (jobEvents maps the script event kind to JOB_CREATED)`).not.toBe("JOB_CREATED");
    }
  } finally {
    await restore();
    removeFixtureScripts(scripts);
    await removeCategory(request, categoryId);
  }
});

test("PP02 a failed download runs its scripts with SAB_PP_STATUS -1 and FAILURE/HEALTH, without a category or password", async ({ request }) => {
  const tag = `pp02-${token()}`;
  const sab = writeBareScript(`${tag}-sab`);
  const nzb = writeFixtureScript(`${tag}-nzb`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script: sab }, { script: nzb }] });
  try {
    // An article that was never posted: every server answers 430.
    const result = await submitProbeNzb(request, tag, [{ messageId: `${tag}-missing@e2e.invalid`, bytes: 4096 }]);
    expect(result).toMatchObject({ accepted: true });
    const jobId = result.jobId!;
    expect(await waitTerminal(request, jobId)).toBe("FAILED");
    const item = await history(request, jobId);
    expect(item.error, "the download failure, not a script failure").toBeTruthy();
    expect(item.error).not.toContain("post-processing scripts ended");
    const results = await waitPostProcessing(request, jobId, [sab, nzb]);
    expect(results.map(entry => [entry.script, entry.status, entry.exitCode])).toEqual([[sab, "SUCCEEDED", 0], [nzb, "SUCCEEDED", 93]]);
    const current = await version(request);
    const previous = ["NONE", "SUCCESS"];
    for (const [index, script] of [sab, nzb].entries()) {
      const [record] = await waitRecords(script, 1, forJob(jobId));
      expectContract(record!, {
        jobId, name: item.name, category: "", password: "", script, instance: await instanceOf(request, script), sabStatus: "-1",
        nzbStatus: "FAILURE/HEALTH", par: "0", unpack: "0", scriptStatus: previous[index]!, version: current, item,
      });
      expect(record!.env.SAB_FAIL_MSG, "the failure message").toBeTruthy();
      expect(Number(record!.env.NZBPP_HEALTH)).toBeLessThan(Number(record!.env.NZBPP_CRITICALHEALTH));
      note("observed", `${script}: SAB_FAIL_MSG=${JSON.stringify(record!.env.SAB_FAIL_MSG)}, history error=${JSON.stringify(item.error)}`);
    }
    const facade = await facadeHistory(request, jobId);
    expect(facade).toMatchObject({ Status: "FAILURE/HEALTH", ParStatus: "NONE", UnpackStatus: "NONE", DeleteStatus: "HEALTH", ScriptStatus: "SUCCESS" });
  } finally {
    await restore();
    removeFixtureScripts([sab, nzb]);
  }
});

/** A gzip member whose only deflate block has the reserved type, so inflating it fails. */
function corruptGzip(): Buffer {
  return Buffer.concat([Buffer.from("1f8b08000000000000030700", "hex"), randomBytes(512)]);
}

test("PP03 an unpacked and an unpack-failed job report PAR/UNPACK status, with a password and no category", async ({ request }) => {
  const tag = `pp03-${token()}`;
  const password = `pp03-pass-${token()}`;
  const sab = writeBareScript(`${tag}-sab`, { body: echoSecrets });
  const nzb = writeFixtureScript(`${tag}-nzb`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script: sab }, { script: nzb }] });
  try {
    const current = await version(request);
    const cases = [
      { name: `${tag}-good`, data: zlib.gzipSync(Buffer.from(`pp03 payload ${tag}\n`.repeat(256))), state: "COMPLETED",
        sabStatus: "0", nzbStatus: "SUCCESS/ALL", unpack: "2", facade: "SUCCESS/ALL" },
      { name: `${tag}-bad`, data: corruptGzip(), state: "FAILED", sabStatus: "2", nzbStatus: "FAILURE/UNPACK", unpack: "1", facade: "FAILURE/HEALTH" },
    ];
    for (const testCase of cases) {
      const jobId = await fileJob(request, testCase.name, `${testCase.name}.gz`, testCase.data, { password });
      expect(await waitTerminal(request, jobId), testCase.name).toBe(testCase.state);
      const item = await history(request, jobId);
      expect.soft(item.hasPassword, "history hasPassword of a job submitted with a password").toBe(true);
      const results = await waitPostProcessing(request, jobId, [sab, nzb]);
      expect(results.map(entry => entry.status), testCase.name).toEqual(["SUCCEEDED", "SUCCEEDED"]);
      for (const [index, script] of [sab, nzb].entries()) {
        const [record] = await waitRecords(script, 1, forJob(jobId));
        expectContract(record!, {
          jobId, name: item.name, category: "", password, script, instance: await instanceOf(request, script),
          sabStatus: testCase.sabStatus, nzbStatus: testCase.nzbStatus, par: "0", unpack: testCase.unpack,
          scriptStatus: index === 0 ? "NONE" : "SUCCESS", version: current, item,
          directory: testCase.state === "COMPLETED" ? item.outputDir! : undefined,
        });
      }
      const output = await scriptOutput(request, results[0]!.outputId!);
      expect(output).toContain("password=[REDACTED]");
      expect(output).not.toContain(password);
      expect((await facadeHistory(request, jobId)).Status, testCase.name).toBe(testCase.facade);
      if (testCase.state === "FAILED") expect(item.error).not.toContain("post-processing scripts ended");
    }
  } finally {
    await restore();
    removeFixtureScripts([sab, nzb]);
  }
});

test("PP04 exit codes 93/94/95, a missing script and a running script, through the job, the chain and the NZBGet facade", async ({ request }) => {
  note("design", "Weaver reads 0, 92 and 93 as success for every script; NZBGet itself reads exit 0 as a failure. A failed script fails the job.");
  const tag = `pp04-${token()}`;
  const n95 = writeFixtureScript(`${tag}-95`, { kinds: ["POST-PROCESSING"], exitCode: 95 });
  const n94 = writeFixtureScript(`${tag}-94`, { kinds: ["POST-PROCESSING"], exitCode: 94 });
  const rec = writeFixtureScript(`${tag}-rec`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const gone = writeFixtureScript(`${tag}-gone`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const gated = writeFixtureScript(`${tag}-gate`, { kinds: ["POST-PROCESSING"], gate: true, exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script: n95 }, { script: n94 }, { script: rec }] });
  let gatedJob = 0;
  try {
    // 95, 94, then a recorder: one failure fails the job, and the chain says so.
    const failing = await job(request, `${tag}-a`);
    expect(await waitTerminal(request, failing)).toBe("FAILED");
    const failingResults = await waitPostProcessing(request, failing, [n95, n94, rec]);
    expect(failingResults.map(result => [result.status, result.exitCode])).toEqual([["SKIPPED", 95], ["FAILED", 94], ["SUCCEEDED", 93]]);
    expect((await history(request, failing)).error).toBe("post-processing scripts ended with failed");
    const [afterFailure] = await waitRecords(rec, 1, forJob(failing));
    expect(afterFailure!.env.NZBPP_SCRIPTSTATUS).toBe("FAILURE");
    let facade = await facadeHistory(request, failing);
    expect(facade.ScriptStatus).toBe("FAILURE");
    expect(facade.ScriptStatuses).toEqual([{ Name: n95, Status: "NONE" }, { Name: n94, Status: "FAILURE" }, { Name: rec, Status: "SUCCESS" }]);

    // 95 then a recorder: nothing has succeeded or failed yet, so NZBGet's
    // NZBPP_SCRIPTSTATUS is still NONE.
    await setScriptLists(request, { global: [{ script: n95 }, { script: rec }] });
    const skipped = await job(request, `${tag}-b`);
    expect(await waitTerminal(request, skipped)).toBe("COMPLETED");
    await waitPostProcessing(request, skipped, [n95, rec]);
    const [afterSkip] = await waitRecords(rec, 1, forJob(skipped));
    expect.soft(afterSkip!.env.NZBPP_SCRIPTSTATUS, "NZBPP_SCRIPTSTATUS after only an exit-95 script").toBe("NONE");
    facade = await facadeHistory(request, skipped);
    expect(facade.ScriptStatus).toBe("SUCCESS");
    expect(facade.ScriptStatuses).toEqual([{ Name: n95, Status: "NONE" }, { Name: rec, Status: "SUCCESS" }]);

    // A listed script that is no longer in the directory: a warning, and the job goes on.
    await setScriptLists(request, { global: [{ script: gone }] });
    removeFixtureScripts([gone]);
    const missing = await job(request, `${tag}-c`);
    expect(await waitTerminal(request, missing)).toBe("COMPLETED");
    const [warning] = await waitPostProcessing(request, missing, [gone]);
    expect(warning).toMatchObject({ status: "WARNING", exitCode: null });
    expect(warning!.errorMessage).toBeTruthy();
    expect((await facadeHistory(request, missing)).ScriptStatuses).toEqual([{ Name: gone, Status: "FAILURE" }]);

    // A script that is running: the facade shows the job executing a script.
    await setScriptLists(request, { global: [{ script: gated }] });
    gatedJob = await job(request, `${tag}-d`);
    await expect.poll(() => gateWaiting(gated, gatedJob), { message: "gated script at its gate", timeout: 0 }).toBe(true);
    const [groups, postqueue] = await withControlKey(request, async key => [
      await nzbgetRpc(request, key, "listgroups", [0]), await nzbgetRpc(request, key, "postqueue", [0]),
    ]) as [Array<Record<string, unknown>>, Array<Record<string, unknown>>];
    expect(groups.find(group => group.NZBID === gatedJob)).toMatchObject({ Status: "EXECUTING_SCRIPT" });
    expect(postqueue.find(entry => entry.NZBID === gatedJob)).toMatchObject({ Stage: "EXECUTING_SCRIPT", ScriptStatus: "RUNNING" });
    await releaseGate(gated, gatedJob);
    expect(await waitTerminal(request, gatedJob)).toBe("COMPLETED");
    gatedJob = 0;
  } finally {
    if (gatedJob && gateWaiting(gated, gatedJob)) await releaseGate(gated, gatedJob);
    await restore();
    removeFixtureScripts([n95, n94, rec, gone, gated]);
  }
});

type DiscoveredScript = {
  name: string; displayName: string; adapter: string; kinds: string[];
  options: Array<{ name: string; optionType: string; required: boolean; defaultValue: string | null }>;
  preset: { triggers: Array<{ trigger: string; queueEvent: string | null }>; inputs: Array<{ name: string; value: string; secret: boolean }> };
};

async function discovered(request: APIRequestContext, name: string): Promise<DiscoveredScript> {
  const listing = (await graphql<{ discoveredScripts: { scripts: DiscoveredScript[]; problems: Array<{ name: string }> } }>(
    request, DISCOVERED_SCRIPTS_QUERY)).discoveredScripts;
  const script = listing.scripts.find(candidate => candidate.name === name);
  expect(script, `${name} discovered`).toBeTruthy();
  return script!;
}

test("PP05 option types come from the header, and jobs are set up and re-applied from it", async ({ request }) => {
  const tag = `pp05-${token()}`;
  const pkg = writeFixturePackage(`${tag}-pkg`, {
    kinds: ["POST-PROCESSING"], exitCode: 93,
    scriptOptions: [
      { name: "Label", value: "pkg-label" }, { name: "Count", value: 7 }, { name: "Ratio", value: 0.25 },
      { name: "Enabled", value: true }, { name: "Token", value: "pkg-token-default", secret: true },
    ],
  });
  const bare = writeFixtureScript(`${tag}-bare`, { kinds: ["POST-PROCESSING"], exitCode: 93, headerOptions: ["ApiKey=hdr-key-default", "SmtpPass=hdr-pass-default", "PassThrough=yes"] });
  const restore = await useScripts(request, {});
  const created: string[] = [];
  try {
    const pkgListing = await discovered(request, pkg);
    expect(pkgListing.adapter).toBe("NZBGET");
    expect(pkgListing.options.map(option => [option.name, option.optionType])).toEqual([
      ["Label", "STRING"], ["Count", "INTEGER"], ["Ratio", "NUMBER"], ["Enabled", "BOOLEAN"], ["Token", "SECRET"],
    ]);
    expect(pkgListing.options.slice(0, 4).map(option => option.defaultValue)).toEqual(["pkg-label", "7", "0.25", "yes"]);
    const bareListing = await discovered(request, bare);
    expect(bareListing.options.map(option => [option.name, option.optionType])).toEqual([
      ["ApiKey", "SECRET"], ["SmtpPass", "SECRET"], ["PassThrough", "STRING"],
    ]);
    expect(bareListing.options[2]!.defaultValue).toBe("yes");
    // A secret's default is never shown, in the options or the preset.
    const listings = JSON.stringify([pkgListing, bareListing]);
    for (const value of ["pkg-token-default", "hdr-key-default", "hdr-pass-default"]) expect(listings).not.toContain(value);
    expect(bareListing.preset.triggers).toEqual([{ trigger: "POST_PROCESSING", queueEvent: null }]);

    const setUp = (await graphql<{ setUpScriptFromHeader: ScriptInstance[] }>(request, SET_UP_SCRIPT_FROM_HEADER_MUTATION, { script: pkg })).setUpScriptFromHeader;
    created.push(...setUp.map(instance => instance.id));
    expect(setUp).toHaveLength(1);
    expect(setUp[0]).toMatchObject({ script: pkg, trigger: "POST_PROCESSING", enabled: true, headerDrift: false, scriptProblem: null });
    expect(setUp[0]!.inputs.map(input => [input.name, input.value])).toEqual(expect.arrayContaining([
      ["Label", "pkg-label"], ["Count", "7"], ["Ratio", "0.25"], ["Enabled", "yes"],
    ]));
    const again = (await graphql<{ setUpScriptFromHeader: ScriptInstance[] }>(request, SET_UP_SCRIPT_FROM_HEADER_MUTATION, { script: pkg })).setUpScriptFromHeader;
    created.push(...again.map(instance => instance.id));
    expect(again, "a second set-up adds nothing").toEqual([]);

    // Change an input, then the header: the job drifts until it is re-applied,
    // which keeps the value it was given and adds what the header now declares.
    const instance = setUp[0]!;
    const inputs = instance.inputs.filter(input => !input.sealed && input.secret === null)
      .map(input => ({ name: input.name, value: input.name === "Label" ? "operator-label" : input.value }));
    await graphql(request, UPDATE_SCRIPT_INSTANCE_MUTATION, { id: instance.id, input: {
      name: instance.name, script: instance.script, trigger: instance.trigger, categories: [], enabled: true, blocking: true,
      timeoutSeconds: null, inputs,
    } });
    writeFixturePackage(pkg, {
      kinds: ["POST-PROCESSING"], exitCode: 93,
      scriptOptions: [
        { name: "Label", value: "pkg-label" }, { name: "Count", value: 7 }, { name: "Ratio", value: 0.25 },
        { name: "Enabled", value: true }, { name: "Token", value: "pkg-token-default", secret: true }, { name: "Added", value: "new-default" },
      ],
    });
    const drifted = (await scriptInstances(request)).find(candidate => candidate.id === instance.id)!;
    expect(drifted.headerDrift).toBe(true);
    const reapplied = (await graphql<{ reapplyScriptHeader: ScriptInstance }>(request, REAPPLY_SCRIPT_HEADER_MUTATION, { id: instance.id })).reapplyScriptHeader;
    expect(reapplied.headerDrift).toBe(false);
    expect(reapplied.inputs.map(input => [input.name, input.value])).toEqual(expect.arrayContaining([
      ["Label", "operator-label"], ["Added", "new-default"],
    ]));

    // The job runs with what it was set up with.
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const [record] = await waitRecords(pkg, 1, forJob(jobId));
    expect(record!.env).toMatchObject({ NZBPO_Label: "operator-label", NZBPO_Added: "new-default", NZBPO_Count: "7", NZBPO_Enabled: "yes" });
  } finally {
    for (const id of created) await graphql(request, DELETE_SCRIPT_INSTANCE_MUTATION, { id });
    await restore();
    removeFixtureScripts([pkg, bare]);
  }
});

test("PP06 a named secret and a sealed input reach the script and appear nowhere else", async ({ request }) => {
  const tag = `pp06-${token()}`;
  const named = `pp06-named-${token()}-value`;
  const sealed = `pp06-sealed-${token()}-value`;
  const renamed = `pp06-renamed-${token()}-value`;
  const pkg = writeFixturePackage(`${tag}-pkg`, {
    kinds: ["POST-PROCESSING"], exitCode: 93,
    scriptOptions: [{ name: "ApiKey", value: "", secret: true }, { name: "Sealed", value: "", secret: true }, { name: "Label", value: "plain" }],
    body: "printf 'api=%s sealed=%s\\n' \"$NZBPO_ApiKey\" \"$NZBPO_Sealed\"\necho \"[WARNING] sealed is $WEAVER_INPUT_SEALED\"\n",
  });
  const restore = await useScripts(request, {});
  let instanceId = "";
  let secretId = "";
  try {
    const secret = (await graphql<{ createSecret: { id: string; name: string } }>(request, CREATE_SECRET_MUTATION, { name: `${tag}-secret`, value: named })).createSecret;
    secretId = secret.id;
    const instance = (await graphql<{ createScriptInstance: ScriptInstance }>(request, CREATE_SCRIPT_INSTANCE_MUTATION, { input: {
      name: `${tag}-job`, script: pkg, trigger: "POST_PROCESSING", categories: [], enabled: true, blocking: true, timeoutSeconds: null,
      inputs: [{ name: "ApiKey", secretId }, { name: "Sealed", value: sealed, secret: true }, { name: "Label", value: "plain" }],
    } })).createScriptInstance;
    instanceId = instance.id;
    const byName = Object.fromEntries(instance.inputs.map(input => [input.name, input]));
    expect(byName.ApiKey).toMatchObject({ value: "", secret: { id: secretId, name: secret.name }, sealed: false });
    expect(byName.Sealed).toMatchObject({ value: "", secret: null, sealed: true });
    expect(byName.Label).toMatchObject({ value: "plain", secret: null, sealed: false });

    const secrets = (await graphql<{ secrets: Array<{ id: string; usedBy: Array<{ id: string }> }> }>(request, SECRETS_QUERY)).secrets;
    expect(secrets.find(candidate => candidate.id === secretId)!.usedBy.map(user => user.id)).toEqual([instanceId]);
    expect((await graphqlErrors(request, DELETE_SECRET_MUTATION, { id: secretId })).length, "a linked secret cannot be deleted").toBeGreaterThan(0);

    const first = await job(request, `${tag}-a`);
    expect(await waitTerminal(request, first)).toBe("COMPLETED");
    const [record] = await waitRecords(pkg, 1, forJob(first));
    expect(record!.env).toMatchObject({
      NZBPO_ApiKey: named, NZBPO_APIKEY: named, SAB_OPTION_APIKEY: named, WEAVER_INPUT_APIKEY: named,
      NZBPO_Sealed: sealed, SAB_OPTION_SEALED: sealed, WEAVER_INPUT_SEALED: sealed, NZBPO_Label: "plain",
    });

    // A changed secret is what the next run gets, under the same link.
    await graphql(request, UPDATE_SECRET_MUTATION, { id: secretId, value: renamed });
    const second = await job(request, `${tag}-b`);
    expect(await waitTerminal(request, second)).toBe("COMPLETED");
    const [secondRecord] = await waitRecords(pkg, 1, forJob(second));
    expect(secondRecord!.env.NZBPO_ApiKey).toBe(renamed);

    const results = [...await waitPostProcessing(request, first, [pkg]), ...await waitPostProcessing(request, second, [pkg])];
    for (const result of results) expect(result.outputTail).toContain("api=[REDACTED] sealed=[REDACTED]");
    const surfaces = JSON.stringify({
      instances: await graphql(request, SCRIPT_INSTANCES_QUERY),
      secrets: await graphql(request, SECRETS_QUERY),
      discovered: await graphql(request, DISCOVERED_SCRIPTS_QUERY),
      results: await Promise.all([first, second].map(jobId => graphql(request, POST_PROCESSING_RESULTS_QUERY, { jobId }))),
      runs: await Promise.all([first, second].map(jobId => scriptRuns(request, { jobId }))),
      outputs: await Promise.all(results.map(result => graphql(request, SCRIPT_RUN_OUTPUT_QUERY, { outputId: result.outputId }))),
      events: await Promise.all([first, second].map(jobId => jobEvents(request, jobId))),
      history: await Promise.all([first, second].map(jobId => history(request, jobId))),
      facade: await Promise.all([first, second].map(jobId => facadeHistory(request, jobId))),
      logs: await serviceLogs(request),
    });
    for (const value of [named, sealed, renamed]) expect(surfaces, "a secret value in an API response or the log").not.toContain(value);
    expect(surfaces).toContain("[REDACTED]");
  } finally {
    if (instanceId) await deleteScriptInstance(request, instanceId);
    if (secretId) await graphql(request, DELETE_SECRET_MUTATION, { id: secretId });
    await restore();
    removeFixtureScripts([pkg]);
  }
});

const CALLBACKS_DIR = "/weaver-data/script-callbacks";

test("PP07 a run's token reads queueItems and historyItems, drives its own run, and nothing else", async ({ request }) => {
  const tag = `pp07-${token()}`;
  const out = `/data/script-callbacks/${tag}`;
  const call = (file: string, document: string) =>
    `wget -qO- --content-on-error --header "Authorization: Bearer $WEAVER_RUN_TOKEN" --header "Content-Type: application/json" --post-data ${
      JSON.stringify(JSON.stringify({ query: document })).replaceAll("$", "\\$")} "$WEAVER_API_URL" > ${out}/${file} 2>&1 || echo "exit $?" >> ${out}/${file}\n`;
  const caller = writeFixtureScript(`${tag}-caller`, {
    kinds: ["POST-PROCESSING"], exitCode: 93,
    body: `mkdir -p ${out}\n${[
      ["queue.json", "query { queueItems { id } }"],
      ["history.json", "query { historyItems { id } }"],
      ["live.json", "query { scriptRun { runId instanceName event kind test jobId } }"],
      ["settings.json", "query { postProcessingSettings { scriptDirectory } }"],
      ["mutation.json", "mutation { deleteSecret(id: \"none\") }"],
      ["parameter.json", "mutation { scriptRun { setParameter(name: \"Chain\", value: \"from-api\") } }"],
      ["log.json", "mutation { scriptRun { log(level: WARNING, text: \"pp07 logged through the api\") } }"],
    ].map(([file, document]) => call(file!, document!)).join("")}`,
  });
  const next = writeFixtureScript(`${tag}-next`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script: caller }, { script: next }] });
  try {
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const [record] = await waitRecords(caller, 1, forJob(jobId));
    const read = (file: string) => {
      const text = fs.readFileSync(path.join(CALLBACKS_DIR, tag, file), "utf8");
      note("observed", `${file}: ${text.slice(0, 400)}`);
      return JSON.parse(text) as { data?: Record<string, unknown> | null; errors?: Array<{ message: string; extensions?: { code?: string } }> };
    };
    const allowed = (file: string, field: string) => {
      const payload = read(file);
      expect(payload.errors ?? [], file).toEqual([]);
      expect(Array.isArray(payload.data?.[field]), file).toBe(true);
    };
    allowed("queue.json", "queueItems");
    allowed("history.json", "historyItems");
    for (const file of ["settings.json", "mutation.json"]) {
      expect(read(file).errors?.map(error => error.extensions?.code), file).toEqual(["NOT_ALLOWED_FOR_SCRIPT_RUN"]);
    }
    const live = read("live.json");
    expect(live.errors ?? []).toEqual([]);
    expect(live.data?.scriptRun).toEqual({
      runId: record!.env.WEAVER_RUN_ID, instanceName: caller, event: "post_processing", kind: "POST_PROCESSING", test: false, jobId,
    });
    for (const file of ["parameter.json", "log.json"]) expect(read(file).errors ?? [], file).toEqual([]);

    const [nextRecord] = await waitRecords(next, 1, forJob(jobId));
    expect(nextRecord!.env, "the parameter set through the API reaches the next script").toMatchObject({ NZBPR_Chain: "from-api" });
    const [result] = await waitPostProcessing(request, jobId, [caller]);
    expect(await scriptOutput(request, result!.outputId!)).toContain("pp07 logged through the api");

    // The run is over, so its token is no credential any more.
    const response = await fetch(new URL(weaverRoute("/graphql"), process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090"), {
      method: "POST",
      headers: { authorization: `Bearer ${record!.env.WEAVER_RUN_TOKEN}`, "content-type": "application/json" },
      body: JSON.stringify({ query: "query { queueItems { id } }" }),
    });
    note("observed", `token after the run: HTTP ${response.status} ${(await response.text()).slice(0, 200)}`);
    expect(response.status).toBe(401);
  } finally {
    await restore();
    removeFixtureScripts([caller, next]);
    fs.rmSync(path.join(CALLBACKS_DIR, tag), { recursive: true, force: true });
  }
});

/** A recorder in Python, for a script the python interpreter runs. */
function pythonRecorder(name: string): string {
  return `import os, random, sys, time
directory = "/data/script-records"
os.makedirs(directory, exist_ok=True)
rid = "%d-%d-%d" % (time.time(), os.getpid(), random.randrange(1 << 32))
lines = ["@SCRIPT=${name}", "@CWD=" + os.getcwd(), "@ARGC=%d" % (len(sys.argv) - 1)]
lines += ["@ARG%d=%s" % (index + 1, value) for index, value in enumerate(sys.argv[1:])]
lines += ["%s=%s" % item for item in os.environ.items()]
temporary = os.path.join(directory, "." + rid)
with open(temporary, "w") as handle:
    handle.write("\\n".join(lines) + "\\n")
os.rename(temporary, os.path.join(directory, rid + ".env"))
print("python script ran")
`;
}

/** A recorder in Go, built under the data directory and run by weaver. */
function goRecorder(name: string): string {
  return `// ### NZBGET POST-PROCESSING SCRIPT ###
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	directory := "/data/script-records"
	_ = os.MkdirAll(directory, 0o755)
	cwd, _ := os.Getwd()
	var record strings.Builder
	fmt.Fprintf(&record, "@SCRIPT=%s\\n@CWD=%s\\n@ARGC=%d\\n", "${name}", cwd, len(os.Args)-1)
	for index, value := range os.Args[1:] {
		fmt.Fprintf(&record, "@ARG%d=%s\\n", index+1, value)
	}
	for _, entry := range os.Environ() {
		record.WriteString(entry + "\\n")
	}
	id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	temporary := filepath.Join(directory, "."+id)
	_ = os.WriteFile(temporary, []byte(record.String()), 0o644)
	_ = os.Rename(temporary, filepath.Join(directory, id+".env"))
	fmt.Println("go script ran")
	os.Exit(93)
}
`;
}

test("PP08 a shebang file without the execute bit, a Python script and a Go script built under the data directory", async ({ request }) => {
  const tag = `pp08-${token()}`;
  const shebang = writeBareScript(`${tag}-shebang`, { body: "echo shebang script ran\n" });
  fs.chmodSync(path.join(SCRIPTS_DIR, shebang), 0o644);
  const python = `${tag}-python.py`;
  fs.writeFileSync(path.join(SCRIPTS_DIR, python), pythonRecorder(python), { mode: 0o644 });
  const go = `${tag}-go.go`;
  fs.writeFileSync(path.join(SCRIPTS_DIR, go), goRecorder(go), { mode: 0o644 });
  const scripts = [shebang, python, go];
  const restore = await useScripts(request, { global: scripts.map(script => ({ script })) },
    { pythonInterpreter: "/usr/bin/python3", goInterpreter: "/usr/local/go/bin/go" });
  try {
    expect((await discovered(request, go)).adapter, "the Go script's header makes it an NZBGet script").toBe("NZBGET");
    const jobId = await job(request, tag);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const item = await history(request, jobId);
    const results = await waitPostProcessing(request, jobId, scripts);
    expect(results.map(result => [result.script, result.status, result.exitCode, result.adapter])).toEqual([
      [shebang, "SUCCEEDED", 0, "SABNZBD"], [python, "SUCCEEDED", 0, "SABNZBD"], [go, "SUCCEEDED", 93, "NZBGET"],
    ]);
    expect(results.map(result => result.outputTail)).toEqual([
      expect.stringContaining("shebang script ran"), expect.stringContaining("python script ran"), expect.stringContaining("go script ran"),
    ]);
    for (const script of scripts) {
      const [record] = await waitRecords(script, 1, forJob(jobId));
      expect(record!.argv, `${script} arguments`).toEqual([item.outputDir, `${item.name}.nzb`, item.name, "", "", "", "0", ""]);
      expect(record!.cwd).toBe(item.outputDir);
      expect(record!.env).toMatchObject({ SAB_CAT: "", SAB_PASSWORD: "", NZBPP_CATEGORY: "", NZBPP_STATUS: "SUCCESS/HEALTH" });
    }
    const [goRecord] = await waitRecords(go, 1, forJob(jobId));
    expect(goRecord!.env).toMatchObject({ GOCACHE: `${goRecord!.env.WEAVER_DATA_DIR}/.weaver-go-cache`, GOPROXY: "off" });
    expect(outsideAllowList(goRecord!.env, ["GOCACHE", "GOPROXY"])).toEqual([]);
    expect(fs.existsSync("/weaver-data/.weaver-go-cache"), "Go's build cache under the data directory").toBe(true);
    expect(fs.existsSync("/weaver-data/.weaver-go-build"), "Go's build output under the data directory").toBe(true);
  } finally {
    await restore();
    removeFixtureScripts(scripts);
  }
});

test("PP09 run order, category scoping, reordering, a disabled job and a fire-and-forget job", async ({ request }) => {
  const tag = `pp09-${token()}`;
  const glob = writeFixtureScript(`${tag}-global`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const cat = writeFixtureScript(`${tag}-category`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const cat2 = writeFixtureScript(`${tag}-category-two`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const off = writeFixtureScript(`${tag}-off`, { kinds: ["POST-PROCESSING"], exitCode: 93 });
  const background = writeFixtureScript(`${tag}-background`, { kinds: ["POST-PROCESSING"], gate: true, exitCode: 94 });
  const categoryId = await addCategory(request, tag);
  const otherId = await addCategory(request, `${tag}-other`);
  const restore = await useScripts(request, {
    global: [{ script: glob }, { script: off, enabled: false }], categories: [{ category: tag, entries: [{ script: cat }, { script: cat2 }] }],
  });
  let backgroundId = "";
  let backgroundJob = 0;
  const ran = async (jobId: number, scripts: string[]) => {
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const results = await waitPostProcessing(request, jobId, scripts);
    return { results: results.map(result => result.script), records: scriptRecords().filter(forJob(jobId)).map(record => record.script) };
  };
  try {
    await setScriptSettings(request, { globalScriptsRun: "ALWAYS" });
    expect(await ran(await job(request, `${tag}-a`, { category: tag }), [glob, cat, cat2]))
      .toEqual({ results: [glob, cat, cat2], records: [glob, cat, cat2] });

    await setScriptSettings(request, { globalScriptsRun: "ONLY_WITHOUT_CATEGORY_SCRIPTS" });
    expect(await ran(await job(request, `${tag}-b`, { category: tag }), [cat, cat2])).toEqual({ results: [cat, cat2], records: [cat, cat2] });
    expect(await ran(await job(request, `${tag}-c`, { category: `${tag}-other` }), [glob])).toEqual({ results: [glob], records: [glob] });

    // Reverse the category's two jobs and move both ahead of the global one:
    // the category's run in their new order, and global jobs still run first.
    const all = (await scriptInstances(request)).filter(instance => instance.trigger === "POST_PROCESSING").sort((a, b) => a.runOrder - b.runOrder);
    const idOf = (script: string) => all.find(instance => instance.script === script)!.id;
    const moved = [idOf(cat2), idOf(cat), idOf(glob)];
    const ids = [...moved, ...all.map(instance => instance.id).filter(id => !moved.includes(id))];
    const reordered = (await graphql<{ reorderScriptInstances: ScriptInstance[] }>(request, REORDER_SCRIPT_INSTANCES_MUTATION,
      { trigger: "POST_PROCESSING", ids })).reorderScriptInstances;
    expect(reordered.filter(instance => instance.trigger === "POST_PROCESSING").map(instance => instance.id)).toEqual(ids);
    await setScriptSettings(request, { globalScriptsRun: "ALWAYS" });
    expect(await ran(await job(request, `${tag}-d`, { category: tag }), [glob, cat2, cat])).toEqual({ results: [glob, cat2, cat], records: [glob, cat2, cat] });
    expect(scriptRecords(off), "a disabled job never runs").toEqual([]);

    // A fire-and-forget job does not hold the download, and the facade leaves it out.
    backgroundId = (await createScriptInstance(request, {
      name: `${tag}-background-job`, script: background, trigger: "POST_PROCESSING", blocking: false, categories: [`${tag}-other`],
    })).id;
    backgroundJob = await job(request, `${tag}-e`, { category: `${tag}-other` });
    await expect.poll(() => gateWaiting(background, backgroundJob), { message: "background run at its gate", timeout: 0 }).toBe(true);
    expect(await waitTerminal(request, backgroundJob), "the job finishes while its background run is held").toBe("COMPLETED");
    expect(gateWaiting(background, backgroundJob)).toBe(true);
    await releaseGate(background, backgroundJob);
    let page: ScriptRunPage | undefined;
    await expect.poll(async () => (page = await scriptRuns(request, { jobId: backgroundJob, script: background })).runs.length,
      { message: "the background run recorded", timeout: 0 }).toBe(1);
    expect(page!.runs[0]).toMatchObject({ background: true, status: "FAILED", exitCode: 94, instanceName: `${tag}-background-job` });
    expect((await history(request, backgroundJob)).state, "a failed background run leaves the job alone").toBe("COMPLETED");
    const facade = await facadeHistory(request, backgroundJob);
    expect(facade.ScriptStatuses.map(status => status.Name)).not.toContain(`${tag}-background-job`);
    expect(facade.ScriptStatus).toBe("SUCCESS");
    backgroundJob = 0;
  } finally {
    if (backgroundJob && gateWaiting(background, backgroundJob)) await releaseGate(background, backgroundJob);
    if (backgroundId) await deleteScriptInstance(request, backgroundId);
    await restore();
    removeFixtureScripts([glob, cat, cat2, off, background]);
    await removeCategory(request, categoryId);
    await removeCategory(request, otherId);
  }
});

test("PP10 the 32 KiB output ring, count-based retention and Runs pagination", async ({ request }) => {
  const tag = `pp10-${token()}`;
  const marker = `pp10-end-${token()}`;
  const loud = writeFixtureScript(`${tag}-loud`, { kinds: ["POST-PROCESSING"], exitCode: 93, body: `${scriptBodies.output(100 * 1024)}echo ${marker}\n` });
  const restore = await useScripts(request, { global: [{ script: loud }] });
  const scripts = [94, 94, 93, 93, 93].map((code, index) =>
    writeFixtureScript(`${tag}-${index}-${code}`, { kinds: ["POST-PROCESSING"], exitCode: code, body: `echo run ${index}\n` }));
  try {
    const loudJob = await job(request, `${tag}-loud`);
    expect(await waitTerminal(request, loudJob)).toBe("COMPLETED");
    const [loudResult] = await waitPostProcessing(request, loudJob, [loud]);
    expect(loudResult!.outputTruncated).toBe(true);
    expect(Buffer.byteLength(loudResult!.outputTail)).toBeLessThanOrEqual(4096);
    expect(loudResult!.outputTail.trimEnd().endsWith(marker)).toBe(true);
    const kept = await scriptOutput(request, loudResult!.outputId!);
    expect(Buffer.byteLength(kept!), "the retained output is the last 32 KiB").toBeLessThanOrEqual(32 * 1024);
    expect(Buffer.byteLength(kept!)).toBeGreaterThan(4096);
    expect(kept!.trimEnd().endsWith(marker)).toBe(true);

    await setScriptLists(request, { global: scripts.map(script => ({ script })) });
    await setScriptSettings(request, { scriptOutputRunsPerJob: 2, scriptOutputFailedRunsPerJob: 1 });
    const jobId = await job(request, `${tag}-runs`);
    expect(await waitTerminal(request, jobId)).toBe("FAILED");
    const [fA, fB, ok1, ok2, ok3] = scripts;
    const retainedOf = (results: ScriptResult[]) => Object.fromEntries(results.map(result => [result.script, result.outputRetained]));
    const want = { [fA!]: false, [fB!]: true, [ok1!]: false, [ok2!]: true, [ok3!]: true };
    let results: ScriptResult[] = [];
    await expect.poll(async () => retainedOf(results = await waitPostProcessing(request, jobId, scripts)),
      { message: "retention keeps the newest two runs and the newest failed one", timeout: 0 }).toEqual(want);
    expect(results.map(result => result.status)).toEqual(["FAILED", "FAILED", "SUCCEEDED", "SUCCEEDED", "SUCCEEDED"]);
    for (const result of results) {
      const output = await scriptOutput(request, result.outputId!);
      if (want[result.script]) expect(output, result.script).toContain("run ");
      else expect(output, result.script).toBeNull();
    }

    const all = await scriptRuns(request, { jobId });
    expect(all.total).toBe(3);
    expect(all.runs.map(run => run.script)).toEqual([ok3, ok2, fB]);
    expect(all.nextBefore).toBeNull();
    expect([...all.statusCounts].sort((a, b) => a.status.localeCompare(b.status))).toEqual([
      { status: "FAILED", count: 1 }, { status: "SUCCEEDED", count: 2 },
    ]);
    const failedOnly = await scriptRuns(request, { jobId, status: "FAILED" });
    expect(failedOnly.runs.map(run => run.script)).toEqual([fB]);
    expect(failedOnly.statusCounts, "the counts leave the status filter out").toEqual(all.statusCounts);

    // Pages follow on from the cursor, never overlap, and end with no cursor.
    const first = await scriptRuns(request, { jobId, limit: 2 });
    expect(first.runs.map(run => run.script)).toEqual([ok3, ok2]);
    expect(first.total).toBe(3);
    expect(first.nextBefore).toBeTruthy();
    const second = await scriptRuns(request, { jobId, limit: 2, before: first.nextBefore });
    expect(second.runs.map(run => run.script)).toEqual([fB]);
    expect(second.total).toBe(3);
    expect(second.nextBefore).toBeNull();
    const finished = [...first.runs, ...second.runs].map(run => run.finishedAtEpochMs);
    expect(finished).toEqual([...finished].sort((a, b) => b - a));
    expect(all.runs[0]).toMatchObject({ jobId, kind: "POST_PROCESSING", event: "post_processing", background: false, outputRetained: true });
  } finally {
    await restore();
    removeFixtureScripts([loud, ...scripts]);
  }
});

test("PP11 a job submitted by URL hands its scripts the URL", async ({ request }) => {
  const tag = `pp11-${token()}`;
  const script = writeBareScript(`${tag}-sab`);
  const fixture = await fixtureState(request);
  const messageId = `${tag}@e2e.invalid`;
  await postProbeArticle(messageId, 4096);
  await setFixtureNzb(request, nzbDocument(tag, [{ messageId, bytes: 4096 }]));
  const url = `http://${fixture.ip}:${fixture.ports.http}/probe.nzb?release=${tag}`;
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const { result, errors } = await submitNzb(request, { url });
    expect(errors).toEqual([]);
    expect(result?.accepted, JSON.stringify(result)).toBe(true);
    const jobId = result!.jobId!;
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    const [record] = await waitRecords(script, 1, forJob(jobId));
    expect.soft(record!.env.SAB_URL, "SAB_URL of a URL submission").toBe(url);
    expect.soft(record!.env.NZBPP_URL, "NZBPP_URL of a URL submission").toBe(url);
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

test("PP12 testing a job runs it with simulated inputs, reports what it asked for, and can be cancelled", async ({ request }) => {
  const tag = `pp12-${token()}`;
  const script = writeFixtureScript(`${tag}-test`, {
    kinds: ["POST-PROCESSING"], exitCode: 93, headerOptions: ["Label=test-label"],
    body: `echo "testing $NZBPO_Label"\n${scriptBodies.directive("NZBPR_Tested", "yes")}`,
  });
  const gated = writeFixtureScript(`${tag}-gated`, { kinds: ["POST-PROCESSING"], gate: true, exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script }, { script: gated }] });
  try {
    const instance = await instanceOf(request, script);
    const started = (await graphql<{ testScriptInstance: ScriptTestRun }>(request, TEST_SCRIPT_INSTANCE_MUTATION, { id: instance.id })).testScriptInstance;
    expect(started).toMatchObject({ instanceId: instance.id, script, kind: "POST_PROCESSING", adapter: "NZBGET" });
    let run: ScriptTestRun | null = null;
    await expect.poll(async () => (run = (await graphql<{ scriptTestRun: ScriptTestRun | null }>(request, SCRIPT_TEST_RUN_QUERY, { id: started.id })).scriptTestRun)?.running,
      { message: "the test run ends", timeout: 0 }).toBe(false);
    note("observed", `test run: ${JSON.stringify(run)}`);
    expect(run!).toMatchObject({ status: "SUCCEEDED", exitCode: 93 });
    expect(run!.log).toContain("testing test-label");
    expect(run!.arguments).toHaveLength(8);
    expect(run!.inputs.length).toBeGreaterThan(0);
    expect(run!.inputs.map(input => input.name)).not.toContain("NZBPO_Label");
    expect(run!.commands.join("\n")).toContain("Tested");
    expect((await scriptRuns(request, { script })).runs, "a test run is not a recorded run").toEqual([]);

    const gatedInstance = await instanceOf(request, gated);
    const held = (await graphql<{ testScriptInstance: ScriptTestRun }>(request, TEST_SCRIPT_INSTANCE_MUTATION, { id: gatedInstance.id })).testScriptInstance;
    await expect.poll(() => waitingGates().some(name => name.startsWith(`${gated}-`)), { message: "the test run at its gate", timeout: 0 }).toBe(true);
    await graphql(request, CANCEL_SCRIPT_TEST_MUTATION, { id: held.id });
    let cancelled: ScriptTestRun | null = null;
    await expect.poll(async () => (cancelled = (await graphql<{ scriptTestRun: ScriptTestRun | null }>(request, SCRIPT_TEST_RUN_QUERY, { id: held.id })).scriptTestRun)?.running,
      { message: "the cancelled test run ends", timeout: 0 }).toBe(false);
    expect(cancelled!.status).toBe("CANCELLED");
  } finally {
    for (const name of waitingGates().filter(candidate => candidate.startsWith(`${gated}-`))) removeStaleGate(gated, name.slice(gated.length + 1));
    await restore();
    removeFixtureScripts([script, gated]);
  }
});

test("PP13 the web app's script documents run against the live schema", async ({ request }) => {
  const settings = (await graphql<{ postProcessingSettings: { scriptDirectory: string } }>(request, POST_PROCESSING_SETTINGS_QUERY)).postProcessingSettings;
  expect(settings.scriptDirectory).toBeTruthy();
  const instances = await graphql<{ scriptInstances: unknown[]; categories: unknown[] }>(request, SCRIPT_INSTANCES_QUERY);
  expect(Array.isArray(instances.scriptInstances)).toBe(true);
  expect(Array.isArray((await graphql<{ secrets: unknown[] }>(request, SECRETS_QUERY)).secrets)).toBe(true);
  const page = await scriptRuns(request, { limit: 25, kind: "POST_PROCESSING" });
  expect(page.runs.length).toBeLessThanOrEqual(25);
  expect(page.total).toBeGreaterThanOrEqual(page.runs.length);
});

const CONCURRENCY_STATE = "pp14-concurrency";

test("PP14 concurrent scripts default to four and a lowered limit applies after a restart @restart", async ({ request }) => {
  const restarted = stage() !== "initial";
  const { tag } = restarted ? loadStageState<{ tag: string }>(CONCURRENCY_STATE) : { tag: `pp14-${token()}` };
  const runTag = `${tag}-${stage()}`;
  const gated = writeFixtureScript(`${tag}-gate`, { kinds: ["POST-PROCESSING"], gate: true, exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script: gated }] });
  const jobs: number[] = [];
  try {
    if (!restarted) {
      expect((await scriptSettings(request)).concurrency).toBe(4);
      jobs.push(await job(request, `${runTag}-a`), await job(request, `${runTag}-b`));
      await expect.poll(() => jobs.every(jobId => gateWaiting(gated, jobId)), { message: "both jobs' scripts run at once", timeout: 0 }).toBe(true);
      for (const jobId of jobs) await releaseGate(gated, jobId);
      for (const jobId of jobs) expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
      return;
    }
    expect((await scriptSettings(request)).concurrency).toBe(1);
    jobs.push(await job(request, `${runTag}-a`), await job(request, `${runTag}-b`));
    let holding = 0;
    await expect.poll(() => (holding = jobs.find(jobId => gateWaiting(gated, jobId)) ?? 0), { message: "one script at its gate", timeout: 0 }).not.toBe(0);
    const waiting = jobs.find(jobId => jobId !== holding)!;
    await expect.poll(async () => metricValue(await metrics(request), "weaver_post_processing_queue_depth") ?? 0,
      { message: "the second job waits for a turn", timeout: 0 }).toBeGreaterThanOrEqual(1);
    expect(scriptRecords(gated).filter(forJob(waiting)), "the second job's script has not started").toEqual([]);
    expect(metricValue(await metrics(request), "weaver_post_processing_active_attempts")).toBe(1);
    await releaseGate(gated, holding);
    await expect.poll(() => gateWaiting(gated, waiting), { message: "the second job's script runs next", timeout: 0 }).toBe(true);
    await releaseGate(gated, waiting);
    for (const jobId of jobs) expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
  } finally {
    for (const jobId of jobs) if (gateWaiting(gated, jobId)) await releaseGate(gated, jobId);
    await restore();
    if (restarted) {
      await setScriptSettings(request, { concurrency: 4 });
      removeFixtureScripts([gated]);
    } else {
      // Applies at the next start-up, which is the restarted stage.
      await setScriptSettings(request, { concurrency: 1 });
      saveStageState(CONCURRENCY_STATE, { tag });
    }
  }
});

test("PP15 rerunning a job's scripts appends runs, and cancelling ends a running script", async ({ request }) => {
  const tag = `pp15-${token()}`;
  const gated = writeFixtureScript(`${tag}-gate`, { kinds: ["POST-PROCESSING"], gate: true, exitCode: 93 });
  const restore = await useScripts(request, { global: [{ script: gated }] });
  let jobId = 0;
  try {
    jobId = await job(request, tag);
    await expect.poll(() => gateWaiting(gated, jobId), { message: "first pass at its gate", timeout: 0 }).toBe(true);
    await releaseGate(gated, jobId);
    expect(await waitTerminal(request, jobId)).toBe("COMPLETED");
    await graphql(request, RERUN_POST_PROCESSING_MUTATION, { jobId });
    await expect.poll(() => gateWaiting(gated, jobId), { message: "rerun at its gate", timeout: 0 }).toBe(true);
    await graphql(request, CANCEL_JOB_POST_PROCESSING_MUTATION, { jobId });
    let page: ScriptRunPage | undefined;
    await expect.poll(async () => (page = await scriptRuns(request, { jobId, script: gated })).total, { message: "two runs recorded", timeout: 0 }).toBe(2);
    expect(page!.runs.map(run => run.status)).toEqual(["CANCELLED", "SUCCEEDED"]);
  } finally {
    if (jobId) removeStaleGate(gated, jobId);
    await restore();
    removeFixtureScripts([gated]);
  }
});
