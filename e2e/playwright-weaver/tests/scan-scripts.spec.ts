import zlib from "node:zlib";
import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, postProbeArticle, test } from "./helpers";
import { fixtureState, setFixtureNzb } from "./support/proxy-fixture";
import { type ScriptRecord, removeFixtureScripts, scriptBodies, scriptRecords, writeFixtureScript } from "./support/script-fixtures";
import {
  type SubmissionResult, jobLessResults, nzbDocument, nzbgetRpc, submitNzb, uploadNzb, useScripts, waitJobLessResults, withControlKey,
} from "./support/script-settings";

/**
 * SCAN scripts (SC01-SC14). Each test lists one SCAN
 * script that prints NZBGet directives, submits an NZB and reads what the
 * queue item became. Downloads are paused for the whole spec so the items
 * stay queued; every job a test creates is cancelled after it.
 */

const token = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
const created: number[] = [];

function note(type: string, description: string): void {
  test.info().annotations.push({ type, description });
}

test.beforeEach(async ({ request }) => {
  created.length = 0;
  await graphql(request, "mutation { pauseAll }");
});

test.afterEach(async ({ request }) => {
  for (const id of created) await graphql(request, "mutation($id: Int!) { cancelJob(id: $id) }", { id });
  await graphql(request, "mutation { resumeAll }");
});

const base64 = (text: string) => Buffer.from(text).toString("base64");
const document = (name: string) => nzbDocument(name, [{ messageId: `${name}@e2e.invalid`, bytes: 4096 }]);
const attribute = (result: SubmissionResult | null, key: string) => result?.item?.attributes.find(entry => entry.key === key)?.value;

/** Submit `name` through GraphQL with no scan script involved. */
async function plain(request: APIRequestContext, name: string): Promise<number> {
  const { result } = await submitNzb(request, { nzbBase64: base64(document(name)), filename: `${name}.nzb` });
  expect(result?.accepted, `submit ${name}`).toBe(true);
  created.push(result!.jobId!);
  return result!.jobId!;
}

type Scanned = { script: string; name: string; result: SubmissionResult | null; errors: string[] };

/** List one SCAN script with `body`, submit an NZB through it, then unlist it. */
async function scanned(request: APIRequestContext, id: string, body: string, input: Record<string, unknown> = {}): Promise<Scanned> {
  const name = `${id.toLowerCase()}-${token()}`;
  const script = writeFixtureScript(`${name}-scan`, { kinds: ["SCAN"], body });
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const { result, errors } = await submitNzb(request, { nzbBase64: base64(document(name)), filename: `${name}.nzb`, ...input });
    if (result?.jobId) created.push(result.jobId);
    expect(scriptRecords(script), `${script} ran once`).toHaveLength(1);
    return { script, name, result, errors };
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
}

const directive = scriptBodies.directive;

async function lastOutput(script: string): Promise<string> {
  return (await waitJobLessResults(script, 1)).at(-1)!.outputTail;
}

async function queueItem(request: APIRequestContext, id: number) {
  return (await graphql<{ queueItem: { state: string; duplicateSummary: { semantic: { normalizedKey: string; score: number } | null } | null } | null }>(request,
    "query($id: Int!) { queueItem(id: $id) { state duplicateSummary { semantic { normalizedKey score } } } }", { id })).queueItem;
}

test("SC01 a Parameter directive becomes a job attribute; an over-long name is refused", async ({ request }) => {
  note("discrepancy", "An invalid parameter name is reported as \"Invalid command\" at ERROR level, not WARNING.");
  const longName = "p".repeat(300);
  const { script, result } = await scanned(request, "SC01", `${directive("NZBPR_sc01key", "sc01-value")}${directive(`NZBPR_${longName}`, "ignored")}`);
  expect(result?.accepted).toBe(true);
  expect(attribute(result, "sc01key")).toBe("sc01-value");
  expect(attribute(result, longName)).toBeUndefined();
  expect(await lastOutput(script)).toContain("Invalid command");
});

for (const [id, key, value] of [
  ["SC02", "DIRECTORY", "/data/sc02-directory"],
  ["SC03", "FINALDIR", "/data/sc03-final"],
  ["SC04", "MARK", "BAD"],
] as const) {
  test(`${id} a ${key} directive is not allowed for scan and the submission still goes ahead`, async ({ request }) => {
    if (id === "SC04") note("discrepancy", "MARK=BAD from a SCAN script does not reject the submission: SCAN refuses the directive (\"Command MARK is not allowed for scan\", logged at WARNING) and the NZB is queued.");
    const { script, result } = await scanned(request, id, directive(key, value));
    expect(result?.accepted, JSON.stringify(result)).toBe(true);
    expect(await lastOutput(script)).toContain(`Command ${key} is not allowed for scan`);
  });
}

test("SC05 a Name directive renames the job", async ({ request }) => {
  const renamed = `sc05-renamed-${token()}`;
  const { result } = await scanned(request, "SC05", directive("NZBNAME", renamed));
  expect(result?.accepted).toBe(true);
  expect(result!.item!.name).toContain(renamed);
});

test("SC06 a Category directive moves the job to that category", async ({ request }) => {
  const category = `sc06-${token()}`;
  const categoryId = (await graphql<{ addCategory: { id: number } }>(request,
    "mutation($input: CategoryInput!) { addCategory(input: $input) { id } }", { input: { name: category } })).addCategory.id;
  try {
    const { result } = await scanned(request, "SC06", directive("CATEGORY", category));
    expect(result?.accepted).toBe(true);
    expect(result!.item!.category).toBe(category);
  } finally {
    await graphql(request, "mutation($id: Int!) { removeCategory(id: $id) { id } }", { id: categoryId });
  }
});

test("SC07 a Priority directive sets the job priority", async ({ request }) => {
  const high = await scanned(request, "SC07", directive("PRIORITY", "100"));
  expect(attribute(high.result, "priority")).toBe("HIGH");
  const low = await scanned(request, "SC07", directive("PRIORITY", "-100"));
  expect(attribute(low.result, "priority")).toBe("LOW");
});

test("SC08 a Top directive queues the job ahead of earlier ones", async ({ request }) => {
  const filler = await plain(request, `sc08-filler-${token()}`);
  const { result } = await scanned(request, "SC08", directive("TOP", "1"));
  expect(result?.accepted).toBe(true);
  const order = (await graphql<{ queueItems: Array<{ id: number }> }>(request, "query { queueItems(first: 1000) { id } }")).queueItems.map(item => item.id);
  expect(order).toContain(filler);
  expect(order.indexOf(result!.jobId!)).toBeLessThan(order.indexOf(filler));
});

test("SC09 a Paused directive adds the job paused", async ({ request }) => {
  const control = await plain(request, `sc09-control-${token()}`);
  const { result } = await scanned(request, "SC09", directive("PAUSED", "1"));
  expect(result?.accepted).toBe(true);
  expect((await queueItem(request, result!.jobId!))?.state).toBe("PAUSED");
  expect((await queueItem(request, control))?.state).not.toBe("PAUSED");
});

test("SC10 a DupeKey directive sets the duplicate key", async ({ request }) => {
  const key = `sc10-key-${token()}`;
  const { result } = await scanned(request, "SC10", directive("DUPEKEY", key));
  expect(attribute(result, "nzbget.dupe_key")).toBe(key);
  const semantic = (await queueItem(request, result!.jobId!))?.duplicateSummary?.semantic;
  expect(semantic?.normalizedKey, "semantic duplicate candidate from the key").toBeTruthy();
});

test("SC11 a DupeScore directive sets the duplicate score", async ({ request }) => {
  const { result } = await scanned(request, "SC11", `${directive("DUPEKEY", `sc11-key-${token()}`)}${directive("DUPESCORE", "42")}`);
  expect(attribute(result, "nzbget.dupe_score")).toBe("42");
  expect((await queueItem(request, result!.jobId!))?.duplicateSummary?.semantic?.score).toBe(42);
});

test("SC12 a DupeMode directive sets the duplicate mode (Score, All, Force)", async ({ request }) => {
  for (const mode of ["Score", "All", "Force"]) {
    const { result } = await scanned(request, "SC12", directive("DUPEMODE", mode));
    expect(result?.accepted, mode).toBe(true);
    // SCORE is the default, so it changes nothing and leaves no attribute.
    expect((attribute(result, "nzbget.dupe_mode") ?? "SCORE").toUpperCase(), mode).toBe(mode.toUpperCase());
  }
});

test("SC13 an NZB over the scan input limit is refused before the scan script runs", async ({ request }) => {
  const name = `sc13-${token()}`;
  const script = writeFixtureScript(`${name}-scan`, { kinds: ["SCAN"] });
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    // Within the 256 MiB upload and 512 MiB decompressed limits, over the 256 MiB scan input limit.
    const total = 257 * 1024 * 1024;
    const head = Buffer.from(document(name).replace("</nzb>", "<!-- "));
    const tail = Buffer.from(" --></nzb>\n");
    const nzb = Buffer.concat([head, Buffer.alloc(total - head.length - tail.length, 0x20), tail]);
    const upload = await uploadNzb(request, `${name}.nzb.gz`, zlib.gzipSync(nzb), "application/gzip");
    if (upload.result?.jobId) created.push(upload.result.jobId);
    expect(upload.result?.accepted ?? false, JSON.stringify(upload)).toBe(false);
    expect(JSON.stringify(upload)).toContain("NZB exceeds scan input limit");
    expect(scriptRecords(script)).toEqual([]);
    expect(await jobLessResults(script)).toEqual([]);
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});

test("SC14 a scan script sees add-to-top and the source URL of an NZBGet append", async ({ request }) => {
  note("coverage", "weaver.submission.add_to_top and weaver.submission.source_url reach the script as NZBNP_TOP and NZBNP_URL.");
  const name = `sc14-${token()}`;
  const script = writeFixtureScript(`${name}-scan`, { kinds: ["SCAN"] });
  const fixture = await fixtureState(request);
  const messageId = `${name}@e2e.invalid`;
  await postProbeArticle(messageId, 4096);
  await setFixtureNzb(request, nzbDocument(name, [{ messageId, bytes: 4096 }]));
  const url = `http://${fixture.ip}:${fixture.ports.http}/probe.nzb?release=${name}`;
  const restore = await useScripts(request, { global: [{ script }] });
  try {
    const jobId = await withControlKey(request, key => nzbgetRpc(request, key, "append",
      [`${name}.nzb`, url, "", 0, true, false, "", 0, "SCORE", []])) as number;
    expect(jobId).toBeGreaterThan(0);
    created.push(jobId);
    let records: ScriptRecord[] = [];
    await expect.poll(() => (records = scriptRecords(script)).length, { message: `${script} ran`, timeout: 0 }).toBeGreaterThanOrEqual(1);
    expect(records[0]!.env).toMatchObject({ NZBNP_TOP: "1", NZBNP_URL: url });
  } finally {
    await restore();
    removeFixtureScripts([script]);
  }
});
