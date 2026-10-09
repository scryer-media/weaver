import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { nextEn } from "@/next/i18n/en";
import { JobScriptResults } from "@/next/components/JobScriptResults";
import { ScriptConfigurationPanel, ScriptListPanel } from "@/next/pages/settings/panels/PostProcessingPanel";
import { ScriptRunsPanel } from "@/next/pages/settings/panels/ScriptRunsPanel";
import { SecretsPanel } from "@/next/pages/settings/panels/SecretsPanel";
import { SettingsShellProvider, type PanelFlags } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

const MASKED = "[REDACTED]";
const has = (name: string) => location.search.includes(name);
const empty = has("empty");
const same = (left: string, right: string) => left.toLowerCase() === right.toLowerCase();

/* ---------------------------------------------------------------- scripts */

const option = (name: string, optionType: string, extra: Record<string, unknown> = {}) => ({
  name, section: null, optionType, displayName: name, description: [] as string[], select: [] as string[],
  required: false, defaultValue: null as string | null, ...extra,
});
const plain = (name: string, value: string) => ({ name, value, secret: false });
// A header never carries a secret's value.
const secret = (name: string) => ({ name, value: "", secret: true });
const on = (trigger: string, queueEvent: string | null = null) => ({ trigger, queueEvent });
const scripts = [
  { name: "notify.py", displayName: "Notify", adapter: "NZBGET", version: "1.4", kinds: ["POST_PROCESSING", "QUEUE"],
    queueEvents: ["NZB_ADDED", "NZB_DOWNLOADED"], taskTimes: [] as string[], options: [
      option("Label", "STRING", { description: ["Shown in the notification title."], required: true }),
      option("Token", "SECRET", { description: ["The service's access token."] }),
      option("Mode", "STRING", { select: ["quiet", "verbose"], defaultValue: "quiet" }),
      option("Attach", "BOOLEAN", { displayName: "Attach the log", defaultValue: "no" }),
    ],
    preset: { triggers: [on("POST_PROCESSING"), on("QUEUE", "NZB_ADDED"), on("QUEUE", "NZB_DOWNLOADED")], taskTimes: [] as string[],
      inputs: [plain("Label", ""), secret("Token"), plain("Mode", "quiet"), plain("Attach", "no")] } },
  // A script with no header declares nothing an instance could be set up from.
  { name: "cleanup.sh", displayName: "Cleanup", adapter: "SABNZBD", version: null as string | null, kinds: ["POST_PROCESSING"],
    queueEvents: [] as string[], taskTimes: [] as string[], options: [] as ReturnType<typeof option>[],
    preset: { triggers: [] as ReturnType<typeof on>[], taskTimes: [] as string[], inputs: [] as ReturnType<typeof plain>[] } },
  { name: "nightly.py", displayName: "Nightly report", adapter: "NZBGET", version: "0.3", kinds: ["SCHEDULER"],
    queueEvents: [], taskTimes: ["04:00", "*:20"], options: [],
    preset: { triggers: [on("SCHEDULER")], taskTimes: ["04:00", "*:20"], inputs: [] } },
  { name: "intake.py", displayName: "Intake filter", adapter: "NZBGET", version: "2.0", kinds: ["SCAN", "FEED"],
    queueEvents: [], taskTimes: [], options: [],
    preset: { triggers: [on("SCAN"), on("FEED")], taskTimes: [], inputs: [] } },
  { name: "archive.py", displayName: "Archive", adapter: "NZBGET", version: "0.9", kinds: ["POST_PROCESSING", "QUEUE", "SCHEDULER"],
    queueEvents: [], taskTimes: ["03:30"], options: [
      option("Target", "STRING", { description: ["Where a finished download is copied."], defaultValue: "/fixture/archive" }),
      option("Key", "SECRET"),
    ],
    preset: { triggers: [on("POST_PROCESSING"), on("SCHEDULER")], taskTimes: ["03:30"],
      inputs: [plain("Target", "/fixture/archive"), secret("Key")] } },
  { name: "plain.sh", displayName: "plain.sh", adapter: "SABNZBD", version: null, kinds: ["POST_PROCESSING"],
    queueEvents: [], taskTimes: [], options: [],
    preset: { triggers: [], taskTimes: [], inputs: [] } },
];

/* -------------------------------------------------------------- instances */

/** A named secret as the daemon stores it: its value is held here and never sent. */
interface StoredSecret { id: string; name: string; value: string; createdAt: string; updatedAt: string }
/** An instance as the daemon stores it: a secret input holds a link to a secret, never a value. */
interface Stored {
  id: string; name: string; script: string; trigger: string; queueEvent: string | null;
  inputs: { name: string; value: string; secretId: string | null }[]; categories: string[];
  enabled: boolean; blocking: boolean; timeoutSeconds: number | null; runOrder: number;
}
const held = (name: string, value: string) => ({ name, value, secretId: null as string | null });
const linked = (name: string, secretId: string) => ({ name, value: "", secretId: secretId as string | null });
const storedSecret = (id: string, name: string, value: string): StoredSecret => ({
  id, name, value, createdAt: "2026-01-01T00:00:00Z", updatedAt: "2026-01-02T00:00:00Z",
});
const instance = (id: string, name: string, script: string, trigger: string, extra: Partial<Stored> = {}): Stored => ({
  id, name, script, trigger, queueEvent: null, inputs: [], categories: [], enabled: true, blocking: true,
  timeoutSeconds: null, runOrder: 0, ...extra,
});

const state = {
  settings: {
    scriptDirectory: "/fixture/scripts", executionEnabled: true, concurrency: 2,
    globalScriptsRun: has("cascade") ? "ONLY_WITHOUT_CATEGORY_SCRIPTS" : "ALWAYS",
    eventScriptConcurrency: 1, eventScriptTimeoutSeconds: 300, fileDownloadedEventInterval: 0,
    scriptOutputCeilingBytes: 1048576, scriptOutputRunsPerJob: 32, scriptOutputRingBytes: 67108864,
    // A size set through the API need not be a whole number of the unit its field shows.
    scriptOutputRunCapBytes: has("uneven") ? 2097000 : 2097152, terminationGraceSeconds: 10,
    pythonInterpreter: null as string | null, powershellInterpreter: null as string | null,
    batchInterpreter: null as string | null, unacceptableExtensions: ["exe", "scr"],
    strictSecurityRefusesExecution: false,
  },
  scripts: {
    scripts: empty ? [] : scripts,
    problems: empty ? [] : [{ name: "broken.py", message: "option 3 declares an unknown type" }],
  },
  secrets: empty ? [] : [
    storedSecret("s1", "Notify token", "fixture-token-1"),
    // Linked by nothing, so it can be deleted.
    storedSecret("s2", "Spare key", "fixture-token-2"),
  ],
  instances: empty ? [] : [
    instance("1", "Notify", "notify.py", "POST_PROCESSING", { inputs: [
      held("Label", "fixture"), linked("Token", "s1"), held("Mode", "quiet"), held("Attach", "no"),
    ] }),
    instance("2", "Tidy tv", "cleanup.sh", "POST_PROCESSING", {
      categories: ["tv"], enabled: false, blocking: false, timeoutSeconds: 600, runOrder: 1,
    }),
    // Its file has gone from the scripts directory.
    instance("3", "Retired", "retired.py", "POST_PROCESSING", { runOrder: 2 }),
    // Saved before the header gained Mode and Attach, and holding an input the header never declared.
    instance("4", "Announce", "notify.py", "QUEUE", {
      queueEvent: "NZB_ADDED", timeoutSeconds: 90, inputs: [held("Label", "queued"), held("Legacy", "1")],
    }),
    // Its secret was never linked, so it holds no input for it.
    // With `shared`, it links the same secret as Notify, so that secret has two users.
    instance("5", "Log removal", "notify.py", "QUEUE", {
      queueEvent: "NZB_DELETED", runOrder: 1, inputs: [
        held("Label", "removed"), held("Mode", "verbose"), held("Attach", "yes"),
        ...(has("shared") ? [linked("Token", "s1")] : []),
      ],
    }),
    instance("6", "Nightly report", "nightly.py", "SCHEDULER", { timeoutSeconds: 3600 }),
    instance("7", "Feed intake", "intake.py", "FEED", { blocking: false }),
  ],
  categories: [{ id: 1, name: "movies" }, { id: 2, name: "tv" }],
};
let lastInstance = 7;
let lastSecret = 2;
// Every instance mutation the screens sent, oldest first.
const requests: { name: string; variables: Record<string, any> }[] = [];

const script = (name: string) => state.scripts.scripts.find((entry) => entry.name === name);

function drifted(entry: Stored): boolean {
  const declared = script(entry.script)?.preset.inputs ?? [];
  return declared.some((input) => {
    const own = entry.inputs.find((candidate) => same(candidate.name, input.name));
    // A secret never linked has nothing a header could fill in.
    return own ? (own.secretId !== null) !== input.secret : !input.secret;
  }) || entry.inputs.some((own) => !declared.some((input) => same(input.name, own.name)));
}

// A scripts directory the daemon cannot list: no script in it can be told to be there.
const unlistable = has("unlistable") ? "could not read /fixture/scripts: permission denied" : null;

/** An instance as the daemon answers with it. */
function view(entry: Stored) {
  const known = unlistable === null && script(entry.script) !== undefined;
  return {
    ...entry,
    inputs: entry.inputs.map((input) => {
      const linkedTo = state.secrets.find((candidate) => candidate.id === input.secretId);
      return { name: input.name, value: linkedTo ? "" : input.value,
        secret: linkedTo ? { id: linkedTo.id, name: linkedTo.name } : null };
    }),
    scriptProblem: known ? null : (unlistable ?? "the script is no longer in the scripts directory"),
    headerDrift: known && drifted(entry),
  };
}

/** The inputs to store for what was sent: each one is a value or a link to a secret, never both. */
function storedInputs(input: Record<string, any>): Stored["inputs"] | string {
  const sent = input.inputs as { name: string; value?: string; secretId?: string }[];
  for (const entry of sent) {
    if ((entry.value === undefined) === (entry.secretId === undefined)) return "an input takes a value or a secret, not both";
    if (entry.secretId !== undefined && !state.secrets.some((candidate) => candidate.id === entry.secretId)) {
      return "secret does not exist";
    }
  }
  return sent.map((entry) => entry.secretId === undefined ? held(entry.name, entry.value ?? "") : linked(entry.name, entry.secretId));
}

/* ---------------------------------------------------------------- secrets */

/** A secret as the daemon answers with it: never its value. */
function secretView(entry: StoredSecret) {
  const usedBy = state.instances.filter((own) => own.inputs.some((input) => input.secretId === entry.id))
    .map((own) => ({ id: own.id, name: own.name }));
  return { id: entry.id, name: entry.name, createdAt: entry.createdAt, updatedAt: entry.updatedAt, usedBy };
}

function nameProblem(name: string, except: string | null): string | null {
  if (name.trim() === "") return "secret name is invalid";
  const taken = state.secrets.find((entry) => entry.id !== except && same(entry.name, name));
  return taken ? `a secret named '${taken.name}' already exists` : null;
}

function createSecret(variables: Record<string, any>) {
  const problem = nameProblem(variables.name, null);
  if (problem) return refused(problem);
  const created = storedSecret(`s${++lastSecret}`, variables.name, variables.value);
  state.secrets.push(created);
  return { data: { createSecret: secretView(created) } };
}

function updateSecret(variables: Record<string, any>) {
  const target = state.secrets.find((entry) => entry.id === variables.id);
  if (!target) return refused("secret does not exist");
  if (variables.name != null) {
    const problem = nameProblem(variables.name, target.id);
    if (problem) return refused(problem);
    target.name = variables.name;
  }
  if (variables.value != null) target.value = variables.value;
  target.updatedAt = "2026-01-03T00:00:00Z";
  return { data: { updateSecret: secretView(target) } };
}

function deleteSecret(id: string) {
  const at = state.secrets.findIndex((entry) => entry.id === id);
  if (at < 0) return refused("secret does not exist");
  const users = secretView(state.secrets[at]).usedBy;
  if (users.length > 0) {
    return refused(`secret '${state.secrets[at].name}' is used by ${users.map((user) => user.name).join(", ")}`);
  }
  state.secrets.splice(at, 1);
  return { data: { deleteSecret: true } };
}

const refused = (message: string) => ({ data: null, errors: [{ message }] });
// An install that requires sign-in changes or deletes a secret only for a
// session whose password was checked lately; adding one is never held back.
const PASSWORD = "fixture-password";
let passwordVerified = !has("signedin");
const REAUTH = {
  data: null,
  errors: [{ message: "recent password verification required", extensions: { code: "REAUTH_REQUIRED" } }],
};
const inTrigger = (trigger: string) => state.instances.filter((entry) => entry.trigger === trigger).length;

function saveInstance(variables: Record<string, any>): Stored | string {
  const input = variables.input;
  if (input.inputs.some((entry: { value?: string }) => (entry.value ?? "").length > 64)) {
    return "input value is invalid";
  }
  const previous = variables.id === undefined ? undefined : state.instances.find((entry) => entry.id === variables.id);
  if (variables.id !== undefined && !previous) {
    return "script instance does not exist";
  }
  const inputs = storedInputs(input);
  if (typeof inputs === "string") {
    return inputs;
  }
  const fields = {
    name: input.name || input.script, script: input.script, trigger: input.trigger, queueEvent: input.queueEvent,
    inputs, categories: input.categories, enabled: input.enabled,
    blocking: input.blocking, timeoutSeconds: input.timeoutSeconds,
  };
  if (previous) {
    return Object.assign(previous, fields);
  }
  const created: Stored = { id: String(++lastInstance), ...fields, runOrder: inTrigger(input.trigger) };
  state.instances.push(created);
  return created;
}

/* -------------------------------------------------------------- test runs */

interface TestRun {
  id: string; instanceId: string; instanceName: string; script: string; event: string; kind: string; adapter: string;
  startedAtEpochMs: number; timeoutSeconds: number; running: boolean; status: string | null; exitCode: number | null;
  durationMs: number | null; errorMessage: string | null; log: string; logTruncated: boolean;
  inputs: { name: string; value: string }[]; arguments: string[]; commands: string[]; commandsTruncated: boolean;
}
// Every test the daemon was asked to start, oldest first. A run does nothing by
// itself: it prints, ends or is forgotten only once the page under test says so,
// and the screen sees that the next time it reads the run.
const tests: { run: TestRun; printed: boolean; finished: boolean; forgotten: boolean; cancelled: boolean }[] = [];

function startTest(id: string) {
  const target = state.instances.find((entry) => entry.id === id);
  if (!target) {
    return refused("the script instance does not exist");
  }
  const number = tests.length + 1;
  const run: TestRun = {
    id: `test-${number}`, instanceId: target.id, instanceName: target.name, script: target.script,
    event: target.trigger === "QUEUE" ? `queue:${target.queueEvent}` : target.trigger.toLowerCase(), kind: target.trigger,
    adapter: script(target.script)?.adapter ?? "NZBGET", startedAtEpochMs: 1767225600000,
    timeoutSeconds: target.timeoutSeconds ?? state.settings.eventScriptTimeoutSeconds,
    running: true, status: null, exitCode: null, durationMs: null, errorMessage: null, log: "", logTruncated: false,
    inputs: [
      { name: "NZBPP_CATEGORY", value: "tv" },
      { name: "NZBPP_DIRECTORY", value: `/fixture/scratch/test-${number}` },
      { name: "NZBPP_NZBNAME", value: "weaver-test-download" },
    ],
    arguments: [`/fixture/scratch/test-${number}`, "weaver-test-download.nzb"], commands: [], commandsTruncated: false,
  };
  tests.push({ run, printed: false, finished: false, forgotten: false, cancelled: false });
  return { data: { testScriptInstance: structuredClone(run) } };
}

/** A run as it stands when it is read again. */
function readTest(id: string) {
  const held = tests.find((entry) => entry.run.id === id);
  if (!held || held.forgotten) {
    return { data: { scriptTestRun: null } };
  }
  const { run } = held;
  if (run.running && held.printed && run.log === "") {
    run.log = `token=${MASKED}\nresolved 1 recipient\n`;
  }
  if (run.running && held.finished) {
    Object.assign(run, {
      running: false, status: "SUCCEEDED", exitCode: 0, durationMs: 1500,
      log: `${run.log}notified 1 recipient\n`, commands: ["[NZB] FINALDIR=/fixture/final", "[NZB] MARK=GOOD"],
    });
  }
  return { data: { scriptTestRun: structuredClone(run) } };
}

function cancelTest(id: string) {
  const held = tests.find((entry) => entry.run.id === id);
  if (!held?.run.running) {
    return { data: { cancelScriptTest: false } };
  }
  held.cancelled = true;
  Object.assign(held.run, { running: false, status: "CANCELLED", durationMs: 800 });
  return { data: { cancelScriptTest: true } };
}

/* ------------------------------------------------------------------- runs */

const run = (script: string, event: string, status: string, outputTail: string, extra: Record<string, unknown> = {}) => ({
  script, event, adapter: "NZBGET", status, exitCode: 0, durationMs: 120, outputTail, outputId: null,
  outputRetained: false, outputTruncated: false, errorMessage: null, finishedAtEpochMs: 1767225600000,
  background: false, instanceId: null as string | null, instanceName: null as string | null, ...extra,
});
const results = empty ? [] : [
  run("notify.py", "queue:NZB_ADDED", "SUCCEEDED", "queued fixture job", { instanceId: "4", instanceName: "Announce" }),
  run("notify.py", "post_processing", "SUCCEEDED", `token=${MASKED}\nnotified 1 recipient`,
    { outputId: "run-1", outputRetained: true, instanceId: "1", instanceName: "Notify" }),
  run("cleanup.sh", "post_processing", "WARNING", "removed 2 samples",
    { exitCode: 3, outputTruncated: true, background: true, instanceId: "2", instanceName: "Tidy tv" }),
  // A run whose instance has since been deleted is known by its script alone.
  run("retired.py", "post_processing", "FAILED", "", { errorMessage: "script is no longer in the scripts directory" }),
];
// Every run the daemon recorded, numbered oldest to newest and held newest first.
const recorded = (id: number, script: string, event: string, kind: string, extra: Record<string, unknown> = {}) => ({
  id: `run-${id}`, jobId: null as number | null, jobName: null as string | null, script, event, kind, background: false,
  adapter: "NZBGET", status: "SUCCEEDED", exitCode: 0 as number | null, durationMs: 240, outputTail: `${script} finished`,
  outputTruncated: false, outputRetained: true, errorMessage: null as string | null,
  finishedAtEpochMs: 1767225600000 + id * 60000, instanceId: null as string | null, instanceName: null as string | null, ...extra,
});
const runs = empty ? [] : [
  recorded(60, "nightly.py", "scheduler:4", "SCHEDULER", { durationMs: 95000, instanceId: "6", instanceName: "Nightly report" }),
  // An instance named after its file reads as the file alone.
  recorded(59, "intake.py", "scan", "SCAN", { durationMs: 1500, outputRetained: false, instanceId: "9", instanceName: "intake.py" }),
  recorded(58, "notify.py", "post_processing", "POST_PROCESSING", { jobId: 7, jobName: "fixture.release.one",
    outputTail: `token=${MASKED}\nnotified 2 recipients`, instanceId: "1", instanceName: "Notify" }),
  recorded(57, "retired.py", "post_processing", "POST_PROCESSING", { jobId: 7, jobName: "fixture.release.one",
    status: "FAILED", exitCode: null, outputTail: "", outputRetained: false,
    errorMessage: "script is no longer in the scripts directory" }),
  recorded(56, "notify.py", "queue:NZB_ADDED", "QUEUE", { jobId: 8, jobName: "fixture.release.two",
    instanceId: "4", instanceName: "Announce" }),
  recorded(55, "cleanup.sh", "post_processing", "POST_PROCESSING", { jobId: 9, background: true, adapter: "SABNZBD",
    status: "WARNING", exitCode: 3, outputTruncated: true, instanceId: "2", instanceName: "Tidy tv" }),
  recorded(54, "intake.py", "feed:2", "FEED", { instanceId: "7", instanceName: "Feed intake" }),
  // Enough older runs that the list needs a second page.
  ...Array.from({ length: 53 }, (_, index) => recorded(53 - index, "sweep.sh", "queue:FILE_DOWNLOADED", "QUEUE",
    { jobId: 153 - index, jobName: `fixture.batch.${53 - index}` })),
];
const retainedOutput: Record<string, string> = {
  "run-1": `token=${MASKED}\nresolved 1 recipient\nnotified 1 recipient`,
  "run-58": `token=${MASKED}\nresolved 2 recipients\nnotified 2 recipients`,
  // Plain lines around one the log parser reads as a record, and one long enough to wrap.
  "run-60": [
    "starting nightly report",
    "2026-01-01T03:00:00.125Z INFO nightly::report: summary written rows=42 path=/fixture/reports/nightly.txt",
    `columns ${"wide ".repeat(60)}end`,
    "warning: 1 feed was unreachable",
    "done",
  ].join("\n") + "\n",
};
// What each request for a page of runs asked for, oldest first.
const scriptRunRequests: Record<string, unknown>[] = [];
// The run whose full output each request asked for, oldest first.
const outputRequests: string[] = [];
function scriptRunPage(variables: Record<string, any>) {
  const ofKind = runs.filter((entry) => !variables.kind || entry.kind === variables.kind);
  const matching = ofKind.filter((entry) => !variables.status || entry.status === variables.status);
  const start = variables.before ? matching.findIndex((entry) => entry.id === variables.before) + 1 : 0;
  const page = matching.slice(start, start + (variables.limit ?? 50));
  // How the runs of the kind ended, whichever status was asked for.
  const ended = new Map<string, number>();
  for (const entry of ofKind) ended.set(entry.status, (ended.get(entry.status) ?? 0) + 1);
  return {
    runs: page,
    nextBefore: start + page.length < matching.length ? page[page.length - 1].id : null,
    total: matching.length,
    statusCounts: [...ended].map(([status, count]) => ({ status, count })),
  };
}

/* ---------------------------------------------------------------- graphql */

function graphql(name: string, variables: Record<string, any>) {
  let mutation = {};
  if (name === "SetPostProcessingSettings") {
    Object.assign(state.settings, variables.input);
    mutation = { setPostProcessingSettings: state.settings };
  } else if (name === "SetPostProcessingScriptDirectory") {
    // A name in another directory need not be the script that was wired up, so every instance is turned off.
    state.settings.scriptDirectory = variables.directory;
    for (const entry of state.instances) entry.enabled = false;
    mutation = { setPostProcessingScriptDirectory: state.settings };
  } else if (name === "DiscoveredScripts" && unlistable !== null) {
    return refused(unlistable);
  } else if (name === "CreateScriptInstance" || name === "UpdateScriptInstance") {
    requests.push({ name, variables });
    const saved = saveInstance(variables);
    if (typeof saved === "string") return refused(saved);
    mutation = name === "CreateScriptInstance" ? { createScriptInstance: view(saved) } : { updateScriptInstance: view(saved) };
  } else if (has("stale") && (name === "DeleteScriptInstance" || name === "ReapplyScriptHeader")) {
    // The instance went away behind the screen's back.
    requests.push({ name, variables });
    return refused("script instance does not exist");
  } else if (name === "DeleteScriptInstance") {
    requests.push({ name, variables });
    const at = state.instances.findIndex((entry) => entry.id === variables.id);
    if (at >= 0) state.instances.splice(at, 1);
    mutation = { deleteScriptInstance: at >= 0 };
  } else if (name === "ReorderScriptInstances") {
    requests.push({ name, variables });
    const moved = (variables.ids as string[]).map((id) =>
      state.instances.find((entry) => entry.id === id && entry.trigger === variables.trigger));
    const stray = moved.indexOf(undefined);
    if (stray >= 0) return refused(`'${variables.ids[stray]}' is not an instance of that trigger`);
    moved.forEach((entry, index) => { entry!.runOrder = index; });
    mutation = { reorderScriptInstances: state.instances.map(view) };
  } else if (name === "SetUpScriptFromHeader") {
    requests.push({ name, variables });
    const from = script(variables.script);
    if (!from || from.preset.triggers.length === 0) return refused("the script's header declares nothing to set up");
    const added: Stored[] = [];
    for (const trigger of from.preset.triggers) {
      const wired = state.instances.some((entry) =>
        entry.script === from.name && entry.trigger === trigger.trigger && entry.queueEvent === trigger.queueEvent);
      if (wired) continue;
      const created = instance(String(++lastInstance), from.name, from.name, trigger.trigger, {
        queueEvent: trigger.queueEvent, runOrder: inTrigger(trigger.trigger),
        inputs: from.preset.inputs.filter((input) => !input.secret).map((input) => held(input.name, input.value)),
      });
      state.instances.push(created);
      added.push(created);
    }
    mutation = { setUpScriptFromHeader: added.map(view) };
  } else if (name === "ReapplyScriptHeader") {
    requests.push({ name, variables });
    const target = state.instances.find((entry) => entry.id === variables.id);
    const from = target ? script(target.script) : undefined;
    if (!target || !from) return refused("script instance does not exist");
    target.inputs = from.preset.inputs.flatMap((declared) => {
      // A link to a secret is kept as it is.
      const own = target.inputs.find((input) => same(input.name, declared.name) && (input.secretId !== null) === declared.secret);
      if (own) return [{ ...own, name: declared.name }];
      return declared.secret ? [] : [held(declared.name, declared.value)];
    });
    mutation = { reapplyScriptHeader: view(target) };
  } else if (name === "CreateSecret") {
    requests.push({ name, variables });
    return createSecret(variables);
  } else if (name === "UpdateSecret") {
    requests.push({ name, variables });
    return passwordVerified ? updateSecret(variables) : REAUTH;
  } else if (name === "DeleteSecret") {
    requests.push({ name, variables });
    return passwordVerified ? deleteSecret(variables.id) : REAUTH;
  } else if (name === "TestScriptInstance") {
    return startTest(variables.id);
  } else if (name === "ScriptTestRun") {
    return readTest(variables.id);
  } else if (name === "CancelScriptTest") {
    return cancelTest(variables.id);
  } else if (name === "ScriptRuns") {
    scriptRunRequests.push(variables);
  } else if (name === "ScriptRunOutput" || name === "ScriptOutput") {
    outputRequests.push(variables.outputId);
  }
  return { data: structuredClone({ postProcessingSettings: state.settings, categories: state.categories,
    scriptInstances: state.instances.map(view), discoveredScripts: state.scripts, secrets: state.secrets.map(secretView),
    postProcessingResults: results, scriptRuns: scriptRunPage(variables), scriptRunRequests,
    scriptOutput: retainedOutput[variables.outputId] ?? null,
    browseDirectories: { currentPath: variables.path ?? state.settings.scriptDirectory, parentPath: "/fixture", entries: [] },
    ...mutation }) };
}
// What the daemon holds, secrets included, and what it was asked to do, for the
// test to read and for it to move a test run along.
Object.assign(window, { scriptsFixture: { instances: state.instances, secrets: state.secrets, requests, tests, outputRequests } });
const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  const url = new URL(String(request), document.baseURI);
  if (url.pathname.endsWith("/api/auth/verify")) {
    passwordVerified = JSON.parse(String(init?.body)).password === PASSWORD;
    return passwordVerified
      ? Response.json({ authenticated: true })
      : Response.json({ error: "invalid credentials" }, { status: 401 });
  }
  if (url.pathname !== "/graphql") return originalFetch(request, init);
  const body = JSON.parse(String(init?.body));
  return Response.json(graphql(body.operationName, body.variables));
};
const dictionary = { ...en, ...nextEn };
// The screen under test: the secrets, every script run, a job's script runs, the script list, or the configuration.
const screen = has("secrets") ? <SecretsPanel />
  : has("runs") ? <ScriptRunsPanel />
  : has("job") ? <JobScriptResults jobId={1} />
  : has("scripts") ? <ScriptListPanel /> : <ScriptConfigurationPanel />;
// The shell's status bar, reduced to the line a panel publishes into it.
const status = document.getElementById("status")!;
const actions = { current: null as { save: () => void; revert: () => void } | null };
const save = document.getElementById("save") as HTMLButtonElement;
save.addEventListener("click", () => actions.current?.save());
const shell = {
  search: new URLSearchParams(location.search).get("search") ?? "", actionsRef: actions,
  setFlags: (flags: PanelFlags) => { status.textContent = flags.status ?? ""; save.disabled = !flags.dirty || flags.busy; },
  controlsHost: document.getElementById("controls"),
};
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key), uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto flex max-w-[1400px] flex-col p-8">{screen}</main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);
