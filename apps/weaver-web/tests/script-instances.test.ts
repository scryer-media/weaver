import assert from "node:assert/strict";
import test from "node:test";
import {
  booleanInputOn,
  booleanInputValue,
  categoryScoped,
  declaredOption,
  formFromInstance,
  formatTimeout,
  groupInstances,
  inputFromForm,
  inputFromInstance,
  inputNameProblem,
  jobSchedule,
  NO_SCHEDULE,
  MAX_TIMEOUT_SECONDS,
  newInstanceForm,
  reorderedIds,
  scheduleFromHeader,
  triggerTitle,
  unwiredTriggers,
  withScript,
  withLinkedSecret,
  withSecret,
  type DiscoveredScript,
  type ScriptInstance,
} from "../src/next/data/script-instances.ts";
import { englishTranslate as t } from "./english-translate.ts";

function instance(id: string, patch: Partial<ScriptInstance> = {}): ScriptInstance {
  return {
    id,
    name: id,
    script: "notify.sh",
    trigger: "POST_PROCESSING",
    queueEvent: null,
    inputs: [],
    categories: [],
    enabled: true,
    blocking: true,
    timeoutSeconds: null,
    schedule: NO_SCHEDULE,
    runOrder: 0,
    scriptProblem: null,
    headerDrift: false,
    ...patch,
  };
}

function script(name: string, patch: Partial<DiscoveredScript> = {}): DiscoveredScript {
  return {
    name,
    displayName: name,
    adapter: "NZBGET",
    kinds: ["POST_PROCESSING"],
    queueEvents: [],
    taskTimes: [],
    version: null,
    options: [],
    preset: { triggers: [], taskTimes: [], inputs: [] },
    ...patch,
  };
}

const NOTIFY = script("notify.sh", {
  kinds: ["POST_PROCESSING", "QUEUE"],
  queueEvents: ["NZB_ADDED"],
  options: [
    { name: "Url", optionType: "STRING", description: [], select: [], required: true, defaultValue: "http://127.0.0.1" },
    { name: "Token", optionType: "SECRET", description: [], select: [], required: false, defaultValue: null },
    { name: "Verbose", optionType: "BOOLEAN", description: [], select: ["yes", "no"], required: false, defaultValue: "no" },
  ],
  preset: {
    triggers: [
      { trigger: "POST_PROCESSING", queueEvent: null },
      { trigger: "QUEUE", queueEvent: "NZB_ADDED" },
    ],
    taskTimes: [],
    inputs: [
      { name: "Url", value: "http://127.0.0.1", secret: false },
      { name: "Token", value: "from-the-header", secret: true },
      { name: "Verbose", value: "no", secret: false },
    ],
  },
});

test("instances are grouped by trigger, and the queue's by event, each in run order", () => {
  const groups = groupInstances([
    instance("feed", { trigger: "FEED", runOrder: 0 }),
    instance("added-late", { trigger: "QUEUE", queueEvent: "NZB_ADDED", runOrder: 3 }),
    instance("post-second", { runOrder: 2 }),
    instance("deleted", { trigger: "QUEUE", queueEvent: "NZB_DELETED", runOrder: 1 }),
    instance("post-first", { runOrder: 1 }),
    instance("added-early", { trigger: "QUEUE", queueEvent: "NZB_ADDED", runOrder: 0 }),
    instance("nightly", { trigger: "SCHEDULER", runOrder: 0 }),
  ]);
  assert.deepEqual(
    groups.map((group) => [group.id, group.instances.map((entry) => entry.id)]),
    [
      ["POST_PROCESSING", ["post-first", "post-second"]],
      ["QUEUE:NZB_ADDED", ["added-early", "added-late"]],
      ["QUEUE:NZB_DELETED", ["deleted"]],
      ["SCHEDULER", ["nightly"]],
      ["FEED", ["feed"]],
    ],
  );
});

test("a trigger nothing runs on has no group, and an unknown queue event still has one", () => {
  assert.deepEqual(groupInstances([]), []);
  const groups = groupInstances([
    instance("odd", { trigger: "QUEUE", queueEvent: "NZB_ARCHIVED" as never }),
    instance("scan", { trigger: "SCAN" }),
  ]);
  assert.deepEqual(groups.map((group) => group.id), ["QUEUE:NZB_ARCHIVED", "SCAN"]);
});

test("a group's heading names the trigger, and the event for the queue", () => {
  assert.equal(triggerTitle(t, "POST_PROCESSING", null), "Post-processing");
  assert.equal(triggerTitle(t, "QUEUE", "NZB_ADDED"), "Queue · NZB_ADDED");
  assert.equal(triggerTitle(t, "QUEUE", null), "Queue");
  assert.equal(triggerTitle(t, "SCHEDULER", null), "Schedule");
  assert.equal(triggerTitle(t, "FEED", "NZB_ADDED"), "Feed");
});

test("a move trades places with the next row of the same group and sends the whole trigger", () => {
  const instances = [
    instance("added-a", { trigger: "QUEUE", queueEvent: "NZB_ADDED", runOrder: 0 }),
    instance("deleted-a", { trigger: "QUEUE", queueEvent: "NZB_DELETED", runOrder: 1 }),
    instance("added-b", { trigger: "QUEUE", queueEvent: "NZB_ADDED", runOrder: 2 }),
    instance("post-a", { runOrder: 0 }),
    instance("post-b", { runOrder: 1 }),
  ];
  // The row below `added-a` is `added-b`; the instance for another event between them stays put.
  assert.deepEqual(reorderedIds(instances, "added-a", 1), {
    trigger: "QUEUE",
    ids: ["added-b", "deleted-a", "added-a"],
  });
  assert.deepEqual(reorderedIds(instances, "post-b", -1), { trigger: "POST_PROCESSING", ids: ["post-b", "post-a"] });
});

test("a move past the end of its group, or of an instance that is gone, asks for nothing", () => {
  const instances = [instance("post-a", { runOrder: 0 }), instance("post-b", { runOrder: 1 })];
  assert.equal(reorderedIds(instances, "post-a", -1), null);
  assert.equal(reorderedIds(instances, "post-b", 1), null);
  assert.equal(reorderedIds(instances, "missing", 1), null);
});

test("the header's triggers stay unwired until an instance of that script runs on each", () => {
  assert.deepEqual(unwiredTriggers(NOTIFY, []), NOTIFY.preset.triggers);
  assert.deepEqual(unwiredTriggers(NOTIFY, [instance("post")]), [{ trigger: "QUEUE", queueEvent: "NZB_ADDED" }]);
  // Another event, and another script's instance, wire nothing of this header's.
  assert.deepEqual(
    unwiredTriggers(NOTIFY, [
      instance("post"),
      instance("deleted", { trigger: "QUEUE", queueEvent: "NZB_DELETED" }),
      instance("other", { script: "cleanup.py", trigger: "QUEUE", queueEvent: "NZB_ADDED" }),
    ]),
    [{ trigger: "QUEUE", queueEvent: "NZB_ADDED" }],
  );
  assert.deepEqual(
    unwiredTriggers(NOTIFY, [instance("post"), instance("added", { trigger: "QUEUE", queueEvent: "NZB_ADDED" })]),
    [],
  );
});

test("a new instance is filled from the header, and a secret never is", () => {
  const form = newInstanceForm(NOTIFY);
  assert.equal(form.script, "notify.sh");
  assert.equal(form.name, "");
  assert.equal(form.trigger, "POST_PROCESSING");
  assert.equal(form.queueEvent, "NZB_ADDED");
  assert.equal(form.timeoutSeconds, 0);
  assert.deepEqual(form.inputs, [
    { name: "Url", value: "http://127.0.0.1", secret: false, secretId: null, own: false, held: false },
    { name: "Token", value: "", secret: true, secretId: null, own: false, held: false },
    { name: "Verbose", value: "no", secret: false, secretId: null, own: false, held: false },
  ]);
  // A secret slot with no secret chosen is not sent: there is nothing to give the script.
  assert.deepEqual(inputFromForm(form).inputs, [
    { name: "Url", value: "http://127.0.0.1" },
    { name: "Verbose", value: "no" },
  ]);
});

test("a new instance of no script yet has nothing to fill it", () => {
  const form = newInstanceForm(undefined);
  assert.equal(form.script, "");
  assert.equal(form.trigger, "POST_PROCESSING");
  assert.deepEqual(form.inputs, []);
});

test("picking another script starts a new instance again and leaves a saved one as it is", () => {
  const typed = { ...newInstanceForm(undefined), name: "Notify the family", blocking: false, timeoutSeconds: 90 };
  const picked = withScript(typed, NOTIFY, true);
  assert.equal(picked.script, "notify.sh");
  assert.equal(picked.name, "Notify the family");
  assert.equal(picked.blocking, false);
  assert.equal(picked.timeoutSeconds, 90);
  assert.equal(picked.inputs.length, 3);

  const saved = formFromInstance(instance("one", { script: "cleanup.py", inputs: [{ name: "Path", value: "/data", secret: null }] }), undefined);
  const moved = withScript(saved, NOTIFY, false);
  assert.equal(moved.script, "notify.sh");
  assert.deepEqual(moved.inputs, saved.inputs);
});

test("a secret input opens on the secret it links and is sent as that link", () => {
  const saved = instance("one", {
    name: "Notify",
    timeoutSeconds: 120,
    inputs: [
      { name: "Url", value: "http://127.0.0.1/hook", secret: null },
      { name: "Token", value: "", secret: { id: "s1", name: "Notify token" } },
    ],
  });
  const form = formFromInstance(saved, NOTIFY);
  assert.deepEqual(form.inputs, [
    { name: "Url", value: "http://127.0.0.1/hook", secret: false, secretId: null, own: false, held: false },
    { name: "Token", value: "", secret: true, secretId: "s1", own: false, held: false },
  ]);
  assert.equal(form.timeoutSeconds, 120);

  assert.deepEqual(inputFromForm(form).inputs, [
    { name: "Url", value: "http://127.0.0.1/hook" },
    { name: "Token", secretId: "s1" },
  ]);

  const relinked = { ...form, inputs: form.inputs.map((input) => (input.secret ? withLinkedSecret(input, "s2") : input)) };
  assert.deepEqual(inputFromForm(relinked).inputs[1], { name: "Token", secretId: "s2" });
});

test("clearing a secret input's link keeps the slot and leaves it out of what is sent", () => {
  const saved = instance("one", {
    inputs: [
      { name: "Url", value: "http://127.0.0.1/hook", secret: null },
      { name: "Token", value: "", secret: { id: "s1", name: "Notify token" } },
    ],
  });
  const form = formFromInstance(saved, NOTIFY);
  const cleared = { ...form, inputs: form.inputs.map((input) => (input.secret ? withLinkedSecret(input, null) : input)) };
  assert.deepEqual(cleared.inputs[1], { name: "Token", value: "", secret: true, secretId: null, own: false, held: false });
  assert.deepEqual(inputFromForm(cleared).inputs, [{ name: "Url", value: "http://127.0.0.1/hook" }]);
  // Linking it again sends the link once more.
  const relinked = { ...cleared, inputs: cleared.inputs.map((input) => (input.secret ? withLinkedSecret(input, "s1") : input)) };
  assert.deepEqual(inputFromForm(relinked).inputs, inputFromForm(form).inputs);
});

test("ticking or clearing an input's secret box empties it either way", () => {
  const plain = { name: "Password", value: "typed", secret: false, secretId: null, own: false, held: false };
  // A value typed in clear never becomes a secret's value.
  assert.deepEqual(withSecret(plain, true), { name: "Password", value: "", secret: true, secretId: null, own: false, held: false });
  // A header's hint is not a lock: a secret slot can be made plain again, and its link goes.
  const linked = { name: "Password", value: "", secret: true, secretId: "s1", own: false, held: false };
  assert.deepEqual(withSecret(linked, false), { name: "Password", value: "", secret: false, secretId: null, own: false, held: false });
});

test("a job's own secret is never read back: blank keeps it, a typed value replaces it", () => {
  const saved = instance("one", {
    inputs: [
      { name: "Url", value: "http://127.0.0.1/hook", secret: null, sealed: false },
      { name: "ApiKey", value: "", secret: null, sealed: true },
    ],
  });
  const form = formFromInstance(saved, undefined);
  assert.deepEqual(form.inputs[1], { name: "ApiKey", value: "", secret: false, secretId: null, own: true, held: true });
  // Left as it opened, it is sent by name alone, which keeps what is saved.
  assert.deepEqual(inputFromForm(form).inputs, [
    { name: "Url", value: "http://127.0.0.1/hook" },
    { name: "ApiKey", secret: true },
  ]);
  // The switch in a job's row sends the job back as it is, its own secret kept the same way.
  assert.deepEqual(inputFromInstance(saved, { enabled: false }).inputs[1], { name: "ApiKey", secret: true });
  // What is typed over it is what is sent.
  const replaced = { ...form, inputs: form.inputs.map((input) => (input.own ? { ...input, value: "typed" } : input)) };
  assert.deepEqual(inputFromForm(replaced).inputs[1], { name: "ApiKey", value: "typed", secret: true });
  // One added in the editor is sent with what was typed, and one with nothing typed is not sent at all.
  const fresh = { name: "Extra", value: "typed", secret: false, secretId: null, own: true, held: false };
  assert.deepEqual(inputFromForm({ ...form, inputs: [fresh] }).inputs, [{ name: "Extra", value: "typed", secret: true }]);
  assert.deepEqual(inputFromForm({ ...form, inputs: [{ ...fresh, value: "" }] }).inputs, []);
  // Made a linked secret, it is the job's own no longer.
  assert.deepEqual(withLinkedSecret(form.inputs[1], "s1"), {
    name: "ApiKey", value: "", secret: true, secretId: "s1", own: false, held: false,
  });
});

test("a secret the header declares and the instance was never given is offered", () => {
  const form = formFromInstance(instance("one", { inputs: [{ name: "Url", value: "x", secret: null }] }), NOTIFY);
  assert.deepEqual(form.inputs.map((input) => [input.name, input.secret, input.secretId]), [
    ["Url", false, null],
    ["Token", true, null],
  ]);
  // The header's name matches whatever case the instance holds it in.
  const held = formFromInstance(
    instance("two", { inputs: [{ name: "token", value: "", secret: { id: "s1", name: "Token" } }] }),
    NOTIFY,
  );
  assert.deepEqual(held.inputs.map((input) => input.name), ["token"]);
});

test("what is sent leaves out what the trigger has no use for", () => {
  const form = {
    ...newInstanceForm(NOTIFY),
    name: "  Notify  ",
    trigger: "QUEUE" as const,
    queueEvent: "NZB_DELETED" as const,
    categories: ["tv"],
    timeoutSeconds: 45,
  };
  const queue = inputFromForm(form);
  assert.equal(queue.name, "Notify");
  assert.equal(queue.queueEvent, "NZB_DELETED");
  assert.deepEqual(queue.categories, ["tv"]);
  assert.equal(queue.timeoutSeconds, 45);

  const feed = inputFromForm({ ...form, trigger: "FEED", timeoutSeconds: 0 });
  assert.equal(feed.queueEvent, null);
  assert.deepEqual(feed.categories, []);
  assert.equal(feed.timeoutSeconds, null);

  assert.equal(inputFromForm({ ...form, timeoutSeconds: MAX_TIMEOUT_SECONDS * 2 }).timeoutSeconds, MAX_TIMEOUT_SECONDS);
});

test("only a download's triggers can be narrowed to a category", () => {
  assert.equal(categoryScoped("POST_PROCESSING"), true);
  assert.equal(categoryScoped("QUEUE"), true);
  assert.equal(categoryScoped("SCAN"), false);
  assert.equal(categoryScoped("SCHEDULER"), false);
  assert.equal(categoryScoped("FEED"), false);
});

test("an instance sent back from its row keeps its secret links and changes only what was asked", () => {
  const saved = instance("one", {
    name: "Notify",
    trigger: "QUEUE",
    queueEvent: "NZB_ADDED",
    categories: ["tv"],
    timeoutSeconds: 30,
    inputs: [
      { name: "Url", value: "http://127.0.0.1/hook", secret: null },
      { name: "Token", value: "", secret: { id: "s1", name: "Notify token" } },
    ],
  });
  assert.deepEqual(inputFromInstance(saved, { enabled: false }), {
    name: "Notify",
    script: "notify.sh",
    trigger: "QUEUE",
    queueEvent: "NZB_ADDED",
    inputs: [
      { name: "Url", value: "http://127.0.0.1/hook" },
      { name: "Token", secretId: "s1" },
    ],
    categories: ["tv"],
    enabled: false,
    blocking: true,
    timeoutSeconds: 30,
    schedule: NO_SCHEDULE,
  });
});

test("an input's name is checked before it is added", () => {
  const inputs = newInstanceForm(NOTIFY).inputs;
  assert.equal(inputNameProblem("Extra", inputs), null);
  assert.equal(inputNameProblem("  Section.Extra_1-b  ", inputs), null);
  assert.equal(inputNameProblem("", inputs), "next.postProcessing.inputNameInvalid");
  assert.equal(inputNameProblem("1st", inputs), "next.postProcessing.inputNameInvalid");
  assert.equal(inputNameProblem("has space", inputs), "next.postProcessing.inputNameInvalid");
  assert.equal(inputNameProblem("a..b", inputs), "next.postProcessing.inputNameInvalid");
  assert.equal(inputNameProblem("x".repeat(129), inputs), "next.postProcessing.inputNameInvalid");
  assert.equal(inputNameProblem("url", inputs), "next.postProcessing.inputNameTaken");
});

test("an input finds what the header says about it, whatever its case", () => {
  assert.equal(declaredOption(NOTIFY, "verbose")?.optionType, "BOOLEAN");
  assert.equal(declaredOption(NOTIFY, "Extra"), undefined);
  assert.equal(declaredOption(undefined, "Url"), undefined);
});

test("a boolean input is written as the daemon writes one", () => {
  assert.equal(booleanInputValue(true), "yes");
  assert.equal(booleanInputValue(false), "no");
  assert.equal(booleanInputOn("yes"), true);
  assert.equal(booleanInputOn(" True "), true);
  assert.equal(booleanInputOn("1"), true);
  assert.equal(booleanInputOn("no"), false);
  assert.equal(booleanInputOn(""), false);
});

test("a timeout reads exactly as it was set", () => {
  assert.equal(formatTimeout(0), "0s");
  assert.equal(formatTimeout(45), "45s");
  assert.equal(formatTimeout(90), "1m 30s");
  assert.equal(formatTimeout(7200), "2h");
  assert.equal(formatTimeout(3661), "1h 1m 1s");
  assert.equal(formatTimeout(MAX_TIMEOUT_SECONDS), "7d");
});

test("a new job's schedule starts from the times its script's header asks for", () => {
  assert.deepEqual(scheduleFromHeader(undefined), { times: "", days: [], startup: false });
  const timed = script("nightly.py", { preset: { triggers: [], taskTimes: ["04:00", "*:20"], inputs: [] } });
  assert.deepEqual(scheduleFromHeader(timed), { times: "04:00, *:20", days: [], startup: false });
  // A run at startup is its switch, not one of the times.
  const atStartup = script("boot.sh", { preset: { triggers: [], taskTimes: ["*", "06:00"], inputs: [] } });
  assert.deepEqual(scheduleFromHeader(atStartup), { times: "06:00", days: [], startup: true });
});

test("a schedule job's run times are its times, its days and whether it runs at startup", () => {
  const form = { times: "", days: [] as string[], startup: false };
  assert.deepEqual(jobSchedule({ ...form, times: " 4:5 ; *:20,23:59, " }), {
    times: ["4:5", "*:20", "23:59"],
    days: [],
    runAtStartup: false,
  });
  assert.deepEqual(jobSchedule({ times: "03:30", days: ["sat", "sun"], startup: true }), {
    times: ["03:30"],
    days: ["sat", "sun"],
    runAtStartup: true,
  });
  // Startup alone, by its switch or typed among the times, is enough.
  for (const alone of [{ ...form, startup: true }, { ...form, times: "*" }]) {
    assert.deepEqual(jobSchedule(alone), { times: [], days: [], runAtStartup: true });
  }
  assert.deepEqual(jobSchedule({ ...form, times: "*, 01:00" }), { times: ["01:00"], days: [], runAtStartup: true });
  // A job with nothing to run at is named, and so is a time the daemon would refuse.
  assert.deepEqual(jobSchedule(form), { problem: "none" });
  assert.deepEqual(jobSchedule({ ...form, times: " , " }), { problem: "none" });
  for (const bad of ["24:00", "12:60", "noon", "1:2:3", "*:", ":30", "**", "012:00"]) {
    assert.deepEqual(jobSchedule({ ...form, times: `01:00, ${bad}` }), { problem: "invalid", time: bad }, bad);
  }
});

test("a schedule job is saved with its run times and a job of any other trigger with none", () => {
  const nightly = script("nightly.py", {
    preset: { triggers: [{ trigger: "SCHEDULER", queueEvent: null }], taskTimes: ["*", "04:00"], inputs: [] },
  });
  const form = newInstanceForm(nightly);
  assert.equal(form.trigger, "SCHEDULER");
  assert.deepEqual(inputFromForm(form).schedule, { days: [], times: ["04:00"], runAtStartup: true });
  assert.deepEqual(inputFromForm({ ...form, trigger: "SCAN" }).schedule, NO_SCHEDULE);
  // A saved job opens on what is saved in it, not on its header.
  const saved = instance("job", {
    script: "nightly.py",
    trigger: "SCHEDULER",
    schedule: { days: ["mon"], times: ["*:15", "23:00"], runAtStartup: false },
  });
  assert.deepEqual(formFromInstance(saved, nightly).schedule, { times: "*:15, 23:00", days: ["mon"], startup: false });
  assert.deepEqual(inputFromInstance(saved).schedule, saved.schedule);
});
