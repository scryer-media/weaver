import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, nntpBodyMetrics, postProbeArticle, resetNntpMetrics, submitProbeNzb, test } from "./helpers";
import { setting } from "./support/datastore";
import { readClock, setClock } from "./support/e2e-clock";
import {
  SYSTEM_EGRESS_ID, graphqlErrors, setSystemEgressQuota, stage, systemEgressQuotaUsage,
} from "./support/network-flow";
import { loadStageState, nzbgetRpc, saveStageState, withControlKey } from "./support/script-settings";
import {
  DAY_MS, Rules, SCHEDULE_FIELDS, type RuleInput, type ScheduleRow, addCountedFeed, at, cancelledJob, clockWhileStoppedPending,
  completedJob, deleteFeed, egressSpeed, feedCount, freshDay, generalSettings, historyItem, hhmm, note, postPaused,
  profileState, queueState, restoreServer, resumeAll, schedules, serverRow, setClockWhileStopped, token, updateSettings,
  waitPaused, waitProfile, waitQueue, watchFolderPaused, withRules, witnessTick,
} from "./support/schedules";

/**
 * Schedule tracks, one action at a time (S-, M-, C-, O-, R-, P-, H- tests).
 *
 * Every hold action is driven through the same matrix: it fires at its time
 * and the opposite rule releases it, a disabled rule never fires, a day filter
 * holds it to its weekday, a rule with several times fires at each, and
 * removing the last rule leaves the state in place. The track tests then show
 * that tracks never end each other, that the model resolves same-minute and
 * per-target overlaps, that quota tracks apply in order and that the
 * effective rate is the lowest of the global, egress and provider limits.
 *
 * Every negative is asserted only after `witnessTick`, which proves the
 * evaluator ran a whole tick that saw the clock already moved.
 *
 * The H tests span a Weaver restart while the harness moves the clock, so a
 * rule's occurrence falls while Weaver is down.
 */

const initialOnly = () => test.skip(stage() !== "initial", "runs in the initial stage");
const MiB = 1024 * 1024;

// ---------------------------------------------------------------------------
// Hold probes: each hold action, how to read its state, and how to release it.

const PROBES = [
  "downloads", "post-processing", "watch-folder", "server", "speed-global", "speed-egress", "speed-server", "profile",
] as const;
type ProbeName = (typeof PROBES)[number];
type RuleAction = Record<string, unknown> & { actionType: string };

type Probe = {
  track: string;
  on: RuleAction;
  off: RuleAction;
  isOn: () => Promise<boolean>;
  /** The witness that uses a track this probe leaves alone. */
  witness: "speed" | "watch";
  /** speed_limit takes one time of day. */
  multipleTimes: boolean;
};

const PROBE_SPEED = 3 * MiB + 17;
const speedRule = (kind: "GLOBAL" | "EGRESS" | "SERVER", id: number | null, bytesPerSec: number) =>
  ({ actionType: "speed_limit", speedLimits: [{ kind, id, bytesPerSec }] });

async function probeFor(request: APIRequestContext, name: ProbeName, key: string): Promise<Probe | null> {
  switch (name) {
    case "downloads":
      return {
        track: "DOWNLOADS", on: { actionType: "pause" }, off: { actionType: "resume" }, witness: "speed", multipleTimes: true,
        isOn: async () => {
          const state = await queueState(request);
          return state.isPaused && state.downloadBlock.kind === "SCHEDULED";
        },
      };
    case "post-processing":
      return {
        track: "POST_PROCESSING", on: { actionType: "pause_post_processing" }, off: { actionType: "resume_post_processing" },
        witness: "speed", multipleTimes: true, isOn: () => postPaused(request, key),
      };
    case "watch-folder":
      return {
        track: "WATCH_FOLDER", on: { actionType: "pause_watch_folder_scanning" }, off: { actionType: "resume_watch_folder_scanning" },
        witness: "speed", multipleTimes: true, isOn: () => watchFolderPaused(request),
      };
    case "server": {
      const backup = await serverRow(request, "nntp2");
      return {
        track: "SERVER",
        on: { actionType: "set_server_active", serverId: backup.id, serverActive: false },
        off: { actionType: "set_server_active", serverId: backup.id, serverActive: true },
        witness: "speed", multipleTimes: true,
        isOn: async () => !(await serverRow(request, "nntp2")).active,
      };
    }
    case "speed-global":
      return {
        track: "SPEED", on: speedRule("GLOBAL", null, PROBE_SPEED), off: speedRule("GLOBAL", null, 0),
        witness: "watch", multipleTimes: false,
        isOn: async () => (await queueState(request)).downloadBlock.scheduledSpeedLimit === PROBE_SPEED,
      };
    case "speed-egress":
      return {
        track: "SPEED", on: speedRule("EGRESS", SYSTEM_EGRESS_ID, PROBE_SPEED), off: speedRule("EGRESS", SYSTEM_EGRESS_ID, 0),
        witness: "watch", multipleTimes: false,
        isOn: async () => (await egressSpeed(request, SYSTEM_EGRESS_ID)) === PROBE_SPEED,
      };
    case "speed-server": {
      const primary = await serverRow(request, "nntp");
      return {
        track: "SPEED", on: speedRule("SERVER", primary.id, PROBE_SPEED), off: speedRule("SERVER", primary.id, 0),
        witness: "watch", multipleTimes: false,
        isOn: async () => (await serverRow(request, "nntp")).maxDownloadSpeed === PROBE_SPEED,
      };
    }
    case "profile": {
      const state = await profileState(request);
      const home = state.scheduled ?? state.active;
      const other = state.available.find(profile => profile !== home);
      if (other === undefined) return null;
      return {
        track: "PROFILE",
        on: { actionType: "hardware_profile", hardwareProfile: other },
        off: { actionType: "hardware_profile", hardwareProfile: home },
        witness: "speed", multipleTimes: true,
        isOn: async () => {
          const now = await profileState(request);
          return now.scheduled === other && now.active === other;
        },
      };
    }
  }
}

async function waitOn(probe: Probe, on: boolean, message: string): Promise<void> {
  await expect.poll(() => probe.isOn(), { message, timeout: 0 }).toBe(on);
}

/** After a witnessed tick, the probe must still read `on`. */
async function holdsAfterTick(request: APIRequestContext, probe: Probe, on: boolean, message: string): Promise<void> {
  await witnessTick(request, probe.witness);
  expect(await probe.isOn(), message).toBe(on);
}

/**
 * Run a matrix case with its probe, then release the probe's state with an
 * off rule at the current minute, which wins over every earlier rule.
 */
async function withProbe(
  request: APIRequestContext, name: ProbeName, tag: string,
  body: (probe: Probe, rules: Rules) => Promise<void>,
): Promise<void> {
  await withControlKey(request, async key => {
    const probe = await probeFor(request, name, key);
    if (probe === null) {
      note("gap", "This machine honours one hardware profile, so a profile rule's change is not observable.");
      test.skip(true, "one hardware profile available");
      return;
    }
    await withRules(request, tag, async rules => {
      try {
        await body(probe, rules);
      } finally {
        await rules.create({ ...probe.off, time: hhmm(readClock()) });
        await waitOn(probe, false, `${tag} released`);
      }
    });
  });
}

// ---------------------------------------------------------------------------
// S: the API contract for every action and every entry field.

const UI_NULLS = {
  label: null, days: null, times: [], everyHourAtMinute: null, serverId: null, serverActive: null,
  quotaMeteringEnabled: null, quotaEgressId: null, pruneFailed: null, pruneCompleted: null, pruneCancelled: null,
  hardwareProfile: null,
};

async function createRaw(request: APIRequestContext, input: Record<string, unknown>): Promise<ScheduleRow[]> {
  return (await graphql<{ createSchedule: ScheduleRow[] }>(request,
    `mutation($input: ScheduleInput!) { createSchedule(input: $input) { ${SCHEDULE_FIELDS} } }`, { input })).createSchedule;
}

test("S01 every action saves in the UI's input shape with its track and fields", async ({ request }) => {
  initialOnly();
  const nntp = await serverRow(request, "nntp");
  const nntp2 = await serverRow(request, "nntp2");
  const profile = (await profileState(request)).available[0]!;
  const cases: Array<{ input: Record<string, unknown>; expected: Partial<ScheduleRow> }> = [
    { input: { actionType: "pause" }, expected: { track: "DOWNLOADS" } },
    { input: { actionType: "resume" }, expected: { track: "DOWNLOADS" } },
    { input: { actionType: "pause_all" }, expected: { track: "DOWNLOADS" } },
    { input: { actionType: "pause_post_processing" }, expected: { track: "POST_PROCESSING" } },
    { input: { actionType: "resume_post_processing" }, expected: { track: "POST_PROCESSING" } },
    { input: { actionType: "pause_watch_folder_scanning" }, expected: { track: "WATCH_FOLDER" } },
    { input: { actionType: "resume_watch_folder_scanning" }, expected: { track: "WATCH_FOLDER" } },
    { input: { actionType: "pause_rss" }, expected: { track: "RSS" } },
    { input: { actionType: "resume_rss" }, expected: { track: "RSS" } },
    {
      input: { actionType: "speed_limit", speedLimits: [
        { kind: "GLOBAL", id: null, bytesPerSec: 1 * MiB }, { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: 2 * MiB },
        { kind: "SERVER", id: nntp.id, bytesPerSec: 3 * MiB }, { kind: "GLOBAL", id: null, bytesPerSec: 4 * MiB },
      ] },
      // The last value given for a target wins.
      expected: { track: "SPEED", speedLimits: [
        { kind: "GLOBAL", id: null, bytesPerSec: 4 * MiB }, { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: 2 * MiB },
        { kind: "SERVER", id: nntp.id, bytesPerSec: 3 * MiB },
      ] },
    },
    { input: { actionType: "speed_limit", speedLimitBytes: 5 * MiB }, expected: { track: "SPEED", speedLimitBytes: 5 * MiB } },
    { input: { actionType: "hardware_profile", hardwareProfile: profile }, expected: { track: "PROFILE", hardwareProfile: profile } },
    {
      input: { actionType: "set_server_active", serverId: nntp2.id, serverActive: false },
      expected: { track: "SERVER", serverId: nntp2.id, serverActive: false },
    },
    {
      input: { actionType: "set_quota_metering", quotaMeteringEnabled: false },
      expected: { track: "QUOTA", quotaMeteringEnabled: false, quotaEgressId: null },
    },
    {
      input: { actionType: "set_quota_metering", quotaMeteringEnabled: true, quotaEgressId: SYSTEM_EGRESS_ID },
      expected: { track: "QUOTA", quotaMeteringEnabled: true, quotaEgressId: SYSTEM_EGRESS_ID },
    },
    {
      input: { actionType: "prune_history", pruneFailed: { deleteFiles: false }, pruneCompleted: { deleteFiles: true }, pruneCancelled: null },
      expected: { track: "ONE_SHOT", pruneFailed: { deleteFiles: false }, pruneCompleted: { deleteFiles: true }, pruneCancelled: null },
    },
    {
      input: { actionType: "prune_history", time: "00:15", everyHourAtMinute: 15, pruneCancelled: { deleteFiles: false } },
      expected: { track: "ONE_SHOT", everyHourAtMinute: 15 },
    },
    {
      input: { actionType: "pause", time: "08:00", times: ["08:00", "12:30", "23:59"], days: ["mon", "sun"] },
      expected: { track: "DOWNLOADS", times: ["08:00", "12:30", "23:59"], days: ["mon", "sun"] },
    },
  ];
  const created: string[] = [];
  try {
    for (const [index, { input, expected }] of cases.entries()) {
      const label = `s01-${index}-${token}`;
      // Disabled, so none of them is ever in force.
      const rows = await createRaw(request, { ...UI_NULLS, time: "03:00", ...input, enabled: false, label });
      const row = rows.find(candidate => candidate.label === label);
      expect(row, `rule ${label} for ${input.actionType}`).toBeTruthy();
      created.push(row!.id);
      expect(row, `rule ${label} for ${input.actionType}`).toMatchObject({
        enabled: false, actionType: input.actionType, days: [], everyHourAtMinute: null, ...expected,
      });
      expect(row!.id).toMatch(/^sched-[0-9a-f]+$/);
    }
    expect(new Set(created).size, "every rule has its own id").toBe(created.length);
    const saved = await schedules(request);
    for (const id of created) expect(saved.some(row => row.id === id), `rule ${id} reads back`).toBe(true);
  } finally {
    for (const id of created) await graphql(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", { id });
  }
});

test("S02 malformed rules are refused and leave the schedules unchanged", async ({ request }) => {
  initialOnly();
  const servers = await graphql<{ servers: Array<{ id: number }> }>(request, "query { servers { id } }");
  const missingServer = Math.max(...servers.servers.map(server => server.id)) + 1;
  const egresses = await graphql<{ egressInterfaces: Array<{ id: number }> }>(request, "query { egressInterfaces { id } }");
  const missingEgress = Math.max(...egresses.egressInterfaces.map(egress => egress.id)) + 1;
  const refusals: Array<[Record<string, unknown>, string]> = [
    [{ actionType: "fetch_rss" }, "unknown schedule actionType: fetch_rss"],
    [{ actionType: "pause", everyHourAtMinute: 5 }, "hourly schedules are only supported for pruning history"],
    [{ actionType: "prune_history", everyHourAtMinute: 60, pruneFailed: { deleteFiles: false } }, "hourly minute must be between 0 and 59"],
    [{ actionType: "prune_history", everyHourAtMinute: 5, times: ["01:00"], pruneFailed: { deleteFiles: false } }, "choose multiple times or hourly, not both"],
    [{ actionType: "speed_limit", times: ["01:00", "02:00"], speedLimitBytes: MiB }, "a speed_limit schedule takes one time of day"],
    [{ actionType: "speed_limit" }, "a speed_limit schedule needs at least one limit"],
    [{ actionType: "speed_limit", speedLimits: [] }, "a speed_limit schedule needs at least one limit"],
    [{ actionType: "pause", time: "24:00" }, "schedule times must be HH:MM"],
    [{ actionType: "pause", times: ["07:60"] }, "schedule times must be HH:MM"],
    [{ actionType: "pause", times: Array.from({ length: 25 }, (_, index) => `${String(index % 24).padStart(2, "0")}:${String(index).padStart(2, "0")}`) }, "at most 24 times per rule"],
    [{ actionType: "set_server_active", serverActive: false }, "set_server_active requires serverId and serverActive"],
    [{ actionType: "set_server_active", serverId: missingServer }, "set_server_active requires serverId and serverActive"],
    [{ actionType: "set_quota_metering" }, "set_quota_metering requires quotaMeteringEnabled"],
    [{ actionType: "prune_history" }, "prune_history requires at least one status"],
    [{ actionType: "hardware_profile" }, "a hardware_profile schedule needs a hardwareProfile"],
    [{ actionType: "set_server_active", serverId: missingServer, serverActive: true }, `server ${missingServer} not found`],
    [{ actionType: "speed_limit", speedLimits: [{ kind: "SERVER", id: missingServer, bytesPerSec: MiB }] }, `server ${missingServer} not found`],
    [{ actionType: "speed_limit", speedLimits: [{ kind: "EGRESS", id: missingEgress, bytesPerSec: MiB }] }, `egress ${missingEgress} not found`],
    [{ actionType: "set_quota_metering", quotaMeteringEnabled: true, quotaEgressId: missingEgress }, `egress ${missingEgress} not found`],
  ];
  const before = await schedules(request);
  for (const [input, message] of refusals) {
    const errors = await graphqlErrors(request,
      `mutation($input: ScheduleInput!) { createSchedule(input: $input) { id } }`,
      { input: { ...UI_NULLS, enabled: false, time: "03:00", label: `s02-${token}`, ...input } });
    expect(errors.join("\n"), `${JSON.stringify(input)} is refused`).toContain(message);
  }
  expect(await schedules(request), "no refused rule was saved").toEqual(before);
});

test("S03 a rule naming an unknown weekday is refused, not widened to every day", async ({ request }) => {
  initialOnly();
  const before = await schedules(request);
  const label = `s03-${token}`;
  const errors = await graphqlErrors(request,
    `mutation($input: ScheduleInput!) { createSchedule(input: $input) { ${SCHEDULE_FIELDS} } }`,
    { input: { ...UI_NULLS, enabled: false, time: "03:00", label, actionType: "pause", days: ["mon", "funday"] } });
  const saved = (await schedules(request)).filter(row => row.label === label);
  for (const row of saved) await graphql(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", { id: row.id });
  expect(saved.map(row => row.days), "an unknown day must not be dropped from the rule").toEqual([]);
  expect(errors, "an unknown day is refused").not.toEqual([]);
  expect(await schedules(request)).toEqual(before);
});

// ---------------------------------------------------------------------------
// C: tracks never end each other. Runs before any pause_all rule is saved, so
// a resume rule holds the Downloads track alone.

test("C01 a downloads rule leaves post-processing, watch intake, speed, servers and the profile alone", async ({ request }) => {
  initialOnly();
  expect(await setting("schedule_pause_all_used"), "no pause_all rule saved yet").toBeUndefined();
  const backup = await serverRow(request, "nntp2");
  const profiles = await profileState(request);
  const home = profiles.scheduled ?? profiles.active;
  const other = profiles.available.find(profile => profile !== home);
  if (other === undefined) note("gap", "One hardware profile available: the profile's independence is not observable.");
  const speed = 6 * MiB + 3;
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withControlKey(request, async key => {
    await withRules(request, "c01", async rules => {
      try {
        await rules.create({ actionType: "resume", time: "10:30" });
        await rules.create({ actionType: "pause", time: "10:00" });
        await rules.create({ actionType: "pause_post_processing", time: "09:00" });
        await rules.create({ actionType: "pause_watch_folder_scanning", time: "09:00" });
        await rules.create({ actionType: "speed_limit", time: "09:00", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: speed }] });
        await rules.create({ actionType: "set_server_active", time: "09:00", serverId: backup.id, serverActive: false });
        if (other !== undefined) await rules.create({ actionType: "hardware_profile", time: "09:00", hardwareProfile: other });

        const othersHeld = async () => {
          const state = await queueState(request);
          return await postPaused(request, key) && await watchFolderPaused(request)
            && state.downloadBlock.scheduledSpeedLimit === speed && !(await serverRow(request, "nntp2")).active
            && (other === undefined || (await profileState(request)).scheduled === other);
        };
        await expect.poll(othersHeld, { message: "C01 the 09:00 rules are in force", timeout: 0 }).toBe(true);
        expect((await queueState(request)).isPaused, "downloads run before 10:00").toBe(false);

        setClock(at(day, 10, 0));
        await waitPaused(request, true, "C01 downloads paused at 10:00");
        setClock(at(day, 10, 30));
        await waitPaused(request, false, "C01 downloads resumed at 10:30");
        // A pause rule at the current minute is the witness: it needs a later
        // tick than the one that resumed downloads, so that tick finished.
        await rules.create({ actionType: "pause", time: "10:30" });
        await waitPaused(request, true, "C01 witness pause");
        expect(await othersHeld(), "the downloads rules ended no other track").toBe(true);

        // The other way round: releasing every other track leaves downloads paused.
        await rules.create({ actionType: "resume_post_processing", time: "10:31" });
        await rules.create({ actionType: "resume_watch_folder_scanning", time: "10:31" });
        await rules.create({ actionType: "speed_limit", time: "10:31", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 0 }] });
        await rules.create({ actionType: "set_server_active", time: "10:31", serverId: backup.id, serverActive: true });
        if (other !== undefined) await rules.create({ actionType: "hardware_profile", time: "10:31", hardwareProfile: home });
        setClock(at(day, 10, 31));
        await expect.poll(async () => !(await postPaused(request, key)) && !(await watchFolderPaused(request))
          && (await queueState(request)).downloadBlock.scheduledSpeedLimit === 0 && (await serverRow(request, "nntp2")).active
          && (other === undefined || (await profileState(request)).scheduled === home),
        { message: "C01 the 10:31 rules released every other track", timeout: 0 }).toBe(true);
        // Downloads come first in every tick, so the tick that released the
        // others had already settled downloads.
        const state = await queueState(request);
        expect(state.isPaused, "downloads stay paused").toBe(true);
        expect(state.downloadBlock.kind).toBe("SCHEDULED");
      } finally {
        await resumeAll(request);
        await nzbgetRpc(request, key, "resumepost", []);
        await updateSettings(request, { watchFolder: { mode: "off", scanningPaused: false } });
        await restoreServer(request, "nntp2");
      }
    });
  });
});

// ---------------------------------------------------------------------------
// M: the hold matrix, every hold action through every occurrence rule.

for (const name of PROBES) {
  test(`M01 ${name}: fires at its time and the opposite rule releases it`, async ({ request }) => {
    initialOnly();
    const day = freshDay();
    setClock(at(day, 9, 59));
    await withProbe(request, name, `m01-${name}`, async (probe, rules) => {
      await rules.create({ ...probe.off, time: "10:30" });
      const on = await rules.create({ ...probe.on, time: "10:00" });
      expect(on.track).toBe(probe.track);
      await holdsAfterTick(request, probe, false, "the previous day's 10:30 release is in force at 09:59");

      setClock(at(day, 10, 0));
      await waitOn(probe, true, `M01 ${name} in force at 10:00`);
      setClock(at(day, 10, 29));
      await holdsAfterTick(request, probe, true, "still in force at 10:29");
      setClock(at(day, 10, 30));
      await waitOn(probe, false, `M01 ${name} released at 10:30`);
    });
  });

  test(`M02 ${name}: a disabled rule never fires, enabling applies it and disabling hands back`, async ({ request }) => {
    initialOnly();
    const day = freshDay();
    setClock(at(day, 9, 59));
    await withProbe(request, name, `m02-${name}`, async (probe, rules) => {
      await rules.create({ ...probe.off, time: "09:00" });
      const on = await rules.create({ ...probe.on, time: "10:00", enabled: false });
      expect(on.enabled).toBe(false);
      setClock(at(day, 10, 0));
      await holdsAfterTick(request, probe, false, "a disabled rule does not fire at its time");
      setClock(at(day, 10, 5));
      await holdsAfterTick(request, probe, false, "nor later");

      await rules.toggle(on.id, true);
      await waitOn(probe, true, `M02 ${name} enabling applies the 10:00 occurrence`);
      await rules.toggle(on.id, false);
      await waitOn(probe, false, `M02 ${name} disabling hands the track back to the 09:00 rule`);
    });
  });

  test(`M03 ${name}: a day filter holds it to its weekday`, async ({ request }) => {
    initialOnly();
    const monday = freshDay("mon");
    const tuesday = new Date(monday.getTime() + DAY_MS);
    const wednesday = new Date(tuesday.getTime() + DAY_MS);
    setClock(at(monday, 9, 59));
    await withProbe(request, name, `m03-${name}`, async (probe, rules) => {
      await rules.create({ ...probe.off, time: "09:00" });
      const on = await rules.create({ ...probe.on, time: "10:00", days: ["tue"] });
      expect(on.days).toEqual(["tue"]);
      setClock(at(monday, 10, 0));
      await holdsAfterTick(request, probe, false, "a Tuesday rule does not fire on Monday");
      setClock(at(tuesday, 9, 59));
      await holdsAfterTick(request, probe, false, "Tuesday 09:59");
      setClock(at(tuesday, 10, 0));
      await waitOn(probe, true, `M03 ${name} fires on Tuesday`);
      setClock(at(wednesday, 9, 0));
      await waitOn(probe, false, `M03 ${name} the every-day 09:00 rule releases it on Wednesday`);
      setClock(at(wednesday, 10, 0));
      await holdsAfterTick(request, probe, false, "nor on Wednesday");
    });
  });

  test(`M04 ${name}: a rule with several times fires at each`, async ({ request }) => {
    initialOnly();
    const day = freshDay();
    setClock(at(day, 9, 59));
    await withProbe(request, name, `m04-${name}`, async (probe, rules) => {
      if (!probe.multipleTimes) {
        note("observed", "A speed_limit rule takes one time of day; S02 asserts the refusal.");
        return;
      }
      await rules.create({ ...probe.off, time: "10:30", times: ["10:30", "11:30"] });
      const on = await rules.create({ ...probe.on, time: "10:00", times: ["10:00", "11:00"] });
      expect(on.times).toEqual(["10:00", "11:00"]);
      await holdsAfterTick(request, probe, false, "the previous day's 11:30 release is in force at 09:59");
      for (const [hour, minute, state] of [[10, 0, true], [10, 30, false], [11, 0, true], [11, 30, false]] as const) {
        setClock(at(day, hour, minute));
        await waitOn(probe, state, `M04 ${name} at ${hour}:${String(minute).padStart(2, "0")}`);
      }
    });
  });

  test(`M05 ${name}: removing the last rule leaves the state in place`, async ({ request }) => {
    initialOnly();
    const day = freshDay();
    setClock(at(day, 9, 59));
    await withProbe(request, name, `m05-${name}`, async (probe, rules) => {
      const off = await rules.create({ ...probe.off, time: "09:00" });
      const on = await rules.create({ ...probe.on, time: "10:00" });
      setClock(at(day, 10, 0));
      await waitOn(probe, true, `M05 ${name} in force at 10:00`);
      await rules.delete(off.id);
      await rules.delete(on.id);
      await holdsAfterTick(request, probe, true, "deleting every rule on the track leaves the state");
      setClock(at(day, 10, 30));
      await holdsAfterTick(request, probe, true, "and nothing replays it later");
      const release = await rules.create({ ...probe.off, time: "10:30" });
      await waitOn(probe, false, `M05 ${name} a new rule takes over`);
      await rules.delete(release.id);
      await holdsAfterTick(request, probe, false, "the released state stays too");
    });
  });
}

// ---------------------------------------------------------------------------
// C: how overlapping rules resolve.

test("C02 at the same minute the later rule in the list wins", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withControlKey(request, async key => {
    await withRules(request, "c02", async rules => {
      try {
        await rules.create({ actionType: "pause", time: "09:00" });
        await rules.create({ actionType: "pause", time: "10:00" });
        await rules.create({ actionType: "resume", time: "10:00" });
        await rules.create({ actionType: "resume_post_processing", time: "09:00" });
        await rules.create({ actionType: "resume_post_processing", time: "10:00" });
        await rules.create({ actionType: "pause_post_processing", time: "10:00" });
        await waitPaused(request, true, "C02 the 09:00 pause holds downloads");
        await expect.poll(() => postPaused(request, key), { message: "C02 post-processing runs before 10:00", timeout: 0 }).toBe(false);
        setClock(at(day, 10, 0));
        await waitPaused(request, false, "C02 the later resume wins at 10:00");
        await expect.poll(() => postPaused(request, key), { message: "C02 the later post-processing pause wins at 10:00", timeout: 0 }).toBe(true);
        await witnessTick(request);
        expect((await queueState(request)).isPaused, "the earlier pause at the same minute does not apply").toBe(false);
      } finally {
        await resumeAll(request);
        await nzbgetRpc(request, key, "resumepost", []);
      }
    });
  });
});

test("C03 each speed target is its own track", async ({ request }) => {
  initialOnly();
  const nntp = await serverRow(request, "nntp");
  const day = freshDay();
  setClock(at(day, 9, 59));
  const read = async () => ({
    global: (await queueState(request)).downloadBlock.scheduledSpeedLimit,
    egress: await egressSpeed(request, SYSTEM_EGRESS_ID),
    server: (await serverRow(request, "nntp")).maxDownloadSpeed,
  });
  const waitSpeeds = (expected: Awaited<ReturnType<typeof read>>, message: string) =>
    expect.poll(read, { message, timeout: 0 }).toEqual(expected);
  await withRules(request, "c03", async rules => {
    try {
      await rules.create({ actionType: "speed_limit", time: "09:00", speedLimits: [
        { kind: "GLOBAL", id: null, bytesPerSec: 0 }, { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: 0 },
        { kind: "SERVER", id: nntp.id, bytesPerSec: 0 },
      ] });
      await waitSpeeds({ global: 0, egress: 0, server: 0 }, "C03 no limits before 10:00");
      const all = await rules.create({ actionType: "speed_limit", time: "10:00", speedLimits: [
        { kind: "GLOBAL", id: null, bytesPerSec: 7 * MiB }, { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: 8 * MiB },
        { kind: "SERVER", id: nntp.id, bytesPerSec: 9 * MiB },
      ] });
      expect(all.track).toBe("SPEED");
      const egressOnly = await rules.create({ actionType: "speed_limit", time: "10:30", speedLimits: [
        { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: 5 * MiB },
      ] });
      const serverOnly = await rules.create({ actionType: "speed_limit", time: "10:45", speedLimits: [
        { kind: "SERVER", id: nntp.id, bytesPerSec: 4 * MiB },
      ] });
      expect([egressOnly.track, serverOnly.track]).toEqual(["SPEED", "SPEED"]);
      // The 09:00 rule is still the latest on every target until 10:00.
      await witnessTick(request, "watch");
      expect(await read()).toEqual({ global: 0, egress: 0, server: 0 });

      setClock(at(day, 10, 0));
      await waitSpeeds({ global: 7 * MiB, egress: 8 * MiB, server: 9 * MiB }, "C03 one rule sets three targets at 10:00");
      setClock(at(day, 10, 30));
      await waitSpeeds({ global: 7 * MiB, egress: 5 * MiB, server: 9 * MiB }, "C03 an egress rule ends only the egress target");
      setClock(at(day, 10, 45));
      await waitSpeeds({ global: 7 * MiB, egress: 5 * MiB, server: 4 * MiB }, "C03 a provider rule ends only the provider target");
      await witnessTick(request, "watch");
      expect(await read(), "nothing else moved").toEqual({ global: 7 * MiB, egress: 5 * MiB, server: 4 * MiB });
    } finally {
      await rules.create({ actionType: "speed_limit", time: hhmm(readClock()), speedLimits: [
        { kind: "GLOBAL", id: null, bytesPerSec: 0 }, { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: 0 },
        { kind: "SERVER", id: nntp.id, bytesPerSec: 0 },
      ] });
      await waitSpeeds({ global: 0, egress: 0, server: 0 }, "C03 cleanup");
    }
  });
});

test("C04 quota tracks apply in order: one egress's rule after the rule for every egress", async ({ request }) => {
  initialOnly();
  note("gap", "Metering state has no API field; it is read from whether a download adds to the System egress's usage.");
  const used = async () => (await systemEgressQuotaUsage(request)).usedBytes;
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "c04", async rules => {
    try {
      await setSystemEgressQuota(request, { enabled: true, period: "DAILY", limitBytes: 2_000_000_000 });
      // The single-egress rule comes first in the list; it still wins because
      // its track is applied after the rule for every egress.
      const single = await rules.create({ actionType: "set_quota_metering", time: "10:00", quotaMeteringEnabled: false, quotaEgressId: SYSTEM_EGRESS_ID });
      const every = await rules.create({ actionType: "set_quota_metering", time: "10:00", quotaMeteringEnabled: true });
      expect([single.track, every.track]).toEqual(["QUOTA", "QUOTA"]);
      expect([single.quotaEgressId, every.quotaEgressId]).toEqual([SYSTEM_EGRESS_ID, null]);
      setClock(at(day, 10, 0));
      await witnessTick(request);
      const before = await used();
      await completedJob(request, "c04-unmetered");
      await witnessTick(request);
      expect(await used(), "the System egress's own rule turned its metering off").toBe(before);

      // Removing an egress's last rule hands it back to the rule for every egress.
      await rules.delete(single.id);
      await witnessTick(request);
      await completedJob(request, "c04-metered");
      await expect.poll(async () => (await used()) > before,
        { message: "C04 metered again under the rule for every egress", timeout: 0 }).toBe(true);
    } finally {
      await setSystemEgressQuota(request, { enabled: false, period: "DAILY", limitBytes: 2_000_000_000 });
    }
  });
});

/** Effective throughput of six 512 KiB articles served by nntp. */
async function rateProbe(request: APIRequestContext, label: string): Promise<number> {
  const bytes = 512 * 1024;
  const name = `c05-${label}-${token}`;
  const articles = Array.from({ length: 6 }, (_, index) => ({ messageId: `${name}-${index}@e2e.invalid`, bytes }));
  for (const article of articles) await postProbeArticle(article.messageId, article.bytes);
  await resetNntpMetrics();
  const startedAt = Date.now();
  const submitted = await submitProbeNzb(request, name, articles);
  expect(submitted).toMatchObject({ accepted: true });
  await expect.poll(async () => (await graphql<{ queueItems: Array<{ id: number }> }>(request, "query { queueItems { id } }"))
    .queueItems.some(({ id }) => id === submitted.jobId), { message: `C05 ${label} left the queue`, timeout: 0 }).toBe(false);
  const elapsedSeconds = Math.max(0.001, (Date.now() - startedAt) / 1_000);
  const served = (await nntpBodyMetrics()).body_bytes;
  expect(served).toBeGreaterThanOrEqual(bytes * articles.length);
  return served / elapsedSeconds;
}

test("C05 the effective rate is the lowest of the global, egress and provider limits", async ({ request }) => {
  initialOnly();
  note("observed", "Throughput can only fall on a slow host, so each case asserts an upper bound below the next looser limit: the tightest limit governed.");
  const nntp = await serverRow(request, "nntp");
  const tight = 256 * 1024;
  const loose = 4 * MiB;
  await withRules(request, "c05", async rules => {
    const time = hhmm(readClock());
    const set = async (global: number, egress: number, server: number) => {
      // Editing the action of the occurrence in force applies it again.
      await rules.update(rule.id, { actionType: "speed_limit", time, label: rule.label, speedLimits: [
        { kind: "GLOBAL", id: null, bytesPerSec: global }, { kind: "EGRESS", id: SYSTEM_EGRESS_ID, bytesPerSec: egress },
        { kind: "SERVER", id: nntp.id, bytesPerSec: server },
      ] });
      await expect.poll(async () => [
        (await queueState(request)).downloadBlock.scheduledSpeedLimit, await egressSpeed(request, SYSTEM_EGRESS_ID),
        (await serverRow(request, "nntp")).maxDownloadSpeed,
      ], { message: `C05 limits ${global}/${egress}/${server}`, timeout: 0 }).toEqual([global, egress, server]);
    };
    const rule = await rules.create({ actionType: "speed_limit", time, speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 0 }] });
    try {
      for (const [label, limits] of [
        ["global", [tight, loose, loose]], ["egress", [loose, tight, loose]], ["provider", [loose, loose, tight]],
      ] as const) {
        await set(limits[0], limits[1], limits[2]);
        expect(await rateProbe(request, label), `the ${label} limit governs`).toBeLessThan(tight * 1.8);
      }
    } finally {
      await set(0, 0, 0);
    }
  });
});

// ---------------------------------------------------------------------------
// O: the operator over a rule.

test("O01 an operator pause gives way to a scheduled resume", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "o01", async rules => {
    try {
      await rules.create({ actionType: "pause", time: "09:00" });
      await rules.create({ actionType: "resume", time: "10:00" });
      await waitQueue(request, state => state.downloadBlock.kind === "SCHEDULED", "O01 the 09:00 pause");
      await resumeAll(request);
      await graphql(request, "mutation { pauseAll }");
      const manual = await waitPaused(request, true, "O01 operator pause");
      expect(manual.downloadBlock.kind).toBe("MANUAL_PAUSE");
      setClock(at(day, 10, 0));
      const resumed = await waitPaused(request, false, "O01 the 10:00 resume lifts the operator's pause");
      expect(resumed.downloadBlock.kind).toBe("NONE");
    } finally {
      await resumeAll(request);
    }
  });
});

test("O02 an operator resume sticks until the rule's next occurrence", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "o02", async rules => {
    try {
      await rules.create({ actionType: "resume", time: "11:00" });
      await rules.create({ actionType: "pause", time: "10:00", times: ["10:00", "10:45"] });
      setClock(at(day, 10, 0));
      await waitQueue(request, state => state.isPaused && state.downloadBlock.kind === "SCHEDULED", "O02 paused at 10:00");
      await resumeAll(request);
      await waitPaused(request, false, "O02 operator resume");
      await witnessTick(request);
      expect((await queueState(request)).isPaused, "the 10:00 pause does not reapply").toBe(false);
      setClock(at(day, 10, 44));
      await witnessTick(request);
      expect((await queueState(request)).isPaused, "nor at 10:44").toBe(false);
      setClock(at(day, 10, 45));
      await waitPaused(request, true, "O02 the 10:45 occurrence pauses again");
      setClock(at(day, 11, 0));
      await waitPaused(request, false, "O02 the 11:00 resume");
    } finally {
      await resumeAll(request);
    }
  });
});

test("O03 an operator speed limit replaces the scheduled one until the next speed rule", async ({ request }) => {
  initialOnly();
  const configured = (await generalSettings(request)).maxDownloadSpeed;
  const operator = 2 * MiB + 9;
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "o03", async rules => {
    try {
      await rules.create({ actionType: "speed_limit", time: "10:00", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 3 * MiB }] });
      await rules.create({ actionType: "speed_limit", time: "10:30", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 5 * MiB }] });
      setClock(at(day, 10, 0));
      await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === 3 * MiB, "O03 scheduled limit at 10:00");
      await graphql(request, "mutation($bytes: Int!) { setSpeedLimit(bytesPerSec: $bytes) }", { bytes: operator });
      const replaced = await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === 0, "O03 the operator's limit replaces the scheduled one");
      expect(replaced.speedLimitBytesPerSec).toBe(operator);
      await witnessTick(request, "watch");
      expect((await queueState(request)).downloadBlock.scheduledSpeedLimit, "the 10:00 rule does not reapply").toBe(0);
      setClock(at(day, 10, 30));
      await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === 5 * MiB, "O03 the next speed rule takes over");
    } finally {
      await updateSettings(request, { maxDownloadSpeed: configured });
      await expect.poll(async () => (await queueState(request)).downloadBlock.scheduledSpeedLimit,
        { message: "O03 cleanup", timeout: 0 }).toBe(0);
    }
  });
});

test("O04 the NZBGet post-processing controls override a rule until its next occurrence", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withControlKey(request, async key => {
    await withRules(request, "o04", async rules => {
      try {
        await rules.create({ actionType: "resume_post_processing", time: "10:30" });
        await rules.create({ actionType: "pause_post_processing", time: "10:00" });
        setClock(at(day, 10, 0));
        await expect.poll(() => postPaused(request, key), { message: "O04 paused at 10:00", timeout: 0 }).toBe(true);
        await nzbgetRpc(request, key, "resumepost", []);
        await expect.poll(() => postPaused(request, key), { message: "O04 operator resume", timeout: 0 }).toBe(false);
        await witnessTick(request);
        expect(await postPaused(request, key), "the 10:00 pause does not reapply").toBe(false);
        setClock(at(day, 10, 30));
        await witnessTick(request);
        await nzbgetRpc(request, key, "pausepost", []);
        await expect.poll(() => postPaused(request, key), { message: "O04 operator pause after 10:30", timeout: 0 }).toBe(true);
        await witnessTick(request);
        expect(await postPaused(request, key), "the 10:30 resume does not reapply").toBe(true);
      } finally {
        await nzbgetRpc(request, key, "resumepost", []);
      }
    });
  });
});

test("O05 an operator watch-folder resume overrides a rule until its next occurrence", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "o05", async rules => {
    try {
      await rules.create({ actionType: "resume_watch_folder_scanning", time: "11:00" });
      await rules.create({ actionType: "pause_watch_folder_scanning", time: "10:00", times: ["10:00", "10:30"] });
      setClock(at(day, 10, 0));
      await expect.poll(() => watchFolderPaused(request), { message: "O05 paused at 10:00", timeout: 0 }).toBe(true);
      await updateSettings(request, { watchFolder: { mode: "off", scanningPaused: false } });
      await witnessTick(request);
      expect(await watchFolderPaused(request), "the 10:00 pause does not reapply").toBe(false);
      setClock(at(day, 10, 30));
      await expect.poll(() => watchFolderPaused(request), { message: "O05 the 10:30 occurrence pauses again", timeout: 0 }).toBe(true);
      setClock(at(day, 11, 0));
      await expect.poll(() => watchFolderPaused(request), { message: "O05 the 11:00 resume", timeout: 0 }).toBe(false);
    } finally {
      await updateSettings(request, { watchFolder: { mode: "off", scanningPaused: false } });
    }
  });
});

test("O06 a scheduled profile stays in force over the operator's choice", async ({ request }) => {
  initialOnly();
  const before = await profileState(request);
  const home = before.scheduled ?? before.active;
  const other = before.available.find(profile => profile !== home);
  test.skip(other === undefined, "one hardware profile available");
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "o06", async rules => {
    try {
      await rules.create({ actionType: "hardware_profile", time: "10:30", hardwareProfile: home });
      await rules.create({ actionType: "hardware_profile", time: "10:00", hardwareProfile: other! });
      setClock(at(day, 10, 0));
      await waitProfile(request, other!, "O06 scheduled at 10:00");
      await graphql(request, "mutation($profile: HardwareProfileGql!) { setHardwareProfile(profile: $profile) { selected } }", { profile: home });
      await witnessTick(request);
      const state = await profileState(request);
      expect(state.selected, "the operator's choice is saved").toBe(home);
      expect([state.scheduled, state.active], "the scheduled profile stays in force").toEqual([other, other]);
      setClock(at(day, 10, 30));
      await waitProfile(request, home, "O06 the 10:30 rule");
    } finally {
      if (before.selected !== null) {
        await graphql(request, "mutation($profile: HardwareProfileGql!) { setHardwareProfile(profile: $profile) { selected } }", { profile: before.selected });
      }
    }
  });
});

test("O07 an operator putting a server back overrides a rule until its next occurrence", async ({ request }) => {
  initialOnly();
  const backup = await serverRow(request, "nntp2");
  const day = freshDay();
  setClock(at(day, 9, 59));
  await withRules(request, "o07", async rules => {
    try {
      await rules.create({ actionType: "set_server_active", time: "10:45", serverId: backup.id, serverActive: true });
      await rules.create({ actionType: "set_server_active", time: "10:00", times: ["10:00", "10:30"], serverId: backup.id, serverActive: false });
      setClock(at(day, 10, 0));
      await expect.poll(async () => (await serverRow(request, "nntp2")).active, { message: "O07 off at 10:00", timeout: 0 }).toBe(false);
      await restoreServer(request, "nntp2");
      await witnessTick(request);
      expect((await serverRow(request, "nntp2")).active, "the 10:00 rule does not reapply").toBe(true);
      setClock(at(day, 10, 30));
      await expect.poll(async () => (await serverRow(request, "nntp2")).active, { message: "O07 off again at 10:30", timeout: 0 }).toBe(false);
      setClock(at(day, 10, 45));
      await expect.poll(async () => (await serverRow(request, "nntp2")).active, { message: "O07 back at 10:45", timeout: 0 }).toBe(true);
    } finally {
      await restoreServer(request, "nntp2");
    }
  });
});

// ---------------------------------------------------------------------------
// P: one-shot occurrences, read through cancelled history rows a prune removes.

const pruneCancelled = { actionType: "prune_history", pruneCancelled: { deleteFiles: false } };
const present = async (request: APIRequestContext, id: number) => (await historyItem(request, id)) !== null;
const waitPruned = (request: APIRequestContext, id: number, message: string) =>
  expect.poll(() => present(request, id), { message, timeout: 0 }).toBe(false);

/** Move the clock and prove a tick saw it before reading what did not happen. */
async function step(request: APIRequestContext, instant: Date, via: "speed" | "watch" = "speed"): Promise<void> {
  setClock(instant);
  await witnessTick(request, via);
}

test("P01 a prune with several times fires once per occurrence and never on a step back", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  await withRules(request, "p01", async rules => {
    await step(request, at(day, 9, 58));
    const rule = await rules.create({ ...pruneCancelled, time: "10:00", times: ["10:00", "10:05"] });
    expect(rule.track).toBe("ONE_SHOT");
    const first = await cancelledJob(request, "p01-first");
    await step(request, at(day, 9, 59));
    expect(await present(request, first), "nothing is pruned before 10:00").toBe(true);
    setClock(at(day, 10, 1));
    await waitPruned(request, first, "P01 the 10:00 occurrence");

    const second = await cancelledJob(request, "p01-second");
    await step(request, at(day, 10, 4));
    expect(await present(request, second), "nothing between occurrences").toBe(true);
    setClock(at(day, 10, 6));
    await waitPruned(request, second, "P01 the 10:05 occurrence");

    // A step back and forward over the same occurrence does not fire it again.
    await step(request, at(day, 10, 2));
    const third = await cancelledJob(request, "p01-third");
    await step(request, at(day, 10, 7));
    expect(await present(request, third), "an occurrence fires once").toBe(true);

    // More than 90 minutes forward only sets a new baseline: the next day's
    // occurrences it skips over do not fire, and the day after fires again.
    await step(request, at(day, 24 + 10, 10));
    expect(await present(request, third), "a long jump over the next day's occurrences fires nothing").toBe(true);
    await step(request, at(day, 48 + 9, 58));
    expect(await present(request, third)).toBe(true);
    setClock(at(day, 48 + 10, 1));
    await waitPruned(request, third, "P01 the occurrence after the new baseline");
  });
});

test("P02 an hourly prune fires at its minute every hour", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  await withRules(request, "p02", async rules => {
    await step(request, at(day, 10, 0));
    const rule = await rules.create({ ...pruneCancelled, time: "00:07", everyHourAtMinute: 7 });
    expect(rule.everyHourAtMinute).toBe(7);
    const first = await cancelledJob(request, "p02-first");
    await step(request, at(day, 10, 6));
    expect(await present(request, first), "not before :07").toBe(true);
    setClock(at(day, 10, 8));
    await waitPruned(request, first, "P02 10:07");
    const second = await cancelledJob(request, "p02-second");
    await step(request, at(day, 11, 6));
    expect(await present(request, second), "not before the next :07").toBe(true);
    setClock(at(day, 11, 8));
    await waitPruned(request, second, "P02 11:07");
  });
});

test("P03 a weekday prune fires on its day and a disabled one never does", async ({ request }) => {
  initialOnly();
  const monday = freshDay("mon");
  const tuesday = new Date(monday.getTime() + DAY_MS);
  await withRules(request, "p03", async rules => {
    await step(request, at(monday, 9, 58));
    const weekly = await rules.create({ ...pruneCancelled, time: "10:00", days: ["tue"] });
    expect(weekly.days).toEqual(["tue"]);
    const disabled = await rules.create({ ...pruneCancelled, time: "10:00", enabled: false });
    const kept = await cancelledJob(request, "p03-kept");
    await step(request, at(monday, 10, 1));
    expect(await present(request, kept), "neither the Tuesday rule nor the disabled one fires on Monday").toBe(true);

    await step(request, at(tuesday, 9, 58));
    await rules.delete(weekly.id);
    await step(request, at(tuesday, 10, 1));
    expect(await present(request, kept), "the disabled rule does not fire on Tuesday").toBe(true);
    await rules.toggle(disabled.id, true);
    await step(request, at(tuesday, 10, 2));
    expect(await present(request, kept), "enabling a one-shot does not fire its passed occurrence").toBe(true);

    await rules.delete(disabled.id);
    const tuesdayOnly = await rules.create({ ...pruneCancelled, time: "10:05", days: ["tue"] });
    expect(tuesdayOnly.days).toEqual(["tue"]);
    setClock(at(tuesday, 10, 6));
    await waitPruned(request, kept, "P03 the Tuesday rule fires on Tuesday");
  });
});

// ---------------------------------------------------------------------------
// R: RSS. The scheduled RSS pause has no API field, so its "not read" side is
// asserted after a witnessed tick only; it can pass falsely if the poller is
// slower than the witness, never fail falsely.

test("R01 pause_rss and resume_rss hold the feed poller", async ({ request }) => {
  initialOnly();
  note("gap", "The RSS track has no API observable; the poller's reads of a counted feed are the only evidence.");
  const day = freshDay();
  setClock(at(day, 9, 59));
  const feeds: number[] = [];
  await withRules(request, "r01", async rules => {
    try {
      const resume = await rules.create({ actionType: "resume_rss", time: "10:30" });
      const pause = await rules.create({ actionType: "pause_rss", time: "10:00" });
      expect([resume.track, pause.track]).toEqual(["RSS", "RSS"]);
      const control = `r01-control-${token}`;
      feeds.push(await addCountedFeed(request, control));
      await expect.poll(() => feedCount(control), { message: "R01 a new feed is read while RSS runs", timeout: 0 }).toBe(1);

      setClock(at(day, 10, 0));
      await witnessTick(request);
      const held = `r01-held-${token}`;
      feeds.push(await addCountedFeed(request, held));
      await witnessTick(request);
      expect(await feedCount(held), "a new feed is not read while RSS is paused").toBe(0);

      setClock(at(day, 10, 30));
      await expect.poll(() => feedCount(held), { message: "R01 the resume reads the waiting feed", timeout: 0 }).toBe(1);
    } finally {
      for (const id of feeds) await deleteFeed(request, id);
    }
  });
});

test("R02 pause_all holds downloads, watch intake and RSS, and resume releases all three", async ({ request }) => {
  initialOnly();
  const day = freshDay();
  setClock(at(day, 9, 59));
  const feeds: number[] = [];
  await withRules(request, "r02", async rules => {
    try {
      await rules.create({ actionType: "resume", time: "10:30" });
      const pauseAll = await rules.create({ actionType: "pause_all", time: "10:00" });
      expect(pauseAll.track).toBe("DOWNLOADS");
      expect(await setting("schedule_pause_all_used"), "saving a pause_all rule records it").toBe("true");

      setClock(at(day, 10, 0));
      await waitPaused(request, true, "R02 downloads paused at 10:00");
      await expect.poll(() => watchFolderPaused(request), { message: "R02 watch intake paused", timeout: 0 }).toBe(true);
      const held = `r02-held-${token}`;
      feeds.push(await addCountedFeed(request, held));
      await witnessTick(request);
      expect(await feedCount(held), "a new feed is not read while pause_all holds RSS").toBe(0);

      setClock(at(day, 10, 30));
      await waitPaused(request, false, "R02 downloads resumed");
      await expect.poll(() => watchFolderPaused(request), { message: "R02 watch intake resumed", timeout: 0 }).toBe(false);
      await expect.poll(() => feedCount(held), { message: "R02 RSS resumed", timeout: 0 }).toBe(1);
    } finally {
      for (const id of feeds) await deleteFeed(request, id);
      await resumeAll(request);
      await updateSettings(request, { watchFolder: { mode: "off", scanningPaused: false } });
    }
  });
});

// ---------------------------------------------------------------------------
// H: occurrences that fall while Weaver is down. The harness moves the clock
// from 09:50 to 10:15 between the stages, with Weaver stopped.

type RestartState = { day: string; ids: string[]; cancelled: number; speed: number; late: number };

test("H01 a restart catches up the latest missed occurrence of each hold and each one-shot, once", async ({ request }) => {
  if (stage() === "initial") {
    const day = freshDay();
    const speed = 3 * MiB;
    const late = 4 * MiB;
    setClock(at(day, 9, 50));
    const rules = new Rules(request, "h01");
    const ids: string[] = [];
    const keep = async (input: RuleInput) => {
      const row = await rules.create(input);
      rules.keep(row.id);
      ids.push(row.id);
      return row;
    };
    await keep({ actionType: "resume", time: "10:30" });
    await keep({ actionType: "pause", time: "10:00" });
    await keep({ actionType: "resume_post_processing", time: "10:30" });
    await keep({ actionType: "pause_post_processing", time: "10:00" });
    // Two missed speed occurrences: only the later one is caught up.
    await keep({ actionType: "speed_limit", time: "10:30", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: 0 }] });
    await keep({ actionType: "speed_limit", time: "10:00", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: speed }] });
    await keep({ actionType: "speed_limit", time: "10:10", speedLimits: [{ kind: "GLOBAL", id: null, bytesPerSec: late }] });
    await keep({ actionType: "pause_watch_folder_scanning", time: "10:05", enabled: false });
    const cancelled = await cancelledJob(request, "h01");
    await keep({ ...pruneCancelled, time: "10:05" });
    await withControlKey(request, async key => {
      await witnessTick(request, "watch");
      const state = await queueState(request);
      expect([state.isPaused, state.downloadBlock.scheduledSpeedLimit, await postPaused(request, key)],
        "the previous day's 10:30 rules are in force at 09:50").toEqual([false, 0, false]);
    });
    expect(await present(request, cancelled)).toBe(true);
    saveStageState("h01", { day: day.toISOString(), ids, cancelled, speed, late } satisfies RestartState);
    setClockWhileStopped(at(day, 10, 15));
    return;
  }
  test.skip(stage() !== "restarted", "runs across the first restart");

  const { day: dayText, ids, cancelled, late } = loadStageState<RestartState>("h01");
  const day = new Date(dayText);
  try {
    expect(clockWhileStoppedPending(), "the harness moved the clock while Weaver was down").toBe(false);
    expect(readClock().toISOString()).toBe(at(day, 10, 15).toISOString());
    await withControlKey(request, async key => {
      await waitQueue(request, state => state.isPaused && state.downloadBlock.kind === "SCHEDULED", "H01 the missed 10:00 pause");
      await expect.poll(() => postPaused(request, key), { message: "H01 the missed post-processing pause", timeout: 0 }).toBe(true);
      await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === late, "H01 only the later missed speed rule");
      await witnessTick(request, "watch");
      expect(await watchFolderPaused(request), "a disabled rule is not caught up").toBe(false);
      await waitPruned(request, cancelled, "H01 the missed 10:05 prune is caught up");
      // The caught-up occurrence fires once: a job cancelled after it waits for the next one.
      const later = await cancelledJob(request, "h01-later");
      await step(request, at(day, 10, 20), "watch");
      expect(await present(request, later), "the caught-up prune does not fire again").toBe(true);

      setClock(at(day, 10, 30));
      await waitPaused(request, false, "H01 the 10:30 resume");
      await expect.poll(() => postPaused(request, key), { message: "H01 the 10:30 post-processing resume", timeout: 0 }).toBe(false);
      await waitQueue(request, state => state.downloadBlock.scheduledSpeedLimit === 0, "H01 the 10:30 speed rule");

      await step(request, at(day, 24 + 10, 0), "watch");
      setClock(at(day, 24 + 10, 6));
      await waitPruned(request, later, "H01 the next day's 10:05 prune");
    });
  } finally {
    for (const id of ids) await graphql(request, "mutation($id: String!) { deleteSchedule(id: $id) { id } }", { id });
    await resumeAll(request);
    await withControlKey(request, key => nzbgetRpc(request, key, "resumepost", []));
    // Deleting the rules leaves the last scheduled limit; the operator's own limit replaces it.
    await updateSettings(request, { maxDownloadSpeed: (await generalSettings(request)).maxDownloadSpeed });
  }
});
