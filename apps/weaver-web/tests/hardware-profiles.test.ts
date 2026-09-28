import assert from "node:assert/strict";
import test from "node:test";
import {
  detectedHardware,
  initialProfile,
  offersProfileChoice,
  profileFacts,
  scheduledProfileNotice,
  type HardwareProfileOption,
  type HardwareProfileSettings,
} from "../src/next/data/hardware-profiles.ts";
import { englishTranslate } from "./english-translate.ts";

const EFFICIENT: HardwareProfileOption = {
  profile: "EFFICIENT",
  sevenzDecodeMemoryBytes: 512 * 1024 * 1024,
  decodeThreads: 2,
  extractThreads: 1,
  maxConcurrentDownloads: 10,
};

const BALANCED: HardwareProfileOption = {
  profile: "BALANCED",
  sevenzDecodeMemoryBytes: 1024 * 1024 * 1024,
  decodeThreads: 4,
  extractThreads: 2,
  maxConcurrentDownloads: null,
};

function settings(overrides: Partial<HardwareProfileSettings> = {}): HardwareProfileSettings {
  return {
    selected: null,
    active: "BALANCED",
    scheduled: null,
    recommended: "BALANCED",
    available: ["EFFICIENT", "BALANCED"],
    options: [EFFICIENT, BALANCED],
    detected: { memoryBytes: 8 * 1024 * 1024 * 1024, cores: 4 },
    ...overrides,
  };
}

test("a machine with one profile is asked nothing", () => {
  const small = settings({
    recommended: "EFFICIENT",
    available: ["EFFICIENT"],
    options: [EFFICIENT],
    detected: { memoryBytes: 2 * 1024 * 1024 * 1024, cores: 2 },
  });
  assert.equal(offersProfileChoice(small), false);
  assert.equal(offersProfileChoice(settings()), true);
});

test("a setting that has not arrived yet asks nothing either", () => {
  assert.equal(offersProfileChoice(null), false);
  assert.equal(offersProfileChoice(undefined), false);
});

test("the picker opens on the choice, and on the recommendation when there is none", () => {
  assert.equal(initialProfile(settings()), "BALANCED");
  assert.equal(initialProfile(settings({ selected: "EFFICIENT" })), "EFFICIENT");
});

test("a saved choice this machine no longer offers opens on the recommendation", () => {
  assert.equal(initialProfile(settings({ selected: "PERFORMANCE" })), "BALANCED");
});

test("a card's lines come from the daemon's numbers", () => {
  assert.deepEqual(profileFacts(englishTranslate, EFFICIENT), [
    "512 MB 7z decode memory",
    "threads: 2 decode · 1 extraction",
    "up to 10 downloads at once",
  ]);
});

test("an uncapped profile says nothing about concurrent downloads", () => {
  const facts = profileFacts(englishTranslate, BALANCED);
  assert.equal(facts.length, 2);
  assert.equal(facts[0], "1.0 GB 7z decode memory");
});

test("the recommendation names what it was judged against", () => {
  assert.equal(
    detectedHardware(englishTranslate, settings()),
    "Recommended for this machine: 8.0 GB RAM, 4 cores.",
  );
});

test("a schedule holding a profile says so, and nothing is said without one", () => {
  assert.equal(scheduledProfileNotice(englishTranslate, settings()), null);
  assert.equal(
    scheduledProfileNotice(
      englishTranslate,
      settings({ selected: "BALANCED", active: "EFFICIENT", scheduled: "EFFICIENT" }),
    ),
    "A schedule has the Efficient profile in force now; your choice applies whenever no schedule rule does.",
  );
});
