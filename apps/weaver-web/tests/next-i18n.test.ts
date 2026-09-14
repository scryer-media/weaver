import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { nextDe } from "../src/next/i18n/de.ts";
import { nextEn } from "../src/next/i18n/en.ts";
import { nextEs } from "../src/next/i18n/es.ts";
import { nextFr } from "../src/next/i18n/fr.ts";
import { nextIt } from "../src/next/i18n/it.ts";
import { nextJa } from "../src/next/i18n/ja.ts";
import { nextKo } from "../src/next/i18n/ko.ts";
import { nextPt } from "../src/next/i18n/pt.ts";
import { nextZh } from "../src/next/i18n/zh.ts";

const SRC = new URL("../src/", import.meta.url).pathname;

const LOCALES = {
  de: nextDe,
  es: nextEs,
  fr: nextFr,
  it: nextIt,
  ja: nextJa,
  ko: nextKo,
  pt: nextPt,
  zh: nextZh,
};

function placeholders(text: string): string[] {
  return [...text.matchAll(/\{\{(\w+)\}\}/g)].map((match) => match[1]!).sort();
}

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      return sourceFiles(path);
    }
    return /\.tsx?$/.test(name) ? [path] : [];
  });
}

/** Keys the classic dictionaries define, which Next screens reuse for shared wording. */
function classicKeys(): Set<string> {
  const keys = new Set<string>();
  for (const file of ["lib/i18n/locales/en.ts", "lib/i18n/duplicate-locales.ts"]) {
    for (const match of readFileSync(join(SRC, file), "utf8").matchAll(/^\s*"([\w.-]+)":/gm)) {
      keys.add(match[1]!);
    }
  }
  return keys;
}

test("every locale carries exactly the English Next keys", () => {
  const english = Object.keys(nextEn).sort();
  for (const [code, dictionary] of Object.entries(LOCALES)) {
    assert.deepEqual(Object.keys(dictionary).sort(), english, `${code} keys differ from en`);
  }
});

test("translations keep every placeholder and are never empty", () => {
  for (const [code, dictionary] of Object.entries(LOCALES)) {
    for (const [key, english] of Object.entries(nextEn)) {
      const translated = dictionary[key] ?? "";
      assert.notEqual(translated.trim(), "", `${code} ${key} is empty`);
      assert.deepEqual(placeholders(translated), placeholders(english), `${code} ${key} placeholders`);
    }
  }
});

test("every key the Next screens name exists", () => {
  const known = new Set([...Object.keys(nextEn), ...classicKeys()]);
  const missing: string[] = [];
  for (const file of sourceFiles(join(SRC, "next"))) {
    if (file.includes(`${join("next", "i18n")}`)) {
      continue;
    }
    const text = readFileSync(file, "utf8");
    for (const match of text.matchAll(/\bt\(\s*"([^"]+)"/g)) {
      if (!known.has(match[1]!)) missing.push(`${file}: ${match[1]}`);
    }
    for (const match of text.matchAll(/countLabel\(\s*t,\s*"([^"]+)"/g)) {
      for (const form of ["one", "other"]) {
        if (!known.has(`${match[1]}.${form}`)) missing.push(`${file}: ${match[1]}.${form}`);
      }
    }
    for (const match of text.matchAll(/"(next\.[\w.]+)"/g)) {
      const key = match[1]!;
      if (!known.has(key) && !(known.has(`${key}.one`) && known.has(`${key}.other`))) {
        missing.push(`${file}: ${key}`);
      }
    }
  }
  assert.deepEqual(missing, []);
});
