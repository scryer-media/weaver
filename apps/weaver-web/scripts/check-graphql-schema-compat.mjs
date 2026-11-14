// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { readFile } from "node:fs/promises";
import { basename } from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildSchema,
  findBreakingChanges,
  findDangerousChanges,
  parse,
  print,
  specifiedDirectives,
} from "graphql";

export function findSchemaCompatibilityChanges(oldSdl, newSdl) {
  // Rust's exporter emits a specified directive only once the schema uses it.
  // Compare an omitted declaration with that same declaration, rather than
  // GraphQL.js's potentially newer spec default. Explicit changes still count.
  const declarations = (sdl) => parse(sdl).definitions.filter(d => d.kind === "DirectiveDefinition");
  const oldDirectives = declarations(oldSdl), newDirectives = declarations(newSdl);
  const specified = new Set(specifiedDirectives.map(d => d.name));
  const missing = (own, other) => other.filter(d => specified.has(d.name.value) && !own.some(o => o.name.value === d.name.value)).map(print).join("\n");
  const oldSchema = buildSchema(`${oldSdl}\n${missing(oldDirectives,newDirectives)}`);
  const newSchema = buildSchema(`${newSdl}\n${missing(newDirectives,oldDirectives)}`);
  return {
    breaking: findBreakingChanges(oldSchema, newSchema),
    // Existing callers can omit newly added optional input fields.
    dangerous: findDangerousChanges(oldSchema, newSchema).filter(
      (change) => change.type !== "OPTIONAL_INPUT_FIELD_ADDED",
    ),
  };
}

export function formatSchemaCompatibilityChanges(changes) {
  const sections = [];
  if (changes.breaking.length > 0) {
    sections.push(formatChangeSection("Breaking changes", changes.breaking));
  }
  if (changes.dangerous.length > 0) {
    sections.push(formatChangeSection("Dangerous changes", changes.dangerous));
  }
  return sections.join("\n\n");
}

export function hasSchemaCompatibilityFailure(
  changes,
  { allowDangerous = false, allowBreaking = false } = {},
) {
  // Permitting breaking changes permits dangerous ones too.
  return (
    (!allowBreaking && changes.breaking.length > 0) ||
    (!allowBreaking && !allowDangerous && changes.dangerous.length > 0)
  );
}

const OPTIONS = ["--allow-dangerous", "--allow-breaking"];

// The permitting options, each at most once and in any order. Anything else,
// or a repeat, is null. `--allow-breaking` covers dangerous changes too.
export function parseSchemaCompatibilityOptions(options) {
  if (options.some((option) => !OPTIONS.includes(option))) return null;
  if (new Set(options).size !== options.length) return null;
  return {
    allowDangerous:
      options.includes("--allow-dangerous") || options.includes("--allow-breaking"),
    allowBreaking: options.includes("--allow-breaking"),
  };
}

function formatChangeSection(title, changes) {
  return [
    `${title}:`,
    ...changes.map((change) => `  - [${change.type}] ${change.description}`),
  ].join("\n");
}

export async function main(args) {
  const [oldSchemaPath, newSchemaPath, ...options] = args;
  const permitted = parseSchemaCompatibilityOptions(options);
  if (!oldSchemaPath || !newSchemaPath || !permitted) {
    console.error(
      "usage: node scripts/check-graphql-schema-compat.mjs OLD_SCHEMA NEW_SCHEMA [--allow-dangerous] [--allow-breaking]",
    );
    return 2;
  }
  const { allowDangerous, allowBreaking } = permitted;

  const [oldSdl, newSdl] = await Promise.all([
    readFile(oldSchemaPath, "utf8"),
    readFile(newSchemaPath, "utf8"),
  ]);
  const changes = findSchemaCompatibilityChanges(oldSdl, newSdl);
  if (hasSchemaCompatibilityFailure(changes, { allowDangerous, allowBreaking })) {
    console.error("GraphQL API compatibility check failed.");
    console.error(`Old schema: ${oldSchemaPath}`);
    console.error(`New schema: ${newSchemaPath}`);
    console.error(formatSchemaCompatibilityChanges(changes));
    return 1;
  }

  if (allowBreaking && changes.breaking.length > 0) {
    console.warn("GraphQL breaking changes explicitly permitted by --allow-breaking:");
    console.warn(formatChangeSection("Breaking changes", changes.breaking));
  }
  if (allowDangerous && changes.dangerous.length > 0) {
    console.warn(
      `GraphQL dangerous changes explicitly permitted by ${allowBreaking ? "--allow-breaking" : "--allow-dangerous"}:`,
    );
    console.warn(formatChangeSection("Dangerous changes", changes.dangerous));
  }

  console.log(
    `GraphQL API compatibility check passed: ${basename(oldSchemaPath)} -> ${basename(newSchemaPath)}`,
  );
  return 0;
}

const invokedPath = process.argv[1] ? fileURLToPath(import.meta.url) : null;
if (invokedPath && process.argv[1] === invokedPath) {
  process.exitCode = await main(process.argv.slice(2));
}
