import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

/**
 * Every GraphQL document the e2e suite sends, checked against the schema the
 * server exports, so a removed or renamed field fails here instead of in a
 * flow run.
 *
 *   WEAVER_GRAPHQL_SDL     the schema to check against, as
 *                          `cargo run --locked -p weaver-server-api --bin
 *                          export-graphql-schema` prints it. The committed
 *                          api/graphql/schema.graphql is the last release's,
 *                          so it is not a default.
 *   WEAVER_GRAPHQL_MODULE  the graphql package's index.mjs (default: the
 *                          copy the web app installs)
 */

const here = path.dirname(fileURLToPath(import.meta.url));
const suite = path.resolve(here, "..");
const repo = path.resolve(suite, "../..");
if (!process.env.WEAVER_GRAPHQL_SDL) throw new Error("set WEAVER_GRAPHQL_SDL to the path of the exported schema");
const sdlPath = path.resolve(process.env.WEAVER_GRAPHQL_SDL);
const modulePath = path.resolve(process.env.WEAVER_GRAPHQL_MODULE ?? path.join(repo, "apps/weaver-web/node_modules/graphql/index.mjs"));
const { buildSchema, parse, validate } = await import(pathToFileURL(modulePath).href);
const schema = buildSchema(fs.readFileSync(sdlPath, "utf8"));

const OPERATION = /^\s*(query|mutation|subscription|fragment)\b/;
const DECLARATION = /(?:const|let)\s+([A-Za-z_$][\w$]*)\s*(?::\s*string\s*)?=\s*$/;

/**
 * The string and template literals in `source`, each as its text parts and
 * the expressions between them, with the constant it initialises if any.
 */
function literals(source) {
  const found = [];
  let index = 0;
  const readQuoted = (quote) => {
    let text = "";
    index += 1;
    while (index < source.length && source[index] !== quote) {
      if (source[index] === "\\") {
        text += escaped(source[index + 1]);
        index += 2;
      } else {
        text += source[index];
        index += 1;
      }
    }
    index += 1;
    return text;
  };
  const skipExpression = () => {
    const start = index;
    let depth = 1;
    while (index < source.length && depth > 0) {
      const char = source[index];
      if (char === "'" || char === "\"") readQuoted(char);
      else if (char === "`") readTemplate();
      else {
        if (char === "{") depth += 1;
        else if (char === "}") depth -= 1;
        index += 1;
      }
    }
    return source.slice(start, index - 1).trim();
  };
  const readTemplate = () => {
    const parts = [""];
    const expressions = [];
    index += 1;
    while (index < source.length && source[index] !== "`") {
      if (source[index] === "\\") {
        parts[parts.length - 1] += escaped(source[index + 1]);
        index += 2;
      } else if (source[index] === "$" && source[index + 1] === "{") {
        index += 2;
        expressions.push(skipExpression());
        parts.push("");
      } else {
        parts[parts.length - 1] += source[index];
        index += 1;
      }
    }
    index += 1;
    return { parts, expressions };
  };
  while (index < source.length) {
    const char = source[index];
    if (char === "/" && source[index + 1] === "/") {
      index = source.indexOf("\n", index);
      if (index < 0) break;
    } else if (char === "/" && source[index + 1] === "*") {
      index = source.indexOf("*/", index + 2) + 2;
      if (index < 2) break;
    } else if (char === "'" || char === "\"" || char === "`") {
      const start = index;
      const declared = DECLARATION.exec(source.slice(Math.max(0, start - 120), start))?.[1] ?? null;
      const literal = char === "`" ? readTemplate() : { parts: [readQuoted(char)], expressions: [] };
      found.push({ ...literal, declared, line: source.slice(0, start).split("\n").length });
    } else {
      index += 1;
    }
  }
  return found;
}

function escaped(char) {
  return { n: "\n", t: "\t", r: "\r" }[char] ?? char ?? "";
}

/** The documents in `files`, with constants resolved across all of them. */
function documents(files) {
  const constants = new Map();
  const all = [];
  for (const [file, source] of files) {
    for (const literal of literals(source)) {
      all.push({ ...literal, file });
      if (literal.declared) constants.set(literal.declared, literal);
    }
  }
  const resolve = (literal, seen = new Set()) => {
    let text = literal.parts[0];
    for (const [position, expression] of literal.expressions.entries()) {
      const constant = constants.get(expression);
      if (!constant || seen.has(expression)) return { unresolved: expression };
      const inner = resolve(constant, new Set([...seen, expression]));
      if (inner.unresolved !== undefined) return inner;
      text += inner.text + literal.parts[position + 1];
    }
    return { text };
  };
  const found = [];
  for (const literal of all) {
    // A bare `mutation Name` matches a request by its operation name and a
    // test title can open with one of the keywords; only a selection is a
    // document.
    if (!OPERATION.test(literal.parts[0]) || !literal.parts.join("").includes("{")) continue;
    found.push({ file: literal.file, line: literal.line, ...resolve(literal) });
  }
  return found;
}

/** Why `document` does not hold up against the schema, if it does not. */
function problems(document) {
  if (document.unresolved !== undefined) return [`cannot resolve \${${document.unresolved}}`];
  try {
    return validate(schema, parse(document.text)).map(error => error.message);
  } catch (error) {
    return [error.message];
  }
}

function sources() {
  const files = [];
  const walk = (directory) => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      if (entry.name === "node_modules" || entry.name.startsWith(".")) continue;
      const full = path.join(directory, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (/\.(ts|mjs)$/.test(entry.name) && !entry.name.endsWith(".test.mjs")) {
        files.push([path.relative(suite, full), fs.readFileSync(full, "utf8")]);
      }
    }
  };
  walk(suite);
  return files.sort(([left], [right]) => left.localeCompare(right));
}

test("finds documents built from constants and rejects a field the schema lacks", () => {
  const found = documents([["sample.ts", [
    "const FIELDS = \"id name\";",
    "const ignored = 'not a document';",
    "await graphql(request, `query { scriptInstances { ${FIELDS} } }`);",
    "await graphql(request, \"query { schedules { id implicit } }\");",
    "await graphql(request, `query { scriptInstances { ${notConstant()} } }`);",
    "postData()?.includes(\"mutation SetLogFilter\");",
    "test(\"subscription loss polls and reconnects\", async () => {});",
  ].join("\n")]]);
  assert.equal(found.length, 3);
  assert.equal(found[0].text, "query { scriptInstances { id name } }");
  assert.deepEqual(problems(found[0]), []);
  assert.deepEqual(problems(found[1]), ["Cannot query field \"implicit\" on type \"Schedule\"."]);
  assert.deepEqual(problems(found[2]), ["cannot resolve ${notConstant()}"]);
});

test("every GraphQL document in the e2e suite is valid against the exported schema", () => {
  const found = documents(sources());
  assert.ok(found.length > 0, "no GraphQL documents found");
  const failures = found.flatMap(document =>
    problems(document).map(problem => `${document.file}:${document.line}: ${problem}`));
  assert.deepEqual(failures, []);
});
