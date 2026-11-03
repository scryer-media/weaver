// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * The service log buffer: reading a line from its record, and folding a
 * fresh snapshot into the lines already held.
 *
 * Kept apart from the hook so the folding — the part with cases — can be
 * tested without React or a socket.
 */

export const LOG_LEVELS = ["error", "warn", "info", "debug", "trace"] as const;
export type LogLevel = (typeof LOG_LEVELS)[number];

export interface LogKeyValue {
  key: string;
  value: string;
  start: number;
  end: number;
}

export interface LogLine {
  /** Stable for as long as the line is buffered, so it can key a row. */
  id: number;
  /** What the server sent: the JSON record, or a plain line. */
  source: string;
  /** The line as text, as the log writes it. */
  raw: string;
  level: LogLevel;
  /** `HH:MM:SS.mmm`, or the empty string when the line has no parseable stamp. */
  time: string;
  target: string;
  message: string;
  kvPairs: LogKeyValue[];
  /** The full stamp, or the empty string. */
  stamp: string;
}

// Tracing's default format: {timestamp} {LEVEL} {target}: {message} {k=v ...}
const TRACING_LINE_RE =
  /^(\d{4}-\d{2}-\d{2}T[\d:.]+(?:Z|[+-]\d{2}:\d{2}))\s+(ERROR|WARN|INFO|DEBUG|TRACE)\s+([\w:]+):\s+(.*)/;
const KV_RE = /(\w+)=("(?:[^"\\]|\\.)*"|\S+)/g;
const LEVEL_RE = /\b(ERROR|WARN|WARNING|INFO|DEBUG|TRACE)\b/i;

function detectLevel(line: string): LogLevel {
  const match = LEVEL_RE.exec(line);
  if (!match) return "info";
  const value = match[1]!.toLowerCase();
  return (value === "warning" ? "warn" : value) as LogLevel;
}

function timeOf(timestamp: string): string {
  const match = /T(\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?)/.exec(timestamp);
  return match ? match[1]! : timestamp;
}

export function parseLogLine(id: number, raw: string): LogLine {
  const match = TRACING_LINE_RE.exec(raw);
  if (!match) {
    return {
      id,
      source: raw,
      raw,
      level: detectLevel(raw),
      time: "",
      target: "",
      message: raw,
      kvPairs: [],
      stamp: "",
    };
  }
  const body = match[4]!;
  const kvPairs: LogKeyValue[] = [];
  KV_RE.lastIndex = 0;
  let kv: RegExpExecArray | null;
  while ((kv = KV_RE.exec(body)) !== null) {
    kvPairs.push({ key: kv[1]!, value: kv[2]!, start: kv.index, end: kv.index + kv[0].length });
  }
  return {
    id,
    source: raw,
    raw,
    level: match[2]!.toLowerCase() as LogLevel,
    time: timeOf(match[1]!),
    target: match[3]!,
    message: body,
    kvPairs,
    stamp: match[1]!,
  };
}

/** A JSON value with its numbers kept as written, so none loses precision. */
type JsonValue =
  | { kind: "string"; text: string }
  | { kind: "literal"; text: string }
  | { kind: "array"; items: JsonValue[] }
  | { kind: "object"; entries: [string, JsonValue][] };

const LITERAL_RE = /-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null/y;

function readJson(source: string): JsonValue | null {
  let at = 0;
  const space = () => {
    while (at < source.length && " \t\n\r".includes(source[at]!)) at += 1;
  };
  const string = (): string => {
    const start = at;
    at += 1;
    while (source[at] !== '"') {
      if (at >= source.length) throw new Error("open string");
      at += source[at] === "\\" ? 2 : 1;
    }
    at += 1;
    return JSON.parse(source.slice(start, at)) as string;
  };
  const value = (): JsonValue => {
    space();
    const char = source[at];
    if (char === '"') return { kind: "string", text: string() };
    if (char === "[" || char === "{") {
      const close = char === "[" ? "]" : "}";
      const items: JsonValue[] = [];
      const entries: [string, JsonValue][] = [];
      at += 1;
      space();
      if (source[at] === close) {
        at += 1;
      } else {
        for (;;) {
          if (char === "{") {
            space();
            if (source[at] !== '"') throw new Error("key");
            const key = string();
            space();
            if (source[at] !== ":") throw new Error("colon");
            at += 1;
            entries.push([key, value()]);
          } else {
            items.push(value());
          }
          space();
          if (source[at] === ",") {
            at += 1;
          } else if (source[at] === close) {
            at += 1;
            break;
          } else {
            throw new Error("separator");
          }
        }
      }
      return char === "[" ? { kind: "array", items } : { kind: "object", entries };
    }
    LITERAL_RE.lastIndex = at;
    const literal = LITERAL_RE.exec(source);
    if (!literal) throw new Error("value");
    at += literal[0].length;
    return { kind: "literal", text: literal[0] };
  };
  try {
    const read = value();
    space();
    return at === source.length ? read : null;
  } catch {
    return null;
  }
}

function writeJson(value: JsonValue, indent: string | null, depth = 0): string {
  if (value.kind === "string") return JSON.stringify(value.text);
  if (value.kind === "literal") return value.text;
  const parts =
    value.kind === "array"
      ? value.items.map((item) => writeJson(item, indent, depth + 1))
      : value.entries.map(
          ([key, item]) =>
            `${JSON.stringify(key)}:${indent === null ? "" : " "}${writeJson(item, indent, depth + 1)}`,
        );
  const [open, close] = value.kind === "array" ? ["[", "]"] : ["{", "}"];
  if (parts.length === 0) return open + close;
  if (indent === null) return open + parts.join(",") + close;
  const inner = "\n" + indent.repeat(depth + 1);
  return open + inner + parts.join("," + inner) + "\n" + indent.repeat(depth) + close;
}

/** Whether the text line quotes `text`: empty, or holding a space, `"`, `=` or a control character. */
function quoted(text: string): boolean {
  if (text === "" || /[\s"=]/.test(text)) return true;
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    if (code < 0x20 || (code >= 0x7f && code <= 0x9f)) return true;
  }
  return false;
}

/** Whether `text` is one string already quoted, every quote inside it escaped. */
function debugQuoted(text: string): boolean {
  if (text.length < 2 || !text.startsWith('"') || !text.endsWith('"')) return false;
  let at = 1;
  while (at < text.length - 1) {
    if (text[at] === "\\") at += 2;
    else if (text[at] === '"') return false;
    else at += 1;
  }
  return at === text.length - 1;
}

/**
 * A field value as the text line writes it. A string already quoted, as a
 * `?` field's Debug text of a string is, stands as it is.
 */
function textValue(value: JsonValue): string {
  if (value.kind !== "string") return writeJson(value, null);
  const text = value.text;
  if (debugQuoted(text)) return text;
  return quoted(text) ? JSON.stringify(text) : text;
}

function entry(value: JsonValue | null | undefined, key: string): JsonValue | undefined {
  return value?.kind === "object" ? value.entries.find(([name]) => name === key)?.[1] : undefined;
}

function text(value: JsonValue | undefined): string | null {
  return value?.kind === "string" ? value.text : null;
}

/**
 * A line from what the server sent. A JSON record becomes the text line the
 * log would have written for it, split into the parts a row shows; anything
 * else is read as a text line.
 */
export function readLogLine(id: number, source: string): LogLine {
  const record = source.startsWith("{") ? readJson(source) : null;
  const stamp = text(entry(record, "timestamp"));
  const levelName = text(entry(record, "level"));
  const target = text(entry(record, "target"));
  if (record === null || stamp === null || levelName === null || target === null) {
    return { ...parseLogLine(id, source), source };
  }

  let head = `${stamp} ${levelName.padStart(5)} `;
  const spans = entry(record, "spans");
  if (spans?.kind === "array" && spans.items.length > 0) {
    for (const span of spans.items) {
      head += text(entry(span, "name")) ?? "";
      const own = entry(span, "fields");
      const fields =
        own?.kind === "object"
          ? own.entries.map(([key, value]) => `${key}=${textValue(value)}`)
          : [];
      if (fields.length > 0) head += `{${fields.join(" ")}}`;
      head += ":";
    }
    head += " ";
  }

  let message = "";
  const kvPairs: LogKeyValue[] = [];
  const fields = entry(record, "fields");
  for (const [key, value] of fields?.kind === "object" ? fields.entries : []) {
    if (message !== "") message += " ";
    if (key === "message") {
      message += value.kind === "string" ? value.text : writeJson(value, null);
    } else {
      const pair = `${key}=${textValue(value)}`;
      kvPairs.push({ key, value: textValue(value), start: message.length, end: message.length + pair.length });
      message += pair;
    }
  }

  const lower = levelName.toLowerCase();
  return {
    id,
    source,
    raw: `${head}${target}:${message === "" ? "" : " " + message}`,
    level: (LOG_LEVELS as readonly string[]).includes(lower) ? (lower as LogLevel) : detectLevel(levelName),
    time: timeOf(stamp),
    target,
    message,
    kvPairs,
    stamp,
  };
}

/** The line's record as indented JSON, or the line itself when it is not one. */
export function recordJson(line: LogLine): string {
  const record = readJson(line.source);
  return record === null ? line.source : writeJson(record, "  ");
}

/** Whether the buffered lines all predate `line`, going by their stamps. */
function endsBefore(buffered: readonly LogLine[], line: LogLine): boolean {
  if (buffered.length === 0) return false;
  const last = buffered[buffered.length - 1]!.stamp;
  return last !== "" && line.stamp !== "" && last < line.stamp;
}

export interface MergedSnapshot {
  lines: LogLine[];
  nextId: number;
  /** False when the snapshot held nothing the buffer did not. */
  changed: boolean;
}

/**
 * Folds a `serviceLogs` snapshot into the buffered lines.
 *
 * The snapshot is asked for again every time the app replaces its GraphQL
 * client, which is every time the window comes back to the foreground, and
 * nearly all of it is then already on screen. A line that is already buffered
 * keeps its object and its id, so the rows showing it are left alone; only
 * lines the buffer has never seen are parsed and numbered. A snapshot with
 * nothing new changes nothing.
 *
 * Buffered lines newer than anything the two share — subscription lines that
 * outran the query — go after the snapshot.
 */
export function mergeSnapshot(
  buffered: readonly LogLine[],
  seed: readonly string[],
  nextId: number,
  max: number,
): MergedSnapshot {
  if (seed.length === 0) {
    return { lines: buffered as LogLine[], nextId, changed: false };
  }

  // The newest buffered line the snapshot also holds. Everything up to it is
  // the snapshot's to place; everything after it arrived since.
  const inSeed = new Set(seed);
  let overlap = -1;
  for (let index = buffered.length - 1; index >= 0; index -= 1) {
    if (inSeed.has(buffered[index]!.source)) {
      overlap = index;
      break;
    }
  }

  // Lines the snapshot may be repeating, oldest first per text so that two
  // identical lines are matched in the order they were buffered.
  const known = new Map<string, LogLine[]>();
  for (let index = 0; index <= overlap; index += 1) {
    const line = buffered[index]!;
    const same = known.get(line.source);
    if (same) same.push(line);
    else known.set(line.source, [line]);
  }

  let id = nextId;
  const merged: LogLine[] = [];
  for (const source of seed) {
    const reused = known.get(source)?.shift();
    if (reused) {
      merged.push(reused);
    } else {
      merged.push(readLogLine(id, source));
      id += 1;
    }
  }
  const apart = buffered.slice(overlap + 1);
  if (overlap < 0 && endsBefore(apart, merged[0]!)) {
    // Nothing shared and the buffer is the older of the two: the tail was
    // away for longer than the snapshot reaches back.
    merged.unshift(...apart);
  } else {
    merged.push(...apart);
  }

  let lines = merged.length > max ? merged.slice(merged.length - max) : merged;

  // Ids double as the running line number, so they have to rise down the
  // page. They do not when the snapshot brought older lines than the buffer
  // held — the first load, with subscription lines already in — and then
  // everything is numbered again, once.
  let ordered = true;
  for (let index = 1; index < lines.length; index += 1) {
    if (lines[index]!.id <= lines[index - 1]!.id) {
      ordered = false;
      break;
    }
  }
  if (!ordered) {
    lines = lines.map((line) => {
      const renumbered = { ...line, id };
      id += 1;
      return renumbered;
    });
  }

  const changed =
    lines.length !== buffered.length || lines.some((line, index) => line !== buffered[index]);
  return { lines: changed ? lines : (buffered as LogLine[]), nextId: id, changed };
}
