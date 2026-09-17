/**
 * The service log buffer: parsing a line, and folding a fresh snapshot into
 * the lines already held.
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
  raw: string;
  level: LogLevel;
  /** `HH:MM:SS.mmm`, or the empty string when the line has no parseable stamp. */
  time: string;
  target: string;
  message: string;
  kvPairs: LogKeyValue[];
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
    return { id, raw, level: detectLevel(raw), time: "", target: "", message: raw, kvPairs: [] };
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
    raw,
    level: match[2]!.toLowerCase() as LogLevel,
    time: timeOf(match[1]!),
    target: match[3]!,
    message: body,
    kvPairs,
  };
}

function stampOf(raw: string): string {
  return TRACING_LINE_RE.exec(raw)?.[1] ?? "";
}

/** Whether the buffered lines all predate `raw`, going by their stamps. */
function endsBefore(buffered: readonly LogLine[], raw: string): boolean {
  if (buffered.length === 0) return false;
  const last = stampOf(buffered[buffered.length - 1]!.raw);
  const first = stampOf(raw);
  return last !== "" && first !== "" && last < first;
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
    if (inSeed.has(buffered[index]!.raw)) {
      overlap = index;
      break;
    }
  }

  // Lines the snapshot may be repeating, oldest first per text so that two
  // identical lines are matched in the order they were buffered.
  const known = new Map<string, LogLine[]>();
  for (let index = 0; index <= overlap; index += 1) {
    const line = buffered[index]!;
    const same = known.get(line.raw);
    if (same) same.push(line);
    else known.set(line.raw, [line]);
  }

  let id = nextId;
  const merged: LogLine[] = [];
  for (const raw of seed) {
    const reused = known.get(raw)?.shift();
    if (reused) {
      merged.push(reused);
    } else {
      merged.push(parseLogLine(id, raw));
      id += 1;
    }
  }
  const apart = buffered.slice(overlap + 1);
  if (overlap < 0 && endsBefore(apart, seed[0]!)) {
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
