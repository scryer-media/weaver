import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "urql";
import { getGraphqlWsClient, useGraphqlClient } from "@/graphql/client";
import { SERVICE_LOGS_QUERY } from "@/graphql/queries";

/**
 * The service log tail.
 *
 * Seeded from `serviceLogs` and kept current by the `serviceLogLines`
 * subscription — the same two operations the classic viewer uses. Lines arrive
 * far faster than React should re-render, so ingestion batches into a ref and
 * only publishes a snapshot on a timer.
 */

const BUFFER_MAX = 2_000;
const INGEST_BATCH_MS = 50;
const RENDER_BATCH_MS = 150;
const SERVICE_LOG_LINES_SUB = `subscription ServiceLogLines { serviceLogLines }`;

export const LOG_LEVELS = ["error", "warn", "info", "debug", "trace"] as const;
export type LogLevel = (typeof LOG_LEVELS)[number];
export type LogLevelFilter = LogLevel | "all";

export interface LogKeyValue {
  key: string;
  value: string;
  start: number;
  end: number;
}

export interface LogLine {
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

function parse(id: number, raw: string): LogLine {
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

export interface ServiceLogs {
  /** Oldest first, so the tail grows at the bottom like a terminal. */
  lines: LogLine[];
  counts: Record<LogLevelFilter, number>;
  bufferedCount: number;
  matchedCount: number;
  paused: boolean;
  setPaused: (paused: boolean) => void;
  connected: boolean;
  /** The recent lines are still on their way; nothing is known to be missing yet. */
  loading: boolean;
}

export function useServiceLogs(level: LogLevelFilter, query: string): ServiceLogs {
  const [paused, setPaused] = useState(false);
  const [buffer, setBuffer] = useState<LogLine[]>([]);
  const [connected, setConnected] = useState(false);

  const bufferRef = useRef<LogLine[]>([]);
  const pendingRef = useRef<string[]>([]);
  const nextIdRef = useRef(0);
  const pausedRef = useRef(paused);
  const ingestTimerRef = useRef<number | null>(null);
  const renderTimerRef = useRef<number | null>(null);

  pausedRef.current = paused;

  const publish = useCallback(() => {
    renderTimerRef.current = null;
    setBuffer(bufferRef.current.slice());
  }, []);

  const scheduleRender = useCallback(() => {
    if (renderTimerRef.current !== null) {
      return;
    }
    renderTimerRef.current = window.setTimeout(publish, RENDER_BATCH_MS);
  }, [publish]);

  const flush = useCallback(() => {
    ingestTimerRef.current = null;
    const pending = pendingRef.current.splice(0, pendingRef.current.length);
    if (pending.length === 0) {
      return;
    }
    const next = bufferRef.current.slice();
    for (const raw of pending) {
      next.push(parse(nextIdRef.current, raw));
      nextIdRef.current += 1;
    }
    bufferRef.current = next.length > BUFFER_MAX ? next.slice(next.length - BUFFER_MAX) : next;
    scheduleRender();
  }, [scheduleRender]);

  const [{ data, fetching }] = useQuery<{ serviceLogs: { lines: string[]; count: number } }>({
    query: SERVICE_LOGS_QUERY,
    variables: { limit: BUFFER_MAX },
  });

  useEffect(() => {
    const seed = data?.serviceLogs?.lines;
    if (!seed) {
      return;
    }
    const seeded = seed.map((raw) => {
      const line = parse(nextIdRef.current, raw);
      nextIdRef.current += 1;
      return line;
    });
    // The snapshot holds every line up to its newest, so a buffered line at or
    // before that one — a subscription line that landed before the query
    // answered, or the previous snapshot on a re-seed — is already in it.
    const newest = seed[seed.length - 1];
    const buffered = bufferRef.current;
    let overlap = -1;
    for (let index = buffered.length - 1; index >= 0; index -= 1) {
      if (buffered[index]!.raw === newest) {
        overlap = index;
        break;
      }
    }
    const merged = [...seeded, ...buffered.slice(overlap + 1)];
    bufferRef.current = merged.slice(-BUFFER_MAX);
    setBuffer(bufferRef.current.slice());
  }, [data]);

  // The app replaces its GraphQL client, socket included, when the page comes
  // back to the foreground; the tail resubscribes on the new socket.
  const graphqlClient = useGraphqlClient();
  useEffect(() => {
    const client = getGraphqlWsClient();
    const unsubscribe = client.subscribe(
      { query: SERVICE_LOG_LINES_SUB },
      {
        next(result: { data?: { serviceLogLines?: string } }) {
          setConnected(true);
          const line = result.data?.serviceLogLines;
          // A paused tail drops new lines rather than buffering them: the
          // status bar promises "scroll freely", not "catch up later".
          if (!line || pausedRef.current) {
            return;
          }
          pendingRef.current.push(line);
          if (ingestTimerRef.current === null) {
            ingestTimerRef.current = window.setTimeout(flush, INGEST_BATCH_MS);
          }
        },
        error() {
          setConnected(false);
        },
        complete() {
          setConnected(false);
        },
      },
    );
    setConnected(true);
    return () => {
      unsubscribe();
      setConnected(false);
    };
  }, [flush, graphqlClient]);

  useEffect(
    () => () => {
      if (ingestTimerRef.current !== null) window.clearTimeout(ingestTimerRef.current);
      if (renderTimerRef.current !== null) window.clearTimeout(renderTimerRef.current);
    },
    [],
  );

  return useMemo(() => {
    const needle = query.trim().toLowerCase();
    const searched =
      needle === "" ? buffer : buffer.filter((line) => line.raw.toLowerCase().includes(needle));

    const counts: Record<LogLevelFilter, number> = {
      all: searched.length,
      error: 0,
      warn: 0,
      info: 0,
      debug: 0,
      trace: 0,
    };
    for (const line of searched) {
      counts[line.level] += 1;
    }

    const matching = level === "all" ? searched : searched.filter((line) => line.level === level);

    return {
      lines: matching,
      counts,
      bufferedCount: buffer.length,
      matchedCount: matching.length,
      paused,
      setPaused,
      connected,
      loading: fetching && !data,
    };
  }, [buffer, connected, data, fetching, level, paused, query]);
}
