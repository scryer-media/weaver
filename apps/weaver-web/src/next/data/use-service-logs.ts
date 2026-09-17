import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "urql";
import { getGraphqlWsClient, useGraphqlClient } from "@/graphql/client";
import { SERVICE_LOGS_QUERY } from "@/graphql/queries";
import {
  LOG_LEVELS,
  mergeSnapshot,
  parseLogLine,
  type LogKeyValue,
  type LogLevel,
  type LogLine,
} from "./service-log-buffer";

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

export { LOG_LEVELS };
export type { LogKeyValue, LogLevel, LogLine };
export type LogLevelFilter = LogLevel | "all";

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
      next.push(parseLogLine(nextIdRef.current, raw));
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
    // A re-seed — the app replaces its GraphQL client whenever the window
    // comes back to the foreground — mostly repeats what is on screen, and
    // must not rebuild it. See `mergeSnapshot`.
    const merged = mergeSnapshot(bufferRef.current, seed, nextIdRef.current, BUFFER_MAX);
    nextIdRef.current = merged.nextId;
    if (!merged.changed) {
      return;
    }
    bufferRef.current = merged.lines;
    setBuffer(merged.lines.slice());
  }, [data]);

  // The app replaces its GraphQL client, socket included, when the page comes
  // back to the foreground; the tail resubscribes on the new socket.
  const graphqlClient = useGraphqlClient();
  useEffect(() => {
    const client = getGraphqlWsClient();
    // graphql-ws completes a sink some time after it is unsubscribed, which
    // by then is after the next subscription has reported itself connected.
    // A replaced subscription has nothing more to say about the tail.
    let current = true;
    const unsubscribe = client.subscribe(
      { query: SERVICE_LOG_LINES_SUB },
      {
        next(result: { data?: { serviceLogLines?: string } }) {
          if (!current) return;
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
          if (current) setConnected(false);
        },
        complete() {
          if (current) setConnected(false);
        },
      },
    );
    setConnected(true);
    return () => {
      current = false;
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
