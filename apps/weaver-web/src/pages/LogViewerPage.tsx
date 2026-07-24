import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "urql";
import { getGraphqlWsClient } from "@/graphql/client";
import { SERVICE_LOGS_QUERY } from "@/graphql/queries";
import { Button } from "@/components/ui/button";
import { SegmentedControl } from "@/components/ui/segmented-control";
import { useTranslate } from "@/lib/context/translate-context";
import { useIsMobile } from "@/lib/hooks/use-mobile";
import { cn } from "@/lib/utils";

const LOG_LEVEL_COLORS: Record<string, string> = {
  error: "text-log-error",
  warn: "text-log-warn",
  info: "text-log-info",
  debug: "text-log-debug",
  trace: "text-log-trace",
};

const LEVEL_OPTIONS = [
  { value: "all", label: "All" },
  { value: "error", label: "ERROR" },
  { value: "warn", label: "WARN" },
  { value: "info", label: "INFO" },
  { value: "debug", label: "DEBUG" },
  { value: "trace", label: "TRACE" },
];

const RAW_BUFFER_MAX = 2000;
const LIVE_TAIL_LINES = 300;
const MAX_RENDERED_LINES = 2000;
const LOG_INGEST_BATCH_MS = 50;
const LOG_RENDER_BATCH_MS = 150;
const KEEP_SUBSCRIPTION_DURING_STRICT_REMOUNT = import.meta.env.DEV;

const SERVICE_LOG_LINES_SUB = `subscription ServiceLogLines { serviceLogLines }`;

function detectLogLevel(line: string): string {
  const match = String(line ?? "").match(/\b(ERROR|WARN|WARNING|INFO|DEBUG|TRACE)\b/i);
  if (!match) return "info";
  if (match[1].toLowerCase() === "warning") return "warn";
  return match[1].toLowerCase();
}

// Tracing default format: {timestamp} {LEVEL} {target}: {message} {key=value ...}
const TRACING_LINE_RE =
  /^(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+(ERROR|WARN|INFO|DEBUG|TRACE)\s+([\w:]+):\s+(.*)/;
const KV_RE = /(\w+)=("(?:[^"\\]|\\.)*"|\S+)/g;

type ParsedLine = {
  timestamp: string;
  level: string;
  target: string;
  message: string;
  kvPairs: { key: string; value: string; start: number; end: number }[];
  raw: string;
};

type RawLogLineEntry = {
  id: number;
  raw: string;
  lower: string;
  level: string;
  parsed?: ParsedLine | null;
};

type LogLineEntry = {
  id: number;
  raw: string;
  lower: string;
  level: string;
  parsed: ParsedLine | null;
};

type LogViewerSnapshot = {
  lines: LogLineEntry[];
  bufferedCount: number;
  matchedCount: number;
  liveTailing: boolean;
};

function parseLine(raw: string): ParsedLine | null {
  const m = TRACING_LINE_RE.exec(raw);
  if (!m) return null;

  const body = m[4];
  const kvPairs: ParsedLine["kvPairs"] = [];
  let kv: RegExpExecArray | null;
  KV_RE.lastIndex = 0;
  while ((kv = KV_RE.exec(body)) !== null) {
    kvPairs.push({
      key: kv[1],
      value: kv[2],
      start: kv.index,
      end: kv.index + kv[0].length,
    });
  }

  return {
    timestamp: m[1],
    level: m[2],
    target: m[3],
    message: body,
    kvPairs,
    raw,
  };
}

function buildRawLogLineEntry(id: number, raw: string): RawLogLineEntry {
  return {
    id,
    raw,
    lower: raw.toLowerCase(),
    level: detectLogLevel(raw),
  };
}

function materializeLogLineEntry(entry: RawLogLineEntry): LogLineEntry {
  if (entry.parsed === undefined) {
    entry.parsed = parseLine(entry.raw);
  }

  return {
    id: entry.id,
    raw: entry.raw,
    lower: entry.lower,
    level: entry.level,
    parsed: entry.parsed,
  };
}

const EMPTY_LOG_SNAPSHOT: LogViewerSnapshot = {
  lines: [],
  bufferedCount: 0,
  matchedCount: 0,
  liveTailing: false,
};

function buildLogViewerSnapshot(
  source: RawLogLineEntry[],
  query: string,
  level: string,
  paused: boolean,
): LogViewerSnapshot {
  const normalizedQuery = query.trim().toLowerCase();
  const hasFilters = normalizedQuery.length > 0 || level !== "all";

  const matching = source.filter((line) => {
    if (normalizedQuery && !line.lower.includes(normalizedQuery)) {
      return false;
    }
    if (level !== "all" && line.level !== level) {
      return false;
    }
    return true;
  });

  const liveTailing = !paused && !hasFilters && matching.length > LIVE_TAIL_LINES;
  const visible = liveTailing
    ? matching.slice(-LIVE_TAIL_LINES)
    : matching.slice(-MAX_RENDERED_LINES);

  return {
    lines: visible.map(materializeLogLineEntry),
    bufferedCount: source.length,
    matchedCount: matching.length,
    liveTailing,
  };
}

function HighlightedLine({ entry }: { entry: LogLineEntry }) {
  const parsed = entry.parsed;
  if (!parsed) {
    return <span className="text-foreground">{entry.raw}</span>;
  }

  const lvl = parsed.level.toLowerCase();
  const levelColor = LOG_LEVEL_COLORS[lvl] ?? "text-muted-foreground";

  // Build message fragments with highlighted key=value spans
  const fragments: React.ReactNode[] = [];
  let cursor = 0;
  for (const kv of parsed.kvPairs) {
    if (kv.start > cursor) {
      fragments.push(
        <span key={`t${cursor}`} className="text-foreground">
          {parsed.message.slice(cursor, kv.start)}
        </span>,
      );
    }
    fragments.push(
      <span key={`k${kv.start}`}>
        <span className="text-status-copying">{kv.key}</span>
        <span className="text-muted-foreground/50">=</span>
        <span className="text-foreground/80">{kv.value}</span>
      </span>,
    );
    cursor = kv.end;
  }
  if (cursor < parsed.message.length) {
    fragments.push(
      <span key={`t${cursor}`} className="text-foreground">
        {parsed.message.slice(cursor)}
      </span>,
    );
  }

  return (
    <span>
      <span className="text-muted-foreground/50">{parsed.timestamp}</span>
      {" "}
      <span className={cn(levelColor, "font-semibold")}>{parsed.level.padStart(5)}</span>
      {" "}
      <span className="text-muted-foreground/70">{parsed.target}</span>
      <span className="text-muted-foreground/50">:</span>
      {" "}
      {fragments}
    </span>
  );
}

export function LogViewerPage() {
  const t = useTranslate();
  const isMobile = useIsMobile();
  const [search, setSearch] = useState("");
  const [level, setLevel] = useState("all");
  const [paused, setPaused] = useState(false);
  const [snapshot, setSnapshot] = useState<LogViewerSnapshot>(EMPTY_LOG_SNAPSHOT);
  const [connected, setConnected] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const autoScrollRef = useRef(true);
  const pausedRef = useRef(paused);
  const searchRef = useRef(search);
  const levelRef = useRef(level);
  const teardownTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const nextLineIdRef = useRef(0);
  const rawBufferRef = useRef<RawLogLineEntry[]>([]);
  const pendingLinesRef = useRef<string[]>([]);
  const ingestTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const snapshotTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const commitSnapshot = useCallback(() => {
    const nextSnapshot = buildLogViewerSnapshot(
      rawBufferRef.current,
      searchRef.current,
      levelRef.current,
      pausedRef.current,
    );

    startTransition(() => {
      setSnapshot(nextSnapshot);
    });
  }, []);

  const scheduleSnapshot = useCallback((immediate = false) => {
    if (snapshotTimerRef.current) {
      if (!immediate) {
        return;
      }
      clearTimeout(snapshotTimerRef.current);
      snapshotTimerRef.current = null;
    }

    snapshotTimerRef.current = setTimeout(() => {
      snapshotTimerRef.current = null;
      commitSnapshot();
    }, immediate ? 0 : LOG_RENDER_BATCH_MS);
  }, [commitSnapshot]);

  const flushPendingLines = useCallback(() => {
    ingestTimerRef.current = null;
    if (pendingLinesRef.current.length === 0) {
      return;
    }

    const pending = pendingLinesRef.current.splice(0, pendingLinesRef.current.length);
    const buffer = rawBufferRef.current;

    for (const line of pending) {
      const id = nextLineIdRef.current;
      nextLineIdRef.current += 1;
      buffer.push(buildRawLogLineEntry(id, line));
    }

    if (buffer.length > RAW_BUFFER_MAX) {
      buffer.splice(0, buffer.length - RAW_BUFFER_MAX);
    }

    scheduleSnapshot();
  }, [scheduleSnapshot]);

  const enqueueLine = useCallback((line: string) => {
    pendingLinesRef.current.push(line);
    if (ingestTimerRef.current) {
      return;
    }

    ingestTimerRef.current = setTimeout(flushPendingLines, LOG_INGEST_BATCH_MS);
  }, [flushPendingLines]);

  useEffect(() => {
    pausedRef.current = paused;
    if (paused && ingestTimerRef.current) {
      clearTimeout(ingestTimerRef.current);
      ingestTimerRef.current = null;
      flushPendingLines();
    }
    scheduleSnapshot(true);
  }, [flushPendingLines, paused, scheduleSnapshot]);

  useEffect(() => {
    searchRef.current = search;
    scheduleSnapshot(true);
  }, [scheduleSnapshot, search]);

  useEffect(() => {
    levelRef.current = level;
    scheduleSnapshot(true);
  }, [level, scheduleSnapshot]);

  // Initial load via query
  const [{ data }] = useQuery({
    query: SERVICE_LOGS_QUERY,
    variables: { limit: RAW_BUFFER_MAX },
  });

  useEffect(() => {
    if (data?.serviceLogs?.lines) {
      const initial: string[] = data.serviceLogs.lines;
      const seeded = initial.map((line) => {
        const id = nextLineIdRef.current;
        nextLineIdRef.current += 1;
        return buildRawLogLineEntry(id, line);
      });
      const existing = rawBufferRef.current;
      rawBufferRef.current = [...seeded, ...existing].slice(-RAW_BUFFER_MAX);
      scheduleSnapshot(true);
    }
  }, [data, scheduleSnapshot]);

  // Subscribe to live log lines via WebSocket
  useEffect(() => {
    if (KEEP_SUBSCRIPTION_DURING_STRICT_REMOUNT && teardownTimer.current) {
      clearTimeout(teardownTimer.current);
      teardownTimer.current = null;
      return;
    }

    const wsClient = getGraphqlWsClient();
    const unsubscribe = wsClient.subscribe(
      { query: SERVICE_LOG_LINES_SUB },
      {
        next(result: { data?: { serviceLogLines?: string } }) {
          const line = result.data?.serviceLogLines;
          if (line && !pausedRef.current) {
            enqueueLine(line);
          }
          setConnected(true);
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
      if (!KEEP_SUBSCRIPTION_DURING_STRICT_REMOUNT) {
        unsubscribe();
        setConnected(false);
        return;
      }

      teardownTimer.current = setTimeout(() => {
        teardownTimer.current = null;
        unsubscribe();
        setConnected(false);
      }, 200);
    };
  }, [enqueueLine]);

  useEffect(
    () => () => {
      if (ingestTimerRef.current) {
        clearTimeout(ingestTimerRef.current);
      }
      if (snapshotTimerRef.current) {
        clearTimeout(snapshotTimerRef.current);
      }
      pendingLinesRef.current = [];
    },
    [],
  );

  // Auto-scroll when new lines arrive
  useEffect(() => {
    if (autoScrollRef.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [snapshot.lines]);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    autoScrollRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  }, []);

  const liveTailNotice = useMemo(() => {
    if (!snapshot.liveTailing) {
      return null;
    }

    return `Live mode is showing the latest ${snapshot.lines.length} lines from ${snapshot.bufferedCount} buffered entries. Pause or filter to inspect more history.`;
  }, [snapshot.bufferedCount, snapshot.liveTailing, snapshot.lines.length]);

  const indicatorLabel = paused ? "Paused" : connected ? "Live" : "Disconnected";
  const indicatorDotClass = connected && !paused ? "bg-status-completed animate-status-pulse" : "bg-muted-foreground";

  return (
    <div className="space-y-4">
      <h1 className="font-space-grotesk text-[34px] font-bold leading-none tracking-tight">
        {t("nav.logs")}
      </h1>

      <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center">
        <SegmentedControl
          value={level}
          onValueChange={setLevel}
          options={LEVEL_OPTIONS}
          size="sm"
          ariaLabel="Level"
        />
        <input
          type="search"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search message, target, fields…"
          className="h-8 w-full rounded-inner border border-border bg-card px-3 font-mono text-[13px] text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring sm:w-64"
        />
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={() => setPaused((p) => !p)}
          >
            {paused ? "Resume" : "Pause"}
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              if (ingestTimerRef.current) {
                clearTimeout(ingestTimerRef.current);
                ingestTimerRef.current = null;
              }
              if (snapshotTimerRef.current) {
                clearTimeout(snapshotTimerRef.current);
                snapshotTimerRef.current = null;
              }
              pendingLinesRef.current = [];
              rawBufferRef.current = [];
              nextLineIdRef.current = 0;
              startTransition(() => {
                setSnapshot(EMPTY_LOG_SNAPSHOT);
              });
              autoScrollRef.current = true;
            }}
          >
            Clear
          </Button>
        </div>
        <div
          role="status"
          aria-label="Log stream status"
          className="flex items-center gap-1.5 text-[13px] text-muted-foreground sm:ml-auto"
        >
          <span className={cn("size-2 rounded-pill", indicatorDotClass)} />
          {indicatorLabel}
        </div>
      </div>
      {liveTailNotice ? (
        <p className="text-[13px] text-muted-foreground">{liveTailNotice}</p>
      ) : null}

      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className={cn(
          "overflow-y-auto rounded-inner border border-border bg-background/40 text-xs leading-5",
          isMobile ? "h-[50vh] min-h-[260px]" : "h-[28rem] min-h-[320px]",
        )}
        style={{
          fontFamily:
            "'Fira Code', 'Fira Mono', 'JetBrains Mono', 'Source Code Pro', 'Cascadia Code', 'Consolas', monospace",
        }}
      >
        {snapshot.lines.length === 0 ? (
          <p className="p-4 text-muted-foreground">No logs available yet.</p>
        ) : (
          <div className="space-y-0.5 p-2">
            {snapshot.lines.map((line, i) => (
              <div key={line.id} className="flex items-start gap-3 rounded-sm px-1 hover:bg-accent/40">
                <span className="w-10 shrink-0 select-none text-right tabular-nums text-muted-foreground/40">
                  {i + 1}
                </span>
                <div className="min-w-0 flex-1 whitespace-pre-wrap break-all">
                  <HighlightedLine entry={line} />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <p className="text-[12.5px] text-muted-foreground">
        {snapshot.lines.length} shown
        {` · ${snapshot.matchedCount} matching`}
        {` · ${snapshot.bufferedCount} buffered`}
        {snapshot.liveTailing ? " · live tail" : ""}
      </p>
    </div>
  );
}
