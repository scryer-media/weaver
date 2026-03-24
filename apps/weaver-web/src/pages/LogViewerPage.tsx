import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "urql";
import { getGraphqlWsClient } from "@/graphql/client";
import { SERVICE_LOGS_QUERY } from "@/graphql/queries";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useTranslate } from "@/lib/context/translate-context";

const LOG_LEVEL_COLORS: Record<string, string> = {
  error: "text-red-400",
  warn: "text-amber-400",
  info: "text-zinc-400",
  debug: "text-emerald-400/70",
  trace: "text-zinc-600",
};

const MAX_BUFFER = 2000;

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

function HighlightedLine({ line }: { line: string }) {
  const parsed = parseLine(line);
  if (!parsed) {
    return <span className="text-foreground">{line}</span>;
  }

  const lvl = parsed.level.toLowerCase();
  const levelColor = LOG_LEVEL_COLORS[lvl] ?? "text-zinc-300";

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
        <span className="text-teal-700 dark:text-teal-300">{kv.key}</span>
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
      <span className={levelColor}>{parsed.level.padStart(5)}</span>
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
  const [search, setSearch] = useState("");
  const [level, setLevel] = useState("all");
  const [paused, setPaused] = useState(false);
  const [lines, setLines] = useState<string[]>([]);
  const [connected, setConnected] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const autoScrollRef = useRef(true);
  const pausedRef = useRef(paused);
  const teardownTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    pausedRef.current = paused;
  });

  // Initial load via query
  const [{ data }] = useQuery({
    query: SERVICE_LOGS_QUERY,
    variables: { limit: MAX_BUFFER },
  });

  useEffect(() => {
    if (data?.serviceLogs?.lines) {
      setLines(data.serviceLogs.lines);
    }
  }, [data]);

  // Subscribe to live log lines via WebSocket
  useEffect(() => {
    if (teardownTimer.current) {
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
            setLines((prev) => {
              const next = [...prev, line];
              return next.length > MAX_BUFFER ? next.slice(next.length - MAX_BUFFER) : next;
            });
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
      teardownTimer.current = setTimeout(() => {
        teardownTimer.current = null;
        unsubscribe();
        setConnected(false);
      }, 200);
    };
  }, []);

  // Auto-scroll when new lines arrive
  useEffect(() => {
    if (autoScrollRef.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines]);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    autoScrollRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  }, []);

  const filteredLines = useMemo(() => {
    const query = search.trim().toLowerCase();
    return lines.filter((line) => {
      if (query && !line.toLowerCase().includes(query)) return false;
      if (level !== "all" && detectLogLevel(line) !== level) return false;
      return true;
    });
  }, [level, lines, search]);

  return (
    <div className="space-y-4">
      <PageHeader title={t("nav.logs")} />

      <div className="grid gap-3 sm:flex sm:flex-wrap sm:items-end">
        <div className="space-y-1">
          <Label className="text-xs text-muted-foreground">Level</Label>
          <Select value={level} onValueChange={setLevel}>
            <SelectTrigger className="w-full sm:w-[100px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All</SelectItem>
              <SelectItem value="error">Error</SelectItem>
              <SelectItem value="warn">Warn</SelectItem>
              <SelectItem value="info">Info</SelectItem>
              <SelectItem value="debug">Debug</SelectItem>
              <SelectItem value="trace">Trace</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1">
          <Label className="text-xs text-muted-foreground">Search</Label>
          <Input
            type="search"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="filter..."
            className="h-8 w-full text-sm sm:w-48"
          />
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
          <Button
            size="sm"
            variant="secondary"
            className="w-full sm:w-auto"
            onClick={() => setPaused((p) => !p)}
          >
            {paused ? "Resume" : "Pause"}
          </Button>
          <Button
            size="sm"
            variant="secondary"
            className="w-full sm:w-auto"
            onClick={() => {
              setLines([]);
              autoScrollRef.current = true;
            }}
          >
            Clear
          </Button>
        </div>
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground sm:ml-auto">
          <span
            className={`inline-block size-2 rounded-full ${connected ? "bg-green-400" : "bg-red-400"}`}
          />
          {connected ? "Live" : "Disconnected"}
          {paused && <span className="text-yellow-400">(paused)</span>}
        </div>
      </div>

      <div
        ref={scrollRef}
        onScroll={handleScroll}
        className="h-[calc(100vh-220px)] min-h-[400px] overflow-y-auto rounded-lg border border-border bg-card text-xs leading-5"
        style={{
          fontFamily:
            "'Fira Code', 'Fira Mono', 'JetBrains Mono', 'Source Code Pro', 'Cascadia Code', 'Consolas', monospace",
        }}
      >
        {filteredLines.length === 0 ? (
          <p className="p-4 text-muted-foreground">No logs available yet.</p>
        ) : (
          <div className="p-2">
            {filteredLines.map((line, i) => (
              <div key={i} className="flex hover:bg-muted/30">
                <span
                  className="mr-3 select-none text-right text-muted-foreground/40"
                  style={{ minWidth: "3ch" }}
                >
                  {i + 1}
                </span>
                <span className="break-all">
                  <HighlightedLine line={line} />
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      <p className="text-xs text-muted-foreground">
        {filteredLines.length} lines
        {level !== "all" ? ` (${level})` : ""}
        {search ? ` matching "${search}"` : ""}
      </p>
    </div>
  );
}
