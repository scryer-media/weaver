import { useState } from "react";
import { cn } from "@/lib/utils";
import { EmptyState } from "../components/chrome";
import { SecondaryButton, TextField } from "../components/controls";
import { formatCount } from "../data/format";
import { LOG_LEVEL_COLORS, WV } from "../data/palette";
import {
  LOG_LEVELS,
  useServiceLogs,
  type LogKeyValue,
  type LogLevelFilter,
  type LogLine,
} from "../data/use-service-logs";
import { NextShell } from "../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../shell/rail-blocks";

const CHIP_OFF = "#3a414f";

const FILTERS: LogLevelFilter[] = ["all", ...LOG_LEVELS];

/** Split the message so its `key=value` tail can be tinted separately. */
function messageFragments(line: LogLine) {
  const fragments: { text: string; kv: LogKeyValue | null }[] = [];
  let cursor = 0;
  for (const pair of line.kvPairs) {
    if (pair.start > cursor) {
      fragments.push({ text: line.message.slice(cursor, pair.start), kv: null });
    }
    fragments.push({ text: line.message.slice(pair.start, pair.end), kv: pair });
    cursor = pair.end;
  }
  if (cursor < line.message.length) {
    fragments.push({ text: line.message.slice(cursor), kv: null });
  }
  return fragments;
}

export function LogsPage() {
  const [level, setLevel] = useState<LogLevelFilter>("all");
  const [query, setQuery] = useState("");
  const logs = useServiceLogs(level, query);

  return (
    <NextShell
      title="Logs"
      note={`live tail · ${formatCount(logs.bufferedCount)} lines buffered`}
      controls={
        <>
          <TextField
            label="Search logs"
            placeholder="Search message, target, field"
            value={query}
            onChange={setQuery}
            className="w-[150px] sm:w-[260px]"
          />
          <SecondaryButton onClick={() => logs.setPaused(!logs.paused)}>
            <span
              aria-hidden="true"
              className="mr-[9px] size-1.5"
              style={{ background: logs.paused ? "#75726b" : WV.accent }}
            />
            {logs.paused ? "Resume tail" : "Pause tail"}
          </SecondaryButton>
        </>
      }
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={
        <div className="flex h-10 flex-none items-center border-b border-wv-hairline bg-wv-list px-4 sm:px-6">
          <div className="wv-xscroll flex min-w-0 items-center gap-4 sm:gap-5">
          {FILTERS.map((entry) => {
            const selected = entry === level;
            const color = entry === "all" ? WV.accent : LOG_LEVEL_COLORS[entry]!;
            return (
              <button
                key={entry}
                type="button"
                aria-pressed={selected}
                onClick={() => setLevel(entry)}
                className={cn(
                  "flex flex-none items-center gap-[7px] font-wv-mono text-[11.5px] tracking-[0.1em] uppercase",
                  selected ? "text-wv-strong" : "text-wv-muted hover:text-wv-secondary",
                )}
              >
                <span
                  aria-hidden="true"
                  className="size-1.5 flex-none"
                  style={{ background: selected ? color : CHIP_OFF }}
                />
                {entry}
                <span className="text-wv-faint">{logs.counts[entry]}</span>
              </button>
            );
          })}
          </div>
          <span className="ml-auto hidden flex-none pl-3 font-wv-mono text-[11px] text-wv-faint sm:inline">
            {logs.paused
              ? "paused — scroll freely"
              : logs.connected
                ? "following new lines"
                : "tail disconnected"}
          </span>
        </div>
      }
      statusRight={`${formatCount(logs.matchedCount)} shown of ${formatCount(logs.bufferedCount)}`}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        {logs.lines.length === 0 ? (
          <EmptyState
            title="No lines match this filter"
            body="Clear the search or pick another level."
          />
        ) : (
          logs.lines.map((line) => (
            <div
              key={line.id}
              className="flex flex-wrap gap-x-[14px] gap-y-0.5 border-b border-wv-log-line px-4 sm:px-6 py-1.5 font-wv-mono text-[11.5px] leading-[1.55] sm:flex-nowrap"
            >
              <span className="w-[34px] flex-none text-right text-wv-dim">{line.id + 1}</span>
              <span className="flex-none text-wv-faint">{line.time}</span>
              <span
                className="w-[44px] flex-none font-semibold uppercase"
                style={{ color: LOG_LEVEL_COLORS[line.level] }}
              >
                {line.level}
              </span>
              <span className="min-w-0 flex-1 truncate text-wv-slate sm:flex-none">
                {line.target}
              </span>
              <span className="w-full min-w-0 break-words text-wv-secondary sm:w-auto sm:flex-1">
                {messageFragments(line).map((fragment, index) => (
                  <span key={index} className={fragment.kv ? "text-wv-info" : undefined}>
                    {fragment.text}
                  </span>
                ))}
              </span>
            </div>
          ))
        )}
      </div>
    </NextShell>
  );
}
