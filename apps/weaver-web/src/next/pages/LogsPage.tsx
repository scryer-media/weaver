import { memo, useLayoutEffect, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useMutation, useQuery } from "urql";
import { LOG_FILTER_QUERY, SET_LOG_FILTER_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { EmptyState } from "../components/chrome";
import { SecondaryButton, Select, TextField } from "../components/controls";
import { formatCount } from "../data/format";
import { LOG_LEVEL_COLORS, WV } from "../data/palette";
import { countLabel } from "../i18n/labels";
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

/** How close to the bottom still counts as following the tail. */
const TAIL_SLACK_PX = 24;

/** A line that does not wrap. Wrapped lines are measured once they render. */
const ROW_ESTIMATE_PX = 31;

const FILTERS: LogLevelFilter[] = ["all", ...LOG_LEVELS];

type LogFilterPreset = "default" | "download" | "directStore" | "everything" | "custom";

/** Directives per preset. Blank asks the server for its startup filter. */
const PRESET_DIRECTIVES: Record<Exclude<LogFilterPreset, "custom">, string> = {
  default: "",
  download: "info,weaver_server_core::pipeline::download=debug,weaver_nntp=debug",
  directStore: "info,weaver_server_core::pipeline::direct_store=debug",
  everything: "debug",
};

type LogFilterState = { directives: string; defaultDirectives: string };

function presetFor(state: LogFilterState): LogFilterPreset {
  if (state.directives === state.defaultDirectives) {
    return "default";
  }
  for (const [preset, directives] of Object.entries(PRESET_DIRECTIVES)) {
    if (directives !== "" && directives === state.directives) {
      return preset as LogFilterPreset;
    }
  }
  return "custom";
}

/**
 * The live log level. The server answers `logFilter` for an admin only, so
 * anyone else never sees the control. A change lasts until the next restart.
 */
function LogFilterBar() {
  const t = useTranslate();
  const [{ data, error: loadError }] = useQuery<{ logFilter: LogFilterState }>({
    query: LOG_FILTER_QUERY,
  });
  const [{ fetching }, setLogFilter] = useMutation<{ setLogFilter: LogFilterState }>(
    SET_LOG_FILTER_MUTATION,
  );
  const [applied, setApplied] = useState<LogFilterState | null>(null);
  // `null` until edited: the field shows what is in force.
  const [edited, setEdited] = useState<string | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  const current = applied ?? data?.logFilter ?? null;

  if (loadError || !current) {
    return null;
  }
  const draft = edited ?? current.directives;

  const apply = async (directives: string) => {
    setFailure(null);
    const result = await setLogFilter({ directives });
    if (result.error || !result.data) {
      setFailure(
        t("next.logs.filter.failed", {
          error: result.error?.graphQLErrors[0]?.message ?? result.error?.message ?? "",
        }),
      );
      return;
    }
    setApplied(result.data.setLogFilter);
    setEdited(null);
  };

  const options: { value: LogFilterPreset; label: string }[] = [
    { value: "default", label: t("next.logs.filter.preset.default") },
    { value: "download", label: t("next.logs.filter.preset.download") },
    { value: "directStore", label: t("next.logs.filter.preset.directStore") },
    { value: "everything", label: t("next.logs.filter.preset.everything") },
    { value: "custom", label: t("next.logs.filter.preset.custom") },
  ];
  const onPreset = (preset: LogFilterPreset) => {
    if (preset === "custom") {
      return;
    }
    void apply(PRESET_DIRECTIVES[preset]);
  };

  return (
    <div className="flex min-h-10 flex-none flex-wrap items-center gap-3 border-b border-wv-hairline bg-wv-list px-4 py-1.5 sm:px-6">
      <span className="flex-none font-wv-mono text-[11.5px] tracking-[0.1em] text-wv-muted uppercase">
        {t("next.logs.filter.label")}
      </span>
      <Select
        label={t("next.logs.filter.label")}
        value={presetFor(current)}
        options={options}
        onChange={onPreset}
        className="w-[190px]"
      />
      <TextField
        label={t("next.logs.filter.directives")}
        placeholder={current.defaultDirectives}
        value={draft}
        onChange={setEdited}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            void apply(draft);
          }
        }}
        className="min-w-0 flex-1 sm:max-w-[420px]"
      />
      <SecondaryButton
        onClick={() => void apply(draft)}
        disabled={fetching || draft.trim() === current.directives}
      >
        {t("next.logs.filter.apply")}
      </SecondaryButton>
      <span className="flex-none font-wv-mono text-[11px] text-wv-faint">
        {failure ?? t("next.logs.filter.note")}
      </span>
    </div>
  );
}

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

/**
 * One line of the log. A buffered line is never edited, so a row renders once
 * for as long as its line stays in view.
 */
const LogRow = memo(function LogRow({ line }: { line: LogLine }) {
  return (
    <div className="flex flex-wrap gap-x-[14px] gap-y-0.5 border-b border-wv-log-line px-4 sm:px-6 py-1.5 font-wv-mono text-[11.5px] leading-[1.55] sm:flex-nowrap">
      <span className="w-[34px] flex-none text-right text-wv-dim">{line.id + 1}</span>
      <span className="flex-none text-wv-faint">{line.time}</span>
      <span
        className="w-[44px] flex-none font-semibold uppercase"
        style={{ color: LOG_LEVEL_COLORS[line.level] }}
      >
        {line.level}
      </span>
      <span className="min-w-0 flex-1 truncate text-wv-slate sm:flex-none">{line.target}</span>
      <span className="w-full min-w-0 break-words text-wv-secondary sm:w-auto sm:flex-1">
        {messageFragments(line).map((fragment, index) => (
          <span key={index} className={fragment.kv ? "text-wv-info" : undefined}>
            {fragment.text}
          </span>
        ))}
      </span>
    </div>
  );
});

export function LogsPage() {
  const t = useTranslate();
  const [level, setLevel] = useState<LogLevelFilter>("all");
  const [query, setQuery] = useState("");
  const logs = useServiceLogs(level, query);

  // The freshest line is at the bottom, so the page opens there and stays
  // there as lines arrive — unless someone has scrolled up to read, in which
  // case new lines must not pull the text out from under them. A new filter is
  // a new view, and starts at its own tail.
  //
  // The buffer holds thousands of lines and grows several times a second, so
  // only the lines in view are in the document: a row per buffered line meant
  // a page-sized layout on every batch, which is enough to stall the window.
  const scrollerRef = useRef<HTMLDivElement>(null);
  const followingRef = useRef(true);
  const lines = logs.lines;
  const virtualizer = useVirtualizer({
    count: lines.length,
    getScrollElement: () => scrollerRef.current,
    getItemKey: (index) => lines[index]?.id ?? index,
    estimateSize: () => ROW_ESTIMATE_PX,
    overscan: 20,
    useFlushSync: false,
  });
  const totalSize = virtualizer.getTotalSize();
  useLayoutEffect(() => {
    followingRef.current = true;
  }, [level, query]);
  // Pinned again whenever the height changes as well as when lines arrive:
  // the rows at the tail are measured after they render, and a wrapped one is
  // taller than its estimate.
  useLayoutEffect(() => {
    const scroller = scrollerRef.current;
    if (scroller && followingRef.current) {
      scroller.scrollTop = scroller.scrollHeight;
    }
  }, [lines, totalSize, level, query]);
  const onScroll = () => {
    const scroller = scrollerRef.current;
    if (scroller) {
      followingRef.current =
        scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight <= TAIL_SLACK_PX;
    }
  };

  return (
    <NextShell
      title={t("next.nav.logs")}
      note={countLabel(t, "next.logs.buffered", logs.bufferedCount, {
        count: formatCount(logs.bufferedCount),
      })}
      controls={
        <>
          <TextField
            label={t("next.logs.search")}
            placeholder={t("next.logs.searchPlaceholder")}
            value={query}
            onChange={setQuery}
            className="w-[150px] sm:w-[260px]"
          />
          <SecondaryButton icon={logs.paused ? "resume" : "pause"} onClick={() => logs.setPaused(!logs.paused)}>
            {logs.paused ? t("next.logs.resumeTail") : t("next.logs.pauseTail")}
          </SecondaryButton>
        </>
      }
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={
        <>
        <LogFilterBar />
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
                {entry === "all" ? t("next.logs.all") : entry}
                <span className="text-wv-faint">{logs.counts[entry]}</span>
              </button>
            );
          })}
          </div>
          <span className="ml-auto hidden flex-none pl-3 font-wv-mono text-[11px] text-wv-faint sm:inline">
            {logs.paused
              ? t("next.logs.pausedNote")
              : logs.connected
                ? t("next.logs.following")
                : t("next.logs.disconnected")}
          </span>
        </div>
        </>
      }
      statusRight={t("next.logs.shown", {
        shown: formatCount(logs.matchedCount),
        total: formatCount(logs.bufferedCount),
      })}
    >
      <div
        ref={scrollerRef}
        onScroll={onScroll}
        className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list"
      >
        {logs.lines.length === 0 && logs.loading ? (
          <EmptyState loading title={t("next.common.loading")} body={t("next.logs.loadingBody")} />
        ) : logs.lines.length === 0 ? (
          <EmptyState
            title={t("next.logs.noMatch")}
            body={t("next.logs.noMatchBody")}
          />
        ) : (
          <div className="relative w-full flex-none" style={{ height: totalSize }}>
            {virtualizer.getVirtualItems().map((item) => (
              <div
                key={item.key}
                ref={virtualizer.measureElement}
                data-index={item.index}
                className="absolute top-0 left-0 w-full"
                style={{ transform: `translateY(${item.start}px)` }}
              >
                <LogRow line={lines[item.index]!} />
              </div>
            ))}
          </div>
        )}
      </div>
    </NextShell>
  );
}
