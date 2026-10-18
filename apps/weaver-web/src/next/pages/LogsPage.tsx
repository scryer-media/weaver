// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import {
  createContext,
  useContext,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type Context,
  type ReactNode,
} from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useMutation, useQuery } from "urql";
import { LOG_FILTER_QUERY, SET_LOG_FILTER_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { EmptyState } from "../components/chrome";
import { SecondaryButton, Select, TextField } from "../components/controls";
import { LogRow } from "../components/LogRow";
import { formatCount } from "../data/format";
import { LOG_LEVEL_COLORS, WV } from "../data/palette";
import { countLabel } from "../i18n/labels";
import { sameElements, shallowEqual, useSettled } from "../data/settled";
import {
  LOG_LEVELS,
  useServiceLogs,
  type LogLevelFilter,
  type ServiceLogs,
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

export function LogsPage() {
  const t = useTranslate();
  const [level, setLevel] = useState<LogLevelFilter>("all");
  const [query, setQuery] = useState("");

  return (
    <ServiceLogsFeed level={level} query={query}>
      <NextShell
        title={t("next.nav.logs")}
        note={<BufferedNote />}
        controls={
          <>
            <TextField
              label={t("next.logs.search")}
              placeholder={t("next.logs.searchPlaceholder")}
              value={query}
              onChange={setQuery}
              className="w-[150px] sm:w-[260px]"
            />
            <TailPauseButton />
          </>
        }
        railMiddle={<AttentionBlock />}
        railFooter={<UptimeBlock />}
        beforeContent={
          <>
            <LogFilterBar />
            <LevelBar level={level} onLevel={setLevel} />
          </>
        }
        statusRight={<ShownCount />}
      >
        <LogList level={level} query={query} />
      </NextShell>
    </ServiceLogsFeed>
  );
}

type LogLinesView = Pick<ServiceLogs, "lines" | "loading">;
type LogCountsView = Pick<ServiceLogs, "counts" | "bufferedCount" | "matchedCount">;
type LogTailView = Pick<ServiceLogs, "paused" | "setPaused" | "connected">;

const LogLinesContext = createContext<LogLinesView | null>(null);
const LogCountsContext = createContext<LogCountsView | null>(null);
const LogTailContext = createContext<LogTailView | null>(null);

function useLogsPart<T>(context: Context<T | null>): T {
  const value = useContext(context);
  if (value === null) {
    throw new Error("useLogsPart must be used inside ServiceLogsFeed");
  }
  return value;
}

/**
 * Owns the tail and hands it to the screen in three parts: the lines, the
 * counts and the tail's own state. The screen inside is passed through as it
 * is, so a batch of lines draws again only what reads a part that changed,
 * not the shell around it.
 */
function ServiceLogsFeed({
  level,
  query,
  children,
}: {
  level: LogLevelFilter;
  query: string;
  children: ReactNode;
}) {
  const logs = useServiceLogs(level, query);
  const lines = useSettled(logs.lines, sameElements);
  const counts = useSettled(logs.counts, shallowEqual);
  const { loading, bufferedCount, matchedCount, paused, setPaused, connected } = logs;
  const linesView = useMemo(() => ({ lines, loading }), [lines, loading]);
  const countsView = useMemo(
    () => ({ counts, bufferedCount, matchedCount }),
    [bufferedCount, counts, matchedCount],
  );
  const tailView = useMemo(() => ({ paused, setPaused, connected }), [connected, paused, setPaused]);
  return (
    <LogLinesContext.Provider value={linesView}>
      <LogCountsContext.Provider value={countsView}>
        <LogTailContext.Provider value={tailView}>{children}</LogTailContext.Provider>
      </LogCountsContext.Provider>
    </LogLinesContext.Provider>
  );
}

function BufferedNote() {
  const t = useTranslate();
  const { bufferedCount } = useLogsPart(LogCountsContext);
  return countLabel(t, "next.logs.buffered", bufferedCount, { count: formatCount(bufferedCount) });
}

function ShownCount() {
  const t = useTranslate();
  const { matchedCount, bufferedCount } = useLogsPart(LogCountsContext);
  return t("next.logs.shown", {
    shown: formatCount(matchedCount),
    total: formatCount(bufferedCount),
  });
}

function TailPauseButton() {
  const t = useTranslate();
  const { paused, setPaused } = useLogsPart(LogTailContext);
  return (
    <SecondaryButton icon={paused ? "resume" : "pause"} onClick={() => setPaused(!paused)}>
      {paused ? t("next.logs.resumeTail") : t("next.logs.pauseTail")}
    </SecondaryButton>
  );
}

/** The level chips with their counts, and whether the tail is following. */
function LevelBar({
  level,
  onLevel,
}: {
  level: LogLevelFilter;
  onLevel: (level: LogLevelFilter) => void;
}) {
  const t = useTranslate();
  const { counts } = useLogsPart(LogCountsContext);
  const { paused, connected } = useLogsPart(LogTailContext);
  return (
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
              onClick={() => onLevel(entry)}
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
              <span className="text-wv-faint">{counts[entry]}</span>
            </button>
          );
        })}
      </div>
      <span className="ml-auto hidden flex-none pl-3 font-wv-mono text-[11px] text-wv-faint sm:inline">
        {paused
          ? t("next.logs.pausedNote")
          : connected
            ? t("next.logs.following")
            : t("next.logs.disconnected")}
      </span>
    </div>
  );
}

/**
 * The lines, of which only those in view are in the document.
 *
 * The freshest line is at the bottom, so the page opens there and stays
 * there as lines arrive — unless someone has scrolled up to read, in which
 * case new lines must not pull the text out from under them. A new filter is
 * a new view, and starts at its own tail.
 *
 * The buffer holds thousands of lines and grows several times a second, so
 * only the lines in view are in the document: a row per buffered line meant
 * a page-sized layout on every batch, which is enough to stall the window.
 * The virtualizer is kept to this component, which redraws on every scroll.
 */
function LogList({ level, query }: { level: LogLevelFilter; query: string }) {
  const t = useTranslate();
  const { lines, loading } = useLogsPart(LogLinesContext);
  const scrollerRef = useRef<HTMLDivElement>(null);
  const followingRef = useRef(true);
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
    <div
      ref={scrollerRef}
      onScroll={onScroll}
      className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list"
    >
      {lines.length === 0 && loading ? (
        <EmptyState loading title={t("next.common.loading")} body={t("next.logs.loadingBody")} />
      ) : lines.length === 0 ? (
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
  );
}
