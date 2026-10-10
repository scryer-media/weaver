import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router";
import { gql, useClient } from "urql";
import { SCRIPT_RUNS_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { FireAndForgetTag, ScriptRunName, ScriptStatusMark, TruncatedTag, scriptRunName } from "../../../components/JobScriptResults";
import { Icon } from "../../../components/icons";
import { Pagination } from "../../../components/Pagination";
import { ScriptOutputLog } from "../../../components/ScriptOutputLog";
import { Tabs } from "../../../components/Tabs";
import { SecondaryButton, Select } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { EM_DASH, formatDate } from "../../../data/format";
import { SCRIPT_KINDS, SCRIPT_KIND_LABELS, type ScriptKind } from "../../../data/script-instances";
import { formatRunDuration, triggerLabel } from "../../../data/script-runs";
import { PanelControls, SettingsBlocks, usePanelStatus, type SettingsBlock } from "../framework";

/**
 * Runs: what each script did, whatever started it.
 *
 * One list, newest first, a page at a time. A job's own screen shows the runs
 * of that job; this one also holds the runs no job owns, such as a scan or a
 * scheduled task. Nothing here is edited, so a row opens in place to show the
 * run's output, which is read only once a row is opened.
 *
 * The tabs over the list narrow it to the runs that ended one way. Each shows
 * how many runs of the chosen trigger ended that way, whichever tab is open.
 */

/** No trigger chosen: every run is listed. */
const ANY_KIND = "";

/** No status chosen: runs are listed however they ended. */
const ANY_STATUS = "ALL";

/** How a run can end, in the order the tabs list them. */
const STATUSES = [
  "SUCCEEDED",
  "WARNING",
  "FAILED",
  "TIMED_OUT",
  "INTERRUPTED",
  "SKIPPED",
  "CANCELLED",
] as const;

type ScriptStatus = (typeof STATUSES)[number];

/** The tabs over the list: every run, then one tab for each way of ending. */
const STATUS_TABS = [ANY_STATUS, ...STATUSES] as const;

/** Translation keys. */
const STATUS_LABELS: Record<ScriptStatus | typeof ANY_STATUS, string> = {
  ALL: "next.scriptRuns.status.all",
  SUCCEEDED: "next.scriptRuns.status.succeeded",
  WARNING: "next.scriptRuns.status.warning",
  FAILED: "next.scriptRuns.status.failed",
  TIMED_OUT: "next.scriptRuns.status.timedOut",
  INTERRUPTED: "next.scriptRuns.status.interrupted",
  SKIPPED: "next.scriptRuns.status.skipped",
  CANCELLED: "next.scriptRuns.status.cancelled",
};

/** The page sizes the list offers; the first is where it starts. */
const PAGE_SIZES = [25, 50, 100] as const;

const OUTPUT_QUERY = gql`query ScriptRunOutput($outputId: String!) { scriptOutput(outputId: $outputId) }`;

interface ScriptRun {
  id: string;
  jobId: number | null;
  jobName: string | null;
  script: string;
  /** The script job that ran; null once it is gone, or for a run older than script jobs. */
  instanceId: string | null;
  instanceName: string | null;
  event: string;
  kind: ScriptKind;
  background: boolean;
  adapter: "SABNZBD" | "NZBGET" | null;
  status: string;
  exitCode: number | null;
  durationMs: number;
  outputTail: string;
  outputTruncated: boolean;
  outputRetained: boolean;
  errorMessage: string | null;
  finishedAtEpochMs: number;
}

interface ScriptRunPage {
  runs: ScriptRun[];
  nextBefore: string | null;
  total: number;
  /** How the runs of the chosen trigger ended; a status none ended with is absent. */
  statusCounts: { status: ScriptStatus; count: number }[];
}

const NO_RUNS: ScriptRunPage = { runs: [], nextBefore: null, total: 0, statusCounts: [] };

/** A run's output once its row has been opened: read, being read, or refused. */
type RunOutput =
  | { state: "loading" }
  | { state: "loaded"; output: string | null }
  | { state: "failed"; message: string };

/** The job a run belonged to: its name when the daemon still knows it. */
function jobLabel(run: ScriptRun): string {
  return run.jobName || `#${run.jobId}`;
}

/** What an open row shows: why it failed, how it ran, and what it printed. */
function RunDetail({
  run,
  output,
  onCopied,
}: {
  run: ScriptRun;
  output: RunOutput | undefined;
  onCopied: (copied: boolean) => void;
}) {
  const t = useTranslate();
  // The retained output, or the excerpt the run ended with when nothing more is kept.
  const retained = output?.state === "loaded" ? output.output : null;
  const reading = run.outputRetained && output?.state === "loading";
  const shown = retained ?? (reading ? null : run.outputTail);
  const gone = !run.outputRetained || (output?.state === "loaded" && output.output === null);
  const name = scriptRunName(run);
  const adapter = run.adapter === null ? null : run.adapter === "SABNZBD" ? "SABnzbd" : "NZBGet";
  const copy = () => {
    if (shown === null || !navigator.clipboard) {
      onCopied(false);
      return;
    }
    navigator.clipboard.writeText(shown).then(
      () => onCopied(true),
      () => onCopied(false),
    );
  };
  return (
    <div className="flex min-w-0 flex-col gap-2">
      {run.errorMessage ? (
        <span className="text-[12.5px] text-wv-error-text">{run.errorMessage}</span>
      ) : null}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
        <span className="font-wv-mono text-[11px] text-wv-muted">
          {[
            run.script,
            t(run.background ? "next.postProcessing.fireAndForget" : "next.postProcessing.blocking"),
            adapter,
            run.event,
          ]
            .filter((part) => part !== null)
            .join(" · ")}
        </span>
        {shown ? (
          <SecondaryButton size="compact" icon="copy" className="ml-auto" onClick={copy}>
            {t("next.scriptRuns.copyOutput")}
          </SecondaryButton>
        ) : null}
      </div>
      {shown ? <ScriptOutputLog output={shown} label={t("next.scriptRuns.outputOf", { name })} /> : null}
      {output?.state === "failed" ? (
        <span role="alert" className="text-[12.5px] text-wv-error-text">
          {output.message}
        </span>
      ) : null}
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 font-wv-mono text-[11px] text-wv-muted">
        {reading ? <span role="status">{t("next.scriptRuns.loadingOutput")}</span> : null}
        {gone ? <span>{t("next.job.scriptOutputGone")}</span> : null}
        {run.outputTruncated ? <span>{t("next.job.scriptOutputTruncated")}</span> : null}
      </div>
    </div>
  );
}

export function ScriptRunsPanel() {
  const t = useTranslate();
  const client = useClient();
  const [kind, setKind] = useState<ScriptKind | typeof ANY_KIND>(ANY_KIND);
  const [status, setStatus] = useState<ScriptStatus | typeof ANY_STATUS>(ANY_STATUS);
  const [pageSize, setPageSize] = useState<number>(PAGE_SIZES[0]);
  const [pageIndex, setPageIndex] = useState(0);
  const [page, setPage] = useState<ScriptRunPage | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState<boolean | null>(null);
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set());
  // A run's output never changes, so once read it is kept for as long as the
  // screen is up, and opening the row again does not ask for it again.
  const [outputs, setOutputs] = useState<ReadonlyMap<string, RunOutput>>(new Map());
  const outputsRef = useRef(outputs);
  outputsRef.current = outputs;
  // Where each page starts: the first at the top, every other below the last
  // run of the one before it. Only the pages reached so far are known.
  const cursorsRef = useRef<(string | null)[]>([null]);
  // Only the newest request may land: a page asked for under one trigger,
  // status or size must not replace the page of another.
  const requestRef = useRef(0);

  /** Shows page `target`, reading the pages before it first when their starts are not known yet. */
  const goTo = useCallback(
    async (target: number) => {
      const request = ++requestRef.current;
      setLoading(true);
      setError(null);
      setCopied(null);
      let index = Math.min(target, cursorsRef.current.length - 1);
      for (;;) {
        const result = await client
          .query<{ scriptRuns: ScriptRunPage }>(
            SCRIPT_RUNS_QUERY,
            {
              limit: pageSize,
              before: cursorsRef.current[index],
              kind: kind === ANY_KIND ? null : kind,
              status: status === ANY_STATUS ? null : status,
            },
            { requestPolicy: "network-only" },
          )
          .toPromise();
        if (request !== requestRef.current) {
          return;
        }
        if (result.error) {
          setLoading(false);
          setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
          setPage((current) => current ?? NO_RUNS);
          return;
        }
        const next = result.data?.scriptRuns ?? NO_RUNS;
        const cursors = cursorsRef.current.slice(0, index + 1);
        if (next.nextBefore !== null) {
          cursors.push(next.nextBefore);
        }
        cursorsRef.current = cursors;
        if (index >= target || next.nextBefore === null) {
          setLoading(false);
          setPage(next);
          setPageIndex(index);
          // A row opened on one page does not stay open over another.
          setOpen(new Set());
          return;
        }
        index += 1;
      }
    },
    [client, kind, pageSize, status],
  );

  // The first page, again whenever the trigger, the status or the page size
  // changes. The rows on screen stay until it lands.
  useEffect(() => {
    cursorsRef.current = [null];
    void goTo(0);
    return () => {
      requestRef.current += 1;
    };
  }, [goTo]);

  usePanelStatus(
    error ?? (copied === null ? null : t(copied ? "next.scriptRuns.copied" : "next.scriptRuns.copyBlocked")),
    error !== null || copied === false,
  );

  const readOutput = async (run: ScriptRun) => {
    // A read that failed is not kept: opening the row again asks once more.
    const held = outputsRef.current.get(run.id);
    if (!run.outputRetained || (held !== undefined && held.state !== "failed")) {
      return;
    }
    setOutputs((current) => new Map(current).set(run.id, { state: "loading" }));
    // A run is kept under its own id, which is what its output is asked for by.
    const result = await client
      .query<{ scriptOutput: string | null }>(OUTPUT_QUERY, { outputId: run.id }, { requestPolicy: "network-only" })
      .toPromise();
    const read: RunOutput = result.error
      ? { state: "failed", message: result.error.graphQLErrors[0]?.message ?? result.error.message }
      : { state: "loaded", output: result.data?.scriptOutput ?? null };
    setOutputs((current) => new Map(current).set(run.id, read));
  };

  const toggle = (id: string) => {
    const run = page?.runs.find((entry) => entry.id === id);
    if (!run) {
      return;
    }
    const opening = !open.has(id);
    setOpen((current) => {
      const next = new Set(current);
      if (opening) {
        next.add(id);
      } else {
        next.delete(id);
      }
      return next;
    });
    if (opening) {
      void readOutput(run);
    }
  };

  const kinds: { value: ScriptKind | typeof ANY_KIND; label: string }[] = [
    { value: ANY_KIND, label: t("next.scriptRuns.allTriggers") },
    ...SCRIPT_KINDS.map((value) => ({ value, label: t(SCRIPT_KIND_LABELS[value]) })),
  ];

  // Until the first page lands there is nothing to count.
  const ended = page === null ? null : new Map(page.statusCounts.map((entry) => [entry.status, entry.count]));
  const tabs = STATUS_TABS.map((id) => ({
    id,
    label: t(STATUS_LABELS[id]),
    count:
      ended === null
        ? undefined
        : id === ANY_STATUS
          ? [...ended.values()].reduce((sum, count) => sum + count, 0)
          : (ended.get(id) ?? 0),
  }));

  const runs = page?.runs ?? [];
  const total = page?.total ?? 0;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "runs",
      title: t("next.job.scriptRuns"),
      note: t("next.scriptRuns.note"),
      columns: "152px minmax(124px, 1fr) minmax(118px, 0.8fr) minmax(72px, 1.2fr) 78px 72px 72px",
      headers: [
        t("next.scriptRuns.finished"),
        t("next.scriptRuns.instance"),
        t("next.scriptRuns.trigger"),
        t("next.job.title"),
        t("table.status"),
        t("next.scriptRuns.exitCode"),
        t("next.scriptRuns.duration"),
      ],
      empty: t(kind === ANY_KIND && status === ANY_STATUS ? "next.job.noScriptRuns" : "next.scriptRuns.noneMatch"),
      onRowClick: toggle,
      rows: runs.map((run) => {
        const finished = formatDate(run.finishedAtEpochMs);
        const trigger = triggerLabel(t, run.event);
        const expanded = open.has(run.id);
        return {
          id: run.id,
          searchText: `${scriptRunName(run)} ${run.script}`,
          expanded,
          detail: expanded ? (
            <RunDetail run={run} output={outputs.get(run.id)} onCopied={setCopied} />
          ) : undefined,
          cells: [
            <span key="finished" className="flex min-w-0 items-center gap-2">
              <Icon
                name="expand"
                size={12}
                className={cn("flex-none text-wv-muted transition-transform", expanded && "rotate-90")}
              />
              <Cell mono className="text-wv-muted" title={finished}>
                {finished}
              </Cell>
            </span>,
            <div key="script" className="flex min-w-0 flex-wrap items-center gap-x-[10px] gap-y-1">
              <ScriptRunName run={run} />
              {run.background ? <FireAndForgetTag /> : null}
              {run.outputTruncated ? <TruncatedTag /> : null}
            </div>,
            <Cell key="trigger" className="text-[12.5px] text-wv-secondary" title={trigger}>
              {trigger}
            </Cell>,
            run.jobId === null ? (
              <Cell key="job" mono className="text-wv-faint">
                {EM_DASH}
              </Cell>
            ) : (
              // The link goes to the job; it must not also open the run under it.
              <Link
                key="job"
                to={`/jobs/${run.jobId}`}
                title={jobLabel(run)}
                onClick={(event) => event.stopPropagation()}
                onKeyDown={(event) => event.stopPropagation()}
                className="min-w-0 truncate text-wv-accent hover:underline"
              >
                {jobLabel(run)}
              </Link>
            ),
            <ScriptStatusMark key="status" status={run.status} />,
            <Cell key="exitCode" mono className="text-wv-muted">
              {run.exitCode ?? EM_DASH}
            </Cell>,
            <Cell key="duration" mono className="text-wv-muted">
              {formatRunDuration(run.durationMs)}
            </Cell>,
          ],
        };
      }),
    },
  ];

  return (
    <>
      <PanelControls>
        <Select
          label={t("next.scriptRuns.filter")}
          value={kind}
          className="min-w-0 sm:min-w-[180px]"
          options={kinds}
          onChange={setKind}
        />
        <SecondaryButton
          icon="refresh"
          onClick={() => {
            cursorsRef.current = [null];
            void goTo(0);
          }}
        >
          {t("action.refresh")}
        </SecondaryButton>
      </PanelControls>

      <Tabs tabs={tabs} active={status} onSelect={setStatus} />

      <SettingsBlocks blocks={blocks} loading={page === null} />

      {page === null ? null : (
        <div aria-busy={loading}>
          <Pagination
            pageIndex={pageIndex}
            pageCount={pageCount}
            onPage={(next) => void goTo(Math.max(0, Math.min(pageCount - 1, next)))}
            pageSize={pageSize}
            pageSizes={PAGE_SIZES}
            onPageSize={setPageSize}
            total={total}
          />
        </div>
      )}
    </>
  );
}
