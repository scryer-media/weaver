import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router";
import { useClient } from "urql";
import { SCRIPT_RUNS_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { Dialog } from "../../../components/Dialog";
import {
  FireAndForgetTag,
  ScriptRunOutput,
  ScriptStatusMark,
} from "../../../components/JobScriptResults";
import { SecondaryButton, Select } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { EM_DASH, formatDate } from "../../../data/format";
import { formatRunDuration, triggerLabel } from "../../../data/script-runs";
import {
  FieldRows,
  PanelControls,
  SettingsBlocks,
  usePanelStatus,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * Runs: what each script did, whatever started it.
 *
 * One list, newest first, read a page at a time. A job's own screen shows the
 * runs of that job; this one also holds the runs no job owns, such as a scan
 * or a scheduled task. Nothing here is edited, so a row opens the run itself.
 */

type ScriptKind = "POST_PROCESSING" | "QUEUE" | "SCAN" | "SCHEDULER" | "FEED";

/** No trigger chosen: every run is listed. */
const ANY_KIND = "";

interface ScriptRun {
  id: string;
  jobId: number | null;
  jobName: string | null;
  script: string;
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
}

/** How many runs one request asks for. */
const PAGE_SIZE = 50;

/** The job a run belonged to: its name when the daemon still knows it. */
function jobLabel(run: ScriptRun): string {
  return run.jobName || `#${run.jobId}`;
}

function RunRecord({ run, onClose }: { run: ScriptRun; onClose: () => void }) {
  const t = useTranslate();
  const fields: FieldSpec[] = [
    {
      id: "finished",
      label: t("next.scriptRuns.finished"),
      control: { kind: "static", value: formatDate(run.finishedAtEpochMs) },
    },
    {
      id: "script",
      label: t("next.postProcessing.script"),
      control: { kind: "static", value: run.script },
    },
    {
      id: "trigger",
      label: t("next.scriptRuns.trigger"),
      control: { kind: "static", value: triggerLabel(t, run.event) },
    },
    {
      id: "runMode",
      label: t("next.postProcessing.runMode"),
      control: {
        kind: "static",
        value: t(run.background ? "next.postProcessing.fireAndForget" : "next.postProcessing.blocking"),
      },
    },
    {
      id: "job",
      label: t("next.job.title"),
      control:
        run.jobId === null
          ? { kind: "static", value: EM_DASH }
          : {
              kind: "custom",
              control: (
                <Link
                  to={`/jobs/${run.jobId}`}
                  className="max-w-[380px] text-right text-[12.5px] break-all text-wv-accent hover:underline"
                >
                  {jobLabel(run)}
                </Link>
              ),
            },
    },
    {
      id: "status",
      label: t("table.status"),
      control: { kind: "custom", control: <ScriptStatusMark status={run.status} /> },
    },
    {
      id: "exitCode",
      label: t("next.scriptRuns.exitCode"),
      control: { kind: "static", value: run.exitCode ?? EM_DASH },
    },
    {
      id: "duration",
      label: t("next.scriptRuns.duration"),
      control: { kind: "static", value: formatRunDuration(run.durationMs) },
    },
    {
      id: "adapter",
      label: t("next.postProcessing.adapter"),
      control: {
        kind: "static",
        value: run.adapter === null ? EM_DASH : run.adapter === "SABNZBD" ? "SABnzbd" : "NZBGet",
      },
    },
  ];
  return (
    <Dialog
      open
      title={run.script}
      note={run.event}
      width={680}
      onDismiss={onClose}
      footer={<SecondaryButton onClick={onClose}>{t("next.networking.close")}</SecondaryButton>}
    >
      <FieldRows fields={fields} />
      <div className="flex flex-none flex-col gap-2 px-4 py-4 sm:px-6">
        {run.errorMessage ? (
          <span className="text-[12.5px] text-wv-error-text">{run.errorMessage}</span>
        ) : null}
        {/* A run is kept under its own id, which is what its output is asked for by. */}
        <ScriptRunOutput run={{ ...run, outputId: run.id }} />
      </div>
    </Dialog>
  );
}

export function ScriptRunsPanel() {
  const t = useTranslate();
  const client = useClient();
  const [kind, setKind] = useState<ScriptKind | typeof ANY_KIND>(ANY_KIND);
  const [runs, setRuns] = useState<ScriptRun[] | null>(null);
  const [nextBefore, setNextBefore] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [opened, setOpened] = useState<ScriptRun | null>(null);
  // Only the newest request may land: a page asked for under one trigger must
  // not join the rows of another.
  const requestRef = useRef(0);

  const load = useCallback(
    async (before: string | null) => {
      const request = ++requestRef.current;
      setLoading(true);
      setError(null);
      const result = await client
        .query<{ scriptRuns: ScriptRunPage }>(
          SCRIPT_RUNS_QUERY,
          { limit: PAGE_SIZE, before, kind: kind === ANY_KIND ? null : kind },
          { requestPolicy: "network-only" },
        )
        .toPromise();
      if (request !== requestRef.current) {
        return;
      }
      setLoading(false);
      if (result.error) {
        setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
        setRuns((current) => current ?? []);
        return;
      }
      const page = result.data?.scriptRuns ?? { runs: [], nextBefore: null };
      setRuns((current) => (before === null ? page.runs : [...(current ?? []), ...page.runs]));
      setNextBefore(page.nextBefore);
    },
    [client, kind],
  );

  // The first page, again whenever the trigger changes. The rows on screen stay
  // until it lands.
  useEffect(() => {
    void load(null);
    return () => {
      requestRef.current += 1;
    };
  }, [load]);

  usePanelStatus(error, error !== null);

  const kinds: { value: ScriptKind | typeof ANY_KIND; label: string }[] = [
    { value: ANY_KIND, label: t("next.scriptRuns.allTriggers") },
    { value: "POST_PROCESSING", label: t("next.postProcessing.kindPostProcessing") },
    { value: "QUEUE", label: t("next.postProcessing.kindQueue") },
    { value: "SCAN", label: t("next.postProcessing.kindScan") },
    { value: "SCHEDULER", label: t("next.schedules.schedule") },
    { value: "FEED", label: t("next.postProcessing.kindFeed") },
  ];

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "runs",
      title: t("next.job.scriptRuns"),
      note: t("next.scriptRuns.note"),
      columns: "152px minmax(124px, 1fr) minmax(118px, 0.8fr) minmax(72px, 1.2fr) 78px 72px 72px",
      headers: [
        t("next.scriptRuns.finished"),
        t("next.postProcessing.script"),
        t("next.scriptRuns.trigger"),
        t("next.job.title"),
        t("table.status"),
        t("next.scriptRuns.exitCode"),
        t("next.scriptRuns.duration"),
      ],
      empty: t("next.job.noScriptRuns"),
      onRowClick: (id) => setOpened(runs?.find((run) => run.id === id) ?? null),
      footer:
        nextBefore === null ? undefined : (
          <SecondaryButton disabled={loading} onClick={() => void load(nextBefore)}>
            {t("next.scriptRuns.loadMore")}
          </SecondaryButton>
        ),
      rows: (runs ?? []).map((run) => {
        const finished = formatDate(run.finishedAtEpochMs);
        const trigger = triggerLabel(t, run.event);
        return {
          id: run.id,
          searchText: run.script,
          cells: [
            <Cell key="finished" mono className="text-wv-muted" title={finished}>
              {finished}
            </Cell>,
            <div key="script" className="flex min-w-0 flex-wrap items-center gap-x-[10px] gap-y-1">
              <Cell mono title={run.script}>
                {run.script}
              </Cell>
              {run.background ? <FireAndForgetTag /> : null}
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
        <SecondaryButton icon="refresh" onClick={() => void load(null)}>
          {t("action.refresh")}
        </SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={runs === null} />

      {opened ? <RunRecord key={opened.id} run={opened} onClose={() => setOpened(null)} /> : null}
    </>
  );
}
