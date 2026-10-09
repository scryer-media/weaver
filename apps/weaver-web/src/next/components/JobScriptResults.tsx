import { useState } from "react";
import { gql, useQuery } from "urql";
import { POST_PROCESSING_RESULTS_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { WV } from "../data/palette";
import { countLabel } from "../i18n/labels";
import { DetailBlock, Square, Tag } from "./chrome";
import { Icon } from "./icons";

/**
 * A job's script runs, grouped by the event that started them.
 *
 * It is one more block of the job screen, so it takes that screen's rhythm: an
 * eyebrow and a count, hairline rows, and a run's state as a square and a word.
 */

/** What a run's output is drawn from, wherever the run was listed. */
export interface ScriptRunOutputSource {
  outputTail: string;
  /** What the daemon keeps the full output under; null when it kept none. */
  outputId: string | null;
  outputRetained: boolean;
  outputTruncated: boolean;
}

/** The instance a run belonged to, when the daemon still knows it. */
export interface ScriptRunInstance {
  script: string;
  instanceId: string | null;
  instanceName: string | null;
}

/** What a run is called: its instance, or the script file when it had none. */
export function scriptRunName(run: ScriptRunInstance): string {
  return run.instanceName || run.script;
}

/** A run's name, with the script file beside it when the instance is named otherwise. */
export function ScriptRunName({ run }: { run: ScriptRunInstance }) {
  const name = scriptRunName(run);
  return (
    <span className="flex min-w-0 items-baseline gap-2">
      <span className="min-w-0 truncate font-wv-mono text-[12.5px] text-wv-fg" title={name}>{name}</span>
      {name === run.script ? null : (
        <span className="min-w-0 truncate font-wv-mono text-[11px] text-wv-muted" title={run.script}>{run.script}</span>
      )}
    </span>
  );
}

interface ScriptResult extends ScriptRunOutputSource, ScriptRunInstance {
  event: string;
  status: string;
  errorMessage: string | null;
  finishedAtEpochMs: number;
  background: boolean;
}

const OUTPUT_QUERY = gql`query ScriptOutput($outputId: String!) { scriptOutput(outputId: $outputId) }`;

/** How a run ended, as the colour of its square and of the word beside it. */
const STATUS_TONE: Record<string, { color: string; text: string }> = {
  SUCCEEDED: { color: WV.accent, text: "text-wv-accent" },
  WARNING: { color: WV.warn, text: "text-wv-warn" },
  FAILED: { color: WV.error, text: "text-wv-error-text" },
  TIMED_OUT: { color: WV.error, text: "text-wv-error-text" },
};

/** A run that was skipped or cancelled did nothing worth a colour. */
const QUIET_TONE = { color: WV.idle, text: "text-wv-muted" };

const QUIET_ACTION = "cursor-pointer text-wv-dim hover:text-wv-fg";

/** How a run ended: a square and the status beside it. */
export function ScriptStatusMark({ status }: { status: string }) {
  const tone = STATUS_TONE[status] ?? QUIET_TONE;
  return (
    <span className="flex flex-none items-center gap-[7px]">
      <Square color={tone.color} />
      <span className={cn("font-wv-mono text-[11px]", tone.text)}>{status}</span>
    </span>
  );
}

/** Marks a run nothing waited for. */
export function FireAndForgetTag() {
  const t = useTranslate();
  return (
    <span className="flex flex-none whitespace-nowrap">
      <Tag>{t("next.postProcessing.fireAndForget")}</Tag>
    </span>
  );
}

/** A run's output: the excerpt it ended with, and the retained output on request. */
export function ScriptRunOutput({ run }: { run: ScriptRunOutputSource }) {
  const t = useTranslate();
  const [expanded, setExpanded] = useState(false);
  const [{ data, fetching, error }] = useQuery<{ scriptOutput: string | null }>({
    query: OUTPUT_QUERY, variables: { outputId: run.outputId ?? "" },
    pause: !expanded || !run.outputId,
  });
  const retained = expanded ? data?.scriptOutput : undefined;
  // The excerpt stays up until the retained output arrives, so the row does not jump.
  const output = retained ?? run.outputTail;
  const gone = t("next.job.scriptOutputGone");
  return (
    <>
      {output ? (
        <pre className="max-h-80 overflow-auto bg-wv-input px-3 py-2 font-wv-mono text-[12px] leading-[1.55] break-words whitespace-pre-wrap text-wv-fg">
          {output}
        </pre>
      ) : null}
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 font-wv-mono text-[11px] text-wv-muted">
        {run.outputRetained && run.outputId ? (
          <button type="button" onClick={() => setExpanded(!expanded)} className={QUIET_ACTION}>
            {t(expanded ? "next.job.scriptOutputExcerpt" : "next.job.scriptOutputShow")}
          </button>
        ) : (
          <span>{gone}</span>
        )}
        {expanded && retained == null ? (
          <span>{fetching ? t("next.common.loading") : (error?.message ?? gone)}</span>
        ) : null}
        {run.outputTruncated ? <span>{t("next.job.scriptOutputTruncated")}</span> : null}
      </div>
    </>
  );
}

function RunOutput({ result }: { result: ScriptResult }) {
  return (
    <div className="flex min-w-0 flex-col gap-2 border-t border-wv-hairline py-[10px] pl-5">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1">
        <span className="flex min-w-0 items-center gap-[10px]">
          <ScriptRunName run={result} />
          {result.background ? <FireAndForgetTag /> : null}
        </span>
        <ScriptStatusMark status={result.status} />
      </div>
      {result.errorMessage ? (
        <span className="text-[12.5px] text-wv-error-text">{result.errorMessage}</span>
      ) : null}
      <ScriptRunOutput run={result} />
    </div>
  );
}

function EventRuns({ event, results }: { event: string; results: ScriptResult[] }) {
  const [open, setOpen] = useState(true);
  const label = `${event} (${results.length})`;
  return (
    <div role="group" aria-label={label} className="flex min-w-0 flex-col border-t border-wv-hairline">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        className="flex h-8 cursor-pointer items-center gap-2 text-left font-wv-mono text-[11px] text-wv-muted hover:text-wv-fg"
      >
        <Icon name="expand" size={12} className={cn("flex-none transition-transform", open && "rotate-90")} />
        {label}
      </button>
      {open
        ? results.map((result, index) => (
            <RunOutput key={result.outputId ?? `${result.script}-${result.finishedAtEpochMs}-${index}`} result={result} />
          ))
        : null}
    </div>
  );
}

export function JobScriptResults({ jobId }: { jobId: number }) {
  const t = useTranslate();
  const [{ data, error }, refresh] = useQuery<{ postProcessingResults: ScriptResult[] }>({
    query: POST_PROCESSING_RESULTS_QUERY, variables: { jobId }, requestPolicy: "cache-and-network",
  });
  const results = data?.postProcessingResults ?? [];
  const groups = new Map<string, ScriptResult[]>();
  for (const result of results) {
    const entries = groups.get(result.event) ?? [];
    entries.push(result);
    groups.set(result.event, entries);
  }
  return (
    <DetailBlock
      id="scripts"
      title={t("next.job.scriptRuns")}
      note={countLabel(t, "next.job.scriptRunCount", results.length)}
      right={
        <button type="button" onClick={() => refresh({ requestPolicy: "network-only" })} className={QUIET_ACTION}>
          {t("action.refresh")}
        </button>
      }
    >
      {error ? (
        <span role="alert" className="pb-3 text-[12.5px] text-wv-error-text">{error.message}</span>
      ) : null}
      {groups.size === 0 ? (
        <span className="text-[12.5px] text-wv-muted">{t("next.job.noScriptRuns")}</span>
      ) : null}
      {[...groups].map(([event, entries]) => (
        <EventRuns key={event} event={event} results={entries} />
      ))}
    </DetailBlock>
  );
}
