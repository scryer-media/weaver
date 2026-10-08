import { useState } from "react";
import { gql, useQuery } from "urql";
import { POST_PROCESSING_RESULTS_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { WV } from "../data/palette";
import { countLabel } from "../i18n/labels";
import { DetailBlock, Square } from "./chrome";
import { Icon } from "./icons";

/**
 * A job's script runs, grouped by the event that started them.
 *
 * It is one more block of the job screen, so it takes that screen's rhythm: an
 * eyebrow and a count, hairline rows, and a run's state as a square and a word.
 */

interface ScriptResult {
  script: string;
  event: string;
  status: string;
  outputTail: string;
  outputId: string | null;
  outputRetained: boolean;
  outputTruncated: boolean;
  errorMessage: string | null;
  finishedAtEpochMs: number;
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

function RunOutput({ result }: { result: ScriptResult }) {
  const t = useTranslate();
  const [expanded, setExpanded] = useState(false);
  const [{ data, fetching, error }] = useQuery<{ scriptOutput: string | null }>({
    query: OUTPUT_QUERY, variables: { outputId: result.outputId ?? "" },
    pause: !expanded || !result.outputId,
  });
  const tone = STATUS_TONE[result.status] ?? QUIET_TONE;
  const retained = expanded ? data?.scriptOutput : undefined;
  // The excerpt stays up until the retained output arrives, so the row does not jump.
  const output = retained ?? result.outputTail;
  const gone = t("next.job.scriptOutputGone");
  return (
    <div className="flex min-w-0 flex-col gap-2 border-t border-wv-hairline py-[10px] pl-5">
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1">
        <span className="min-w-0 truncate font-wv-mono text-[12.5px] text-wv-fg">{result.script}</span>
        <span className="flex flex-none items-center gap-[7px]">
          <Square color={tone.color} />
          <span className={cn("font-wv-mono text-[11px]", tone.text)}>{result.status}</span>
        </span>
      </div>
      {result.errorMessage ? (
        <span className="text-[12.5px] text-wv-error-text">{result.errorMessage}</span>
      ) : null}
      {output ? (
        <pre className="max-h-80 overflow-auto bg-wv-input px-3 py-2 font-wv-mono text-[12px] leading-[1.55] break-words whitespace-pre-wrap text-wv-fg">
          {output}
        </pre>
      ) : null}
      <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 font-wv-mono text-[11px] text-wv-muted">
        {result.outputRetained && result.outputId ? (
          <button type="button" onClick={() => setExpanded(!expanded)} className={QUIET_ACTION}>
            {t(expanded ? "next.job.scriptOutputExcerpt" : "next.job.scriptOutputShow")}
          </button>
        ) : (
          <span>{gone}</span>
        )}
        {expanded && retained == null ? (
          <span>{fetching ? t("next.common.loading") : (error?.message ?? gone)}</span>
        ) : null}
        {result.outputTruncated ? <span>{t("next.job.scriptOutputTruncated")}</span> : null}
      </div>
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
