import { useState } from "react";
import { gql, useQuery } from "urql";
import { POST_PROCESSING_RESULTS_QUERY } from "@/graphql/queries";

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

function RunOutput({ result }: { result: ScriptResult }) {
  const [expanded, setExpanded] = useState(false);
  const [{ data, fetching, error }] = useQuery<{ scriptOutput: string | null }>({
    query: OUTPUT_QUERY, variables: { outputId: result.outputId ?? "" },
    pause: !expanded || !result.outputId,
  });
  return <div className="space-y-2 border p-3">
    <div className="flex flex-wrap justify-between gap-2 text-sm"><strong>{result.script}</strong><span>{result.status}</span></div>
    {result.errorMessage && <p className="text-sm">{result.errorMessage}</p>}
    <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words text-xs">{expanded ? data?.scriptOutput ?? (fetching ? "Loading output…" : error?.message ?? "Output is no longer retained.") : result.outputTail}</pre>
    {result.outputRetained && result.outputId
      ? <button className="text-sm underline" onClick={() => setExpanded(!expanded)}>{expanded ? "Show excerpt" : "Show retained output"}</button>
      : <p className="text-xs">Full output is no longer retained.</p>}
    {result.outputTruncated && <p className="text-xs">Capture limit reached; output was truncated.</p>}
  </div>;
}

export function JobScriptResults({ jobId }: { jobId: number }) {
  const [{ data, error }, refresh] = useQuery<{ postProcessingResults: ScriptResult[] }>({
    query: POST_PROCESSING_RESULTS_QUERY, variables: { jobId }, requestPolicy: "cache-and-network",
  });
  const groups = new Map<string, ScriptResult[]>();
  for (const result of data?.postProcessingResults ?? []) {
    const entries = groups.get(result.event) ?? [];
    entries.push(result);
    groups.set(result.event, entries);
  }
  return <section className="my-4 space-y-3 border p-4">
    <div className="flex justify-between"><h2 className="font-semibold">Script runs</h2><button className="text-sm underline" onClick={() => refresh({ requestPolicy: "network-only" })}>Refresh</button></div>
    {error && <p role="alert">{error.message}</p>}
    {groups.size === 0 && <p className="text-sm">No script runs recorded.</p>}
    {[...groups].map(([event, results]) => <details key={event} open className="space-y-2">
      <summary className="cursor-pointer text-sm font-medium">{event} ({results.length})</summary>
      {results.map((result, index) => <RunOutput key={result.outputId ?? `${result.script}-${result.finishedAtEpochMs}-${index}`} result={result} />)}
    </details>)}
  </section>;
}
