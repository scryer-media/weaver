export type ScriptKind = "POST_PROCESSING" | "QUEUE" | "SCAN" | "SCHEDULER" | "FEED";

export interface ScriptDeclarations {
  kinds: ScriptKind[];
  queueEvents: string[];
  taskTimes: string[];
}

const labels: Record<ScriptKind, string> = {
  POST_PROCESSING: "Post-processing",
  QUEUE: "Queue",
  SCAN: "Scan",
  SCHEDULER: "Scheduler",
  FEED: "Feed",
};

export function ScriptKinds({ script }: { script: ScriptDeclarations }) {
  return (
    <div className="flex min-w-0 flex-col gap-1 whitespace-normal text-xs">
      <div className="flex flex-wrap gap-1">
        {script.kinds.map((kind) => (
          <span key={kind} className="rounded border px-1.5 py-0.5">{labels[kind]}</span>
        ))}
      </div>
      {script.kinds.includes("QUEUE") && (
        <span className="break-words">Declared events: {script.queueEvents.join(", ") || "None recognised"}</span>
      )}
      {script.taskTimes.length > 0 && <span>Task times: {script.taskTimes.join(", ")}</span>}
    </div>
  );
}
