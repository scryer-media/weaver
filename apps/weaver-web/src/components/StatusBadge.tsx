const colors: Record<string, string> = {
  QUEUED: "bg-zinc-600 text-zinc-200",
  DOWNLOADING: "bg-blue-600 text-blue-100",
  VERIFYING: "bg-yellow-600 text-yellow-100",
  REPAIRING: "bg-orange-600 text-orange-100",
  EXTRACTING: "bg-purple-600 text-purple-100",
  COMPLETE: "bg-green-600 text-green-100",
  FAILED: "bg-red-600 text-red-100",
  PAUSED: "bg-zinc-600 text-zinc-300",
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span
      className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${colors[status] ?? "bg-zinc-700 text-zinc-300"}`}
    >
      {status}
    </span>
  );
}
