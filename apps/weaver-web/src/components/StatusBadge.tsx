import { JobStatusBadge } from "@/components/JobStatusBadge";

export function StatusBadge({ status }: { status: string }) {
  return <JobStatusBadge status={status} />;
}
