import { useCallback, useState } from "react";
import { useClient } from "urql";
import { JOB_SUPPORT_REPORT_QUERY } from "@/graphql/queries";
import { saveBlobAsDownload } from "@/lib/download";

/**
 * A report the daemon writes about an NZB, and about the job that downloaded
 * it when there is one. The daemon leaves out every name, path, host and
 * password, so both forms are safe to paste where support happens.
 */
export interface SupportReport {
  /** Compact text, at most 80 columns. */
  text: string;
  /** Every field of the report, as pretty-printed JSON. */
  json: string;
}

export interface JobSupportReportResponse {
  jobSupportReport: SupportReport;
}

export interface AnalyzeNzbResponse {
  analyzeNzb: SupportReport;
}

/** The text wrapped in a fence, so a paste into an issue or chat keeps its columns. */
export function fencedReport(report: SupportReport): string {
  const body = report.text.endsWith("\n") ? report.text : `${report.text}\n`;
  return `\`\`\`\n${body}\`\`\`\n`;
}

/** Copy the text form; resolves false when the browser refuses the clipboard. */
export async function copySupportReport(report: SupportReport): Promise<boolean> {
  if (!navigator.clipboard) {
    return false;
  }
  try {
    await navigator.clipboard.writeText(fencedReport(report));
    return true;
  } catch {
    return false;
  }
}

function reportFingerprint(report: SupportReport): string | null {
  try {
    const parsed = JSON.parse(report.json) as {
      nzb?: { identity?: { fingerprint?: unknown } };
    };
    const fingerprint = parsed.nzb?.identity?.fingerprint;
    return typeof fingerprint === "string" && /^[0-9a-f]+$/.test(fingerprint)
      ? fingerprint.slice(0, 12)
      : null;
  } catch {
    return null;
  }
}

/** Save the JSON form under a name that carries no title. */
export function downloadSupportReportJson(report: SupportReport, jobId?: number) {
  const stem = jobId === undefined ? reportFingerprint(report) ?? "nzb" : `job-${jobId}`;
  saveBlobAsDownload(
    new Blob([report.json], { type: "application/json" }),
    `weaver-support-${stem}.json`,
  );
}

/**
 * Fetches a job's report on demand, never cached: the job section changes as
 * the job runs, and a stale copy is the wrong thing to hand to support.
 */
export function useJobSupportReport(jobId: number) {
  const client = useClient();
  const [busy, setBusy] = useState(false);

  const fetchReport = useCallback(async (): Promise<SupportReport> => {
    setBusy(true);
    try {
      const result = await client
        .query<JobSupportReportResponse>(
          JOB_SUPPORT_REPORT_QUERY,
          { jobId },
          { requestPolicy: "network-only" },
        )
        .toPromise();
      if (result.error || !result.data?.jobSupportReport) {
        throw new Error(result.error?.message ?? "no report");
      }
      return result.data.jobSupportReport;
    } finally {
      setBusy(false);
    }
  }, [client, jobId]);

  return { busy, fetchReport };
}
