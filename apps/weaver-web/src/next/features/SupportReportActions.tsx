import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { SecondaryButton } from "../components/controls";
import {
  copySupportReport,
  downloadSupportReportJson,
  useJobSupportReport,
  type SupportReport,
} from "../data/support-report";

// Outside the component, so the component itself compiles: what each action reports.

async function copyReport(t: Translate, fetchReport: () => Promise<SupportReport>): Promise<string> {
  try {
    const report = await fetchReport();
    return (await copySupportReport(report)) ? t("next.support.copied") : t("next.support.copyBlocked");
  } catch {
    return t("next.support.failed");
  }
}

async function downloadReport(t: Translate, fetchReport: () => Promise<SupportReport>, jobId: number): Promise<string> {
  try {
    downloadSupportReportJson(await fetchReport(), jobId);
    return t("next.support.saved");
  } catch {
    return t("next.support.failed");
  }
}

/**
 * "Copy support report" and "Download JSON" for one job, in any state.
 *
 * The outcome is reported through `onReport` so each surface shows it where
 * it already shows the result of its other actions.
 */
export function SupportReportActions({
  jobId,
  onReport,
  size = "default",
  className,
}: {
  jobId: number;
  onReport: (message: string) => void;
  size?: "default" | "compact";
  className?: string;
}) {
  const t = useTranslate();
  const { busy, fetchReport } = useJobSupportReport(jobId);

  const copy = async () => {
    onReport(await copyReport(t, fetchReport));
  };

  const download = async () => {
    onReport(await downloadReport(t, fetchReport, jobId));
  };

  return (
    <>
      <SecondaryButton
        icon="copy"
        size={size}
        className={className}
        disabled={busy}
        onClick={() => {
          void copy();
        }}
      >
        {t("next.support.copyReport")}
      </SecondaryButton>
      <SecondaryButton
        icon="downloadFile"
        size={size}
        className={className}
        disabled={busy}
        onClick={() => {
          void download();
        }}
      >
        {t("next.support.downloadJson")}
      </SecondaryButton>
    </>
  );
}
