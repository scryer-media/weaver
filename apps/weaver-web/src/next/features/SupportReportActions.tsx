import { useTranslate } from "@/lib/context/translate-context";
import { SecondaryButton } from "../components/controls";
import {
  copySupportReport,
  downloadSupportReportJson,
  useJobSupportReport,
} from "../data/support-report";

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
    try {
      const report = await fetchReport();
      onReport(
        (await copySupportReport(report)) ? t("next.support.copied") : t("next.support.copyBlocked"),
      );
    } catch {
      onReport(t("next.support.failed"));
    }
  };

  const download = async () => {
    try {
      downloadSupportReportJson(await fetchReport(), jobId);
      onReport(t("next.support.saved"));
    } catch {
      onReport(t("next.support.failed"));
    }
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
