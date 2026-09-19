import { useApplicationUpgrade, RunProgress } from "@/features/updates/application-upgrade-card";
import { useTranslate } from "@/lib/context/translate-context";
import { SectionHeader } from "../components/chrome";
import { PrimaryButton } from "../components/controls";

/**
 * The in-application upgrade on the Next UI's System Info page: the same state
 * and install action as the classic card, drawn in the Next page's rows.
 */
export function ApplicationUpgradeSection() {
  const t = useTranslate();
  const { status, installable, run, install, starting, startError } = useApplicationUpgrade();
  if (!status) {
    return null;
  }

  const row = "border-b border-wv-hairline px-4 sm:px-6 py-[11px] text-[13px]";
  return (
    <>
      <SectionHeader label={t("applicationUpgrade.title")} />
      <div className={`${row} flex flex-wrap items-center gap-x-6 gap-y-2 text-wv-muted`}>
        <span className="min-w-0 flex-[1_1_260px]">
          {status.updateAvailable && status.updateVersion
            ? t("applicationUpgrade.available", { version: status.updateVersion })
            : t("applicationUpgrade.upToDate", { version: status.currentVersion })}
        </span>
        {installable ? (
          <PrimaryButton icon="update" disabled={starting} onClick={() => void install()}>
            {t("applicationUpgrade.install", { version: installable.version })}
          </PrimaryButton>
        ) : null}
      </div>

      {status.updateAvailable && !status.eligible ? (
        <div className={`${row} text-wv-muted`}>
          {t("applicationUpgrade.notEligible", { reason: status.eligibilityReason })}
        </div>
      ) : null}

      {status.activeRun ? (
        <RunProgress run={status.activeRun} className={`${row} text-wv-muted`} />
      ) : null}

      {!status.activeRun && run?.status === "FAILED" ? (
        <div role="alert" className={`${row} text-wv-error`}>
          {t("applicationUpgrade.failed", { version: run.targetVersion, error: run.error ?? "" })}
        </div>
      ) : null}

      {!status.activeRun && run?.status === "COMPLETED" ? (
        <div className={`${row} text-wv-muted`}>
          {t("applicationUpgrade.completed", { version: run.targetVersion })}
        </div>
      ) : null}

      {startError ? (
        <div role="alert" className={`${row} text-wv-error`}>
          {startError}
        </div>
      ) : null}
    </>
  );
}
