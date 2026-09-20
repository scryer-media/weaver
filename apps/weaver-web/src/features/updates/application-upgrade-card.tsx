import { useState } from "react";
import { CircleArrowUp } from "lucide-react";
import { useMutation, useQuery, useSubscription } from "urql";

import { SectionCard } from "@/components/SectionCard";
import { Button } from "@/components/ui/button";
import {
  APPLICATION_UPGRADE_STATUS_QUERY,
  APPLICATION_UPGRADE_SUBSCRIPTION,
  START_APPLICATION_UPGRADE_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import {
  downloadPercent,
  installableUpgrade,
  type ApplicationUpgradeRun,
  type ApplicationUpgradeStatus,
} from "@/features/updates/application-upgrade";

/**
 * The in-application upgrade, on the page that already answers "what am I
 * running".
 *
 * The button only ever sends back the release the server itself advertised, so
 * the UI cannot ask for a version the release checker has not found. An
 * installation someone else manages (a container, Homebrew, winget, a Windows
 * service) says so instead of offering a button that would be refused.
 */
/**
 * Upgrade state and the install action, shared by the classic card and the
 * Next UI's System Info section so both offer the same button for the same
 * release.
 */
export function useApplicationUpgrade() {
  const [{ data: queryData }] = useQuery<{
    applicationUpgradeStatus: ApplicationUpgradeStatus;
  }>({ query: APPLICATION_UPGRADE_STATUS_QUERY, requestPolicy: "cache-and-network" });
  const [{ data: liveData }] = useSubscription<{
    applicationUpgradeUpdates: ApplicationUpgradeStatus;
  }>({ query: APPLICATION_UPGRADE_SUBSCRIPTION });
  const [, startUpgrade] = useMutation(START_APPLICATION_UPGRADE_MUTATION);
  const [startError, setStartError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  const status = liveData?.applicationUpgradeUpdates ?? queryData?.applicationUpgradeStatus;
  const installable = status ? installableUpgrade(status) : null;
  const run = status ? (status.activeRun ?? status.latestRun) : null;

  const install = async () => {
    if (!installable) return;
    setStarting(true);
    setStartError(null);
    const result = await startUpgrade({
      input: { expectedTag: installable.tag, expectedVersion: installable.version },
    });
    setStarting(false);
    if (result.error) {
      setStartError(result.error.graphQLErrors[0]?.message ?? result.error.message);
    }
  };

  return { status, installable, run, install, starting, startError };
}

export function ApplicationUpgradeCard() {
  const t = useTranslate();
  const { status, installable, run, install, starting, startError } = useApplicationUpgrade();
  if (!status) {
    return null;
  }

  return (
    <SectionCard title={t("applicationUpgrade.title")}>
      <div className="flex flex-col gap-3">
        <p className="text-sm text-muted-foreground">
          {status.updateAvailable && status.updateVersion
            ? t("applicationUpgrade.available", { version: status.updateVersion })
            : t("applicationUpgrade.upToDate", { version: status.currentVersion })}
        </p>

        {status.updateAvailable && !status.eligible ? (
          <p className="text-sm text-muted-foreground">
            {t("applicationUpgrade.notEligible", { reason: status.eligibilityReason })}
          </p>
        ) : null}

        {installable ? (
          <Button
            type="button"
            onClick={install}
            disabled={starting}
            className="w-fit gap-1.5"
          >
            <CircleArrowUp className="size-4" aria-hidden="true" />
            {t("applicationUpgrade.install", { version: installable.version })}
          </Button>
        ) : null}

        {status.activeRun ? <RunProgress run={status.activeRun} /> : null}

        {!status.activeRun && run?.status === "FAILED" ? (
          <p role="alert" className="text-sm text-status-failed">
            {t("applicationUpgrade.failed", {
              version: run.targetVersion,
              error: run.error ?? "",
            })}
          </p>
        ) : null}

        {!status.activeRun && run?.status === "COMPLETED" ? (
          <p className="text-sm text-muted-foreground">
            {t("applicationUpgrade.completed", { version: run.targetVersion })}
          </p>
        ) : null}

        {startError ? (
          <p role="alert" className="text-sm text-status-failed">
            {startError}
          </p>
        ) : null}
      </div>
    </SectionCard>
  );
}

/** The phase, and the download percentage once the artifact's size is known. */
export function RunProgress({
  run,
  className = "text-sm text-muted-foreground",
}: {
  run: ApplicationUpgradeRun;
  className?: string;
}) {
  const t = useTranslate();
  const percent = downloadPercent(run);
  return (
    <div role="status" className={className}>
      {t(`applicationUpgrade.phase.${run.phase}`, { version: run.targetVersion })}
      {run.phase === "downloading" && percent != null ? ` · ${percent}%` : null}
    </div>
  );
}
