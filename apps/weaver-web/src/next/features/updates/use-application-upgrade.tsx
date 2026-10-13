import { useState } from "react";
import { useMutation, useQuery, useSubscription } from "urql";

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
} from "@/next/features/updates/application-upgrade";

/**
 * The in-application upgrade state and install action.
 *
 * The install action only ever sends back the release the server itself
 * advertised, so the UI cannot ask for a version the release checker has not
 * found. An installation someone else manages (a container, Homebrew, winget,
 * a Windows service) says so instead of offering a button that would be
 * refused.
 */

/** The server's upgrade state, kept live. */
export function useApplicationUpgradeStatus(): ApplicationUpgradeStatus | undefined {
  const [{ data: queryData }] = useQuery<{
    applicationUpgradeStatus: ApplicationUpgradeStatus;
  }>({ query: APPLICATION_UPGRADE_STATUS_QUERY, requestPolicy: "cache-and-network" });
  const [{ data: liveData }] = useSubscription<{
    applicationUpgradeUpdates: ApplicationUpgradeStatus;
  }>({ query: APPLICATION_UPGRADE_SUBSCRIPTION });
  return liveData?.applicationUpgradeUpdates ?? queryData?.applicationUpgradeStatus;
}

/** Upgrade state and the install action for the System Info section. */
export function useApplicationUpgrade() {
  const status = useApplicationUpgradeStatus();
  const [, startUpgrade] = useMutation(START_APPLICATION_UPGRADE_MUTATION);
  const [startError, setStartError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

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

/** The phase, and the download percentage once the artifact's size is known. */
export function RunProgress({
  run,
  className = "text-wv-muted",
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
