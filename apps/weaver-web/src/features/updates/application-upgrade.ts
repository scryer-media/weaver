/**
 * Client-side mirror of the server's in-application upgrade types.
 *
 * Field names are the camelCase form async-graphql emits, so a selection set
 * can be assigned to these types directly.
 */
export type ApplicationUpgradeRunStatus = "RUNNING" | "COMPLETED" | "FAILED";

export interface ApplicationUpgradeRun {
  runId: string;
  status: ApplicationUpgradeRunStatus;
  phase: string;
  downloadedBytes: number;
  totalBytes: number;
  targetVersion: string;
  targetTag: string;
  fromVersion: string;
  error: string | null;
  startedAtEpochMs: number;
  completedAtEpochMs: number | null;
}

export interface ApplicationUpgradeStatus {
  currentVersion: string;
  updateVersion: string | null;
  updateTag: string | null;
  updateAvailable: boolean;
  installationKind: string;
  managementOwner: string;
  eligible: boolean;
  eligibilityReason: string;
  activeRun: ApplicationUpgradeRun | null;
  latestRun: ApplicationUpgradeRun | null;
}

/**
 * The release this installation may install, or `undefined`.
 *
 * Deliberately stricter than `updateAvailable`: the Install button sends the
 * tag and version back to the server, so both have to be present, the
 * installation has to be one the server manages itself, and no run may already
 * be in flight. A status missing any of that renders the plain state instead of
 * a button that would only be refused.
 */
export function installableUpgrade(
  status: ApplicationUpgradeStatus | undefined,
): { version: string; tag: string } | undefined {
  if (!status?.updateAvailable || !status.eligible || status.activeRun) {
    return undefined;
  }
  const version = status.updateVersion;
  const tag = status.updateTag;
  if (!version || !tag) {
    return undefined;
  }
  return { version, tag };
}

/** How far the download has got, or `undefined` while no size is known yet. */
export function downloadPercent(run: ApplicationUpgradeRun): number | undefined {
  if (run.totalBytes <= 0) {
    return undefined;
  }
  return Math.min(100, Math.round((run.downloadedBytes / run.totalBytes) * 100));
}
