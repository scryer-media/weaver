/**
 * Client-side mirror of the server's `UpdateStatus` GraphQL type.
 *
 * Field names are the camelCase form async-graphql emits, so a selection set
 * can be assigned to this type directly.
 */
export interface UpdateStatus {
  currentVersion: string;
  latestVersion: string | null;
  updateAvailable: boolean;
  releaseUrl: string | null;
  publishedAtEpochMs: number | null;
  checking: boolean;
  lastCheckedAtEpochMs: number | null;
  lastSuccessfulCheckAtEpochMs: number | null;
  lastError: string | null;
}

/**
 * The release to advertise in the nav, or `undefined` when there is nothing to
 * show.
 *
 * Deliberately stricter than `updateAvailable` alone: the notification is a
 * link, so it is only worth rendering when there is both a version to name and
 * a URL to open. A status that claims an update but carries no release URL
 * (a partially-populated snapshot, or a checker that has not finished its first
 * successful fetch) renders nothing rather than a dead link.
 */
export function releaseNotification(
  status: UpdateStatus | undefined,
): { version: string; url: string } | undefined {
  if (!status?.updateAvailable) {
    return undefined;
  }
  const version = status.latestVersion;
  const url = status.releaseUrl;
  if (!version || !url) {
    return undefined;
  }
  return { version, url };
}
