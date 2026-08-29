import type { APIRequestContext } from "@playwright/test";

import { graphql, submitProbeNzb } from "../../helpers";

export async function makeRestoreTargetNonPristine(
  request: APIRequestContext,
  name: string,
) {
  return submitProbeNzb(
    request,
    name,
    [{ messageId: "weaver-restore-blocked@e2e.invalid", bytes: 1 }],
  );
}

export async function seedRestorableHistoryMetadata(
  request: APIRequestContext,
  name: string,
): Promise<number> {
  await graphql<{ pauseAll: boolean }>(
    request,
    "mutation WeaverE2EBackupHistoryPause { pauseAll }",
  );
  const result = await submitProbeNzb(
    request,
    name,
    [{ messageId: `${name}@backup-history.e2e.invalid`, bytes: 1 }],
  );
  if (!result.accepted || result.jobId == null) {
    throw new Error(`failed to seed backup history metadata: ${JSON.stringify(result)}`);
  }
  const cancelled = await graphql<{ cancelJob: boolean }>(
    request,
    "mutation WeaverE2EBackupHistoryCancel($id: Int!) { cancelJob(id: $id) }",
    { id: result.jobId },
  );
  if (!cancelled.cancelJob) {
    throw new Error(`failed to cancel backup history seed job ${result.jobId}`);
  }
  return result.jobId;
}
