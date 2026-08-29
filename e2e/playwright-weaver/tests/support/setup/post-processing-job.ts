import type { APIRequestContext } from "@playwright/test";

import { expect, graphql, postProbeArticle, submitProbeNzb } from "../../helpers";

/// `state` rather than `status`: the field is `HistoryItem.state` of type
/// `QueueItemState`, whose terminal values are `COMPLETED` and `FAILED`.
export type HistoryItem = { id: number; state: string; outputDir?: string | null };

export type ScriptResult = {
  script: string;
  status: string;
  exitCode: number | null;
};

/** Poll history until `jobId` is terminal, and return its row. */
export async function waitForTerminalJob(
  request: APIRequestContext,
  jobId: number,
): Promise<HistoryItem> {
  let item: HistoryItem | null = null;
  await expect
    .poll(
      async () => {
        const data = await graphql<{ historyItem: HistoryItem | null }>(
          request,
          `query WeaverE2EPostProcessingHistory($id: Int!) {
            historyItem(id: $id) { id state outputDir }
          }`,
          { id: jobId },
        );
        item = data.historyItem;
        return item?.state ?? "PENDING";
      },
      { timeout: 120_000, intervals: [500, 1_000, 2_000] },
    )
    .toMatch(/^(COMPLETED|FAILED)$/);
  if (!item) throw new Error(`job ${jobId} never reached history`);
  return item;
}

/** Script results recorded on a job, whatever the UI is currently showing. */
export async function scriptResults(
  request: APIRequestContext,
  jobId: number,
): Promise<ScriptResult[]> {
  const data = await graphql<{ postProcessingResults: ScriptResult[] }>(
    request,
    `query WeaverE2EPostProcessingResults($jobId: Int!) {
      postProcessingResults(jobId: $jobId) { script status exitCode }
    }`,
    { jobId },
  );
  return data.postProcessingResults;
}

/** Submit a one-article job that will run the configured scripts on completion. */
export async function runJobThroughPostProcessing(
  request: APIRequestContext,
  name: string,
): Promise<HistoryItem> {
  const messageId = `${name}@post-processing.e2e.invalid`;
  await postProbeArticle(messageId, 1024);
  const submission = await submitProbeNzb(request, name, [{ messageId, bytes: 1024 }]);
  expect(submission.accepted, JSON.stringify(submission)).toBeTruthy();
  expect(submission.jobId).not.toBeNull();
  return waitForTerminalJob(request, submission.jobId!);
}
