import type { APIRequestContext } from "@playwright/test";

import { graphql, submitProbeNzb } from "../../helpers";

type HistoryPage = {
  historyPage: {
    totalCount: number;
  };
};

export async function seedRuntimeHistory(
  request: APIRequestContext,
  prefix: string,
  count = 26,
): Promise<{ newest: string; oldest: string }> {
  const normalizedPrefix = prefix.replace(/[^a-z0-9-]+/gi, "-").toLowerCase();
  const names = Array.from(
    { length: count },
    (_, index) => `${normalizedPrefix}-${String(index + 1).padStart(2, "0")}`,
  );

  await graphql<{ pauseAll: boolean }>(
    request,
    "mutation WeaverE2ERuntimeHistoryPause { pauseAll }",
  );
  const before = await historyCount(request);
  const jobIds: number[] = [];
  for (const [index, name] of names.entries()) {
    const result = await submitProbeNzb(
      request,
      name,
      [{
        messageId: `${normalizedPrefix}-${index + 1}@runtime-history.e2e.invalid`,
        bytes: 1,
      }],
    );
    if (!result.accepted || result.jobId == null) {
      throw new Error(`failed to seed runtime history marker ${name}: ${JSON.stringify(result)}`);
    }
    jobIds.push(result.jobId);
  }

  for (const jobId of jobIds) {
    const result = await graphql<{ cancelJob: boolean }>(
      request,
      "mutation WeaverE2ERuntimeHistoryCancel($id: Int!) { cancelJob(id: $id) }",
      { id: jobId },
    );
    if (!result.cancelJob) {
      throw new Error(`failed to cancel runtime history seed job ${jobId}`);
    }
  }

  const expected = before + count;
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if ((await historyCount(request)) >= expected) {
      return { newest: names.at(-1)!, oldest: names[0] };
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`runtime history did not reach ${expected} entries`);
}

async function historyCount(request: APIRequestContext): Promise<number> {
  const data = await graphql<HistoryPage>(
    request,
    `query WeaverE2ERuntimeHistoryCount($input: HistoryPageInput!) {
      historyPage(input: $input) { totalCount }
    }`,
    {
      input: {
        pageIndex: 0,
        pageSize: 1,
        sortField: "COMPLETED_AT",
        sortDirection: "DESC",
      },
    },
  );
  return data.historyPage.totalCount;
}
