// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { APIRequestContext } from "@playwright/test";

import { graphql, submitProbeNzb } from "../../helpers";
import { waitTerminal } from "../downloads";

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

  for (const jobId of jobIds) {
    await waitTerminal(request, jobId);
  }
  return { newest: names.at(-1)!, oldest: names[0] };
}
