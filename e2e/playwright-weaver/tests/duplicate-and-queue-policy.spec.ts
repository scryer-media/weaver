import {
  expect,
  graphql,
  submitProbeNzb,
  test,
  updateConfiguredServer,
} from "./helpers";

test("duplicate policy produces block, pause, and force decisions without pipeline assertions", async ({ request }) => {
  for (const host of ["nntp", "nntp2"]) {
    await updateConfiguredServer(request, host, { active: false });
  }
  const blockingPolicy = {
    strictActiveOrSuccess: "BLOCK",
    strictFailedOrCancelled: "ACCEPT",
    articleLayoutActiveOrSuccess: "PAUSE",
    articleLayoutFailedOrCancelled: "ACCEPT",
    articleSet: "BLOCK",
    normalizedName: "PAUSE",
  };
  const data = await graphql<{ updateSettings: { duplicatePolicy: Record<string, string> } }>(
    request,
    `mutation WeaverE2EDuplicatePolicy($input: GeneralSettingsInput!) {
      updateSettings(input: $input) {
        duplicatePolicy {
          strictActiveOrSuccess strictFailedOrCancelled
          articleLayoutActiveOrSuccess articleLayoutFailedOrCancelled
          articleSet normalizedName
        }
      }
    }`,
    { input: { duplicatePolicy: blockingPolicy } },
  );
  expect(data.updateSettings.duplicatePolicy).toEqual(blockingPolicy);

  // Hold the queue globally so these metadata-equivalent probes cannot become
  // download-pipeline acceptance coverage.
  expect(
    (await graphql<{ pauseAll: boolean }>(
      request,
      "mutation WeaverE2EPauseAll { pauseAll }",
    )).pauseAll,
  ).toBeTruthy();

  const article = { messageId: "weaver-duplicate-probe@e2e.invalid", bytes: 1_024 };
  const first = await submitProbeNzb(request, "weaver-duplicate-probe", [article]);
  expect(first).toMatchObject({ accepted: true, status: "ACCEPTED" });
  expect(first.jobId).not.toBeNull();

  const blocked = await submitProbeNzb(request, "weaver-duplicate-probe", [article]);
  expect(blocked).toMatchObject({
    accepted: false,
    status: "BLOCKED",
    duplicateDecision: { action: "BLOCK", forceBypassed: false },
  });

  const pausingPolicy = {
    ...blockingPolicy,
    strictActiveOrSuccess: "PAUSE",
    articleSet: "PAUSE",
  };
  await graphql(
    request,
    `mutation WeaverE2EPausingDuplicatePolicy($input: GeneralSettingsInput!) {
      updateSettings(input: $input) { duplicatePolicy { strictActiveOrSuccess } }
    }`,
    { input: { duplicatePolicy: pausingPolicy } },
  );
  const paused = await submitProbeNzb(request, "weaver-duplicate-probe", [article]);
  expect(paused).toMatchObject({
    accepted: true,
    status: "PAUSED",
    duplicateDecision: { action: "PAUSE", forceBypassed: false },
  });

  const forced = await submitProbeNzb(
    request,
    "weaver-duplicate-probe",
    [article],
    { force: true },
  );
  expect(forced).toMatchObject({
    accepted: true,
    status: "FORCE_ACCEPTED",
    duplicateDecision: { forceBypassed: true },
  });

  const queued = [
    first.jobId,
    paused.jobId,
    forced.jobId,
  ].filter(
    (value): value is number => value != null,
  );
  const locallyPaused = await graphql<{
    pauseQueueItem: { success: boolean; item: { id: number; state: string } };
  }>(
    request,
    `mutation WeaverE2EPauseQueueItem($id: Int!) {
      pauseQueueItem(id: $id) { success item { id state } }
    }`,
    { id: queued[0] },
  );
  expect(locallyPaused.pauseQueueItem).toMatchObject({
    success: true,
    item: { id: queued[0], state: "PAUSED" },
  });
  const locallyResumed = await graphql<{
    resumeQueueItem: { success: boolean; item: { id: number; state: string } };
  }>(
    request,
    `mutation WeaverE2EResumeQueueItem($id: Int!) {
      resumeQueueItem(id: $id) { success item { id state } }
    }`,
    { id: queued[0] },
  );
  expect(locallyResumed.resumeQueueItem.success).toBeTruthy();

  expect(
    (await graphql<{ reorderQueueItem: boolean }>(
      request,
      `mutation WeaverE2EReorderQueueItem($id: Int!) {
        reorderQueueItem(id: $id, kind: TOP)
      }`,
      { id: queued.at(-1) },
    )).reorderQueueItem,
  ).toBeTruthy();
  const order = await graphql<{ queueItems: Array<{ id: number }> }>(
    request,
    "query WeaverE2EQueueOrder { queueItems { id } }",
  );
  expect(order.queueItems[0]?.id).toBe(queued.at(-1));

  expect(
    (await graphql<{ resumeAll: boolean }>(
      request,
      "mutation WeaverE2EResumeAll { resumeAll }",
    )).resumeAll,
  ).toBeTruthy();
  expect(
    (await graphql<{ pauseAll: boolean }>(
      request,
      "mutation WeaverE2ERePauseAll { pauseAll }",
    )).pauseAll,
  ).toBeTruthy();

  const semanticWinner = await submitProbeNzb(
    request,
    "weaver-semantic-winner",
    [{ messageId: "weaver-semantic-winner@e2e.invalid", bytes: 1 }],
    { dupeKey: "e2e-replacement-group", dupeScore: 10, dupeMode: "SCORE" },
  );
  expect(semanticWinner).toMatchObject({ accepted: true, status: "ACCEPTED" });
  expect(semanticWinner.jobId).not.toBeNull();
  const semanticCandidate = await submitProbeNzb(
    request,
    "weaver-semantic-candidate",
    [{ messageId: "weaver-semantic-candidate@e2e.invalid", bytes: 1 }],
    { dupeKey: "e2e-replacement-group", dupeScore: 10, dupeMode: "SCORE" },
  );
  expect(semanticCandidate).toMatchObject({ accepted: true, status: "PARKED" });
  const replacement = await graphql<{
    markDuplicateBad: { accepted: boolean; jobId: number | null; message: string | null };
  }>(
    request,
    `mutation WeaverE2EReplaceSemanticWinner($id: Int!) {
      markDuplicateBad(id: $id) { accepted jobId message }
    }`,
    { id: semanticWinner.jobId },
  );
  expect(replacement.markDuplicateBad.accepted).toBeTruthy();
  expect(replacement.markDuplicateBad.jobId).not.toBeNull();
  expect(replacement.markDuplicateBad.jobId).not.toBe(semanticWinner.jobId);
  // Promotion only materializes a queued replacement. Mark-good is reserved
  // for a candidate that has actually succeeded; proving that rejection keeps
  // this flow from turning into download-pipeline acceptance coverage.
  expect(
    (await graphql<{ markDuplicateGood: boolean }>(
      request,
      "mutation WeaverE2EMarkQueuedReplacementGood($id: Int!) { markDuplicateGood(id: $id) }",
      { id: replacement.markDuplicateBad.jobId },
    )).markDuplicateGood,
  ).toBeFalsy();

  for (const jobId of [
    ...queued,
    replacement.markDuplicateBad.jobId,
  ].filter((value): value is number => value != null)) {
    expect(
      (await graphql<{ cancelQueueItem: { success: boolean } }>(
        request,
        `mutation WeaverE2ECancelProbe($id: Int!) {
          cancelQueueItem(id: $id) { success }
        }`,
        { id: jobId },
      )).cancelQueueItem.success,
    ).toBeTruthy();
  }
});
