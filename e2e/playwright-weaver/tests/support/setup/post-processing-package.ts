import fs from "node:fs";
import path from "node:path";
import type { APIRequestContext } from "@playwright/test";

import { graphql } from "../../helpers";

export const POST_PROCESSING_E2E_PROFILE = "e2e-extension-profile";
export const POST_PROCESSING_E2E_EXTENSION_ID = "e2e.lifecycle-extension";
export const POST_PROCESSING_E2E_SECRET = "weaver-e2e-secret-must-never-appear";

type PostProcessingSeed = {
  jobIds: number[];
  runIds: string[];
};

export function seedPostProcessingPackage(): void {
  const packageRoot = "/weaver-data/scripts/e2e-lifecycle-extension";
  const executable = path.join(packageRoot, "bin", "run");
  const payload = "x".repeat(1024);
  fs.rmSync(packageRoot, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(executable), { recursive: true });
  fs.writeFileSync(
    path.join(packageRoot, "weaver-extension.json"),
    `${JSON.stringify(
      {
        schema_version: 1,
        kind: "native",
        id: POST_PROCESSING_E2E_EXTENSION_ID,
        name: "E2E Lifecycle Extension",
        version: "1.0.0",
        entrypoint: "bin/run",
        commands: [],
        options: [{ name: "TOKEN", type: "secret" }],
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    executable,
    `#!/bin/sh
i=0
payload='${payload}'
while [ "$i" -lt 5000 ]; do
  printf 'e2e-log-%s context=%s payload=%s\\n' "$i" "$WEAVER_PP_CONTEXT" "$payload"
  i=$((i + 1))
done
`,
    { mode: 0o755 },
  );
}

export const POST_PROCESSING_FAILING_EXTENSION_ID = "e2e.failing-extension";
export const POST_PROCESSING_FAILING_EXTENSION_NAME = "E2E Failing Extension";
export const POST_PROCESSING_QUIET_EXTENSION_ID = "e2e.quiet-extension";
export const POST_PROCESSING_QUIET_EXTENSION_NAME = "E2E Quiet Extension";
export const POST_PROCESSING_SLOW_EXTENSION_ID = "e2e.slow-extension";
export const POST_PROCESSING_SLOW_EXTENSION_NAME = "E2E Slow Extension";

type ExtensionPackageSpec = {
  slug: string;
  id: string;
  name: string;
  script: string;
};

function writeExtensionPackage({ slug, id, name, script }: ExtensionPackageSpec): void {
  const packageRoot = `/weaver-data/scripts/${slug}`;
  const executable = path.join(packageRoot, "bin", "run");
  fs.rmSync(packageRoot, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(executable), { recursive: true });
  fs.writeFileSync(
    path.join(packageRoot, "weaver-extension.json"),
    `${JSON.stringify(
      {
        schema_version: 1,
        kind: "native",
        id,
        name,
        version: "1.0.0",
        entrypoint: "bin/run",
        commands: [],
        options: [],
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(executable, script, { mode: 0o755 });
}

/**
 * Extensions whose *execution outcome* varies, so the suite can cover the
 * dispositions the always-succeeding lifecycle extension cannot reach:
 * failure, continue-after-failure, and timeout. They stay deliberately quiet
 * (a couple of lines) so these runs do not pay the lifecycle extension's
 * log-truncation cost.
 */
export function seedPostProcessingExecutionPackages(): void {
  writeExtensionPackage({
    slug: "e2e-failing-extension",
    id: POST_PROCESSING_FAILING_EXTENSION_ID,
    name: POST_PROCESSING_FAILING_EXTENSION_NAME,
    script: `#!/bin/sh
printf 'e2e-failing-extension refusing to process %s\\n' "$WEAVER_PP_CONTEXT"
exit 3
`,
  });
  writeExtensionPackage({
    slug: "e2e-quiet-extension",
    id: POST_PROCESSING_QUIET_EXTENSION_ID,
    name: POST_PROCESSING_QUIET_EXTENSION_NAME,
    script: `#!/bin/sh
printf 'e2e-quiet-extension processed %s\\n' "$WEAVER_PP_CONTEXT"
`,
  });
  writeExtensionPackage({
    slug: "e2e-slow-extension",
    id: POST_PROCESSING_SLOW_EXTENSION_ID,
    name: POST_PROCESSING_SLOW_EXTENSION_NAME,
    script: `#!/bin/sh
printf 'e2e-slow-extension sleeping for %s\\n' "$WEAVER_PP_CONTEXT"
sleep 120
`,
  });
}

export async function preparePostProcessingJobs(
  request: APIRequestContext,
): Promise<PostProcessingSeed> {
  return seedPostProcessingRuns(request);
}


/**
 * Prepare jobs for `profileId` and enqueue a run for each, returning the first
 * job ID for inspection.
 *
 * The seed mutation only accepts the exact pair of job IDs it prepared, so both
 * are always enqueued. The caller must have the post-processing queue paused:
 * the mutation refuses to enqueue otherwise.
 */
export async function seedPostProcessingRunsForProfile(
  request: APIRequestContext,
  profileId: string,
): Promise<number> {
  const prepared = await seedPostProcessingRuns(request, undefined, profileId);
  if (prepared.jobIds.length !== 2) {
    throw new Error(
      `expected the post-processing seed to prepare 2 jobs, got ${prepared.jobIds.length}`,
    );
  }
  await seedPostProcessingRuns(request, prepared.jobIds, profileId);
  return prepared.jobIds[0];
}

export async function enqueuePostProcessingRuns(
  request: APIRequestContext,
  jobIds: number[],
): Promise<PostProcessingSeed> {
  return seedPostProcessingRuns(request, jobIds);
}

async function seedPostProcessingRuns(
  request: APIRequestContext,
  jobIds?: number[],
  profileId: string = POST_PROCESSING_E2E_PROFILE,
): Promise<PostProcessingSeed> {
  const data = await graphql<{ seedE2EPostProcessingRuns: PostProcessingSeed }>(
    request,
    `mutation WeaverE2ESeedPostProcessing($profileId: String!, $jobIds: [Int!]) {
      seedE2EPostProcessingRuns(profileId: $profileId, jobIds: $jobIds) {
        jobIds
        runIds
      }
    }`,
    {
      profileId,
      jobIds: jobIds ?? null,
    },
  );
  return data.seedE2EPostProcessingRuns;
}
