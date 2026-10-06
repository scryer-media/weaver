import type { APIRequestContext } from "@playwright/test";
import { expect, graphql, postMultipartProbeArticle, submitProbeNzb } from "../helpers";

/**
 * Probe downloads for the network and script specs: post `count` yEnc parts
 * of one file to an NNTP server, submit them as a single multipart NZB and
 * follow the job by id.
 */
export type DownloadOptions = {
  count?: number;
  partBytes?: number;
  /** NNTP server the articles are posted to (the Weaver server reads them back). */
  nntpHost?: string;
  nntpPort?: number;
  extraInput?: Record<string, unknown>;
};

export async function postProbeFile(name: string, options: DownloadOptions = {}): Promise<Array<{ messageId: string; bytes: number }>> {
  const count = options.count ?? 32;
  const size = options.partBytes ?? 64 * 1024;
  const articles = Array.from({ length: count }, (_, index) => ({ messageId: `${name}-${index}@e2e.invalid`, bytes: size }));
  for (const [index, article] of articles.entries()) {
    await postMultipartProbeArticle(article.messageId, size, {
      filename: `${name}.bin`, number: index + 1, total: count,
      begin: index * size + 1, end: (index + 1) * size, totalBytes: count * size,
    }, options.nntpHost ?? "nntp", options.nntpPort ?? 119);
  }
  return articles;
}

export async function startDownload(request: APIRequestContext, name: string, options: DownloadOptions = {}): Promise<number> {
  const articles = await postProbeFile(name, options);
  const result = await submitProbeNzb(request, name, articles, options.extraInput ?? {}, "single-multipart-file");
  expect(result, `submit ${name}`).toMatchObject({ accepted: true });
  expect(result.jobId).not.toBeNull();
  return result.jobId!;
}

export async function jobState(request: APIRequestContext, id: number): Promise<string> {
  return (await graphql<{ historyItem: { state: string } | null }>(request,
    "query($id: Int!) { historyItem(id: $id) { state } }", { id })).historyItem?.state ?? "PENDING";
}

const TERMINAL = new Set(["COMPLETED", "FAILED"]);

/** Wait for job `id` to reach a terminal state; returns it. */
export async function waitTerminal(request: APIRequestContext, id: number): Promise<string> {
  let state = "PENDING";
  await expect.poll(async () => TERMINAL.has(state = await jobState(request, id)), { message: `job ${id} terminal`, timeout: 0 }).toBe(true);
  return state;
}
