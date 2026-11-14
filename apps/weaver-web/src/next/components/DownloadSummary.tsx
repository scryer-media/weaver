// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { jobIds } from "@/next/testids";
import { DetailBlock, Field } from "./chrome";
import { EM_DASH, formatCount, formatDate } from "../data/format";
import {
  downloadPanelState,
  downloadTree,
  nodePassword,
  serverWord,
  showsTree,
  type DownloadRecoveryData,
  type DownloadSummaryData,
  type DownloadTreeNode,
} from "../data/download-summary";
import { countLabel } from "../i18n/labels";

/**
 * How the download went: the post's facts, the archives as a nested list,
 * and the recovery sets. Built once as the job finished; a queued job and an
 * entry from before it was kept say so instead.
 */
export function DownloadSummaryPanel({
  inQueue,
  summary,
}: {
  inQueue: boolean;
  summary: DownloadSummaryData | null | undefined;
}) {
  const t = useTranslate();
  const state = downloadPanelState(inQueue, summary);
  const tree = useMemo(() => (summary ? downloadTree(summary.nodes) : []), [summary]);
  return (
    <DetailBlock
      id="download"
      title={t("next.download.title")}
      tone="panel"
      bodyClassName="gap-[13px]"
    >
      <div data-testid={jobIds.jobDownload} className="flex min-w-0 flex-col gap-[13px]">
        {state === "queued" ? (
          <span data-testid={jobIds.jobDownloadPending} className="text-[12.5px] text-wv-muted">
            {t("next.download.pending")}
          </span>
        ) : state === "unrecorded" || !summary ? (
          <span data-testid={jobIds.jobDownloadUnrecorded} className="text-[12.5px] text-wv-muted">
            {t("next.download.unrecorded")}
          </span>
        ) : (
          <>
            <PostFacts summary={summary} />
            {showsTree(summary) ? (
              <div className="flex min-w-0 flex-col gap-[6px]">
                <Field
                  variant="inline"
                  label={t("next.download.archives")}
                  value={
                    summary.nodes.length < summary.nodeCount
                      ? t("next.download.archivesShown", {
                          shown: formatCount(summary.nodes.length),
                          count: formatCount(summary.nodeCount),
                        })
                      : `${formatCount(summary.nodeCount)} · ${t("next.download.depth")} ${formatCount(summary.maxDepth)}`
                  }
                />
                <NodeList branches={tree} />
              </div>
            ) : null}
            {summary.recovery.length === 0 ? null : (
              <div data-testid={jobIds.jobDownloadRecovery} className="flex min-w-0 flex-col gap-[6px]">
                <Field
                  variant="inline"
                  label={t("next.download.recovery")}
                  value={
                    summary.recovery.length < summary.recoveryCount
                      ? t("next.download.recoveryShown", {
                          shown: formatCount(summary.recovery.length),
                          count: formatCount(summary.recoveryCount),
                        })
                      : formatCount(summary.recoveryCount)
                  }
                />
                {summary.recovery.map((object, index) => (
                  <RecoveryObject key={index} object={object} />
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </DetailBlock>
  );
}

function RecoveryObject({ object }: { object: DownloadRecoveryData }) {
  const t = useTranslate();
  return (
    <div className="flex min-w-0 flex-col gap-[3px] pl-3">
      <span className="font-wv-mono text-[11.5px] text-wv-fg">
        {serverWord(t, "kind", object.kind)}
        {object.outcome ? ` · ${serverWord(t, "outcome", object.outcome)}` : ""}
      </span>
      {object.sets.map((set, setIndex) => {
        const line = t("next.download.recoverySet", {
          blocks: formatCount(set.blocksReachable),
          damaged: formatCount(set.damagedSlices),
          repaired: formatCount(set.slicesRepaired),
        });
        return (
          <span key={setIndex} className="font-wv-mono text-[11px] text-wv-tertiary">
            {set.outcome ? `${serverWord(t, "outcome", set.outcome)} · ${line}` : line}
          </span>
        );
      })}
      {object.sets.length < object.setCount ? (
        <span className="font-wv-mono text-[11px] text-wv-muted">
          {t("next.download.setsShown", {
            shown: formatCount(object.sets.length),
            count: formatCount(object.setCount),
          })}
        </span>
      ) : null}
    </div>
  );
}

function PostFacts({ summary }: { summary: DownloadSummaryData }) {
  const t = useTranslate();
  const { post } = summary;
  const posted =
    post.oldestPost === null
      ? EM_DASH
      : post.newestPost !== null && post.newestPost !== post.oldestPost
        ? `${formatDate(post.oldestPost * 1000)} – ${formatDate(post.newestPost * 1000)}`
        : formatDate(post.oldestPost * 1000);
  return (
    <div
      className="grid gap-x-4 gap-y-[13px]"
      style={{ gridTemplateColumns: "repeat(auto-fit, minmax(104px, 1fr))" }}
    >
      {summary.layout ? (
        <Field label={t("next.download.layout")} value={t(`next.download.layout.${summary.layout}`)} />
      ) : null}
      <Field label={t("next.download.files")} value={formatCount(post.files)} />
      <Field label={t("next.download.articles")} value={formatCount(post.articles)} />
      {post.missingArticles > 0 ? (
        <Field label={t("next.download.missing")} value={formatCount(post.missingArticles)} />
      ) : null}
      {post.failedArticles > 0 ? (
        <Field label={t("next.download.failed")} value={formatCount(post.failedArticles)} />
      ) : null}
      <Field label={t("next.download.posted")} value={posted} title={posted} />
      <Field label={t("next.download.groups")} value={formatCount(post.groups)} />
    </div>
  );
}

function Label({ children, tone = "plain" }: { children: string; tone?: "plain" | "warn" }) {
  return (
    <span
      className={
        tone === "warn"
          ? "border border-wv-warn/60 px-[5px] font-wv-mono text-[10px] leading-[16px] text-wv-warn"
          : "border border-wv-control px-[5px] font-wv-mono text-[10px] leading-[16px] text-wv-tertiary"
      }
    >
      {children}
    </span>
  );
}

/** One level of the tree; each node's children are a list nested in its item. */
function NodeList({ branches, nested = false }: { branches: DownloadTreeNode[]; nested?: boolean }) {
  return (
    <ul
      className={
        nested
          ? "mt-[5px] flex min-w-0 list-disc flex-col gap-[5px] pl-4 marker:text-wv-dim"
          : "flex min-w-0 list-disc flex-col gap-[5px] pl-4 marker:text-wv-dim"
      }
    >
      {branches.map((branch) => (
        <li key={branch.node.id} className="min-w-0">
          <NodeLine branch={branch} />
          {branch.children.length > 0 ? <NodeList branches={branch.children} nested /> : null}
        </li>
      ))}
    </ul>
  );
}

function NodeLine({ branch: { node } }: { branch: DownloadTreeNode }) {
  const t = useTranslate();
  const password = nodePassword(node);
  const kind = serverWord(t, "kind", node.kind);
  return (
    <div
      data-testid={jobIds.jobDownloadNode(node.id)}
      className="inline-flex min-w-0 max-w-full flex-wrap items-baseline gap-x-[6px] gap-y-[3px] align-baseline"
    >
      <span className="min-w-0 truncate font-wv-mono text-[11.5px] text-wv-fg" title={node.name ?? undefined}>
        {node.name ?? kind}
      </span>
      <Label>{kind}</Label>
      {node.volumes > 1 ? (
        <Label>{countLabel(t, "next.download.volumes", node.volumes, { count: formatCount(node.volumes) })}</Label>
      ) : null}
      {node.givenUp ? null : (
        <Label>{node.extracted ? t("next.download.extracted") : t("next.download.streamed")}</Label>
      )}
      {node.encrypted || password?.state === "opened" ? <Label>{t("next.download.encrypted")}</Label> : null}
      {node.encryptedHeaders ? <Label>{t("next.download.encryptedHeaders")}</Label> : null}
      {password?.state === "origin" ? (
        <span data-testid={jobIds.jobDownloadPasswordOrigin(password.group)}>
          <Label>{t(`next.download.password.${password.group}`)}</Label>
        </span>
      ) : password?.state === "needed" ? (
        <Label tone="warn">{t("next.download.passwordNeeded")}</Label>
      ) : null}
      {node.givenUp ? (
        <Label tone="warn">
          {node.reason
            ? `${t("next.download.gaveUp")}: ${serverWord(t, "reason", node.reason)}`
            : t("next.download.gaveUp")}
        </Label>
      ) : null}
    </div>
  );
}
