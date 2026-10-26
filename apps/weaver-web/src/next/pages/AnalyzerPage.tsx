// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useRef, useState } from "react";
import { useMutation } from "urql";
import { ANALYZE_NZB_MUTATION, ANALYZE_TORRENT_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { DetailBlock, EmptyState } from "../components/chrome";
import { PrimaryButton, SecondaryButton, TextField } from "../components/controls";
import { ANALYZER_ACCEPT, fileKind, isMagnet, maxBytes } from "../data/analyzer";
import { formatSize } from "../data/format";
import {
  copySupportReport,
  downloadSupportReportJson,
  type AnalyzeNzbResponse,
  type AnalyzeTorrentResponse,
  type SupportReport,
} from "../data/support-report";
import { NextShell } from "../shell/NextShell";

/** Base64 in slices, since one `String.fromCharCode` call cannot take a whole NZB. */
function toBase64(bytes: Uint8Array): string {
  const SLICE = 0x8000;
  let binary = "";
  for (let start = 0; start < bytes.length; start += SLICE) {
    binary += String.fromCharCode(...bytes.subarray(start, start + SLICE));
  }
  return btoa(binary);
}

type Outcome = { report: SupportReport } | { error: string };

/** The report, or the message to show in its place. */
function outcomeOf(
  result: { error?: { graphQLErrors: { message: string }[] }; data?: SupportReport },
  failed: string,
): Outcome {
  if (result.error || !result.data) {
    return { error: result.error?.graphQLErrors[0]?.message ?? failed };
  }
  return { report: result.data };
}

/**
 * Tools → Analyzer.
 *
 * Reads an NZB, a .torrent file or a magnet link without adding it and shows
 * the same redacted report a job's "Copy support report" produces, minus the
 * job. Nothing is queued and nothing is stored.
 */
export function AnalyzerPage() {
  const t = useTranslate();
  const [, analyzeNzb] = useMutation<AnalyzeNzbResponse>(ANALYZE_NZB_MUTATION);
  const [, analyzeTorrent] = useMutation<AnalyzeTorrentResponse>(ANALYZE_TORRENT_MUTATION);
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);
  const [busy, setBusy] = useState(false);
  const [source, setSource] = useState<{ name: string; size: number } | null>(null);
  const [magnet, setMagnet] = useState("");
  const [report, setReport] = useState<SupportReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const run = async (work: () => Promise<Outcome>, failed: string) => {
    setBusy(true);
    setError(null);
    setNotice(null);
    let outcome: Outcome;
    try {
      outcome = await work();
    } catch {
      outcome = { error: failed };
    }
    if ("report" in outcome) {
      setReport(outcome.report);
    } else {
      setReport(null);
      setError(outcome.error);
    }
    setBusy(false);
  };

  const analyzeFile = async (file: File | undefined) => {
    if (!file) {
      return;
    }
    setSource({ name: file.name, size: file.size });
    const failed = t("next.analyzer.failed");
    await run(async () => {
      const kind = fileKind(new Uint8Array(await file.slice(0, 2).arrayBuffer()));
      if (file.size > maxBytes(kind)) {
        return { error: t("next.analyzer.tooLarge", { size: formatSize(maxBytes(kind)) }) };
      }
      const bytes = new Uint8Array(await file.arrayBuffer());
      if (kind === "torrent") {
        const result = await analyzeTorrent({ input: { torrentBase64: toBase64(bytes) } });
        return outcomeOf({ error: result.error, data: result.data?.analyzeTorrent }, failed);
      }
      const result = await analyzeNzb({ input: { nzbBase64: toBase64(bytes) } });
      return outcomeOf({ error: result.error, data: result.data?.analyzeNzb }, failed);
    }, failed);
  };

  const analyzeMagnet = async (link: string) => {
    setSource(null);
    const failed = t("next.analyzer.magnetFailed");
    await run(async () => {
      const result = await analyzeTorrent({ input: { magnet: link.trim() } });
      return outcomeOf({ error: result.error, data: result.data?.analyzeTorrent }, failed);
    }, failed);
  };

  return (
    <NextShell
      title={t("next.nav.analyzer")}
      note={t("next.analyzer.note")}
      controls={
        report === null ? undefined : (
          <>
            <SecondaryButton
              icon="copy"
              onClick={() => {
                void copySupportReport(report).then((copied) =>
                  setNotice(copied ? t("next.support.copied") : t("next.support.copyBlocked")),
                );
              }}
            >
              {t("next.support.copyReport")}
            </SecondaryButton>
            <SecondaryButton
              icon="downloadFile"
              onClick={() => {
                downloadSupportReportJson(report);
                setNotice(t("next.support.saved"));
              }}
            >
              {t("next.support.downloadJson")}
            </SecondaryButton>
          </>
        )
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto">
        <DetailBlock title={t("next.analyzer.source")}>
          <button
            type="button"
            disabled={busy}
            onClick={() => inputRef.current?.click()}
            onDragOver={(event) => {
              event.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDrop={(event) => {
              event.preventDefault();
              setDragging(false);
              const file = event.dataTransfer.files[0];
              const text = event.dataTransfer.getData("text/plain");
              if (file === undefined && isMagnet(text)) {
                setMagnet(text.trim());
                void analyzeMagnet(text);
              } else {
                void analyzeFile(file);
              }
            }}
            className={cn(
              "flex h-[104px] cursor-pointer flex-col items-center justify-center gap-2 border border-dashed text-[13px]",
              dragging
                ? "border-wv-accent bg-wv-selected text-wv-strong"
                : "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover-strong",
            )}
          >
            <input
              ref={inputRef}
              type="file"
              accept={ANALYZER_ACCEPT}
              className="hidden"
              onChange={(event) => {
                void analyzeFile(event.target.files?.[0]);
                event.target.value = "";
              }}
            />
            <span className="font-medium text-wv-fg">
              {busy ? t("next.analyzer.analyzing") : t("next.analyzer.dropZone")}
            </span>
            {source === null ? null : (
              <span className="font-wv-mono text-[11px] text-wv-faint">
                {`${source.name} · ${formatSize(source.size)}`}
              </span>
            )}
          </button>
          <form
            className="mt-3 flex gap-2"
            onSubmit={(event) => {
              event.preventDefault();
              void analyzeMagnet(magnet);
            }}
          >
            <TextField
              value={magnet}
              onChange={setMagnet}
              label={t("next.analyzer.magnet")}
              placeholder="magnet:?xt=urn:btih:"
              className="min-w-0 flex-1"
              autoComplete="off"
            />
            <PrimaryButton type="submit" disabled={busy || !isMagnet(magnet)}>
              {t("next.analyzer.analyze")}
            </PrimaryButton>
          </form>
          {error === null ? null : (
            <div className="mt-4 border border-wv-danger-border bg-wv-button px-4 py-3 text-[12.5px] text-wv-error-text">
              {error}
            </div>
          )}
        </DetailBlock>
        {report === null ? (
          <EmptyState title={t("next.analyzer.emptyTitle")} body={t("next.analyzer.emptyBody")} />
        ) : (
          <DetailBlock title={t("next.analyzer.report")} right={notice ?? undefined}>
            <pre className="overflow-x-auto whitespace-pre font-wv-mono text-[12px] leading-[1.55] text-wv-fg">
              {report.text}
            </pre>
          </DetailBlock>
        )}
      </div>
    </NextShell>
  );
}
