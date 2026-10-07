import { useRef, useState } from "react";
import { useMutation } from "urql";
import { ANALYZE_NZB_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { DetailBlock, EmptyState } from "../components/chrome";
import { SecondaryButton } from "../components/controls";
import { formatSize } from "../data/format";
import {
  copySupportReport,
  downloadSupportReportJson,
  type AnalyzeNzbResponse,
  type SupportReport,
} from "../data/support-report";
import { NextShell } from "../shell/NextShell";

/** What the picker offers. The daemon reads compression from the bytes, not the name. */
const ANALYZER_ACCEPT = ".nzb,.gz,.zst,.xz";

/** Base64 in slices, since one `String.fromCharCode` call cannot take a whole NZB. */
function toBase64(bytes: Uint8Array): string {
  const SLICE = 0x8000;
  let binary = "";
  for (let start = 0; start < bytes.length; start += SLICE) {
    binary += String.fromCharCode(...bytes.subarray(start, start + SLICE));
  }
  return btoa(binary);
}

/**
 * Tools → NZB analyzer.
 *
 * Reads an NZB without submitting it and shows the same redacted report a
 * job's "Copy support report" produces, minus the job. Nothing is queued and
 * nothing is stored.
 */
export function NzbAnalyzerPage() {
  const t = useTranslate();
  const [, analyzeNzb] = useMutation<AnalyzeNzbResponse>(ANALYZE_NZB_MUTATION);
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);
  const [busy, setBusy] = useState(false);
  const [source, setSource] = useState<{ name: string; size: number } | null>(null);
  const [report, setReport] = useState<SupportReport | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const analyze = async (file: File | undefined) => {
    if (!file) {
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    setSource({ name: file.name, size: file.size });
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      const result = await analyzeNzb({ input: { nzbBase64: toBase64(bytes) } });
      if (result.error || !result.data?.analyzeNzb) {
        setReport(null);
        setError(result.error?.graphQLErrors[0]?.message ?? t("next.analyzer.failed"));
        return;
      }
      setReport(result.data.analyzeNzb);
    } catch {
      setReport(null);
      setError(t("next.analyzer.failed"));
    } finally {
      setBusy(false);
    }
  };

  return (
    <NextShell
      title={t("next.nav.nzbAnalyzer")}
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
              void analyze(event.dataTransfer.files[0]);
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
                void analyze(event.target.files?.[0]);
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
