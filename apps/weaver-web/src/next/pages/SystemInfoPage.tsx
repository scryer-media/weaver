import { useEffect, useState } from "react";
import { useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { readDownloadErrorMessage, saveResponseAsDownload } from "@/lib/download";
import { Bar, EmptyState, KeyValueRow, SectionHeader } from "../components/chrome";
import { PrimaryButton, SecondaryButton } from "../components/controls";
import { EM_DASH, formatCount, formatDuration, formatSize } from "../data/format";
import { WV } from "../data/palette";
import { useMetricsSnapshot } from "../data/use-metrics-series";
import { NextShell } from "../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../shell/rail-blocks";

interface DiskCapacity {
  totalBytes: number;
  usedBytes: number;
  freeBytes: number;
}

interface ConfiguredStorage {
  labels: string[];
  path: string;
  capacity: DiskCapacity | null;
  error: string | null;
}

interface SystemInfo {
  version: string;
  uptimeSeconds: number;
  deployment: string;
  operatingSystem: string;
  architecture: string;
  databaseEngine: string;
  compute: {
    physicalCores: number;
    logicalCores: number;
    cgroupLimit: number | null;
    decoderTier: string;
    simdFeatures: string[];
  };
  memory: {
    totalBytes: number;
    availableAtStartupBytes: number;
    cgroupLimitBytes: number | null;
    effectiveLimitBytes: number;
  };
  primaryStorage: {
    storageClass: string;
    filesystem: string;
    startupRandomReadIops: number;
  };
  configuredStorage: ConfiguredStorage[];
}

/** GraphQL enums arrive SCREAMING_CASE; the design sets them in sentence case. */
function enumLabel(value: string): string {
  if (!value) return EM_DASH;
  return value.charAt(0) + value.slice(1).toLowerCase().replace(/_/g, " ");
}

function usageBarColor(percent: number): string {
  if (percent >= 85) return WV.error;
  if (percent >= 65) return WV.warn;
  return WV.accent;
}

function reportText(info: SystemInfo, cacheInUse: string): string {
  const lines = [
    `Weaver ${info.version}`,
    `Deployment: ${enumLabel(info.deployment)} · ${info.architecture}`,
    `Operating system: ${enumLabel(info.operatingSystem)}`,
    `Database: ${enumLabel(info.databaseEngine)}`,
    `Uptime: ${formatDuration(info.uptimeSeconds)}`,
    `Decoder tier: ${info.compute.decoderTier}`,
    `SIMD: ${info.compute.simdFeatures.join(", ") || "none detected"}`,
    `Cores: ${info.compute.physicalCores} physical · ${info.compute.logicalCores} logical`,
    `CPU quota: ${info.compute.cgroupLimit == null ? "not limited" : String(info.compute.cgroupLimit)}`,
    `Memory: ${formatSize(info.memory.totalBytes)} total · ${formatSize(info.memory.availableAtStartupBytes)} free at startup`,
    `Container memory limit: ${info.memory.cgroupLimitBytes == null ? "not limited" : formatSize(info.memory.cgroupLimitBytes)}`,
    `Article cache in use: ${cacheInUse}`,
    `Primary storage: ${info.primaryStorage.filesystem} · ${info.primaryStorage.storageClass} · ${formatCount(info.primaryStorage.startupRandomReadIops)} IOPS`,
  ];
  // Paths are deliberately omitted — the on-screen section says as much, and a
  // report pasted into a public tracker should not carry someone's library
  // layout.
  return lines.join("\n");
}

/**
 * System info.
 *
 * Dense key/value sections, one row per fact, exactly as the handoff draws it;
 * the values are weaver's own `systemInfo` payload with nothing added.
 */
export function SystemInfoPage() {
  const t = useTranslate();
  const [{ data, error }] = useQuery<{ systemInfo: SystemInfo }>({
    query: SYSTEM_INFO_QUERY,
    requestPolicy: "cache-and-network",
  });
  const metrics = useMetricsSnapshot();
  const [copied, setCopied] = useState(false);
  const [busy, setBusy] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);
  const [collectedAt] = useState(() => new Date());

  useEffect(() => {
    if (!copied) {
      return;
    }
    const timer = window.setTimeout(() => setCopied(false), 2_000);
    return () => window.clearTimeout(timer);
  }, [copied]);

  const info = data?.systemInfo;
  const cacheBytes = metrics?.decodePendingBytes ?? 0;
  const cacheLimit = metrics?.decodePressureHardLimitBytes ?? 0;
  const cacheInUse =
    cacheLimit > 0 ? `${formatSize(cacheBytes)} of ${formatSize(cacheLimit)}` : formatSize(cacheBytes);

  // Streamed straight off the endpoint: collection holds the request open for
  // the gap between two metrics samples, so the button stays busy throughout.
  async function downloadDiagnostics() {
    setBusy(true);
    setFailure(null);
    try {
      const response = await fetch(new URL("api/system/diagnostics", document.baseURI).href, {
        headers: authHeaders(),
        credentials: "same-origin",
      });
      if (!response.ok) {
        throw new Error(await readDownloadErrorMessage(response, t("systemInfo.diagnosticsFailed")));
      }
      await saveResponseAsDownload(response, `weaver-diagnostics-${Date.now()}.tar.zst`);
    } catch (caught) {
      setFailure(caught instanceof Error ? caught.message : t("systemInfo.diagnosticsFailed"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <NextShell
      title="System info"
      note="runtime, hardware and storage"
      controls={
        <>
          <SecondaryButton
            disabled={!info}
            onClick={() => {
              if (!info) return;
              void navigator.clipboard
                ?.writeText(reportText(info, cacheInUse))
                .then(() => setCopied(true))
                .catch(() => setFailure("Could not copy to the clipboard."));
            }}
          >
            {copied ? "Copied" : "Copy for a bug report"}
          </SecondaryButton>
          <PrimaryButton disabled={busy} onClick={() => void downloadDiagnostics()}>
            {busy ? "Collecting" : "Download diagnostics"}
          </PrimaryButton>
        </>
      }
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      statusRight={
        failure ?? `collected at ${collectedAt.toLocaleTimeString([], { hour12: false })}`
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        {!info ? (
          <EmptyState
            title={error ? "System info is unavailable" : "Loading"}
            body={error ? error.message : "Reading the daemon's runtime facts."}
          />
        ) : (
          <>
            <SectionHeader
              label="Software"
              note={`v${info.version} · up ${formatDuration(info.uptimeSeconds)}`}
            />
            <KeyValueRow label="Version" value={info.version} />
            <KeyValueRow
              label="Deployment"
              value={`${enumLabel(info.deployment)} · ${info.architecture}`}
            />
            <KeyValueRow label="Operating system" value={enumLabel(info.operatingSystem)} />
            <KeyValueRow label="Database" value={enumLabel(info.databaseEngine)} />
            <KeyValueRow label="Uptime" value={formatDuration(info.uptimeSeconds)} />

            <SectionHeader label="Compute" note="decoder and CPU" />
            <KeyValueRow label="Decoder tier" value={info.compute.decoderTier} />
            <KeyValueRow
              label="Detected SIMD"
              value={info.compute.simdFeatures.join(", ") || "none detected"}
            />
            <KeyValueRow
              label="Cores"
              value={`${info.compute.physicalCores} physical · ${info.compute.logicalCores} logical`}
            />
            <KeyValueRow
              label="Container CPU quota"
              value={info.compute.cgroupLimit == null ? "Not limited" : String(info.compute.cgroupLimit)}
            />

            <SectionHeader label="Memory" note="host and container" />
            <KeyValueRow label="Total memory" value={formatSize(info.memory.totalBytes)} />
            <KeyValueRow
              label="Available at startup"
              value={formatSize(info.memory.availableAtStartupBytes)}
            />
            <KeyValueRow
              label="Container limit"
              value={
                info.memory.cgroupLimitBytes == null
                  ? "Not limited"
                  : formatSize(info.memory.cgroupLimitBytes)
              }
            />
            <KeyValueRow label="Effective limit" value={formatSize(info.memory.effectiveLimitBytes)} />
            <KeyValueRow label="Article cache in use" value={cacheInUse} />

            <SectionHeader label="Primary storage" note="measured at startup" />
            <KeyValueRow label="Filesystem" value={info.primaryStorage.filesystem} />
            <KeyValueRow label="Storage class" value={info.primaryStorage.storageClass} />
            <KeyValueRow
              label="Random-read benchmark"
              value={`${formatCount(info.primaryStorage.startupRandomReadIops)} IOPS`}
            />

            <SectionHeader label="Configured storage" note="paths are hidden in exported reports" />
            {info.configuredStorage.length === 0 ? (
              <div className="px-4 sm:px-6 py-5 text-[13px] text-wv-muted">
                No storage locations are configured yet.
              </div>
            ) : (
              info.configuredStorage.map((volume) => {
                const percent =
                  volume.capacity && volume.capacity.totalBytes > 0
                    ? (volume.capacity.usedBytes / volume.capacity.totalBytes) * 100
                    : 0;
                return (
                  <div
                    key={volume.path}
                    className="flex flex-wrap items-center gap-x-5 gap-y-3 border-b border-wv-hairline px-4 sm:px-6 py-[13px] hover:bg-wv-cell-hover"
                  >
                    <div className="flex min-w-0 flex-[1_1_260px] flex-col gap-1">
                      <span className="truncate text-[13.5px] font-medium tracking-[-0.005em] text-wv-fg">
                        {volume.labels.join(" · ") || "storage"}
                      </span>
                      <span title={volume.path} className="truncate font-wv-mono text-[11.5px] text-wv-faint">
                        {volume.path}
                      </span>
                    </div>
                    <div className="ml-auto flex items-center gap-5">
                      <Bar percent={percent} color={usageBarColor(percent)} className="w-full max-w-[220px]" />
                      <span className="w-[46px] text-right font-wv-mono text-[12.5px] text-wv-secondary">
                        {Math.round(percent)}%
                      </span>
                      <span className="w-[168px] text-right font-wv-mono text-[12.5px] text-wv-muted">
                        {volume.capacity
                          ? `${formatSize(volume.capacity.usedBytes)} of ${formatSize(volume.capacity.totalBytes)}`
                          : (volume.error ?? EM_DASH)}
                      </span>
                    </div>
                  </div>
                );
              })
            )}
          </>
        )}
      </div>
    </NextShell>
  );
}
