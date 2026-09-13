import { useEffect, useState } from "react";
import { useQuery } from "urql";
import { authHeaders } from "@/graphql/client";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { readDownloadErrorMessage, saveResponseAsDownload } from "@/lib/download";
import { EmptyState, KeyValueRow, SectionHeader } from "../components/chrome";
import { PrimaryButton, SecondaryButton } from "../components/controls";
import { StorageUsage } from "../components/storage";
import { EM_DASH, formatCount, formatDuration, formatSize } from "../data/format";
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

/**
 * The bug-report text. It stays in English whatever the interface language: it
 * is read by whoever triages the report, not by the person who copied it.
 */
function reportText(info: SystemInfo, cacheBytes: number, cacheLimit: number): string {
  const cacheInUse =
    cacheLimit > 0 ? `${formatSize(cacheBytes)} of ${formatSize(cacheLimit)}` : formatSize(cacheBytes);
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
    cacheLimit > 0
      ? t("next.system.usedOf", { used: formatSize(cacheBytes), total: formatSize(cacheLimit) })
      : formatSize(cacheBytes);

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
      title={t("next.nav.systemInfo")}
      note={t("next.system.note")}
      controls={
        <>
          <SecondaryButton
            icon="copy"
            disabled={!info}
            onClick={() => {
              if (!info) return;
              void navigator.clipboard
                ?.writeText(reportText(info, cacheBytes, cacheLimit))
                .then(() => setCopied(true))
                .catch(() => setFailure(t("next.system.copyFailed")));
            }}
          >
            {copied ? t("next.system.copied") : t("next.system.copyReport")}
          </SecondaryButton>
          <PrimaryButton icon="downloadFile" disabled={busy} onClick={() => void downloadDiagnostics()}>
            {busy ? t("next.system.collecting") : t("next.system.downloadDiagnostics")}
          </PrimaryButton>
        </>
      }
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      statusRight={
        failure ??
        t("next.system.collectedAt", {
          time: collectedAt.toLocaleTimeString([], { hour12: false }),
        })
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        {!info ? (
          <EmptyState
            loading={!error}
            title={error ? t("next.system.unavailable") : t("next.common.loading")}
            body={error ? error.message : t("next.system.loadingBody")}
          />
        ) : (
          <>
            <SectionHeader
              label={t("next.system.software")}
              note={t("next.system.softwareNote", {
                version: info.version,
                uptime: formatDuration(info.uptimeSeconds),
              })}
            />
            <KeyValueRow label={t("next.system.version")} value={info.version} />
            <KeyValueRow
              label={t("next.system.deployment")}
              value={`${enumLabel(info.deployment)} · ${info.architecture}`}
            />
            <KeyValueRow label={t("next.system.operatingSystem")} value={enumLabel(info.operatingSystem)} />
            <KeyValueRow label={t("next.system.database")} value={enumLabel(info.databaseEngine)} />
            <KeyValueRow label={t("next.rail.uptime")} value={formatDuration(info.uptimeSeconds)} />

            <SectionHeader label={t("next.system.compute")} note={t("next.system.computeNote")} />
            <KeyValueRow label={t("next.system.decoderTier")} value={info.compute.decoderTier} />
            <KeyValueRow
              label={t("next.system.simd")}
              value={info.compute.simdFeatures.join(", ") || t("next.system.noneDetected")}
            />
            <KeyValueRow
              label={t("next.system.cores")}
              value={t("next.system.coresValue", {
                physical: info.compute.physicalCores,
                logical: info.compute.logicalCores,
              })}
            />
            <KeyValueRow
              label={t("next.system.cpuQuota")}
              value={
                info.compute.cgroupLimit == null
                  ? t("next.system.notLimited")
                  : String(info.compute.cgroupLimit)
              }
            />

            <SectionHeader label={t("next.system.memory")} note={t("next.system.memoryNote")} />
            <KeyValueRow label={t("next.system.totalMemory")} value={formatSize(info.memory.totalBytes)} />
            <KeyValueRow
              label={t("next.system.availableAtStartup")}
              value={formatSize(info.memory.availableAtStartupBytes)}
            />
            <KeyValueRow
              label={t("next.system.containerLimit")}
              value={
                info.memory.cgroupLimitBytes == null
                  ? t("next.system.notLimited")
                  : formatSize(info.memory.cgroupLimitBytes)
              }
            />
            <KeyValueRow
              label={t("next.system.effectiveLimit")}
              value={formatSize(info.memory.effectiveLimitBytes)}
            />
            <KeyValueRow label={t("next.system.articleCache")} value={cacheInUse} />

            <SectionHeader
              label={t("next.system.primaryStorage")}
              note={t("next.system.primaryStorageNote")}
            />
            <KeyValueRow label={t("next.system.filesystem")} value={info.primaryStorage.filesystem} />
            <KeyValueRow label={t("next.system.storageClass")} value={info.primaryStorage.storageClass} />
            <KeyValueRow
              label={t("next.system.randomRead")}
              value={`${formatCount(info.primaryStorage.startupRandomReadIops)} IOPS`}
            />

            <SectionHeader
              label={t("next.system.configuredStorage")}
              note={t("next.system.configuredStorageNote")}
            />
            {info.configuredStorage.length === 0 ? (
              <div className="px-4 sm:px-6 py-5 text-[13px] text-wv-muted">
                {t("next.system.noStorage")}
              </div>
            ) : (
              info.configuredStorage.map((volume) => (
                <div
                  key={volume.path}
                  className="flex flex-wrap items-center gap-x-5 gap-y-3 border-b border-wv-hairline px-4 sm:px-6 py-[13px] hover:bg-wv-cell-hover"
                >
                  <div className="flex min-w-0 flex-[1_1_260px] flex-col gap-1">
                    <span className="truncate text-[13.5px] font-medium tracking-[-0.005em] text-wv-fg">
                      {volume.labels.join(" · ") || t("next.system.storageFallback")}
                    </span>
                    <span title={volume.path} className="truncate font-wv-mono text-[11.5px] text-wv-faint">
                      {volume.path}
                    </span>
                  </div>
                  <StorageUsage
                    label={
                      volume.capacity
                        ? t("next.system.usedOf", {
                            used: formatSize(volume.capacity.usedBytes),
                            total: formatSize(volume.capacity.totalBytes),
                          })
                        : EM_DASH
                    }
                    capacity={volume.capacity}
                    error={volume.error}
                    className="ml-auto w-[260px] max-w-full"
                  />
                </div>
              ))
            )}
          </>
        )}
      </div>
    </NextShell>
  );
}
