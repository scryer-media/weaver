import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { useQuery } from "urql";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { formatBytes } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { SYSTEM_INFO_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

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

interface SystemInfoData {
  systemInfo: SystemInfo;
}

interface UptimeAnchor {
  seconds: number;
  receivedAtMs: number;
}

export function SystemInfoPage() {
  const t = useTranslate();
  const [{ data, fetching, error }, refresh] = useQuery<SystemInfoData>({
    query: SYSTEM_INFO_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [uptimeAnchor, setUptimeAnchor] = useState<UptimeAnchor | null>(null);

  useEffect(() => {
    if (data?.systemInfo.uptimeSeconds == null) return;
    const receivedAtMs = Date.now();
    setNowMs(receivedAtMs);
    setUptimeAnchor({ seconds: data.systemInfo.uptimeSeconds, receivedAtMs });
  }, [data?.systemInfo.uptimeSeconds]);

  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  const uptimeSeconds = uptimeAnchor
    ? uptimeAnchor.seconds + Math.max(0, nowMs - uptimeAnchor.receivedAtMs) / 1000
    : 0;
  const info = data?.systemInfo;

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("systemInfo.title")}
        description={t("systemInfo.description")}
        actions={
          <Button
            variant="outline"
            onClick={() => refresh({ requestPolicy: "network-only" })}
            disabled={fetching}
          >
            <RefreshCw className={cn("size-4", fetching && "animate-spin")} />
            {t("action.refresh")}
          </Button>
        }
      />

      {error && !info ? (
        <SectionCard title={t("systemInfo.unavailable")}>
          <p className="text-sm text-status-failed">{error.message}</p>
        </SectionCard>
      ) : null}

      {!info && fetching ? (
        <div role="status" className="text-sm text-muted-foreground">
          {t("label.loading")}
        </div>
      ) : null}

      {info ? (
        <>
          <div className="grid gap-4 lg:grid-cols-2">
            <SectionCard title={t("systemInfo.softwareDeployment")}>
              <DetailGrid
                items={[
                  [t("systemInfo.version"), `v${info.version}`],
                  [t("systemInfo.uptime"), formatUptime(uptimeSeconds)],
                  [t("systemInfo.deployment"), enumLabel(info.deployment)],
                  [t("systemInfo.operatingSystem"), operatingSystemLabel(info.operatingSystem)],
                  [t("systemInfo.architecture"), info.architecture],
                  [t("systemInfo.database"), enumLabel(info.databaseEngine)],
                ]}
              />
            </SectionCard>

            <SectionCard title={t("systemInfo.compute")}>
              <DetailGrid
                items={[
                  [t("systemInfo.decoderTier"), decoderTierLabel(info.compute.decoderTier)],
                  [t("systemInfo.physicalCores"), String(info.compute.physicalCores)],
                  [t("systemInfo.logicalCores"), String(info.compute.logicalCores)],
                  [
                    t("systemInfo.cpuQuota"),
                    info.compute.cgroupLimit == null
                      ? t("systemInfo.notLimited")
                      : `${formatNumber(info.compute.cgroupLimit)} CPUs`,
                  ],
                  [
                    t("systemInfo.simdFeatures"),
                    info.compute.simdFeatures.length
                      ? info.compute.simdFeatures.join(", ")
                      : t("systemInfo.noneDetected"),
                  ],
                ]}
              />
            </SectionCard>

            <SectionCard title={t("systemInfo.memory")}>
              <DetailGrid
                items={[
                  [t("systemInfo.totalMemory"), formatBytes(info.memory.totalBytes)],
                  [
                    t("systemInfo.availableAtStartup"),
                    formatBytes(info.memory.availableAtStartupBytes),
                  ],
                  [
                    t("systemInfo.memoryLimit"),
                    info.memory.cgroupLimitBytes == null
                      ? t("systemInfo.notLimited")
                      : formatBytes(info.memory.cgroupLimitBytes),
                  ],
                  [
                    t("systemInfo.effectiveMemory"),
                    formatBytes(info.memory.effectiveLimitBytes),
                  ],
                ]}
              />
            </SectionCard>

            <SectionCard title={t("systemInfo.primaryStorage")}>
              <DetailGrid
                items={[
                  [t("systemInfo.storageClass"), info.primaryStorage.storageClass],
                  [t("systemInfo.filesystem"), info.primaryStorage.filesystem],
                  [
                    t("systemInfo.startupRandomIops"),
                    `${Math.round(info.primaryStorage.startupRandomReadIops).toLocaleString()} IOPS`,
                  ],
                ]}
              />
            </SectionCard>
          </div>

          <SectionCard
            title={t("systemInfo.configuredStorage")}
            description={t("systemInfo.configuredStoragePrivacy")}
            collapsible
            defaultOpen
          >
            {info.configuredStorage.length ? (
              <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                {info.configuredStorage.map((storage) => (
                  <StorageCard key={storage.path} storage={storage} />
                ))}
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">
                {t("systemInfo.noConfiguredStorage")}
              </p>
            )}
          </SectionCard>
        </>
      ) : null}
    </div>
  );
}

function DetailGrid({ items }: { items: Array<[string, string]> }) {
  return (
    <dl className="grid gap-x-6 gap-y-5 sm:grid-cols-2">
      {items.map(([label, value]) => (
        <div key={label} className="min-w-0">
          <dt className="text-[10.5px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
            {label}
          </dt>
          <dd className="mt-1 break-words text-[15px] font-semibold text-foreground">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function StorageCard({ storage }: { storage: ConfiguredStorage }) {
  const t = useTranslate();
  const capacity = storage.capacity;
  const percent = capacity && capacity.totalBytes > 0
    ? (capacity.usedBytes / capacity.totalBytes) * 100
    : 0;
  const barClass = percent >= 85
    ? "bg-status-failed"
    : percent >= 65
      ? "bg-status-paused"
      : "bg-status-downloading";

  return (
    <div className="rounded-inner border border-border bg-background/40 p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 text-[13px] font-semibold text-foreground">
          {storage.labels.join(" · ")}
        </div>
        {capacity ? (
          <span className="shrink-0 text-[12px] tabular-nums text-muted-foreground">
            {Math.round(percent)}%
          </span>
        ) : (
          <span className="shrink-0 text-[11px] font-semibold text-status-failed">
            {t("systemInfo.unavailable")}
          </span>
        )}
      </div>
      <div className="mt-1 break-all font-mono text-[11px] text-muted-foreground" title={storage.path}>
        {storage.path}
      </div>
      {capacity ? (
        <>
          <div className="mt-3 h-2 overflow-hidden rounded-pill bg-secondary">
            <div
              className={cn(
                "h-full rounded-pill transition-[width] duration-500 motion-reduce:transition-none",
                barClass,
              )}
              style={{ width: `${Math.min(100, percent)}%` }}
            />
          </div>
          <div className="mt-2.5 flex items-center justify-between gap-3 text-[12px]">
            <span className="font-medium text-foreground">
              {formatBytes(capacity.usedBytes)}{" "}
              <span className="font-normal text-muted-foreground">
                / {formatBytes(capacity.totalBytes)}
              </span>
            </span>
            <span className="shrink-0 text-muted-foreground">
              {formatBytes(capacity.freeBytes)} {t("metrics.diskFree")}
            </span>
          </div>
        </>
      ) : (
        <p className="mt-3 text-[12px] text-status-failed">
          {storage.error ?? t("systemInfo.capacityUnavailable")}
        </p>
      )}
    </div>
  );
}

function formatUptime(seconds: number): string {
  const total = Math.max(0, Math.floor(seconds));
  const days = Math.floor(total / 86_400);
  const hours = Math.floor((total % 86_400) / 3_600);
  const minutes = Math.floor((total % 3_600) / 60);
  const secs = total % 60;
  if (days > 0) return `${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function enumLabel(value: string): string {
  return value
    .toLowerCase()
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function operatingSystemLabel(value: string): string {
  if (value === "MACOS") return "macOS";
  return enumLabel(value);
}

function decoderTierLabel(value: string): string {
  const labels: Record<string, string> = {
    AVX_512_VBMI_2: "AVX-512 + VBMI2",
    AVX_2: "AVX2",
    AVX: "AVX",
    SSE_41: "SSE 4.1",
    SSSE_3: "SSSE3",
    SSE_2: "SSE2",
    NEON: "NEON",
    SCALAR: "Scalar",
  };
  return labels[value] ?? enumLabel(value);
}

function formatNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2).replace(/0+$/, "").replace(/\.$/, "");
}
