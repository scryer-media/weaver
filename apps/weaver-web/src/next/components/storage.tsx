import { Eyebrow, Pie } from "./chrome";
import {
  mountUsedPercent,
  storageMounts,
  type StorageMount,
  type StorageVolume,
} from "../data/storage-mounts";
import { usageColor, WV } from "../data/palette";
import { formatSize } from "../data/format";
import { cn } from "@/lib/utils";

export { mountUsedPercent, storageMounts };
export type { StorageMount, StorageVolume };

/**
 * Every volume weaver writes to, and how full each one is.
 *
 * This replaced a menu that made you pick one path to look at: the question
 * "am I about to run out of room" is about all of them at once, and a disk you
 * have to go looking for is a disk you find out about too late.
 *
 * `stack` is the rail's column; `row` is the header strip.
 */
/**
 * One volume's pie and the two lines beside it: what it is, then how full.
 *
 * The same piece wherever a disk is shown, so a 70% disk reads the same on
 * Downloads, Completed and System info.
 */
export function StorageUsage({
  label,
  title,
  capacity,
  error,
  size = 46,
  className,
}: {
  label: string;
  /** Hover text for the label, when it stands for more than it says. */
  title?: string;
  capacity: StorageVolume["capacity"];
  error: string | null;
  size?: number;
  className?: string;
}) {
  const percent = capacity && capacity.totalBytes > 0 ? (capacity.usedBytes / capacity.totalBytes) * 100 : 0;
  return (
    <div className={cn("flex min-w-0 items-center gap-2.5", className)}>
      <Pie percent={percent} color={capacity ? usageColor(percent) : WV.inert} size={size} />
      <div className="min-w-0">
        <div className="truncate font-wv-mono text-[11px] text-wv-tertiary" title={title}>
          {label}
        </div>
        <div className="truncate text-[11px] text-wv-muted" title={capacity ? undefined : (error ?? undefined)}>
          {capacity
            ? `${Math.round(percent)}% full · ${formatSize(capacity.freeBytes)} free`
            : (error ?? "no capacity reported")}
        </div>
      </div>
    </div>
  );
}

export function StorageMounts({
  volumes,
  layout = "row",
  className,
}: {
  volumes: readonly StorageVolume[];
  layout?: "row" | "stack";
  className?: string;
}) {
  const mounts = storageMounts(volumes);
  const stacked = layout === "stack";

  if (mounts.length === 0) {
    return (
      <div className={cn("min-w-0", className)}>
        {stacked ? null : <Eyebrow tone="rail">Storage</Eyebrow>}
        <div className="mt-1.5 text-[12px] text-wv-muted">no storage configured</div>
      </div>
    );
  }

  return (
    <div className={cn("min-w-0", className)}>
      {stacked ? null : <Eyebrow tone="rail">Storage</Eyebrow>}
      <div
        className={cn(
          "min-w-0",
          stacked ? "flex flex-col gap-3" : "mt-2 flex flex-wrap items-center gap-x-7 gap-y-3",
        )}
      >
        {mounts.map((mount) => (
          <StorageUsage
            key={mount.key}
            label={mount.label}
            title={mount.paths.join("\n")}
            capacity={mount.capacity}
            error={mount.error}
            size={stacked ? 34 : 46}
          />
        ))}
      </div>
    </div>
  );
}
