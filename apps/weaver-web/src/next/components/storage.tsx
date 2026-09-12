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
        {mounts.map((mount) => {
          const percent = mountUsedPercent(mount);
          return (
            <div key={mount.key} className="flex min-w-0 items-center gap-2.5">
              <Pie
                percent={percent}
                color={mount.capacity ? usageColor(percent) : WV.inert}
                size={stacked ? 34 : 46}
              />
              <div className="min-w-0">
                <div
                  className="truncate font-wv-mono text-[11px] text-wv-tertiary"
                  title={mount.paths.join("\n")}
                >
                  {mount.label}
                </div>
                <div className="truncate text-[11px] text-wv-muted">
                  {mount.capacity
                    ? `${Math.round(percent)}% full · ${formatSize(mount.capacity.freeBytes)} free`
                    : (mount.error ?? "no capacity reported")}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
