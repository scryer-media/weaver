import { Eyebrow, Pie } from "./chrome";
import { usageColor, WV } from "../data/palette";
import { formatSize } from "../data/format";
import { cn } from "@/lib/utils";

/**
 * Configured storage, as the daemon reports it.
 *
 * One entry per configured path, carrying the category labels that resolve to
 * it — which is why a naive list of these reads as a list of categories rather
 * than a list of disks.
 */
export interface StorageVolume {
  labels: string[];
  path: string;
  error: string | null;
  capacity: { totalBytes: number; usedBytes: number; freeBytes: number } | null;
}

export interface StorageMount {
  key: string;
  /** The deepest directory every path in the group shares. */
  label: string;
  paths: string[];
  capacity: StorageVolume["capacity"];
  error: string | null;
}

const MIB = 1024 * 1024;

/**
 * Collapse configured paths onto the volumes they actually live on.
 *
 * Several categories commonly point into one filesystem, and showing that disk
 * once per category says the box has more room than it does. Nothing in the
 * payload names the mount, so the capacity figures are the fingerprint: two
 * paths on one filesystem are handed the same numbers by the same `statvfs`.
 * Used bytes are compared to the mebibyte so a sampling skew between two reads
 * of a busy disk does not split it in half; total bytes have to match exactly,
 * which is what keeps two genuinely different disks apart.
 *
 * A path whose capacity could not be read has no fingerprint to group on, so it
 * stands alone and says why.
 */
export function storageMounts(volumes: readonly StorageVolume[]): StorageMount[] {
  const mounts = new Map<string, StorageMount>();
  for (const volume of volumes) {
    const key = volume.capacity
      ? `fs:${volume.capacity.totalBytes}:${Math.round(volume.capacity.usedBytes / MIB)}`
      : `path:${volume.path}`;
    const existing = mounts.get(key);
    if (existing) {
      existing.paths.push(volume.path);
      existing.label = sharedPath(existing.paths);
      existing.error ??= volume.error;
      continue;
    }
    mounts.set(key, {
      key,
      label: volume.path,
      paths: [volume.path],
      capacity: volume.capacity,
      error: volume.error,
    });
  }
  return [...mounts.values()];
}

/** The common parent of a mount's paths, or the first path if they share none. */
function sharedPath(paths: readonly string[]): string {
  if (paths.length === 1) {
    return paths[0];
  }
  const split = paths.map((path) => path.split("/"));
  const shared: string[] = [];
  for (let index = 0; index < split[0].length; index += 1) {
    const segment = split[0][index];
    if (!split.every((parts) => parts[index] === segment)) {
      break;
    }
    shared.push(segment);
  }
  const prefix = shared.join("/");
  return prefix.length > 1 ? prefix : paths[0];
}

export function mountUsedPercent(mount: StorageMount): number {
  if (!mount.capacity || mount.capacity.totalBytes <= 0) {
    return 0;
  }
  return (mount.capacity.usedBytes / mount.capacity.totalBytes) * 100;
}

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
