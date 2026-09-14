/**
 * Configured storage, reduced from a list of paths to a list of disks.
 *
 * Pure shaping, kept out of the component so it can be tested directly: the
 * rules below are the whole reason the rail shows what it shows, and they are
 * easier to get wrong than they look.
 */

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
 * of a busy disk does not split it in half; total bytes have to match exactly.
 *
 * A fingerprint alone cannot tell two identical, equally full disks apart, so
 * it only merges paths that are nested: a path joins a disk when it sits under,
 * or above, a path already on it. Two matching drives mounted side by side stay
 * two pies; the cost is that sibling folders on one disk with no configured
 * parent between them are drawn once each.
 *
 * A path with no capacity has no fingerprint, and the common reason is a
 * category directory the daemon has not had cause to create yet. Rather than
 * draw it as a disk of its own — a second entry for a filesystem already on
 * screen, wearing an error where its pie should be — it folds into the
 * configured ancestor it will be created under. Only a path with no such
 * ancestor stands alone, and then it says why.
 *
 * The fold deliberately requires the ancestor to have read its own capacity,
 * so it can never merge two paths that turned out to be separate mounts: a
 * mount nested under another reports its own figures, which keeps it apart on
 * the fingerprint, and this pass never looks at a path that reported any.
 */
export function storageMounts(volumes: readonly StorageVolume[]): StorageMount[] {
  const mounts = new Map<string, StorageMount>();
  const probed: { path: string; key: string }[] = [];
  const unprobed: StorageVolume[] = [];

  // Shallowest first, so a parent is on its disk before the folders under it
  // look for one and two siblings meet through it.
  const byDepth = [...volumes].sort(
    (left, right) => normalizeSeparators(left.path).length - normalizeSeparators(right.path).length,
  );
  for (const volume of byDepth) {
    if (!volume.capacity) {
      unprobed.push(volume);
      continue;
    }
    const fingerprint = `fs:${volume.capacity.totalBytes}:${Math.round(volume.capacity.usedBytes / MIB)}`;
    const existing = [...mounts.values()].find(
      (mount) =>
        mount.key.startsWith(`${fingerprint}#`) &&
        mount.paths.some((path) => contains(path, volume.path) || contains(volume.path, path)),
    );
    if (existing) {
      probed.push({ path: volume.path, key: existing.key });
      existing.paths.push(volume.path);
      existing.label = sharedPath(existing.paths);
      continue;
    }
    const key = `${fingerprint}#${volume.path}`;
    probed.push({ path: volume.path, key });
    mounts.set(key, {
      key,
      label: volume.path,
      paths: [volume.path],
      capacity: volume.capacity,
      error: null,
    });
  }

  for (const volume of unprobed) {
    // The nearest ancestor, so a category under the complete library folds
    // into the library rather than into the data directory above it.
    const parent = probed
      .filter((candidate) => contains(candidate.path, volume.path))
      .sort((left, right) => right.path.length - left.path.length)[0];
    const host = parent && mounts.get(parent.key);
    if (host) {
      host.paths.push(volume.path);
      host.label = sharedPath(host.paths);
      continue;
    }
    const key = `path:${volume.path}`;
    mounts.set(key, {
      key,
      label: volume.path,
      paths: [volume.path],
      capacity: null,
      error: volume.error,
    });
  }
  // Back in the order the daemon listed the paths.
  const order = new Map(volumes.map((volume, index) => [volume.path, index]));
  const first = (mount: StorageMount) =>
    Math.min(...mount.paths.map((path) => order.get(path) ?? Number.MAX_SAFE_INTEGER));
  return [...mounts.values()].sort((left, right) => first(left) - first(right));
}

/** Whether `child` sits under `parent`, comparing whole path segments. */
function contains(parent: string, child: string): boolean {
  const outer = normalizeSeparators(parent);
  const inner = normalizeSeparators(child);
  return inner.length > outer.length && inner.startsWith(outer.endsWith("/") ? outer : `${outer}/`);
}

/** Windows paths arrive with backslashes; only the comparison cares. */
function normalizeSeparators(path: string): string {
  return path.replaceAll("\\", "/");
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
