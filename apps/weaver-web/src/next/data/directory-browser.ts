/**
 * Path arithmetic for the folder picker.
 *
 * The daemon lists its own filesystem, which is POSIX on most installs and
 * Windows on some, so a path's root and separator come from the path itself
 * rather than from the browser the picker runs in.
 */

export interface DirectoryEntry {
  name: string;
  path: string;
}

export interface PathCrumb {
  label: string;
  path: string;
}

/** The root a path starts from and the separator it uses. */
function pathRoot(path: string): { root: string; separator: string } | null {
  const unc = /^(\\\\[^\\]+\\[^\\]+)(?:\\|$)/.exec(path);
  if (unc) {
    return { root: `${unc[1]}\\`, separator: "\\" };
  }
  const drive = /^([A-Za-z]:)(?:[\\/]|$)/.exec(path);
  if (drive) {
    const separator = path.includes("/") && !path.includes("\\") ? "/" : "\\";
    return { root: `${drive[1]}${separator}`, separator };
  }
  if (path.startsWith("/")) {
    return { root: "/", separator: "/" };
  }
  return null;
}

/**
 * The folders from the root down to `path`, each with the path that opens it.
 * A relative path has no root to walk from and yields a single crumb.
 */
export function pathCrumbs(path: string): PathCrumb[] {
  const trimmed = path.trim();
  const origin = pathRoot(trimmed);
  if (!origin) {
    return trimmed === "" ? [] : [{ label: trimmed, path: trimmed }];
  }
  const crumbs: PathCrumb[] = [{ label: origin.root, path: origin.root }];
  let current = origin.root;
  for (const segment of trimmed.slice(origin.root.length).split(/[\\/]+/)) {
    if (segment === "") continue;
    current = current.endsWith(origin.separator) ? `${current}${segment}` : `${current}${origin.separator}${segment}`;
    crumbs.push({ label: segment, path: current });
  }
  return crumbs;
}

/** The folder above `path`, or null at a root. */
export function parentPath(path: string): string | null {
  const crumbs = pathCrumbs(path);
  return crumbs.length < 2 ? null : crumbs[crumbs.length - 2]!.path;
}

/** Entries whose name contains `query`, ignoring case; every entry for a blank query. */
export function filterEntries<T extends DirectoryEntry>(entries: readonly T[], query: string): readonly T[] {
  const needle = query.trim().toLowerCase();
  return needle === "" ? entries : entries.filter((entry) => entry.name.toLowerCase().includes(needle));
}

/** "1,204 folders", or "12 of 1,204 folders" while a filter hides some. */
export function describeEntryCount(shown: number, total: number): string {
  const noun = total === 1 ? "folder" : "folders";
  return shown === total
    ? `${total.toLocaleString("en-US")} ${noun}`
    : `${shown.toLocaleString("en-US")} of ${total.toLocaleString("en-US")} ${noun}`;
}
