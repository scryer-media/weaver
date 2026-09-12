import type { ConfiguredCategory } from "./next-data";

/**
 * The category rail, as facets.
 *
 * The list is built from the *configured* categories first, in the order the
 * daemon returns them — that is a list a person wrote, so it is not ours to
 * alphabetise. A category with nothing in it right now still appears: it is
 * something you can filter by, and a rail that only shows what the queue
 * happens to hold changes shape every time a job finishes.
 *
 * Names that are not configured are added after them, alphabetically, so a
 * category that was renamed or deleted while jobs still carry it stays
 * reachable instead of stranding those rows behind a facet that no longer
 * exists.
 */

/**
 * The facet key for a job with no category at all.
 *
 * A real category named `uncategorised` would collide with it. That is a price
 * worth paying for a key that reads plainly in a URL and in a test, and the
 * two mean nearly the same thing to a person anyway.
 */
export const UNCATEGORISED = "uncategorised";

/** No facets: the cleared state, shared so it is referentially stable. */
export const NO_FACETS: ReadonlySet<string> = new Set<string>();

export interface CategoryEntry {
  /** `null` is the "all categories" row; anything else is a real category name. */
  key: string | null;
  label: string;
  /**
   * Rows this facet would show on its own, counted before any facet is
   * applied, so the numbers beside the other rows do not move as you select.
   *
   * Omitted where the client cannot count truthfully: history is paginated on
   * the server, so the only number available is "how many on this page", which
   * is not the answer to the question a number there appears to answer.
   */
  count?: number;
}

/** A row that carries a category, which is all the facet count needs of it. */
interface Categorised {
  category?: string | null;
}

export function facetKey(row: Categorised): string {
  return row.category || UNCATEGORISED;
}

export function categoryFacets({
  configured,
  rows,
  extras = [],
}: {
  configured: readonly ConfiguredCategory[];
  /** Rows to count, or omitted where no honest count is available. */
  rows?: readonly Categorised[];
  /** Category names to keep reachable even when nothing configured matches. */
  extras?: readonly string[];
}): CategoryEntry[] {
  const counts = rows === undefined ? undefined : new Map<string, number>();
  if (counts && rows) {
    for (const row of rows) {
      const key = facetKey(row);
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
  }

  const configuredNames = new Set(configured.map((category) => category.name));
  const straggling = new Set<string>();
  for (const name of [...extras, ...(counts?.keys() ?? [])]) {
    if (name !== UNCATEGORISED && !configuredNames.has(name)) {
      straggling.add(name);
    }
  }

  const entries: CategoryEntry[] = [{ key: null, label: "All categories", count: rows?.length }];
  const push = (name: string) => {
    entries.push({
      key: name,
      label: name,
      count: counts === undefined ? undefined : (counts.get(name) ?? 0),
    });
  };
  for (const name of configuredNames) {
    push(name);
  }
  for (const name of [...straggling].sort((left, right) => left.localeCompare(right))) {
    push(name);
  }
  // Uncategorised is a facet, but only once there is something in it — or when
  // there is no way to tell, which is history.
  if (counts === undefined || (counts.get(UNCATEGORISED) ?? 0) > 0) {
    entries.push({
      key: UNCATEGORISED,
      label: "Uncategorised",
      count: counts?.get(UNCATEGORISED),
    });
  }
  return entries;
}

/** The facet set with `key` flipped; an empty result is the cleared state. */
export function toggleFacet(current: ReadonlySet<string>, key: string): ReadonlySet<string> {
  const next = new Set(current);
  if (!next.delete(key)) {
    next.add(key);
  }
  return next.size === 0 ? NO_FACETS : next;
}

/**
 * The facet set as the daemon's history filter wants it.
 *
 * `uncategorised` becomes the empty string, which is what a row with no
 * category compares equal to on the server; an empty selection becomes
 * `undefined` so the filter is absent from the request rather than present
 * and vacuous.
 */
export function facetsToCategories(facets: ReadonlySet<string>): string[] | undefined {
  if (facets.size === 0) {
    return undefined;
  }
  return [...facets].map((key) => (key === UNCATEGORISED ? "" : key));
}
