/**
 * The settings search: what a query matches, and which panels have results.
 *
 * Every panel describes itself as blocks — sections of labelled fields, tables
 * of records, and the odd custom surface — and this module decides which of
 * those survive a query. It knows nothing about React, so the node test runner
 * loads it directly.
 *
 * A query is split on whitespace and every term must appear, in any order and
 * case, somewhere in the item's text or in the title of the block that holds
 * it: "proxy timeout" finds the Timeout row of a proxy section. A block whose
 * title matches survives whole.
 */

export interface SearchableField {
  label: string;
  help?: string;
  /** Extra text the search box should match — a path, a host, a unit. */
  keywords?: string;
  /** A row behind another row's toggle; shut rows still match, and a match opens them. */
  collapsed?: boolean;
}

export interface SearchableRow {
  /** Everything about this record the search box should match. */
  searchText: string;
}

export interface SearchableGroup {
  title: string;
  rows: readonly SearchableRow[];
}

export type SearchableBlock =
  | { kind: "section"; title: string; fields: readonly SearchableField[] }
  | {
      kind: "table";
      title: string;
      rows: readonly SearchableRow[];
      groups?: readonly SearchableGroup[];
    }
  | { kind: "custom"; title: string; searchText: string };

/** The lower-cased terms of a query; none for a blank one. */
export function searchTerms(query: string): string[] {
  return query.trim().toLowerCase().split(/\s+/).filter((term) => term !== "");
}

/** Whether every term occurs in `text`. No terms match everything. */
export function matchesTerms(text: string, terms: readonly string[]): boolean {
  const haystack = text.toLowerCase();
  return terms.every((term) => haystack.includes(term));
}

function fieldText(field: SearchableField): string {
  return `${field.label} ${field.help ?? ""} ${field.keywords ?? ""}`;
}

/**
 * Open the shut rows that match on their own text. A row is only shut because
 * the toggle above it is off; once the search found it, hiding it again would
 * leave a result that cannot be seen.
 */
function revealMatches<F extends SearchableField>(fields: readonly F[], terms: readonly string[]): F[] {
  return fields.map((field) =>
    field.collapsed === true && matchesTerms(fieldText(field), terms)
      ? { ...field, collapsed: false }
      : field,
  );
}

/**
 * What survives of one block, or null when nothing in it matches.
 *
 * The block comes back as the same kind with fewer fields, rows or groups and
 * every other property untouched, so the renderer draws it unchanged.
 */
export function filterBlock<B extends SearchableBlock>(block: B, terms: readonly string[]): B | null {
  if (terms.length === 0) {
    return block;
  }
  const titleMatches = matchesTerms(block.title, terms);
  switch (block.kind) {
    case "section": {
      if (titleMatches) {
        return { ...block, fields: revealMatches(block.fields, terms) };
      }
      const fields = block.fields
        .filter((field) => matchesTerms(`${block.title} ${fieldText(field)}`, terms))
        .map((field) => (field.collapsed === true ? { ...field, collapsed: false } : field));
      return fields.length > 0 ? { ...block, fields } : null;
    }
    case "table": {
      if (titleMatches) {
        return block;
      }
      const rows = block.rows.filter((row) => matchesTerms(`${block.title} ${row.searchText}`, terms));
      // A group's heading answers for its rows, as a block's title does.
      const groups = (block.groups ?? []).map((group) =>
        matchesTerms(`${block.title} ${group.title}`, terms)
          ? group
          : {
              ...group,
              rows: group.rows.filter((row) =>
                matchesTerms(`${block.title} ${group.title} ${row.searchText}`, terms),
              ),
            },
      );
      return rows.length > 0 || groups.some((group) => group.rows.length > 0)
        ? { ...block, rows, groups }
        : null;
    }
    case "custom":
      return titleMatches || matchesTerms(`${block.title} ${block.searchText}`, terms) ? block : null;
  }
}

/** The blocks a query leaves, in their order; all of them for a blank query. */
export function filterBlocks<B extends SearchableBlock>(blocks: readonly B[], query: string): B[] {
  const terms = searchTerms(query);
  return blocks.flatMap((block) => {
    const kept = filterBlock(block, terms);
    return kept === null ? [] : [kept];
  });
}

/* ------------------------------------------------------- across the panels */

/** What one rendered list of blocks found for the current query. */
export interface SearchReport {
  /** Blocks that survived the query. */
  count: number;
  /** Its settings are still loading, so it cannot answer yet. */
  loading: boolean;
}

const NOTHING: SearchReport = { count: 0, loading: false };

/**
 * Collects what every mounted list of blocks found, per panel.
 *
 * A panel can draw several lists — Networking's proxies page draws its pools
 * and its proxies — so each list reports under its own key and a panel's
 * answer is the sum of its lists. The shell and each panel's heading subscribe
 * to it to decide what to show.
 */
export class SearchRegistry {
  private readonly entries = new Map<string, SearchReport & { panel: string }>();
  private readonly listeners = new Set<() => void>();

  report(key: string, panel: string, report: SearchReport): void {
    const previous = this.entries.get(key);
    if (
      previous !== undefined
      && previous.panel === panel
      && previous.count === report.count
      && previous.loading === report.loading
    ) {
      return;
    }
    this.entries.set(key, { panel, count: report.count, loading: report.loading });
    this.emit();
  }

  remove(key: string): void {
    if (this.entries.delete(key)) {
      this.emit();
    }
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  /** One panel's answer: its lists' matches summed, loading while any list is. */
  panel(slug: string): SearchReport {
    let count = 0;
    let loading = false;
    for (const entry of this.entries.values()) {
      if (entry.panel === slug) {
        count += entry.count;
        loading ||= entry.loading;
      }
    }
    return count === 0 && !loading ? NOTHING : { count, loading };
  }

  private emit(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

/** What the settings area shows for a query across every panel. */
export type SearchOutcome =
  /** The panels with matches, in rail order. */
  | { state: "results"; panels: string[] }
  /** Nothing yet, and some panel is still loading its settings. */
  | { state: "loading" }
  | { state: "empty" };

/**
 * Assemble the cross-panel result from each panel's report, in rail order.
 *
 * Results show as soon as any panel has them, even while others still load:
 * a slow panel joins the list when it answers. Only an empty list waits.
 */
export function searchOutcome(
  slugs: readonly string[],
  report: (slug: string) => SearchReport,
): SearchOutcome {
  const panels: string[] = [];
  let loading = false;
  for (const slug of slugs) {
    const answer = report(slug);
    if (answer.count > 0) {
      panels.push(slug);
    }
    loading ||= answer.loading;
  }
  if (panels.length > 0) {
    return { state: "results", panels };
  }
  return loading ? { state: "loading" } : { state: "empty" };
}
