import type { CSSProperties } from "react";

/**
 * Grid tracks that change with the viewport.
 *
 * The handoff specifies exact `grid-template-columns` for every list, written
 * for a wide window. A phone cannot hold six tracks, so a table declares the
 * sets it wants and `wv-cols` (see `theme.css`) picks one per width — the
 * alternative was a breakpoint fork inside each screen, which is the bespoke
 * thing this UI is trying not to have.
 *
 * A plain string still means "these tracks at every width", which is right for
 * the grids that already collapse on their own (`repeat(auto-fit, …)`).
 *
 * Steps fall back through the smaller ones, so `{ base, lg }` is a complete
 * answer and the sets you skip cost nothing.
 */
export type Columns =
  | string
  | {
      /** Below 640px. */
      base: string;
      /** 640px and up. */
      sm?: string;
      /** 1024px and up. */
      lg?: string;
      /** 1280px and up. */
      xl?: string;
    };

/** The class that reads the properties below; pair it with `columnStyle`. */
export const COLS_CLASS = "wv-cols";

export function columnStyle(columns: Columns, extra?: CSSProperties): CSSProperties {
  const tracks =
    typeof columns === "string"
      ? { "--wv-cols": columns }
      : {
          "--wv-cols": columns.base,
          ...(columns.sm === undefined ? {} : { "--wv-cols-sm": columns.sm }),
          ...(columns.lg === undefined ? {} : { "--wv-cols-lg": columns.lg }),
          ...(columns.xl === undefined ? {} : { "--wv-cols-xl": columns.xl }),
        };
  return { ...extra, ...tracks } as CSSProperties;
}
