import type { ReactNode } from "react";
import { SectionHeader, Square } from "./chrome";

export interface ChartSeries {
  key: string;
  /** Uppercased in the legend. */
  label: string;
  color: string;
  /** Area fill under the line. The handoff gives it to the lead series only. */
  fill?: string;
  /** Oldest sample first. Series may be shorter than each other. */
  values: readonly number[];
  /** Pre-formatted legend value; the chart never guesses a unit. */
  value: ReactNode;
}

/**
 * Pick the axis maximum.
 *
 * The prototypes hard-code one scale per chart, which cannot survive real
 * series — a 9 MB/s ceiling clips the moment a gigabit link shows up. Instead
 * the top gridline is the smallest "nice" number (1, 2, 2.5 or 5 × a power of
 * ten) at or above the largest sample, which keeps the three labels below it
 * readable and stops the line from ever touching the top border.
 */
function niceCeiling(max: number): number {
  if (!Number.isFinite(max) || max <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(max));
  const normalised = max / magnitude;
  const step = normalised <= 1 ? 1 : normalised <= 2 ? 2 : normalised <= 2.5 ? 2.5 : normalised <= 5 ? 5 : 10;
  return step * magnitude;
}

function pointsFor(values: readonly number[], ceiling: number): string {
  if (values.length === 0) return "";
  if (values.length === 1) {
    const y = 100 - (values[0]! / ceiling) * 100;
    return `0,${y.toFixed(2)} 100,${y.toFixed(2)}`;
  }
  const step = 100 / (values.length - 1);
  return values
    .map((value, index) => {
      const x = index * step;
      const y = 100 - (Math.max(0, value) / ceiling) * 100;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

/**
 * A plot box plus a legend column.
 *
 * The plot is drawn in a unit viewBox stretched by `preserveAspectRatio="none"`,
 * so it resizes with the column without re-projecting any points; the stroke is
 * kept at a true 1.6px by `vector-effect: non-scaling-stroke`.
 */
export function Chart({
  title,
  note,
  series,
  xLabels,
  formatValue,
}: {
  title: string;
  note?: ReactNode;
  series: readonly ChartSeries[];
  /** Three labels, oldest to newest. */
  xLabels: readonly string[];
  /** Renders the four y-axis labels. */
  formatValue: (value: number) => string;
}) {
  const max = series.reduce(
    (highest, entry) => entry.values.reduce((inner, value) => (value > inner ? value : inner), highest),
    0,
  );
  const ceiling = niceCeiling(max);
  const yLabels = [ceiling, (ceiling * 2) / 3, ceiling / 3, 0];

  return (
    <section className="flex flex-none flex-col">
      <SectionHeader label={title} note={note} sticky={false} />
      <div className="flex flex-wrap items-start gap-x-6 gap-y-5 px-4 sm:px-6 py-5">
        <div className="flex min-w-[280px] flex-[1_1_360px] gap-3">
          <div className="flex h-[148px] w-[62px] flex-none flex-col justify-between text-right font-wv-mono text-[10px] text-wv-faint">
            {yLabels.map((value, index) => (
              <span key={index}>{formatValue(value)}</span>
            ))}
          </div>
          <div className="flex min-w-0 flex-1 flex-col">
            <div className="relative h-[148px] border-b border-l border-wv-axis">
              {[0, 33, 66].map((offset) => (
                <div
                  key={offset}
                  className="absolute inset-x-0 border-t border-wv-hairline"
                  style={{ top: `${offset}%` }}
                />
              ))}
              {series.map((entry) => {
                const points = pointsFor(entry.values, ceiling);
                if (points === "") return null;
                return (
                  <svg
                    key={entry.key}
                    className="absolute inset-0 h-full w-full overflow-visible"
                    viewBox="0 0 100 100"
                    preserveAspectRatio="none"
                    aria-hidden="true"
                  >
                    {entry.fill === undefined ? null : (
                      <polygon points={`${points} 100,100 0,100`} fill={entry.fill} />
                    )}
                    <polyline
                      points={points}
                      fill="none"
                      stroke={entry.color}
                      strokeWidth={1.6}
                      strokeLinejoin="round"
                      strokeLinecap="round"
                      vectorEffect="non-scaling-stroke"
                    />
                  </svg>
                );
              })}
            </div>
            <div className="mt-[7px] flex justify-between font-wv-mono text-[10.5px] text-wv-faint">
              {xLabels.map((label, index) => (
                <span key={index}>{label}</span>
              ))}
            </div>
          </div>
        </div>
        <div className="flex w-full flex-none flex-col self-stretch lg:w-[250px]">
          {series.map((entry) => (
            <div
              key={entry.key}
              className="flex flex-1 items-center gap-[10px] border-b border-wv-hairline py-2 last:border-b-0"
            >
              <Square color={entry.color} />
              <span className="min-w-0 truncate font-wv-mono text-[10.5px] tracking-[0.1em] text-wv-muted uppercase">
                {entry.label}
              </span>
              <span className="ml-auto font-wv-mono text-[13px] whitespace-nowrap text-wv-secondary">
                {entry.value}
              </span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
