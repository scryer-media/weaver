import { useId } from "react";
import { cn } from "@/lib/utils";

interface SparklineProps {
  /** Data points, oldest → newest. */
  values: number[];
  /** Color comes from `currentColor`; set it with a text-* class (e.g. text-status-completed). */
  className?: string;
  /** viewBox width (the SVG itself scales to its container width). */
  width?: number;
  height?: number;
  strokeWidth?: number;
  /** Fill the area under the line with a faint wash of the current color. */
  fill?: boolean;
  /** Floor for the y-scale so a flat/low series still reads sensibly. */
  minScale?: number;
  /** Optional dashed cap line (e.g. a speed limit), in the same units as `values`. */
  capValue?: number | null;
}

/**
 * Lightweight inline-SVG sparkline. Deliberately dependency-free (no uPlot) so it
 * is cheap enough for the always-mounted sidebar speed card and per-row trends.
 */
export function Sparkline({
  values,
  className,
  width = 200,
  height = 26,
  strokeWidth = 1.6,
  fill = false,
  minScale = 6,
  capValue = null,
}: SparklineProps) {
  const gradientId = useId();
  if (values.length === 0) {
    return (
      <svg
        width="100%"
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        className={cn("block overflow-visible", className)}
        aria-hidden="true"
      />
    );
  }

  const usable = height - 2;
  const max = Math.max(minScale, ...values);
  const n = values.length;
  const points = values.map((value, index) => {
    const x = n === 1 ? 0 : (index / (n - 1)) * width;
    const y = height - (Math.max(0, value) / max) * usable;
    return [x, y] as const;
  });

  const pointsAttr = points.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const areaPath = fill
    ? `M ${points.map(([x, y]) => `${x.toFixed(1)} ${y.toFixed(1)}`).join(" L ")} L ${width} ${height} L 0 ${height} Z`
    : null;
  const capY =
    capValue != null && capValue > 0 ? Math.max(1, height - (Math.min(capValue, max) / max) * usable) : null;

  return (
    <svg
      width="100%"
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      className={cn("block overflow-visible", className)}
      aria-hidden="true"
    >
      {areaPath ? (
        <>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="currentColor" stopOpacity={0.22} />
              <stop offset="100%" stopColor="currentColor" stopOpacity={0} />
            </linearGradient>
          </defs>
          <path d={areaPath} fill={`url(#${gradientId})`} stroke="none" />
        </>
      ) : null}
      {capY != null ? (
        <line
          x1="0"
          y1={capY}
          x2={width}
          y2={capY}
          className="text-status-failed"
          stroke="currentColor"
          strokeWidth="1"
          strokeDasharray="4 3"
          vectorEffect="non-scaling-stroke"
        />
      ) : null}
      <polyline
        points={pointsAttr}
        fill="none"
        stroke="currentColor"
        strokeWidth={strokeWidth}
        strokeLinejoin="round"
        strokeLinecap="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}
