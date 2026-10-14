import { useEffect, useId, useRef, useState } from "react";
import { cn } from "@/lib/utils";

/** One closed bin: when it closed, in ms since measuring began, and the rate over it. */
export type RatePoint = { at: number; rate: number };

/** How much of the run the chart shows: the newest this many ms. */
const WINDOW_MS = 30_000;
const HEIGHT = 132;
/** Room at the top for the scale and the leading dot's halo. */
const HEAD = 18;

/**
 * The smallest round figure at or above `value` in the unit a rate is shown in
 * (KB/s, MB/s, GB/s), so the scale reads cleanly and moves seldom.
 */
function niceCeiling(value: number): number {
  if (value <= 0) {
    return 1;
  }
  const unit = 1024 ** Math.min(Math.max(Math.floor(Math.log(value) / Math.log(1024)), 0), 4);
  const scaled = value / unit;
  const power = 10 ** Math.floor(Math.log10(scaled));
  return ([1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].find((step) => step * power >= scaled) ?? 10) * power * unit;
}

/** How long the chart takes to move on to a newly closed bin; the polls arrive about a second apart. */
const GLIDE_MS = 1000;

/**
 * The point on the run's clock the chart shows, gliding on to each newly closed
 * bin over a second rather than jumping. A clock that goes back is a new run, and
 * jumps there.
 */
function useGlide(target: number): number {
  const [shown, setShown] = useState(target);
  const current = useRef(target);
  useEffect(() => {
    const from = current.current;
    if (target <= from || window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) {
      current.current = target;
      setShown(target);
      return;
    }
    const since = performance.now();
    let frame = requestAnimationFrame(function step(now: number) {
      const progress = Math.min(1, Math.max(0, (now - since) / GLIDE_MS));
      current.current = from + (target - from) * progress;
      setShown(current.current);
      if (progress < 1) {
        frame = requestAnimationFrame(step);
      }
    });
    return () => cancelAnimationFrame(frame);
  }, [target]);
  return shown;
}

/**
 * A rate as it goes, chasing the newest second: the line draws on to the right
 * until the window is full, then the window slides so the newest second stays
 * at the right edge. Each poll moves it on over a second rather than jumping.
 * The reveal, the slide and the leading dot all follow one clock, and the dot is
 * placed on the line as drawn, so it never leaves it.
 */
export function LiveChart({
  label,
  points,
  formatRate,
  color = "var(--wv-accent)",
}: {
  label: string;
  points: RatePoint[];
  formatRate: (rate: number) => string;
  color?: string;
}) {
  const fill = useId();
  const clip = useId();
  const box = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const element = box.current;
    if (!element) {
      return;
    }
    const observer = new ResizeObserver(([entry]) => setWidth(Math.round(entry?.contentRect.width ?? 0)));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const latest = points.at(-1);
  const end = latest?.at ?? 0;
  const shown = useGlide(end);
  const ceiling = niceCeiling(Math.max(0, ...points.filter((point) => point.at > end - WINDOW_MS * 1.5).map((point) => point.rate)));
  const scale = width / WINDOW_MS;
  const y = (rate: number) => HEIGHT - (Math.min(rate, ceiling) / ceiling) * (HEIGHT - HEAD);
  // The line is laid out on the run's own clock from 0; the window is moved over it.
  const shift = Math.min(0, width - shown * scale);
  const reach = shown * scale + shift;
  const vertices = [{ at: 0, rate: points[0]?.rate ?? 0 }, ...points]
    .filter((point, index, all) => (all[index + 1]?.at ?? Infinity) >= shown - WINDOW_MS - 2000)
    .map((point) => ({ x: point.at * scale, y: y(point.rate) }));
  const line = vertices.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`);
  const first = line[0]?.split(",")[0] ?? "0";
  // Where the line stands at the shown moment, between the two corners either side of it.
  const at = shown * scale;
  const after = vertices.findIndex((point) => point.x >= at);
  const before = vertices[after - 1];
  const next = vertices[after] ?? vertices.at(-1);
  const dot =
    before && next && next.x > before.x
      ? before.y + ((next.y - before.y) * (at - before.x)) / (next.x - before.x)
      : (next?.y ?? HEIGHT);

  return (
    <div ref={box} className="relative h-[132px] w-full min-w-0" role="img" aria-label={label}>
      {width > 0 ? (
        <svg width={width} height={HEIGHT} className="block overflow-visible">
          <defs>
            <linearGradient id={fill} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.28" />
              <stop offset="100%" stopColor={color} stopOpacity="0" />
            </linearGradient>
            <clipPath id={clip}>
              {/* What has arrived so far, revealed as it arrives. */}
              <rect x={reach - width} y={-HEAD} width={width} height={HEIGHT + 2 * HEAD} />
            </clipPath>
          </defs>
          <line x1={0} x2={width} y1={HEIGHT - 0.5} y2={HEIGHT - 0.5} className="stroke-wv-hairline" />
          <line x1={0} x2={width} y1={HEAD} y2={HEAD} strokeDasharray="2 4" className="stroke-wv-hairline" />
          <text x={0} y={HEAD - 6} className="fill-wv-muted font-wv-mono text-[10.5px]">
            {formatRate(ceiling)}
          </text>
          {points.length ? (
            <g clipPath={`url(#${clip})`}>
              <g transform={`translate(${shift.toFixed(1)} 0)`}>
                <polygon points={`${first},${HEIGHT} ${line.join(" ")} ${(end * scale).toFixed(1)},${HEIGHT}`} fill={`url(#${fill})`} />
                <polyline points={line.join(" ")} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
              </g>
            </g>
          ) : null}
          <g className={cn(!latest && "opacity-0")} transform={`translate(${reach.toFixed(1)} ${dot.toFixed(1)})`}>
            <circle r={8} fill={color} opacity={0.25} className="origin-center [transform-box:fill-box] motion-safe:animate-ping" />
            <circle r={3.5} fill={color} />
          </g>
        </svg>
      ) : null}
    </div>
  );
}
