import { useEffect, useMemo, useRef } from "react";
import { useTheme } from "next-themes";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import {
  formatMetricValue,
  type ChartColorToken,
  type MetricScale,
  type MetricValueFormat,
} from "@/lib/metrics";
import { cn } from "@/lib/utils";

type AlignedData = uPlot.AlignedData;

export interface TimeSeriesChartSeries {
  label: string;
  colorToken: ChartColorToken;
  format: MetricValueFormat;
  scale?: MetricScale;
}

interface TimeSeriesChartProps {
  data: AlignedData;
  series: TimeSeriesChartSeries[];
  leftAxisFormat: MetricValueFormat;
  rightAxisFormat?: MetricValueFormat;
  emptyLabel: string;
  height?: number;
  className?: string;
}

function readThemeColors(themeMode: "light" | "dark") {
  const styles = getComputedStyle(document.documentElement);
  const fallbackPalette =
    themeMode === "dark"
      ? {
          sky: "#38bdf8",
          teal: "#2dd4bf",
          orange: "#fb923c",
          violet: "#a78bfa",
          magenta: "#e879f9",
          red: "#f87171",
          amber: "#fbbf24",
          indigo: "#818cf8",
          green: "#4ade80",
        }
      : {
          sky: "#0369a1",
          teal: "#0f766e",
          orange: "#ea580c",
          violet: "#7c3aed",
          magenta: "#a21caf",
          red: "#dc2626",
          amber: "#d97706",
          indigo: "#4f46e5",
          green: "#16a34a",
        };
  return {
    axis: styles.getPropertyValue("--muted-foreground").trim() || "#64748b",
    grid: styles.getPropertyValue("--border-color").trim() || "#e2e8f0",
    palette: {
      sky: styles.getPropertyValue("--chart-sky").trim() || fallbackPalette.sky,
      teal: styles.getPropertyValue("--chart-teal").trim() || fallbackPalette.teal,
      orange: styles.getPropertyValue("--chart-orange").trim() || fallbackPalette.orange,
      violet: styles.getPropertyValue("--chart-violet").trim() || fallbackPalette.violet,
      magenta: styles.getPropertyValue("--chart-magenta").trim() || fallbackPalette.magenta,
      red: styles.getPropertyValue("--chart-red").trim() || fallbackPalette.red,
      amber: styles.getPropertyValue("--chart-amber").trim() || fallbackPalette.amber,
      indigo: styles.getPropertyValue("--chart-indigo").trim() || fallbackPalette.indigo,
      green: styles.getPropertyValue("--chart-green").trim() || fallbackPalette.green,
    } satisfies Record<ChartColorToken, string>,
  };
}

function formatTimeTick(timestampSec: number, spanSec: number): string {
  const date = new Date(timestampSec * 1000);
  if (spanSec >= 24 * 60 * 60) {
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
    }).format(date);
  }
  if (spanSec >= 6 * 60 * 60) {
    return new Intl.DateTimeFormat(undefined, {
      hour: "numeric",
      minute: "2-digit",
    }).format(date);
  }
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function latestSeriesValue(data: AlignedData, seriesIndex: number): number {
  const values = data[seriesIndex + 1] as (number | null | undefined)[] | undefined;
  if (!values || values.length === 0) {
    return 0;
  }
  const nextValue = values[values.length - 1];
  return typeof nextValue === "number" && Number.isFinite(nextValue) ? nextValue : 0;
}

export function TimeSeriesChart({
  data,
  series = [],
  leftAxisFormat,
  rightAxisFormat,
  emptyLabel,
  height = 240,
  className,
}: TimeSeriesChartProps) {
  const { resolvedTheme } = useTheme();
  const hostRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  const dataRef = useRef<AlignedData>(data);
  const hasData = data[0]?.length > 0;
  const themeMode = resolvedTheme === "dark" ? "dark" : "light";
  const themeColors = useMemo(() => readThemeColors(themeMode), [themeMode]);
  const resolvedSeries = useMemo(
    () =>
      series.map((item) => ({
        ...item,
        color: themeColors.palette[item.colorToken],
      })),
    [series, themeColors],
  );

  const latestValues = useMemo(
    () => resolvedSeries.map((_, index) => latestSeriesValue(data, index)),
    [data, resolvedSeries],
  );
  const timeSpanBucket = useMemo(() => {
    const timestamps = data[0] as number[] | undefined;
    if (!timestamps || timestamps.length < 2) {
      return 0;
    }
    return Math.round((timestamps[timestamps.length - 1] - timestamps[0]) / 60);
  }, [data]);

  const chartConfig = useMemo(
    () => ({
      height,
      leftAxisFormat,
      rightAxisFormat,
      timeSpanBucket,
      series: resolvedSeries.map((item) => ({
        label: item.label,
        color: item.color,
        scale: item.scale ?? "left",
      })),
    }),
    [height, leftAxisFormat, resolvedSeries, rightAxisFormat, timeSpanBucket],
  );

  const configKey = useMemo(
    () => JSON.stringify(chartConfig),
    [chartConfig],
  );

  const stableConfig = useMemo(
    () =>
      JSON.parse(configKey) as {
        height: number;
        leftAxisFormat: MetricValueFormat;
        rightAxisFormat?: MetricValueFormat;
        timeSpanBucket: number;
        series: Array<{
          label: string;
          color: string;
          scale: MetricScale;
        }>;
      },
    [configKey],
  );

  useEffect(() => {
    if (!hostRef.current || !hasData) {
      plotRef.current?.destroy();
      plotRef.current = null;
      return;
    }

    const host = hostRef.current;
    const chartSeries = stableConfig.series;
    const hasRightAxis = chartSeries.some((item) => item.scale === "right");
    const timestamps = dataRef.current[0] as number[];
    const spanSec =
      timestamps.length > 1 ? timestamps[timestamps.length - 1] - timestamps[0] : 0;
    const width = Math.max(280, Math.floor(host.clientWidth || 280));

    const options: uPlot.Options = {
      width,
      height: stableConfig.height,
      ms: 1e-3,
      padding: [8, 8, 0, 8],
      legend: { show: false },
      cursor: {
        x: true,
        y: true,
        focus: { prox: 24 },
      },
      scales: {
        x: { time: true },
        y: { auto: true },
        ...(hasRightAxis ? { y2: { auto: true } } : {}),
      },
      axes: [
        {
          stroke: themeColors.axis,
          grid: { stroke: themeColors.grid, width: 1 },
          values: (_self, splits) =>
            splits.map((value) => formatTimeTick(Number(value), spanSec)),
        },
        {
          scale: "y",
          side: 3,
          stroke: themeColors.axis,
          size: 68,
          grid: { stroke: themeColors.grid, width: 1 },
          values: (_self, splits) =>
            splits.map((value) => formatMetricValue(stableConfig.leftAxisFormat, Number(value))),
        },
        ...(hasRightAxis
          ? [
              {
                scale: "y2",
                side: 1,
                stroke: themeColors.axis,
                size: 68,
                grid: { show: false },
                values: (_self, splits) =>
                  splits.map((value) =>
                    formatMetricValue(
                      stableConfig.rightAxisFormat ?? stableConfig.leftAxisFormat,
                      Number(value),
                    ),
                  ),
              } satisfies uPlot.Axis,
            ]
          : []),
      ],
      series: [
        {},
        ...chartSeries.map(
          (item) =>
            ({
              label: item.label,
              stroke: item.color,
              width: 2,
              scale: item.scale === "right" ? "y2" : "y",
              points: { show: false },
              spanGaps: false,
            }) satisfies uPlot.Series,
        ),
      ],
    };

    const plot = new uPlot(options, dataRef.current, host);
    plotRef.current = plot;

    const resizeObserver = new ResizeObserver((entries) => {
      const nextWidth = Math.max(
        280,
        Math.floor(entries[0]?.contentRect.width ?? host.clientWidth ?? 280),
      );
      plot.setSize({ width: nextWidth, height: stableConfig.height });
    });
    resizeObserver.observe(host);

    return () => {
      resizeObserver.disconnect();
      plot.destroy();
      plotRef.current = null;
    };
  }, [hasData, stableConfig, themeColors]);

  useEffect(() => {
    if (hasData) {
      dataRef.current = data;
      plotRef.current?.setData(data);
    }
  }, [data, hasData]);

  return (
    <div className={cn("space-y-4", className)}>
      <div
        className="relative overflow-hidden rounded-2xl border border-border/70 bg-field/60 p-2"
        style={{ height }}
      >
        {hasData ? <div ref={hostRef} className="h-full w-full" /> : null}
        {!hasData ? (
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            {emptyLabel}
          </div>
        ) : null}
      </div>

      <div className="grid gap-2 sm:grid-cols-2">
        {resolvedSeries.map((item, index) => (
          <div
            key={`${item.label}-${index}`}
            className="rounded-2xl border border-border/60 bg-background/60 px-3 py-2"
          >
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.14em] text-muted-foreground">
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: item.color }}
                aria-hidden
              />
              <span>{item.label}</span>
            </div>
            <div className="mt-2 text-sm font-semibold text-foreground">
              {formatMetricValue(item.format, latestValues[index] ?? 0)}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
