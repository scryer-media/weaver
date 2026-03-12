import { useMemo, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";
import { useTranslate } from "@/lib/context/translate-context";

export interface TimelineSpan {
  startedAt: number;
  endedAt: number | null;
  state: "RUNNING" | "COMPLETE" | "FAILED";
  label: string | null;
}

export interface TimelineLane {
  stage:
    | "PENDING_DOWNLOAD"
    | "DOWNLOADING"
    | "PAUSED"
    | "VERIFYING"
    | "REPAIRING"
    | "EXTRACTING"
    | "INTERRUPTED"
    | "FINAL_MOVE";
  spans: TimelineSpan[];
}

export interface ExtractionMemberSpan {
  kind: "EXTRACTING" | "WAITING_FOR_VOLUME" | "APPENDING";
  startedAt: number;
  endedAt: number | null;
  state: "RUNNING" | "COMPLETE" | "FAILED";
  label: string | null;
}

export interface ExtractionMemberTimeline {
  member: string;
  state: "RUNNING" | "INTERRUPTED" | "COMPLETE" | "AWAITING_REPAIR" | "FAILED";
  error: string | null;
  spans: ExtractionMemberSpan[];
}

export interface ExtractionTimelineGroup {
  setName: string;
  members: ExtractionMemberTimeline[];
}

export interface JobTimelineData {
  startedAt: number;
  endedAt: number | null;
  outcome: string;
  lanes: TimelineLane[];
  extractionGroups: ExtractionTimelineGroup[];
}

type BadgeVariant = "success" | "warning" | "destructive" | "info" | "muted" | "outline";

type DetailItem = {
  label: string;
  value: string;
};

type PlotSpan = {
  key: string;
  startedAt: number;
  endedAt: number | null;
  state: TimelineSpan["state"];
  colorClass: string;
  title: string;
  subtitle?: string | null;
  dashed?: boolean;
  details: DetailItem[];
};

type PlotRow = {
  key: string;
  title: string;
  displayTitle?: string | null;
  tone: "stage" | "member";
  details: DetailItem[];
  badge?: {
    label: string;
    variant: BadgeVariant;
  };
  spans: PlotSpan[];
};

const JOB_STAGE_KEYS: Record<TimelineLane["stage"], string> = {
  PENDING_DOWNLOAD: "timeline.pendingDownload",
  DOWNLOADING: "timeline.downloading",
  PAUSED: "timeline.paused",
  VERIFYING: "timeline.verifying",
  REPAIRING: "timeline.repairing",
  EXTRACTING: "timeline.extracting",
  INTERRUPTED: "timeline.interrupted",
  FINAL_MOVE: "timeline.finalMove",
};

function laneColor(stage: TimelineLane["stage"]): string {
  switch (stage) {
    case "PENDING_DOWNLOAD":
      return "bg-slate-400/75";
    case "DOWNLOADING":
      return "bg-sky-500/90";
    case "PAUSED":
      return "bg-cyan-500/85";
    case "VERIFYING":
      return "bg-amber-500/90";
    case "REPAIRING":
      return "bg-orange-500/90";
    case "EXTRACTING":
      return "bg-violet-500/90";
    case "INTERRUPTED":
      return "bg-rose-500/70";
    case "FINAL_MOVE":
      return "bg-emerald-500/90";
  }
}

function spanColor(kind: ExtractionMemberSpan["kind"]): string {
  switch (kind) {
    case "WAITING_FOR_VOLUME":
      return "bg-slate-300/85";
    case "APPENDING":
      return "bg-teal-500/90";
    case "EXTRACTING":
    default:
      return "bg-violet-500/90";
  }
}

function stateVariant(
  state: TimelineSpan["state"] | ExtractionMemberTimeline["state"],
): Exclude<BadgeVariant, "outline"> {
  switch (state) {
    case "COMPLETE":
      return "success";
    case "FAILED":
      return "destructive";
    case "AWAITING_REPAIR":
      return "warning";
    case "INTERRUPTED":
      return "muted";
    case "RUNNING":
      return "info";
    default:
      return "muted";
  }
}

function stateLabel(
  t: ReturnType<typeof useTranslate>,
  state: ExtractionMemberTimeline["state"],
) {
  switch (state) {
    case "COMPLETE":
      return t("timeline.memberComplete");
    case "AWAITING_REPAIR":
      return t("timeline.memberAwaitingRepair");
    case "FAILED":
      return t("timeline.memberFailed");
    case "INTERRUPTED":
      return t("timeline.memberInterrupted");
    case "RUNNING":
    default:
      return t("timeline.memberRunning");
  }
}

function spanStateLabel(
  t: ReturnType<typeof useTranslate>,
  state: TimelineSpan["state"],
): string {
  switch (state) {
    case "COMPLETE":
      return t("timeline.memberComplete");
    case "FAILED":
      return t("timeline.memberFailed");
    case "RUNNING":
    default:
      return t("timeline.memberRunning");
  }
}

function spanKindLabel(
  t: ReturnType<typeof useTranslate>,
  kind: ExtractionMemberSpan["kind"],
) {
  switch (kind) {
    case "WAITING_FOR_VOLUME":
      return t("timeline.waitingForVolume");
    case "APPENDING":
      return t("timeline.appending");
    case "EXTRACTING":
    default:
      return t("timeline.extracting");
  }
}

function outcomeVariant(outcome: string): Exclude<BadgeVariant, "outline"> {
  switch (outcome) {
    case "COMPLETE":
      return "success";
    case "FAILED":
      return "destructive";
    case "PAUSED":
      return "warning";
    case "RUNNING":
    case "DOWNLOADING":
    case "VERIFYING":
    case "REPAIRING":
    case "EXTRACTING":
      return "info";
    default:
      return "muted";
  }
}

function formatOutcome(outcome: string) {
  return outcome
    .toLowerCase()
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const totalSeconds = Math.round(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatTime(ms: number) {
  return new Date(ms).toLocaleTimeString();
}

function rowTimingLabel(
  t: ReturnType<typeof useTranslate>,
  spans: { startedAt: number; endedAt: number | null }[],
) {
  const latestEndedAt = spans.reduce<number | null>(
    (latest, span) =>
      span.endedAt == null ? latest : latest == null || span.endedAt > latest ? span.endedAt : latest,
    null,
  );
  if (latestEndedAt == null) {
    return t("timeline.active");
  }
  return formatTime(latestEndedAt);
}

function sumSpans(spans: { startedAt: number; endedAt: number | null }[], axisEnd: number) {
  return spans.reduce((total, span) => total + ((span.endedAt ?? axisEnd) - span.startedAt), 0);
}

function spanStyle(startedAt: number, endedAt: number | null, axisStart: number, axisEnd: number) {
  const safeEnd = endedAt ?? axisEnd;
  const total = Math.max(axisEnd - axisStart, 1);
  const clampedStart = Math.max(axisStart, startedAt);
  const clampedEnd = Math.min(axisEnd, Math.max(clampedStart, safeEnd));
  const left = ((clampedStart - axisStart) / total) * 100;
  const width = Math.max(((clampedEnd - clampedStart) / total) * 100, 0.35);

  return {
    left: `${Math.max(0, Math.min(100, left))}%`,
    width: `${Math.max(0.35, Math.min(100, width))}%`,
  };
}

function pointStyle(at: number, axisStart: number, axisEnd: number) {
  const total = Math.max(axisEnd - axisStart, 1);
  const clamped = Math.max(axisStart, Math.min(axisEnd, at));
  const left = ((clamped - axisStart) / total) * 100;

  return {
    left: `${Math.max(0, Math.min(100, left))}%`,
  };
}

function tickLabels(axisStart: number, axisEnd: number) {
  const total = Math.max(axisEnd - axisStart, 1);
  return [0, 0.25, 0.5, 0.75, 1].map((ratio) => ({
    ratio,
    label: formatDuration(total * ratio),
  }));
}

function memberName(member: string) {
  return member.split("/").pop() ?? member;
}

function DetailList({ items }: { items: DetailItem[] }) {
  return (
    <div className="grid gap-1.5">
      {items.map((item) => (
        <div
          key={`${item.label}:${item.value}`}
          className="grid grid-cols-[7rem_minmax(0,1fr)] gap-3"
        >
          <div className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground/80">
            {item.label}
          </div>
          <div className="min-w-0 break-words text-sm text-foreground">{item.value}</div>
        </div>
      ))}
    </div>
  );
}

function TimelineSpanPopover({
  axisStart,
  axisEnd,
  row,
  span,
}: {
  axisStart: number;
  axisEnd: number;
  row: PlotRow;
  span: PlotSpan;
}) {
  const t = useTranslate();
  const [open, setOpen] = useState(false);

  const handlePointerEnter = () => setOpen(true);
  const handlePointerLeave = () => setOpen(false);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className={cn(
            "absolute top-1/2 z-10 h-4 -translate-y-1/2 transition-opacity hover:opacity-95 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/60 before:absolute before:-inset-x-2 before:-inset-y-1 before:content-['']",
          )}
          style={spanStyle(span.startedAt, span.endedAt, axisStart, axisEnd)}
          aria-label={`${row.title}: ${span.title}`}
          onPointerEnter={handlePointerEnter}
          onPointerLeave={handlePointerLeave}
          onFocus={handlePointerEnter}
          onBlur={handlePointerLeave}
          onClick={() => setOpen((current) => !current)}
        >
          <span
            className={cn(
              "absolute inset-0 rounded-[3px] border border-background/50 shadow-sm",
              span.colorClass,
              span.state === "FAILED" && "bg-destructive/85",
              span.dashed && "border-dashed",
            )}
          />
        </button>
      </PopoverTrigger>
      <PopoverContent
        side="top"
        sideOffset={4}
        className="w-[36rem] max-w-[calc(100vw-2rem)] space-y-3"
        onPointerEnter={handlePointerEnter}
        onPointerLeave={handlePointerLeave}
      >
        <div className="flex items-start justify-between gap-3">
          <div className="space-y-1">
            <div className="break-words text-sm font-medium text-foreground">{row.title}</div>
            <div className="text-xs text-muted-foreground">{span.title}</div>
          </div>
          {row.badge ? <Badge variant={row.badge.variant}>{row.badge.label}</Badge> : null}
        </div>
        <DetailList
          items={[
            ...row.details,
            {
              label: t("timeline.segment"),
              value: span.title,
            },
            ...(span.subtitle
              ? [
                  {
                    label: t("timeline.detail"),
                    value: span.subtitle,
                  },
                ]
              : []),
            ...span.details,
            {
              label: t("timeline.started"),
              value: formatTime(span.startedAt),
            },
            {
              label: t("timeline.ended"),
              value: formatTime(span.endedAt ?? axisEnd),
            },
            {
              label: t("timeline.spanDuration"),
              value: formatDuration((span.endedAt ?? axisEnd) - span.startedAt),
            },
            {
              label: t("timeline.state"),
              value: spanStateLabel(t, span.state),
            },
          ]}
        />
      </PopoverContent>
    </Popover>
  );
}

function SharedPlot({
  axisStart,
  axisEnd,
  rows,
}: {
  axisStart: number;
  axisEnd: number;
  rows: PlotRow[];
}) {
  const ticks = useMemo(() => tickLabels(axisStart, axisEnd), [axisEnd, axisStart]);

  return (
    <div className="overflow-hidden rounded-2xl border border-border/60 bg-background/20">
      <div className="grid grid-cols-1 md:grid-cols-[minmax(16rem,22rem)_1fr]">
        <div className="hidden border-b border-border/50 px-3 py-1.5 md:block" />
        <div className="border-b border-border/50 px-3 py-1.5">
          <div className="flex items-center justify-between text-[10px] uppercase tracking-[0.18em] text-muted-foreground/80">
            {ticks.map((tick) => (
              <span key={tick.ratio}>{tick.label}</span>
            ))}
          </div>
        </div>

        {rows.map((row, index) => {
          const isLast = index === rows.length - 1;
          const rowPaddingClass = row.tone === "stage" ? "px-3 py-2.5" : "px-3 py-1.5";
          const rowTitleClass =
            row.tone === "stage"
              ? "font-medium text-foreground"
              : "text-[13px] leading-4 text-muted-foreground";
          const plotHeightClass = row.tone === "stage" ? "h-7" : "h-6";

          return (
            <div key={row.key} className="contents">
              <div className={cn(rowPaddingClass, !isLast && "border-b border-border/35")}>
                <div
                  className={cn(
                    "min-w-0 break-words whitespace-normal text-sm leading-4",
                    rowTitleClass,
                  )}
                >
                  {row.displayTitle ?? row.title}
                </div>
              </div>

              <div className={cn(rowPaddingClass, !isLast && "border-b border-border/35")}>
                <div className={cn("relative", plotHeightClass)}>
                  <div className="absolute inset-x-0 top-1/2 h-px -translate-y-1/2 bg-border/20" />
                  {ticks.slice(1, -1).map((tick) => (
                    <div
                      key={`${row.key}-${tick.ratio}`}
                      className="absolute inset-y-0 w-px bg-border/40"
                      style={{ left: `${tick.ratio * 100}%` }}
                    />
                  ))}

                  {row.spans.map((span) => (
                    <div key={span.key}>
                      <TimelineSpanPopover
                        axisStart={axisStart}
                        axisEnd={axisEnd}
                        row={row}
                        span={span}
                      />
                      {span.endedAt != null ? (
                        <div
                          className={cn(
                            "pointer-events-none absolute top-1/2 z-20 h-4 w-[2px] -translate-x-1/2 -translate-y-1/2 bg-foreground/80",
                            span.state === "FAILED" && "bg-destructive",
                          )}
                          style={pointStyle(span.endedAt, axisStart, axisEnd)}
                        />
                      ) : null}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function memberRows(
  t: ReturnType<typeof useTranslate>,
  timeline: JobTimelineData,
  axisEnd: number,
): PlotRow[] {
  return timeline.extractionGroups
    .flatMap((group) =>
      group.members.map((member) => ({
        key: `${group.setName}:${member.member}`,
        title: memberName(member.member),
        displayTitle: "",
        tone: "member" as const,
        details: [
          {
            label: t("timeline.status"),
            value: stateLabel(t, member.state),
          },
          {
            label: t("timeline.totalDuration"),
            value: formatDuration(sumSpans(member.spans, axisEnd)),
          },
          {
            label: t("timeline.ended"),
            value: rowTimingLabel(t, member.spans),
          },
          {
            label: t("timeline.set"),
            value: group.setName,
          },
          ...(member.error
            ? [
                {
                  label: t("timeline.error"),
                  value: member.error,
                },
              ]
            : []),
        ],
        badge: {
          label: stateLabel(t, member.state),
          variant: stateVariant(member.state),
        },
        spans: member.spans.map((span, index) => ({
          key: `${group.setName}:${member.member}:${span.kind}:${index}`,
          startedAt: span.startedAt,
          endedAt: span.endedAt,
          state: span.state,
          colorClass: spanColor(span.kind),
          title: spanKindLabel(t, span.kind),
          subtitle: span.label,
          details: span.label
            ? [
                {
                  label: t("timeline.detail"),
                  value: span.label,
                },
              ]
            : [],
        })),
      })),
    )
    .sort((left, right) => {
      const leftStart = left.spans[0]?.startedAt ?? Number.MAX_SAFE_INTEGER;
      const rightStart = right.spans[0]?.startedAt ?? Number.MAX_SAFE_INTEGER;
      return leftStart - rightStart || left.title.localeCompare(right.title);
    });
}

export function PipelineTimelineCard({
  timeline,
}: {
  timeline: JobTimelineData | null | undefined;
}) {
  const t = useTranslate();
  const axisEnd = useMemo(() => timeline?.endedAt ?? Date.now(), [timeline?.endedAt]);

  if (!timeline || (timeline.lanes.length === 0 && timeline.extractionGroups.length === 0)) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>{t("timeline.title")}</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          {t("timeline.noData")}
        </CardContent>
      </Card>
    );
  }

  const axisStart = timeline.startedAt;
  const stageRows: PlotRow[] = timeline.lanes.map((lane) => ({
    key: `stage:${lane.stage}`,
    title: t(JOB_STAGE_KEYS[lane.stage]),
    tone: "stage",
    details: [
      {
        label: t("timeline.totalDuration"),
        value: formatDuration(sumSpans(lane.spans, axisEnd)),
      },
      {
        label: t("timeline.ended"),
        value: rowTimingLabel(t, lane.spans),
      },
      ...(lane.stage === "INTERRUPTED"
        ? [
            {
              label: t("timeline.detail"),
              value: t("timeline.restartHint"),
            },
          ]
        : []),
    ],
    spans: lane.spans.map((span, index) => ({
      key: `${lane.stage}:${index}`,
      startedAt: span.startedAt,
      endedAt: span.endedAt,
      state: span.state,
      colorClass: laneColor(lane.stage),
      title: span.label ?? t(JOB_STAGE_KEYS[lane.stage]),
      subtitle:
        span.label && span.label !== t(JOB_STAGE_KEYS[lane.stage])
          ? t(JOB_STAGE_KEYS[lane.stage])
          : null,
      dashed: lane.stage === "INTERRUPTED",
      details: [],
    })),
  }));
  const extractionRows = memberRows(t, timeline, axisEnd);
  const rows = [...stageRows, ...extractionRows];

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="space-y-1">
            <CardTitle>{t("timeline.title")}</CardTitle>
            <div className="text-xs text-muted-foreground">{t("timeline.sharedAxisHint")}</div>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <Badge variant="outline">{formatDuration(axisEnd - axisStart)}</Badge>
            {extractionRows.length > 0 ? (
              <Badge variant="outline">
                {t(
                  extractionRows.length === 1
                    ? "timeline.fileCountSingular"
                    : "timeline.fileCountPlural",
                  { count: extractionRows.length },
                )}
              </Badge>
            ) : null}
            <Badge variant={outcomeVariant(timeline.outcome)}>
              {formatOutcome(timeline.outcome)}
            </Badge>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <SharedPlot axisStart={axisStart} axisEnd={axisEnd} rows={rows} />
      </CardContent>
    </Card>
  );
}
