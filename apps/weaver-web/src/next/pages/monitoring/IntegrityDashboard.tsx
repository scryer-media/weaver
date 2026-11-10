// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { EmptyState, MetricCell, SectionHeader } from "../../components/chrome";
import { columnStyle } from "../../components/columns";
import { useNow } from "../../data/clock";
import { averageSeconds, dailyRepairShare, percentOf, stagesInOrder, STAGE_ORDER } from "../../data/dashboards";
import { EM_DASH, formatCount, formatDuration, formatSize } from "../../data/format";
import { NextShell } from "../../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../../shell/rail-blocks";
import { DailyBars } from "./DailyBars";
import { DashboardNav } from "./dashboards";
import { UsageRangeBar, useUsageRange } from "./RangeBar";
import { useIntegrityHistory } from "./usage";

const CAUSES = ["repair", "password", "disk_full", "unpack", "move", "health", "duplicate"];

const STAGE_COLUMNS = "minmax(140px, 2fr) repeat(3, minmax(76px, 1fr))";

const cell = "truncate text-right font-wv-mono text-[12.5px] text-wv-secondary";

function causeLabel(t: Translate, cause: string): string {
  return t(CAUSES.includes(cause) ? `next.integrity.cause.${cause}` : "next.integrity.cause.other");
}

function stageLabel(t: Translate, stage: string): string {
  return (STAGE_ORDER as readonly string[]).includes(stage) ? t(`next.integrity.stage.${stage}`) : stage;
}

function percentText(value: number | null): string {
  return value === null ? EM_DASH : `${value.toFixed(1)}%`;
}

export function IntegrityDashboard() {
  const t = useTranslate();
  const now = useNow(60_000);
  const rangeState = useUsageRange(now);
  const { integrity, isLoading, error } = useIntegrityHistory(rangeState.range);

  const finished = integrity?.finished.range.events ?? 0;
  const repaired = integrity?.repaired.range.events ?? 0;
  const recovery = integrity?.recovery ?? { availableBytes: 0, fetchedBytes: 0 };
  const failures = [...(integrity?.failures ?? [])].sort((left, right) => right.count - left.count);
  const failed = failures.reduce((sum, failure) => sum + failure.count, 0);
  const stages = stagesInOrder(integrity?.stages ?? []).filter((stage) => stage.range.events > 0);
  const days = integrity?.finished.days ?? [];
  const shares = dailyRepairShare(days, integrity?.repaired.days ?? []);

  return (
    <NextShell
      title={t("next.nav.monitoring")}
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={<DashboardNav active="integrity" />}
      statusRight={error ?? undefined}
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <UsageRangeBar state={rangeState} rangeStart={integrity?.rangeStart} rangeEnd={integrity?.rangeEnd} />

        <div
          className="wv-cols grid flex-none border-b border-wv-hairline bg-wv-app"
          style={columnStyle({ base: "repeat(1, minmax(0, 1fr))", sm: "repeat(3, minmax(0, 1fr))" })}
        >
          <MetricCell
            eyebrow={t("next.integrity.repaired")}
            value={percentText(percentOf(repaired, finished))}
            note={t("next.integrity.ofJobs", { repaired: formatCount(repaired), finished: formatCount(finished) })}
          />
          <MetricCell
            eyebrow={t("next.integrity.recovery")}
            value={percentText(percentOf(recovery.fetchedBytes, recovery.availableBytes))}
            note={t("next.integrity.recoveryOf", {
              fetched: formatSize(recovery.fetchedBytes),
              available: formatSize(recovery.availableBytes),
            })}
          />
          <MetricCell
            last
            eyebrow={t("next.integrity.failed")}
            value={formatCount(failed)}
            valueClassName={failed > 0 ? "text-wv-warn" : undefined}
            note={failures[0] ? causeLabel(t, failures[0].cause) : EM_DASH}
          />
        </div>

        <section className="flex flex-none flex-col">
          <SectionHeader label={t("next.integrity.repairsPerDay")} sticky={false} />
          {repaired === 0 ? (
            <EmptyState
              testId="integrity-no-repairs"
              title={t("next.integrity.noRepairs")}
              body={t("next.integrity.noRepairsBody")}
              loading={isLoading}
            />
          ) : (
            <DailyBars
              days={days.map((day) => day.day)}
              values={shares}
              format={(value) => `${value.toFixed(1)}%`}
              label={t("next.integrity.repairsPerDay")}
            />
          )}
        </section>

        <section className="flex flex-none flex-col">
          <SectionHeader label={t("next.integrity.causes")} sticky={false} />
          {failures.length === 0 ? (
            <EmptyState
              testId="integrity-no-failures"
              title={t("next.integrity.noFailures")}
              body={t("next.integrity.noFailuresBody")}
              loading={isLoading}
            />
          ) : (
            failures.map((failure) => (
              <div
                key={failure.cause}
                data-testid={`integrity-cause-${failure.cause || "other"}`}
                className="flex items-center justify-between gap-4 border-b border-wv-hairline px-4 py-3 sm:px-6"
              >
                <span className="truncate text-[13px] text-wv-fg">{causeLabel(t, failure.cause)}</span>
                <span className={cell}>{formatCount(failure.count)}</span>
              </div>
            ))
          )}
        </section>

        <section className="flex flex-none flex-col">
          <SectionHeader label={t("next.integrity.stages")} sticky={false} />
          {stages.length === 0 ? (
            <EmptyState
              testId="integrity-no-stages"
              title={t("next.integrity.noStages")}
              body={t("next.integrity.noStagesBody")}
              loading={isLoading}
            />
          ) : (
            <div className="overflow-x-auto">
              <div className="min-w-[440px]">
                <div
                  className="grid items-center gap-x-4 border-b border-wv-hairline px-4 py-2 text-[10.5px] uppercase tracking-[0.12em] text-wv-faint sm:px-6"
                  style={{ gridTemplateColumns: STAGE_COLUMNS }}
                >
                  <span>{t("next.integrity.stage")}</span>
                  <span className="text-right">{t("next.integrity.runs")}</span>
                  <span className="text-right">{t("next.integrity.average")}</span>
                  <span className="text-right">{t("next.integrity.total")}</span>
                </div>
                {stages.map((stage) => (
                  <div
                    key={stage.id}
                    data-testid={`integrity-stage-${stage.id}`}
                    className="grid items-center gap-x-4 border-b border-wv-hairline px-4 py-3 hover:bg-wv-cell-hover sm:px-6"
                    style={{ gridTemplateColumns: STAGE_COLUMNS }}
                  >
                    <span className="truncate text-[13px] text-wv-fg">{stageLabel(t, stage.id)}</span>
                    <span className={cell}>{formatCount(stage.range.events)}</span>
                    <span className={cell}>{formatDuration(averageSeconds(stage.range) ?? 0)}</span>
                    <span className={cell}>{formatDuration(stage.range.durationMs / 1000)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </section>
      </div>
    </NextShell>
  );
}
