import { useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import {
  adjustWeight,
  appendLeg,
  removeLeg,
  routeProblem,
  routeTargets,
  type Egress,
  type Leg,
  type LegFlow,
  type NetworkRoute,
  type ProxyPool,
  type Rung,
} from "@/lib/networking";
import { chainHopProfiles, proxyLabels, type ProxyProfile } from "@/lib/proxies";
import { cn } from "@/lib/utils";
import { Square } from "../../components/chrome";
import { NumberField, SecondaryButton, Segmented, Select } from "../../components/controls";
import { WV } from "../../data/palette";
import {
  CheckRow,
  ControlLabel,
  IconButton,
  routeProblemText,
  stateLabel,
  StatusMark,
} from "./presentation";

/** One hue per leg, repeated past four: the weight bar and each leg's marker agree. */
const LEG_COLORS = [WV.info, WV.violet, WV.gold, WV.green] as const;
export const legColor = (position: number) => LEG_COLORS[position % LEG_COLORS.length]!;

const MAX_LEGS = 8;
const MAX_RUNGS = 8;

function move<T>(items: T[], index: number, delta: number): T[] {
  const next = [...items];
  [next[index], next[index + delta]] = [next[index + delta]!, next[index]!];
  return next;
}

const emptyRung = (): Rung => ({ kind: "PROXY", proxyId: null, poolId: null, chainIds: [] });

/**
 * A consumer's route: the legs its connections are split across, the path each
 * leg takes from its egress, and what happens to a leg's share when it drops.
 *
 * Built narrow-first. It lives in the routes page's dialog and in a server's
 * or feed's own editor, where it gets a third of the dialog, so every row
 * wraps rather than asking for width.
 */
export function RouteEditor({
  value,
  onChange,
  egresses,
  profiles,
  pools,
  cap = 1,
  rss = false,
  status = [],
}: {
  value: NetworkRoute;
  onChange: (route: NetworkRoute) => void;
  egresses: Egress[];
  profiles: ProxyProfile[];
  pools: ProxyPool[];
  cap?: number;
  rss?: boolean;
  status?: LegFlow[];
}) {
  const t = useTranslate();
  const [down, setDown] = useState<Set<number>>(new Set());
  const update = (index: number, leg: Leg) =>
    onChange({ ...value, legs: value.legs.map((current, position) => (position === index ? leg : current)) });
  const targets = routeTargets(value, cap, down);
  const assigned = targets.reduce((sum, target) => sum + target, 0);
  const weight = (index: number, next: number) => onChange({ ...value, legs: adjustWeight(value.legs, index, next) });
  const problem = routeProblem(value);
  const reorder = (legs: Leg[]) => {
    setDown(new Set());
    onChange({ ...value, legs });
  };

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <p className="text-[12.5px] leading-[1.5] text-pretty text-wv-muted">
        {rss ? t("next.networking.route.introRss") : t("next.networking.route.intro", { count: cap })}
      </p>

      <div
        aria-label={t("next.networking.route.distribution")}
        className="flex h-8 min-w-0 border border-wv-control"
      >
        {value.legs.map((leg, index) => (
          <div
            key={index}
            className="relative flex min-w-0 items-center justify-center font-wv-mono text-[11.5px] font-medium"
            style={{ flex: leg.weight, background: legColor(index), color: WV.onSpan }}
          >
            <span className="truncate px-1">{leg.weight}%</span>
            {index < value.legs.length - 1 ? (
              <div
                role="slider"
                tabIndex={0}
                aria-label={t("next.networking.route.legWeight", { position: index + 1 })}
                aria-valuemin={1}
                aria-valuemax={leg.weight + value.legs[index + 1]!.weight - 1}
                aria-valuenow={leg.weight}
                className="absolute -top-[5px] -right-[5px] z-[1] h-[40px] w-[10px] cursor-ew-resize touch-none border-2 border-wv-app bg-wv-fg outline-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-wv-accent"
                onPointerDown={(event) => event.currentTarget.setPointerCapture(event.pointerId)}
                onPointerMove={(event) => {
                  if (!event.currentTarget.hasPointerCapture(event.pointerId)) {
                    return;
                  }
                  const bounds = event.currentTarget.parentElement!.parentElement!.getBoundingClientRect();
                  const prior = value.legs.slice(0, index).reduce((sum, current) => sum + current.weight, 0);
                  weight(index, ((event.clientX - bounds.left) / bounds.width) * 100 - prior);
                }}
                onPointerUp={(event) => event.currentTarget.releasePointerCapture(event.pointerId)}
                onKeyDown={(event) => {
                  if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
                    event.preventDefault();
                    weight(index, leg.weight + (event.key === "ArrowLeft" ? -1 : 1));
                  }
                }}
              />
            ) : null}
          </div>
        ))}
      </div>

      {down.size > 0 ? (
        <p role="status" className="border-l-2 border-wv-warn pl-3 text-[12px] leading-[1.5] text-wv-secondary">
          {t("next.networking.route.preview", { assigned, parked: cap - assigned })}
        </p>
      ) : null}

      {value.legs.map((leg, index) => {
        const path = leg.path;
        const setRungs = (rungs: Rung[]) => update(index, { ...leg, path: { ...path, rungs } });
        const live = status.find((sample) => sample.position === index);
        return (
          <fieldset key={index} className="flex min-w-0 flex-col gap-3 border border-wv-control bg-wv-chrome px-3 pt-1 pb-3 sm:px-4">
            <legend className="flex items-center gap-2 px-1 font-wv-title text-[12.5px] font-semibold text-wv-fg">
              <Square color={legColor(index)} />
              {t("next.networking.route.leg", { position: index + 1 })}
            </legend>
            {live ? (
              <StatusMark
                state={live.state}
                label={stateLabel(t, live.state)}
                detail={t("next.networking.route.live", {
                  open: live.open,
                  target: live.target,
                  source: live.sourceAddress ?? t("next.networking.route.noSource"),
                })}
              />
            ) : null}
            <div className="flex min-w-0 flex-wrap items-end gap-3">
              <ControlLabel label={t("next.networking.route.egress")} className="flex-[1_1_180px]">
                <Select
                  label={t("next.networking.route.egress")}
                  value={String(leg.egressId)}
                  onChange={(next) => update(index, { ...leg, egressId: Number(next) })}
                  className="w-full min-w-0"
                  options={egresses.map((egress) => ({
                    value: String(egress.id),
                    label: egress.enabled ? egress.name : `${egress.name} · ${t("next.networking.disabled")}`,
                  }))}
                />
              </ControlLabel>
              <ControlLabel label={t("next.networking.route.weight")}>
                <NumberField
                  label={t("next.networking.route.weight")}
                  value={leg.weight}
                  min={1}
                  max={100}
                  suffix="%"
                  disabled={value.legs.length === 1}
                  onChange={(next) => weight(index, next)}
                />
              </ControlLabel>
            </div>
            <ControlLabel label={t("next.networking.route.path")}>
              <Segmented
                label={t("next.networking.route.path")}
                value={path.kind}
                onChange={(kind) => update(index, { ...leg, path: { kind, rungs: [], directFallback: false } })}
                className="self-start"
                options={[
                  { value: "DIRECT", label: t("next.networking.route.direct") },
                  { value: "LADDER", label: t("next.networking.route.ladder") },
                ]}
              />
            </ControlLabel>

            {path.kind === "LADDER" ? (
              <div className="flex min-w-0 flex-col gap-2">
                <ol className="flex min-w-0 flex-col">
                  {path.rungs.map((rung, rungIndex) => (
                    <li
                      key={rungIndex}
                      className="flex min-w-0 flex-wrap items-center gap-2 border-t border-wv-hairline py-[10px] first:border-t-0"
                    >
                      <span className="w-[52px] flex-none font-wv-mono text-[11px] text-wv-muted">
                        {t("next.networking.route.rung", { position: rungIndex + 1 })}
                      </span>
                      <Segmented
                        size="compact"
                        label={t("next.networking.route.rungType", { leg: index + 1, rung: rungIndex + 1 })}
                        value={rung.kind}
                        onChange={(kind) =>
                          setRungs(path.rungs.map((current, n) => (n === rungIndex ? { ...emptyRung(), kind } : current)))
                        }
                        options={[
                          { value: "PROXY", label: t("next.networking.route.proxy") },
                          { value: "POOL", label: t("next.networking.route.pool") },
                          { value: "CHAIN", label: t("next.networking.route.chain") },
                        ]}
                      />
                      <div className="ml-auto flex flex-none items-center gap-1">
                        <IconButton
                          icon="moveUp"
                          label={t("next.networking.route.rungUp")}
                          disabled={rungIndex === 0}
                          onClick={() => setRungs(move(path.rungs, rungIndex, -1))}
                        />
                        <IconButton
                          icon="moveDown"
                          label={t("next.networking.route.rungDown")}
                          disabled={rungIndex === path.rungs.length - 1}
                          onClick={() => setRungs(move(path.rungs, rungIndex, 1))}
                        />
                        <IconButton
                          icon="remove"
                          label={t("next.networking.route.removeRung")}
                          onClick={() => setRungs(path.rungs.filter((_, n) => n !== rungIndex))}
                        />
                      </div>
                      {rung.kind === "CHAIN" ? (
                        <div className="flex w-full min-w-0 flex-wrap gap-2">
                          {[0, 1, 2].map((hop) => (
                            <Select
                              key={hop}
                              label={t("next.networking.route.hop", { position: hop + 1 })}
                              value={rung.chainIds[hop] == null ? "" : String(rung.chainIds[hop])}
                              className="w-full min-w-0 sm:w-[200px]"
                              onChange={(next) => {
                                const ids = [...rung.chainIds];
                                if (next) {
                                  ids[hop] = Number(next);
                                } else {
                                  ids.splice(hop, 1);
                                }
                                setRungs(path.rungs.map((current, n) => (n === rungIndex ? { ...current, chainIds: ids } : current)));
                              }}
                              options={[
                                {
                                  value: "",
                                  label: hop === 2 ? t("next.networking.route.optionalHop") : t("next.networking.route.chooseProxy"),
                                },
                                ...chainHopProfiles(profiles, rung.chainIds, hop)
                                  .map((profile) => ({
                                    value: String(profile.id),
                                    label: `${profile.name} · ${proxyLabels[profile.kind]}`,
                                  })),
                              ]}
                            />
                          ))}
                        </div>
                      ) : (
                        <Select
                          label={t("next.networking.route.rungTarget", { leg: index + 1, rung: rungIndex + 1 })}
                          value={String((rung.kind === "PROXY" ? rung.proxyId : rung.poolId) ?? "")}
                          className="w-full min-w-0"
                          onChange={(next) =>
                            setRungs(
                              path.rungs.map((current, n) =>
                                n === rungIndex
                                  ? { ...current, [current.kind === "PROXY" ? "proxyId" : "poolId"]: next ? Number(next) : null }
                                  : current,
                              ),
                            )
                          }
                          options={[
                            {
                              value: "",
                              label: rung.kind === "PROXY" ? t("next.networking.route.chooseProxy") : t("next.networking.route.choosePool"),
                            },
                            ...(rung.kind === "PROXY" ? profiles : pools).map((target) => ({
                              value: String(target.id),
                              label: target.enabled ? target.name : `${target.name} · ${t("next.networking.disabled")}`,
                            })),
                          ]}
                        />
                      )}
                    </li>
                  ))}
                </ol>
                {path.rungs.length === 0 ? (
                  <p className="text-[12px] text-wv-muted">{t("next.networking.route.noRungs")}</p>
                ) : null}
                <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
                  <SecondaryButton
                    size="compact"
                    icon="add"
                    disabled={path.rungs.length >= MAX_RUNGS}
                    onClick={() => setRungs([...path.rungs, emptyRung()])}
                  >
                    {t("next.networking.route.addRung")}
                  </SecondaryButton>
                  <CheckRow
                    checked={path.directFallback}
                    onChange={(checked) => update(index, { ...leg, path: { ...path, directFallback: checked } })}
                    label={t("next.networking.route.directFallback")}
                  />
                </div>
              </div>
            ) : null}

            <div className="flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-wv-hairline pt-3">
              {rss ? null : (
                <span className="font-wv-mono text-[11.5px] text-wv-muted">
                  {t("next.networking.route.target", { count: targets[index] ?? 0 })}
                </span>
              )}
              <CheckRow
                checked={down.has(index)}
                label={t("next.networking.route.simulateDown")}
                onChange={(checked) =>
                  setDown((current) => {
                    const next = new Set(current);
                    if (checked) {
                      next.add(index);
                    } else {
                      next.delete(index);
                    }
                    return next;
                  })
                }
              />
              <div className="ml-auto flex flex-none items-center gap-1">
                <IconButton
                  icon="moveUp"
                  label={t("next.networking.route.legUp")}
                  disabled={index === 0}
                  onClick={() => reorder(move(value.legs, index, -1))}
                />
                <IconButton
                  icon="moveDown"
                  label={t("next.networking.route.legDown")}
                  disabled={index === value.legs.length - 1}
                  onClick={() => reorder(move(value.legs, index, 1))}
                />
                <IconButton
                  icon="remove"
                  label={t("next.networking.route.removeLeg")}
                  disabled={value.legs.length === 1}
                  onClick={() => reorder(removeLeg(value.legs, index))}
                />
              </div>
            </div>
          </fieldset>
        );
      })}

      <div className="flex flex-wrap items-end gap-x-4 gap-y-3">
        <SecondaryButton
          icon="add"
          disabled={value.legs.length >= MAX_LEGS}
          onClick={() => onChange({ ...value, legs: appendLeg(value.legs) })}
        >
          {t("next.networking.route.addLeg")}
        </SecondaryButton>
        <ControlLabel label={t("next.networking.route.failover")}>
          <Segmented
            label={t("next.networking.route.failover")}
            value={value.failover}
            onChange={(failover) => onChange({ ...value, failover })}
            options={[
              { value: "REDISTRIBUTE", label: t("next.networking.route.redistribute") },
              { value: "HOLD", label: t("next.networking.route.hold") },
            ]}
          />
        </ControlLabel>
      </div>

      {problem ? (
        <p role="alert" className={cn("text-[12.5px] leading-[1.5] text-wv-error-text")}>
          {routeProblemText(t, problem)}
        </p>
      ) : null}
    </div>
  );
}
