import { useEffect, useState, type ReactNode } from "react";
import { useLocation } from "react-router";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import {
  noOverrides,
  routeInput,
  routeTargets,
  streamTargets,
  testedPlan,
  testedRoute,
  toggleDown,
  type BandwidthProvider,
  type Egress,
  type PathOverride,
  type ProxyPool,
  type SpeedTestInput,
  type SpeedTestRun,
} from "@/lib/networking";
import type { ProxyProfile } from "@/lib/proxies";
import { Chart } from "../../components/Chart";
import { CheckBox, SecondaryButton, Select } from "../../components/controls";
import { Cell } from "../../components/rows";
import { Eyebrow } from "../../components/chrome";
import { splitBytes, splitSpeed } from "../../data/format";
import { SettingsBlocks, type SettingsBlock } from "../../pages/settings/framework";
import { defaultRoute, type Consumer, type NetworkingData } from "./data";
import { CheckRow, StatusMark } from "./presentation";
import { RoutePlanView } from "./RouteView";
import {
  defaultForm,
  RunResult,
  sabLabel,
  sizeLabel,
  SpeedTestRunner,
  TestControls,
  useSpeedTestData,
  useSpeedTestRun,
  type TestForm,
} from "./SpeedTest";

const bytesLabel = (bytes: number) => {
  const { value, unit } = splitBytes(bytes);
  return `${value} ${unit}`;
};
const rateLabel = (bytesPerSecond: number) => {
  const { value, unit } = splitSpeed(bytesPerSecond);
  return `${value} ${unit}`;
};

/** A run's status as the networking screens draw a state. */
const RUN_STATE: Record<SpeedTestRun["status"], string> = {
  RUNNING: "PROBING",
  COMPLETED: "UP",
  CANCELLED: "HOLD",
  FAILED: "FAILING",
};

/** The egresses, proxies and pools a run took, under the names they had when it ran. */
function namedAsTested(run: SpeedTestRun, data: NetworkingData) {
  const names = (kind: string) =>
    new Map(run.tested.names.filter((entry) => entry.kind === kind).map((entry) => [entry.id, entry.name]));
  const egressNames = names("egress");
  const proxyNames = names("proxy");
  const poolNames = names("pool");
  const egresses: Egress[] = data.egressInterfaces.map((egress) => ({ ...egress, name: egressNames.get(egress.id) ?? egress.name }));
  for (const [id, name] of egressNames) {
    if (!egresses.some((egress) => egress.id === id)) {
      egresses.push({ id, name, bindingKind: "SYSTEM", interfaceName: null, sourceAddress: null, enabled: true, maxDownloadSpeed: 0, health: "UNKNOWN", reason: null });
    }
  }
  const profiles: ProxyProfile[] = data.proxyProfiles.map((profile) => ({ ...profile, name: proxyNames.get(profile.id) ?? profile.name }));
  const pools: ProxyPool[] = data.proxyPools.map((pool) => ({ ...pool, name: poolNames.get(pool.id) ?? pool.name }));
  return { egresses, profiles, pools, proxyName: (id: number) => proxyNames.get(id) ?? profiles.find((profile) => profile.id === id)?.name };
}

/** What a run had down, shortly: "leg 2", "leg 1 rung 2", an egress's or a proxy's name. */
function downSummary(t: Translate, overrides: PathOverride, name: (kind: string, id: number) => string): string {
  const parts = [
    ...overrides.downEgresses.map((id) => name("egress", id)),
    ...overrides.downLegs.map((leg) => t("next.networking.speed.downLeg", { position: leg + 1 })),
    ...overrides.downRungs.map((rung) => t("next.networking.speed.downRung", { leg: rung.leg + 1, rung: rung.rung + 1 })),
    ...overrides.downProxies.map((id) => name("proxy", id)),
  ];
  return parts.length ? parts.join(", ") : "—";
}

/** What a run measured with, as it was asked: the provider, or the NZB's size. */
function kindSummary(t: Translate, run: SpeedTestRun, providers: BandwidthProvider[]): string {
  const { bandwidth, usenet } = run.request;
  if (bandwidth) {
    const id = bandwidth.providerId;
    return id ? (providers.find((provider) => provider.id === id)?.name ?? id) : t("next.networking.speed.customUrl");
  }
  return t("next.networking.speed.kindUsenet", {
    size: usenet?.sabSize ? sabLabel(t, usenet.sabSize) : t("next.networking.speed.url"),
  });
}

/** A run's request as a new request for the same consumer: the response's own objects carry fields an input does not take. */
function requestInput(run: SpeedTestRun, owner: Consumer): SpeedTestInput {
  const { bandwidth, usenet, maxDurationSeconds } = run.request;
  return {
    kind: owner.kind,
    id: owner.id,
    // The route as it was measured, not as it is now.
    route: routeInput(testedRoute(run)),
    overrides: {
      downLegs: run.tested.overrides.downLegs,
      downRungs: run.tested.overrides.downRungs.map(({ leg, rung }) => ({ leg, rung })),
      downProxies: run.tested.overrides.downProxies,
      downEgresses: run.tested.overrides.downEgresses,
    },
    maxDurationSeconds,
    ...(bandwidth
      ? {
          bandwidth: {
            providerId: bandwidth.providerId,
            server: bandwidth.server,
            customUrl: bandwidth.customUrl,
            bytes: bandwidth.bytes,
            streams: bandwidth.streams,
          },
        }
      : {}),
    ...(usenet ? { usenet: { sabSize: usenet.sabSize, nzbUrl: usenet.nzbUrl, connections: usenet.connections } } : {}),
  };
}

/** One small table inside a run: a header row and its rows on the same tracks. */
function MiniTable({ label, columns, headers, rows }: { label: string; columns: string; headers: string[]; rows: ReactNode[][] }) {
  return (
    <div role="table" aria-label={label} className="min-w-0 overflow-x-auto">
      <div className="min-w-[560px]">
        <div role="row" className="grid gap-4 border-b border-wv-hairline py-2" style={{ gridTemplateColumns: columns }}>
          {headers.map((header) => (
            <Eyebrow key={header} tone="rail" className="truncate">
              <span role="columnheader">{header}</span>
            </Eyebrow>
          ))}
        </div>
        {rows.map((cells, index) => (
          <div key={index} role="row" className="grid gap-4 border-b border-wv-hairline py-2 text-[12.5px]" style={{ gridTemplateColumns: columns }}>
            {cells.map((cell, column) => (
              <div key={column} role="cell" className="min-w-0 truncate font-wv-mono text-[12px] text-wv-secondary">
                {cell}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

/**
 * One run as it was: the route it measured, drawn from the snapshot it kept, what
 * each leg and connection did, the Usenet counts, and its bytes second by second.
 */
function RunDetail({
  run,
  data,
  onRunAgain,
  busy,
}: {
  run: SpeedTestRun;
  data: NetworkingData;
  onRunAgain: (run: SpeedTestRun) => void;
  busy: boolean;
}) {
  const t = useTranslate();
  const named = namedAsTested(run, data);
  const route = testedRoute(run);
  const result = run.result;
  const egressName = (id: number) => named.egresses.find((egress) => egress.id === id)?.name ?? String(id);
  const legs = [...new Set((result?.samples ?? []).map((sample) => sample.leg))].sort((a, b) => a - b);
  const seconds = [...new Set((result?.samples ?? []).map((sample) => sample.atMs))].sort((a, b) => a - b);
  const series = legs.map((leg, index) => ({
    key: `leg-${leg}`,
    label: t("next.networking.speed.legRate", { position: leg + 1 }),
    color: ["var(--wv-accent)", "#6aa7e8", "#b48ef0", "#d3a43e"][index % 4]!,
    values: seconds.map((at) => (result?.samples ?? []).filter((sample) => sample.leg === leg && sample.atMs === at).reduce((sum, sample) => sum + sample.bytes, 0)),
    value: rateLabel(result?.legs.find((entry) => entry.position === leg)?.averageBps ?? 0),
  }));
  const usenet = result?.usenet;
  // Each stream asked for the size, so the run expected both multiplied.
  const requested = run.request.bandwidth;
  const rungCount = (position: number) => {
    const path = run.tested.legs.find((leg) => leg.position === position)?.path;
    return path?.kind === "LADDER" ? path.rungs.length : 0;
  };
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <RoutePlanView
        consumer={{ key: run.consumer, name: run.consumerName, cap: run.tested.legs.reduce((sum, leg) => sum + leg.target, 0) }}
        route={route}
        targets={run.tested.legs.map((leg) => leg.target)}
        legDown={run.tested.legs.map((leg) => leg.down)}
        overrides={run.tested.overrides}
        egresses={named.egresses}
        profiles={named.profiles}
        pools={named.pools}
      />
      {requested ? (
        <p aria-label={t("next.networking.speed.requested")} className="flex flex-wrap gap-x-4 font-wv-mono text-[12px] text-wv-secondary">
          <span>{`${t("next.networking.speed.size")} ${sizeLabel(requested.bytes)}`}</span>
          <span>{`${t("next.networking.speed.streams")} ${requested.streams}`}</span>
          <span>{`${t("next.networking.speed.total")} ${sizeLabel(requested.bytes * requested.streams)}`}</span>
        </p>
      ) : null}
      <RunResult run={run} />
      {result ? (
        <>
          <MiniTable
            label={t("next.networking.speed.legs")}
            columns="64px 96px 104px 104px 64px minmax(0, 1fr)"
            headers={[
              t("next.networking.speed.leg"),
              t("next.networking.speed.downloaded"),
              t("next.networking.speed.average"),
              t("next.networking.speed.peak"),
              t("next.networking.speed.connections"),
              t("next.networking.speed.source"),
            ]}
            rows={run.tested.legs.map((leg) => {
              const measured = result.legs.find((entry) => entry.position === leg.position);
              return [
                t("next.networking.speed.downLeg", { position: leg.position + 1 }),
                leg.down ? t("next.networking.speed.downSwitch") : bytesLabel(measured?.bytes ?? 0),
                leg.down ? (leg.downReason ?? "—") : rateLabel(measured?.averageBps ?? 0),
                leg.down ? "—" : rateLabel(measured?.peakBps ?? 0),
                leg.down ? "0" : String(measured?.connections ?? 0),
                measured?.sourceAddresses.join(", ") || "—",
              ];
            })}
          />
          <MiniTable
            label={t("next.networking.speed.connectionsTable")}
            columns="64px minmax(0, 1.4fr) minmax(0, 1fr) 72px 96px minmax(0, 1fr)"
            headers={[
              t("next.networking.speed.leg"),
              t("next.networking.speed.path"),
              t("next.networking.speed.source"),
              t("next.networking.speed.connect"),
              t("next.networking.speed.downloaded"),
              t("next.networking.speed.error"),
            ]}
            rows={result.connections.map((connection) => [
              t("next.networking.speed.downLeg", { position: connection.leg + 1 }),
              [
                egressName(connection.path.egressId),
                // Through a pool, the member it dialled; otherwise each proxy in turn.
                ...(connection.path.poolId !== null
                  ? [
                      named.pools.find((pool) => pool.id === connection.path.poolId)?.name ?? String(connection.path.poolId),
                      ...(connection.path.memberId !== null ? [named.proxyName(connection.path.memberId) ?? String(connection.path.memberId)] : []),
                    ]
                  : connection.path.proxyIds.map((id) => named.proxyName(id) ?? String(id))),
                // A rung past the ladder's own is its direct fallback.
                ...(connection.path.rung !== null && connection.path.rung >= rungCount(connection.leg) ? [t("next.networking.speed.direct")] : []),
              ].join(" > "),
              connection.sourceAddress ?? "—",
              `${connection.connectMs} ms`,
              bytesLabel(connection.bytes),
              connection.error ?? "—",
            ])}
          />
          {usenet ? (
            <MiniTable
              label={t("next.networking.speed.articles")}
              columns="repeat(7, minmax(0, 1fr))"
              headers={[
                t("next.networking.speed.requested"),
                t("next.networking.speed.delivered"),
                t("next.networking.speed.missing"),
                t("next.networking.speed.failed"),
                t("next.networking.speed.declared"),
                t("next.networking.speed.transport"),
                t("next.networking.speed.pipelining"),
              ]}
              rows={[
                [
                  String(usenet.articlesRequested),
                  String(usenet.delivered),
                  String(usenet.missing),
                  String(usenet.failed),
                  bytesLabel(usenet.declaredBytes),
                  usenet.tls ? `TLS${usenet.tlsCipherSuite ? ` · ${usenet.tlsCipherSuite}` : ""}` : t("next.networking.speed.plaintext"),
                  usenet.pipelining ? t("next.common.on") : t("next.common.off"),
                ],
              ]}
            />
          ) : null}
          {result.bandwidth ? (
            <p className="truncate font-wv-mono text-[12px] text-wv-muted">
              {result.bandwidth.server ? `${result.bandwidth.url} · ${result.bandwidth.server}` : result.bandwidth.url}
            </p>
          ) : null}
          {seconds.length ? (
            <Chart
              title={t("next.networking.speed.chart")}
              series={series}
              xLabels={["0 s", `${Math.round(seconds.length / 2)} s`, `${seconds.length} s`]}
              formatValue={rateLabel}
            />
          ) : null}
        </>
      ) : null}
      <div className="flex">
        <SecondaryButton icon="refresh" size="compact" disabled={busy || run.status === "RUNNING"} onClick={() => onRunAgain(run)}>
          {t("next.networking.speed.runAgain")}
        </SecondaryButton>
      </div>
    </div>
  );
}

/**
 * Networking → Speed test: pick a route, take legs, rungs, proxies or egresses
 * down, run a bandwidth or a Usenet test through exactly what is left, and read
 * it against the runs before it.
 */
export function SpeedPage({ data, consumers }: { data: NetworkingData; consumers: Consumer[] }) {
  const t = useTranslate();
  const location = useLocation();
  const requested = new URLSearchParams(location.search).get("consumer");
  const [selected, setSelected] = useState<string>(requested ?? consumers[0]?.key ?? "");
  const consumer = consumers.find((candidate) => candidate.key === selected) ?? consumers[0];
  const [overrides, setOverrides] = useState<PathOverride>(noOverrides());
  const [form, setForm] = useState<TestForm>(() => defaultForm(consumer ?? { kind: "SERVER", cap: 1 }));
  const [allRoutes, setAllRoutes] = useState(false);
  const [open, setOpen] = useState<Set<number>>(new Set());
  const [checked, setChecked] = useState<number[]>([]);
  const { providers, runs, refresh } = useSpeedTestData();
  const speedTest = useSpeedTestRun(refresh);
  const { run: active, setRun } = speedTest;

  // A run already going when the page opens is followed here as if started here.
  const running = runs.find((run) => run.status === "RUNNING");
  useEffect(() => {
    if (running && !active) {
      setRun(running);
    }
  }, [running, active, setRun]);

  if (!consumer) {
    return (
      <SettingsBlocks
        blocks={[{ kind: "table", id: "speed-runs", title: t("next.networking.speed.runs"), columns: "1fr", headers: [], rows: [], empty: t("next.networking.routes.empty") }]}
      />
    );
  }

  const choose = (key: string) => {
    const next = consumers.find((candidate) => candidate.key === key);
    setSelected(key);
    setOverrides(noOverrides());
    if (next) {
      setForm(defaultForm(next));
    }
  };
  const route = consumer.route ?? defaultRoute();
  const down = new Set(
    testedPlan(route, overrides, data.proxyPools)
      .legDown.flatMap((isDown, position) => (isDown ? [position] : [])),
  );
  const targets = form.kind === "USENET" ? routeTargets(route, form.connections, down) : streamTargets(route, form.streams, down);
  const name = (kind: string, id: number) => {
    if (kind === "egress") return data.egressInterfaces.find((egress) => egress.id === id)?.name ?? String(id);
    return data.proxyProfiles.find((profile) => profile.id === id)?.name ?? String(id);
  };

  const runAgain = (run: SpeedTestRun) => {
    const owner = consumers.find((candidate) => candidate.key === run.consumer);
    if (!owner) {
      return;
    }
    void speedTest.start(requestInput(run, owner));
  };

  const shown = allRoutes ? runs : runs.filter((run) => run.consumer === consumer.key);
  const compared = checked.map((id) => runs.find((run) => run.id === id)).filter((run): run is SpeedTestRun => !!run);
  const detail = (run: SpeedTestRun) => (
    <RunDetail run={run} data={data} onRunAgain={runAgain} busy={speedTest.busy} />
  );

  const blocks: (SettingsBlock | null)[] = [
    {
      kind: "custom",
      id: "speed-route",
      title: t("next.networking.speed.routeBlock"),
      searchText: consumers.map((candidate) => candidate.name).join(" "),
      body: (
        <div className="flex min-w-0 flex-col">
          <div className="flex flex-wrap items-end gap-3 border-b border-wv-hairline px-4 py-3 sm:px-6">
            <Select
              label={t("next.networking.speed.route")}
              value={consumer.key}
              onChange={choose}
              className="min-w-[240px]"
              options={consumers.map((candidate) => ({ value: candidate.key, label: candidate.name }))}
            />
          </div>
          <RoutePlanView
            consumer={consumer}
            route={route}
            targets={targets}
            overrides={overrides}
            egresses={data.egressInterfaces}
            profiles={data.proxyProfiles}
            pools={data.proxyPools}
            onToggle={(target) => setOverrides((current) => toggleDown(current, target))}
          />
        </div>
      ),
    },
    {
      kind: "custom",
      id: "speed-test",
      title: t("next.networking.speed.testBlock"),
      searchText: [t("next.networking.speed.bandwidth"), t("next.networking.speed.usenet"), ...providers.map((provider) => provider.name)].join(" "),
      body: (
        <>
          <TestControls form={form} onChange={setForm} consumer={consumer} providers={providers} />
          <SpeedTestRunner consumer={consumer} route={null} overrides={overrides} form={form} providers={providers} speedTest={speedTest} />
        </>
      ),
    },
    {
      kind: "table",
      id: "speed-runs",
      title: t("next.networking.speed.runs"),
      note: t("next.networking.speed.runsNote"),
      columns: "20px 84px minmax(0, 1fr) minmax(0, 0.8fr) minmax(0, 0.8fr) 48px 88px 88px 80px 80px 104px",
      headers: [
        "",
        t("next.networking.speed.started"),
        t("next.networking.speed.route"),
        t("next.networking.speed.kind"),
        t("next.networking.speed.down"),
        t("next.networking.speed.width"),
        t("next.networking.speed.average"),
        t("next.networking.speed.peak"),
        t("next.networking.speed.downloaded"),
        t("next.networking.speed.decoded"),
        t("next.networking.speed.statusColumn"),
      ],
      empty: t("next.networking.speed.runsEmpty"),
      onRowClick: (id) =>
        setOpen((current) => {
          const next = new Set(current);
          if (!next.delete(Number(id))) {
            next.add(Number(id));
          }
          return next;
        }),
      rows: shown.map((run) => {
        const result = run.result;
        const startedAt = new Date(run.startedAt);
        return {
          id: String(run.id),
          searchText: `${run.consumerName} ${run.kind}`,
          expanded: open.has(run.id),
          detail: open.has(run.id) ? detail(run) : undefined,
          cells: [
            <CheckBox
              key="compare"
              checked={checked.includes(run.id)}
              label={t("next.networking.speed.compareRun", { id: run.id })}
              onChange={(next) =>
                setChecked((current) => (next ? [...current.filter((id) => id !== run.id), run.id].slice(-2) : current.filter((id) => id !== run.id)))
              }
            />,
            <Cell key="started" mono title={startedAt.toLocaleString()}>
              {startedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
            </Cell>,
            <Cell key="route" title={run.consumerName}>
              {run.consumerName}
            </Cell>,
            <Cell key="kind" mono className="text-wv-secondary">
              {kindSummary(t, run, providers)}
            </Cell>,
            <Cell key="down" mono className="text-wv-secondary">
              {downSummary(t, run.tested.overrides, (kind, id) => run.tested.names.find((entry) => entry.kind === kind && entry.id === id)?.name ?? name(kind, id))}
            </Cell>,
            <Cell key="width" mono>
              {run.tested.legs.reduce((sum, leg) => sum + leg.target, 0)}
            </Cell>,
            <Cell key="average" mono>
              {result ? rateLabel(result.averageBps) : "—"}
            </Cell>,
            <Cell key="peak" mono>
              {result ? rateLabel(result.peakBps) : "—"}
            </Cell>,
            <Cell key="downloaded" mono>
              {result ? bytesLabel(result.bytes) : "—"}
            </Cell>,
            <Cell key="decoded" mono>
              {run.kind === "USENET" && result?.usenet ? bytesLabel(result.usenet.decodedBytes) : ""}
            </Cell>,
            <StatusMark key="status" state={RUN_STATE[run.status]} label={t(`next.networking.speed.status.${run.status}`)} />,
          ],
        };
      }),
      footer: <CheckRow checked={allRoutes} onChange={setAllRoutes} label={t("next.networking.speed.allRoutes")} />,
    },
    compared.length === 2
      ? {
          kind: "custom",
          id: "speed-compare",
          title: t("next.networking.speed.compare"),
          searchText: "",
          body: (
            <div className="grid min-w-0 gap-6 px-4 py-4 sm:px-6 xl:grid-cols-2">
              {compared.map((run) => (
                <div key={run.id} className="min-w-0">
                  {detail(run)}
                </div>
              ))}
            </div>
          ),
        }
      : null,
  ];

  return <SettingsBlocks blocks={blocks} />;
}
