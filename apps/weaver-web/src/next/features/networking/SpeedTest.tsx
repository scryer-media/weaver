import { useEffect, useRef, useState, type ReactNode } from "react";
import { useClient, useQuery } from "urql";
import {
  BANDWIDTH_PROVIDER_SERVERS_QUERY,
  CANCEL_SPEED_TEST,
  SPEED_TEST_QUERY,
  SPEED_TEST_RUN_QUERY,
  START_SPEED_TEST,
} from "@/graphql/networking";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import {
  routeInput,
  type BandwidthProvider,
  type BandwidthProviderServer,
  type NetworkRoute,
  type PathOverride,
  type SabTestSize,
  type SpeedTestInput,
  type SpeedTestKind,
  type SpeedTestRun,
} from "@/lib/networking";
import { refusalMessage } from "@/lib/refusal";
import { cn } from "@/lib/utils";
import { Eyebrow, MetricCell, MetricStrip } from "../../components/chrome";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { DangerButton, NumberField, PrimaryButton, SecondaryButton, Segmented, Select, TextField } from "../../components/controls";
import { EM_DASH, splitBytes, splitSpeed } from "../../data/format";
import { useNextData } from "../../data/next-data";
import type { Consumer } from "./data";
import { LiveChart, type RatePoint } from "./LiveChart";
import { ControlLabel } from "./presentation";

/** What the operator has chosen to run, before it is a request. */
export type TestForm = {
  kind: SpeedTestKind;
  /** Empty for a custom URL. */
  providerId: string;
  server: string;
  customUrl: string;
  nzb: SabTestSize | "URL" | "FILE";
  nzbUrl: string;
  /** One NZB file, sent with the request and held by the daemon only for as long as the run is. */
  nzbFile: File | null;
  connections: number;
  /** How long a Usenet test may take. A bandwidth test ends itself once its rate holds steady. */
  maxDuration: number;
};

/** The decode chart's line, apart from the download chart's accent. */
const DECODE_COLOR = "#6aa7e8";
const SAB_SIZES: SabTestSize[] = ["MB_100", "GB_1", "GB_10"];
export const MAX_DURATION = 900;
/** The job statuses that are fetching from a server, which a run pauses. */
const FETCHING = new Set(["DOWNLOADING", "FETCHING_REPAIR_DATA", "FINALIZING_DOWNLOAD"]);

export function defaultForm(consumer: Pick<Consumer, "kind" | "cap">): TestForm {
  return {
    kind: "BANDWIDTH",
    providerId: "cloudflare",
    server: "",
    customUrl: "",
    nzb: "MB_100",
    nzbUrl: "",
    nzbFile: null,
    connections: consumer.cap,
    maxDuration: 120,
  };
}

const rateLabel = (bytesPerSecond: number) => {
  const { value, unit } = splitSpeed(bytesPerSecond);
  return `${value} ${unit}`;
};
export const sabLabel = (t: Translate, size: SabTestSize) => t(`next.networking.speed.sab.${size}`);

/** The host part of a URL, or null when it is not one yet. */
export function urlHost(url: string): string | null {
  try {
    return new URL(url.trim()).host || null;
  } catch {
    return null;
  }
}

/** The servers a provider that needs one offers this consumer, while the form is on that provider. */
export function useProviderServers(
  form: TestForm,
  consumer: Pick<Consumer, "kind" | "id">,
  provider: BandwidthProvider | undefined,
): BandwidthProviderServer[] {
  const [{ data }] = useQuery<{ bandwidthProviderServers: BandwidthProviderServer[] }>({
    query: BANDWIDTH_PROVIDER_SERVERS_QUERY,
    variables: { providerId: form.providerId, kind: consumer.kind, id: consumer.id },
    pause: form.kind !== "BANDWIDTH" || !provider?.needsServer,
  });
  return provider?.needsServer ? (data?.bandwidthProviderServers ?? []) : [];
}

/** What a form measures against, as the box a route's flow ends at: its name, then where it is. */
export function formEndpoint(
  t: Translate,
  form: TestForm,
  consumer: Pick<Consumer, "name">,
  provider: BandwidthProvider | undefined,
  servers: BandwidthProviderServer[],
): { title: string; detail: string } {
  if (form.kind === "USENET") {
    const nzb =
      form.nzb === "URL"
        ? (urlHost(form.nzbUrl) ?? t("next.networking.speed.nzbUrl"))
        : form.nzb === "FILE"
          ? (form.nzbFile?.name ?? t("next.networking.speed.nzbFile"))
          : sabLabel(t, form.nzb);
    return { title: consumer.name, detail: `${t("next.networking.speed.usenet")} · ${nzb}` };
  }
  if (!form.providerId) {
    return { title: t("next.networking.speed.customUrl"), detail: urlHost(form.customUrl) ?? "https://" };
  }
  if (provider?.needsServer) {
    const server = servers.find((candidate) => candidate.id === form.server);
    return { title: provider.name, detail: server?.host ?? t("next.networking.speed.nearestServer") };
  }
  return { title: provider?.name ?? form.providerId, detail: provider?.host ?? t("next.networking.speed.bandwidth") };
}

/** Why the form cannot be run as it stands, in one line, or null. */
export function formProblem(
  t: Translate,
  form: TestForm,
  consumer: Pick<Consumer, "kind">,
): string | null {
  if (form.kind === "USENET" && consumer.kind === "RSS") {
    return t("next.networking.speed.usenetNeedsServer");
  }
  if (form.kind === "BANDWIDTH" && !form.providerId && !/^https:\/\/\S+$/i.test(form.customUrl.trim())) {
    return t("next.networking.speed.customUrlInvalid");
  }
  if (form.kind === "USENET" && form.nzb === "URL" && !/^https?:\/\/\S+$/i.test(form.nzbUrl.trim())) {
    return t("next.networking.speed.nzbUrlInvalid");
  }
  if (form.kind === "USENET" && form.nzb === "FILE" && !isNzbFile(form.nzbFile)) {
    return t("next.networking.speed.nzbFileMissing");
  }
  return null;
}

/** Only a single `.nzb` file is taken: no archives of them, and no other kind of file. */
const isNzbFile = (file: File | null): file is File => file !== null && /\.nzb$/i.test(file.name);

/** The request a form makes, for a consumer's saved route (`route` null) or a draft of it. */
export function speedTestInput(
  consumer: Pick<Consumer, "kind" | "id">,
  route: NetworkRoute | null,
  overrides: PathOverride,
  form: TestForm,
  provider: BandwidthProvider | undefined,
): SpeedTestInput {
  const input: SpeedTestInput = {
    kind: consumer.kind,
    id: consumer.id,
    overrides,
  };
  if (route) {
    input.route = routeInput(route);
  }
  if (form.kind === "BANDWIDTH") {
    // The daemon picks the download, the connections and when to stop.
    input.bandwidth = form.providerId
      ? { providerId: form.providerId, ...(provider?.needsServer && form.server ? { server: form.server } : {}) }
      : { customUrl: form.customUrl.trim() };
  } else {
    input.usenet = {
      ...(form.nzb === "URL"
        ? { nzbUrl: form.nzbUrl.trim() }
        : form.nzb === "FILE"
          ? { nzbUpload: form.nzbFile }
          : { sabSize: form.nzb }),
      connections: form.connections,
      maxDurationSeconds: Math.max(1, Math.min(MAX_DURATION, Math.round(form.maxDuration))),
    };
  }
  return input;
}

/** Bandwidth providers and the runs the daemon still holds, newest first. */
export function useSpeedTestData() {
  const [{ data, error }, refresh] = useQuery<{ bandwidthProviders: BandwidthProvider[]; speedTestRuns: SpeedTestRun[] }>({
    query: SPEED_TEST_QUERY,
    requestPolicy: "cache-and-network",
  });
  return {
    providers: data?.bandwidthProviders ?? [],
    runs: data?.speedTestRuns ?? [],
    error,
    refresh: () => refresh({ requestPolicy: "network-only" }),
  };
}

/**
 * One run as it goes: started here, or picked up while it was running. While it
 * runs it is read again once a second, which is how its progress arrives.
 */
export function useSpeedTestRun(onFinished: () => void) {
  const t = useTranslate();
  const client = useClient();
  const [run, setRun] = useState<SpeedTestRun | null>(null);
  const [busy, setBusy] = useState(false);
  const [refusal, setRefusal] = useState<string | null>(null);
  /** A run waiting on the operator to agree to pause downloads. */
  const [pending, setPending] = useState<SpeedTestInput | null>(null);
  const { isPaused, queue } = useNextData();
  // Jobs, not the header gauge: a Usenet run's own bytes drive the gauge, and it can still read them after the run.
  const downloading = !isPaused && queue.jobs.some((job) => FETCHING.has(job.status.toUpperCase()));
  const runningId = run?.status === "RUNNING" ? run.id : null;
  // The poll outlives renders, so it reads the latest callback rather than restarting for each.
  const finished = useRef(onFinished);
  useEffect(() => {
    finished.current = onFinished;
  });

  useEffect(() => {
    if (runningId === null) {
      return;
    }
    let live = true;
    const timer = setInterval(() => {
      void client
        .query<{ speedTestRun: SpeedTestRun | null }>(SPEED_TEST_RUN_QUERY, { id: runningId }, { requestPolicy: "network-only" })
        .toPromise()
        .then((result) => {
          const next = result.data?.speedTestRun;
          if (live && next) {
            setRun(next);
            if (next.status !== "RUNNING") {
              finished.current();
            }
          }
        });
    }, 1000);
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, [client, runningId]);

  const begin = async (input: SpeedTestInput) => {
    setBusy(true);
    setRefusal(null);
    try {
      const result = await client.mutation<{ startSpeedTest: SpeedTestRun }>(START_SPEED_TEST, { input }).toPromise();
      if (result.error || !result.data) {
        setRefusal(result.error ? refusalMessage(t, result.error) : t("next.networking.requestFailed"));
        return;
      }
      setRun(result.data.startSpeedTest);
      finished.current();
    } catch (caught) {
      setRefusal(caught instanceof Error ? caught.message : t("next.networking.requestFailed"));
    } finally {
      setBusy(false);
    }
  };

  const cancel = async () => {
    if (!run) {
      return;
    }
    const result = await client.mutation<{ cancelSpeedTest: boolean }>(CANCEL_SPEED_TEST, { id: run.id }).toPromise();
    if (result.error) {
      setRefusal(refusalMessage(t, result.error));
      return;
    }
    if (result.data?.cancelSpeedTest === false) {
      // It had already stopped: read where it ended up.
      const latest = await client
        .query<{ speedTestRun: SpeedTestRun | null }>(SPEED_TEST_RUN_QUERY, { id: run.id }, { requestPolicy: "network-only" })
        .toPromise();
      if (latest.data?.speedTestRun) {
        setRun(latest.data.speedTestRun);
        finished.current();
      }
    }
  };

  // A run pauses the queue, so while anything is downloading it asks first.
  const start = (input: SpeedTestInput) => {
    if (downloading) {
      setPending(input);
      return;
    }
    void begin(input);
  };
  const dialog = (
    <ConfirmDialog
      open={pending !== null}
      title={t("next.networking.speed.pauseTitle")}
      body={t("next.networking.speed.pauseBody")}
      confirmLabel={t("next.networking.speed.pauseConfirm")}
      destructive={false}
      busy={busy}
      onConfirm={() => {
        const input = pending;
        setPending(null);
        if (input) {
          void begin(input);
        }
      }}
      onDismiss={() => setPending(null)}
    />
  );

  return { run, setRun, start, cancel, busy, refusal, dialog };
}

/** The controls for what to run: the kind of test, what it measures against, and how long a Usenet test may take. */
export function TestControls({
  form,
  onChange,
  consumer,
  providers,
}: {
  form: TestForm;
  onChange: (form: TestForm) => void;
  consumer: Pick<Consumer, "kind" | "id" | "cap">;
  providers: BandwidthProvider[];
}) {
  const t = useTranslate();
  const set = (patch: Partial<TestForm>) => onChange({ ...form, ...patch });
  const provider = providers.find((candidate) => candidate.id === form.providerId);
  const servers = useProviderServers(form, consumer, provider);
  const fileRef = useRef<HTMLInputElement>(null);

  return (
    <div className="flex min-w-0 flex-col gap-4 px-4 py-4 sm:px-6">
      <ControlLabel label={t("next.networking.speed.test")}>
        <Segmented
          label={t("next.networking.speed.test")}
          value={form.kind}
          onChange={(kind) => set({ kind })}
          className="self-start"
          options={[
            { value: "BANDWIDTH", label: t("next.networking.speed.bandwidth") },
            { value: "USENET", label: t("next.networking.speed.usenet"), disabled: consumer.kind === "RSS" },
          ]}
        />
      </ControlLabel>
      <div className="flex min-w-0 flex-wrap items-end gap-3">
        {form.kind === "BANDWIDTH" ? (
          <>
            <ControlLabel label={t("next.networking.speed.provider")}>
              <Select
                label={t("next.networking.speed.provider")}
                value={form.providerId}
                onChange={(providerId) => set({ providerId, server: "" })}
                options={[
                  ...providers.map((candidate) => ({ value: candidate.id, label: candidate.name })),
                  { value: "", label: t("next.networking.speed.customUrl") },
                ]}
              />
            </ControlLabel>
            {provider?.needsServer ? (
              <ControlLabel label={t("next.networking.speed.server")}>
                <Select
                  label={t("next.networking.speed.server")}
                  value={form.server}
                  onChange={(server) => set({ server })}
                  options={[
                    { value: "", label: t("next.networking.speed.nearestServer") },
                    ...servers.map((server) => ({
                      value: server.id,
                      label: server.location ? `${server.name} · ${server.location}` : server.name,
                    })),
                  ]}
                />
              </ControlLabel>
            ) : null}
            {form.providerId ? null : (
              <ControlLabel label={t("next.networking.speed.url")} className="min-w-0 flex-[0_1_480px]">
                <TextField
                  label={t("next.networking.speed.url")}
                  value={form.customUrl}
                  onChange={(customUrl) => set({ customUrl })}
                  placeholder="https://"
                  type="url"
                  className="w-full min-w-0"
                />
              </ControlLabel>
            )}
          </>
        ) : (
          <>
            <ControlLabel label={t("next.networking.speed.nzb")}>
              <Select
                label={t("next.networking.speed.nzb")}
                value={form.nzb}
                onChange={(nzb) => set({ nzb })}
                className="min-w-[120px]"
                options={[
                  ...SAB_SIZES.map((size) => ({ value: size, label: sabLabel(t, size) })),
                  { value: "URL" as const, label: t("next.networking.speed.url") },
                  { value: "FILE" as const, label: t("next.networking.speed.file") },
                ]}
              />
            </ControlLabel>
            {form.nzb === "FILE" ? (
              <ControlLabel label={t("next.networking.speed.nzbFile")} className="min-w-0 max-w-[480px]">
                <div className="flex h-[34px] min-w-0 items-center gap-3">
                  <input
                    ref={fileRef}
                    type="file"
                    accept=".nzb"
                    aria-label={t("next.networking.speed.nzbFile")}
                    className="hidden"
                    onChange={(event) => {
                      const nzbFile = event.target.files?.[0] ?? null;
                      // Cleared so the same file, edited, can be chosen again.
                      event.target.value = "";
                      if (nzbFile) {
                        set({ nzbFile });
                      }
                    }}
                  />
                  <SecondaryButton icon="chooseFile" onClick={() => fileRef.current?.click()}>
                    {t("next.networking.speed.chooseFile")}
                  </SecondaryButton>
                  {form.nzbFile ? (
                    <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg" title={form.nzbFile.name}>
                      {form.nzbFile.name}
                    </span>
                  ) : null}
                </div>
              </ControlLabel>
            ) : null}
            {form.nzb === "URL" ? (
              <ControlLabel label={t("next.networking.speed.nzbUrl")} className="min-w-0 flex-[0_1_480px]">
                <TextField
                  label={t("next.networking.speed.nzbUrl")}
                  value={form.nzbUrl}
                  onChange={(nzbUrl) => set({ nzbUrl })}
                  placeholder="https://"
                  type="url"
                  className="w-full min-w-0"
                />
              </ControlLabel>
            ) : null}
            <ControlLabel label={t("next.networking.speed.connections")}>
              <NumberField
                label={t("next.networking.speed.connections")}
                value={form.connections}
                min={1}
                max={Math.max(1, consumer.cap)}
                onChange={(connections) => set({ connections })}
              />
            </ControlLabel>
            <ControlLabel label={t("next.networking.speed.maxDuration")}>
              <NumberField
                label={t("next.networking.speed.maxDuration")}
                value={form.maxDuration}
                min={1}
                max={MAX_DURATION}
                suffix="s"
                onChange={(maxDuration) => set({ maxDuration })}
              />
            </ControlLabel>
          </>
        )}
      </div>
    </div>
  );
}

/** When each bin closed, oldest first: every leg is sampled on the same tick. */
const binEnds = (run: SpeedTestRun) => [...new Set((run.result?.samples ?? []).map((sample) => sample.atMs))].sort((a, b) => a - b);

/**
 * The rate over the newest bin, in all and per leg: the rate right now. A bin
 * spans the time since the one before it closed, as the daemon rates it.
 */
export function lastSecond(run: SpeedTestRun): { total: number; legs: Map<number, number> } {
  const ends = binEnds(run);
  const newest = ends.at(-1);
  const width = Math.max(1, (newest ?? 0) - (ends.at(-2) ?? 0));
  const legs = new Map<number, number>();
  for (const sample of run.result?.samples ?? []) {
    if (sample.atMs === newest) {
      legs.set(sample.leg, (legs.get(sample.leg) ?? 0) + (sample.bytes * 1000) / width);
    }
  }
  return { total: [...legs.values()].reduce((sum, rate) => sum + rate, 0), legs };
}

/**
 * A run's rate bin by bin over every leg: of the bytes off the wire, or of
 * what they decoded to. Empty until the run measures.
 */
export function rateSeries(run: SpeedTestRun, field: "bytes" | "decodedBytes" = "bytes"): RatePoint[] {
  const bins = new Map<number, number>();
  for (const sample of run.result?.samples ?? []) {
    bins.set(sample.atMs, (bins.get(sample.atMs) ?? 0) + sample[field]);
  }
  let previous = 0;
  return binEnds(run).map((at) => {
    const width = Math.max(1, at - previous);
    previous = at;
    return { at, rate: ((bins.get(at) ?? 0) * 1000) / width };
  });
}

/** What a running test reports doing, in the order it does it. */
const PHASES = ["preparing", "pausing", "draining", "leasing", "measuring", "releasing"];

/** What the gate is doing, in words: pausing downloads, waiting for what is in flight, or measuring. */
export function phaseLabel(t: Translate, phase: string | null): string {
  if (!phase) {
    return t("next.networking.speed.status.RUNNING");
  }
  const draining = /^draining\s+(\d+)$/i.exec(phase.trim());
  if (draining) {
    return t("next.networking.speed.phase.draining", { count: Number(draining[1]) });
  }
  const word = phase.trim().toLowerCase();
  if (PHASES.includes(word)) {
    return t(`next.networking.speed.phase.${word}`, { count: 0 });
  }
  return phase;
}

/** One rate a running test follows: the figure now, large, over a chart chasing it. */
function LiveFigure({ label, points, color, className }: { label: string; points: RatePoint[]; color?: string; className?: string }) {
  const latest = points.at(-1);
  const split = latest ? splitSpeed(latest.rate) : null;
  return (
    <div className={cn("flex min-w-0 flex-col gap-4 px-4 pt-5 pb-4 sm:px-6", className)}>
      <Eyebrow tone="rail">{label}</Eyebrow>
      <div className="flex items-baseline gap-2 whitespace-nowrap">
        <span className="font-semibold leading-none tracking-[-0.03em] text-wv-fg tabular-nums text-[60px] sm:text-[76px]">
          {split?.value ?? EM_DASH}
        </span>
        {split ? <span className="text-[18px] text-wv-muted">{split.unit}</span> : null}
      </div>
      <LiveChart label={label} points={points} color={color} formatRate={rateLabel} />
    </div>
  );
}

/**
 * A running test: the gate's state and Cancel, then the rate now as a large
 * figure over a chart chasing it, then what it has moved and each leg's rate.
 * A Usenet test charts what it decodes beside what it downloads.
 */
export function RunProgress({ run, onCancel }: { run: SpeedTestRun; onCancel: () => void }) {
  const t = useTranslate();
  const usenet = run.kind === "USENET";
  const now = lastSecond(run);
  const bytes = splitBytes(run.result?.bytes ?? 0);
  const decoded = splitBytes(run.result?.usenet?.decodedBytes ?? 0);
  return (
    <section aria-label={t("next.networking.speed.progress")} className="flex flex-col border-b border-wv-hairline">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-wv-hairline px-4 py-3 sm:px-6">
        <span className="flex min-w-0 items-baseline gap-3">
          <Eyebrow tone="rail">{t("next.networking.speed.gate")}</Eyebrow>
          <span className="truncate text-[13px] text-wv-fg">{phaseLabel(t, run.phase)}</span>
        </span>
        <DangerButton icon="close" onClick={onCancel}>
          {t("next.networking.speed.cancel")}
        </DangerButton>
      </div>
      <div className={cn("grid min-w-0", usenet && "lg:grid-cols-2")}>
        <LiveFigure label={t("next.networking.speed.speed")} points={rateSeries(run)} />
        {usenet ? (
          <LiveFigure
            label={t("next.networking.speed.decode")}
            points={rateSeries(run, "decodedBytes")}
            color={DECODE_COLOR}
            className="border-t border-wv-hairline lg:border-t-0 lg:border-l"
          />
        ) : null}
      </div>
      <MetricStrip min={132}>
        {usenet ? <MetricCell variant="stat" eyebrow={t("next.networking.speed.downloaded")} value={bytes.value} unit={bytes.unit} /> : null}
        {usenet ? <MetricCell variant="stat" eyebrow={t("next.networking.speed.decoded")} value={decoded.value} unit={decoded.unit} /> : null}
        {run.tested.legs
          .filter((leg) => !leg.down)
          .map((leg) => (
            <MetricCell
              key={leg.position}
              variant="stat"
              eyebrow={t("next.networking.speed.legRate", { position: leg.position + 1 })}
              value={rateLabel(now.legs.get(leg.position) ?? 0)}
              valueClassName="text-wv-fg text-[15px]"
            />
          ))}
      </MetricStrip>
    </section>
  );
}

/** A figure a finished run reports. */
export type RunStat = "average" | "peak" | "downloaded" | "decoded";
/** The figures a run beat another run on, each with the margin it beat it by. */
export type BetterStats = Partial<Record<RunStat, string>>;

/** Where `run` beat `other`: each figure both report that came out higher, and by how much. */
export function betterStats(run: SpeedTestRun, other: SpeedTestRun): BetterStats {
  const figures = (candidate: SpeedTestRun): Record<RunStat, number | undefined> => {
    const result = candidate.result;
    return {
      average: result?.averageBps,
      peak: result?.peakBps,
      downloaded: result?.bytes,
      decoded: result?.usenet?.decodedBytes,
    };
  };
  const ours = figures(run);
  const theirs = figures(other);
  const better: BetterStats = {};
  for (const stat of Object.keys(ours) as RunStat[]) {
    const mine = ours[stat];
    const against = theirs[stat];
    if (mine === undefined || against === undefined || mine <= against) {
      continue;
    }
    const margin = against > 0 ? ((mine - against) / against) * 100 : null;
    better[stat] = margin === null ? "" : `+${margin >= 10 ? Math.round(margin) : margin.toFixed(1)}%`;
  }
  return better;
}

/**
 * What a finished run measured: its average with its peak beside it, large,
 * then the bytes it moved. A Usenet run shows what came off the wire and what
 * that decoded to side by side, at the same weight; a bandwidth run has only
 * the first. Set against another run, each figure it beat is marked, with the
 * margin it beat it by.
 */
export function RunResult({ run, better = {} }: { run: SpeedTestRun; better?: BetterStats }) {
  const t = useTranslate();
  const result = run.result;
  const failed = run.status === "FAILED" || run.status === "CANCELLED";
  const figure = (stat: RunStat, split: { value: string; unit: string }) => ({
    value: split.value,
    unit: split.unit,
    valueClassName: better[stat] !== undefined ? "text-wv-accent" : "text-wv-fg",
    ...(better[stat] !== undefined
      ? {
          note: better[stat] || undefined,
          children: <span aria-hidden="true" className="absolute inset-x-0 top-0 h-[3px] bg-wv-accent" />,
        }
      : {}),
  });
  return (
    <section aria-label={t("next.networking.speed.result")} className="flex flex-col border-b border-wv-hairline">
      {failed ? (
        <p role={run.status === "FAILED" ? "alert" : "status"} className="px-4 py-3 text-[12.5px] text-wv-error-text sm:px-6">
          {run.error ? `${t(`next.networking.speed.status.${run.status}`)} · ${run.error}` : t(`next.networking.speed.status.${run.status}`)}
        </p>
      ) : null}
      {result ? (
        <>
          <MetricStrip min={200}>
            <MetricCell variant="hero" eyebrow={t("next.networking.speed.average")} {...figure("average", splitSpeed(result.averageBps))} />
            <MetricCell variant="hero" eyebrow={t("next.networking.speed.peak")} {...figure("peak", splitSpeed(result.peakBps))} />
          </MetricStrip>
          {run.kind === "USENET" ? (
            <MetricStrip min={140}>
              <MetricCell variant="stat" eyebrow={t("next.networking.speed.downloaded")} {...figure("downloaded", splitBytes(result.bytes))} />
              <MetricCell
                variant="stat"
                eyebrow={t("next.networking.speed.decoded")}
                {...figure("decoded", splitBytes(result.usenet?.decodedBytes ?? 0))}
              />
            </MetricStrip>
          ) : null}
        </>
      ) : null}
    </section>
  );
}

/**
 * Run a test and follow it: the Run button, the one-line refusal when it cannot
 * start, then the progress strip while it runs and the result once it has.
 */
export function SpeedTestRunner({
  consumer,
  route,
  overrides,
  form,
  providers,
  speedTest,
  extra,
}: {
  consumer: Pick<Consumer, "kind" | "id" | "cap">;
  /** A draft to test instead of the saved route. */
  route: NetworkRoute | null;
  overrides: PathOverride;
  form: TestForm;
  providers: BandwidthProvider[];
  speedTest: ReturnType<typeof useSpeedTestRun>;
  /** Anything else for the action row, such as Run again. */
  extra?: ReactNode;
}) {
  const t = useTranslate();
  const { run, start, cancel, busy, refusal } = speedTest;
  const provider = providers.find((candidate) => candidate.id === form.providerId);
  const problem = formProblem(t, form, consumer);
  const running = run?.status === "RUNNING";
  return (
    <div className="flex min-w-0 flex-col">
      <div className="flex flex-wrap items-center gap-3 border-b border-wv-hairline px-4 py-3 sm:px-6">
        <PrimaryButton
          icon="go"
          disabled={busy || running || problem !== null}
          onClick={() => start(speedTestInput(consumer, route, overrides, form, provider))}
        >
          {t("next.networking.speed.run")}
        </PrimaryButton>
        {extra}
        {problem || refusal ? (
          <span role="alert" className="min-w-0 text-[12.5px] text-wv-error-text">
            {refusal ?? problem}
          </span>
        ) : null}
      </div>
      {run && running ? <RunProgress run={run} onCancel={() => void cancel()} /> : null}
      {run && !running ? <RunResult run={run} /> : null}
    </div>
  );
}
