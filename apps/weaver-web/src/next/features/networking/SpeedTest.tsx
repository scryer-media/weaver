import { useEffect, useRef, useState, type ReactNode } from "react";
import { useClient, useQuery } from "urql";
import {
  BANDWIDTH_PROVIDER_SERVERS_QUERY,
  CANCEL_SPEED_TEST,
  SPEED_TEST_QUERY,
  SPEED_TEST_RUN_QUERY,
  START_SPEED_TEST,
} from "@/graphql/networking";
import { useLiveSpeed } from "@/lib/context/live-data-context";
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
import { MetricCell, MetricStrip } from "../../components/chrome";
import { NumberField, PrimaryButton, SecondaryButton, Segmented, Select, TextField } from "../../components/controls";
import { splitBytes, splitSpeed } from "../../data/format";
import type { Consumer } from "./data";
import { ControlLabel } from "./presentation";

/** What the operator has chosen to run, before it is a request. */
export type TestForm = {
  kind: SpeedTestKind;
  /** Empty for a custom URL. */
  providerId: string;
  server: string;
  customUrl: string;
  /** What each stream asks for; null for the first size the provider offers. */
  bytes: number | null;
  streams: number;
  nzb: SabTestSize | "URL";
  nzbUrl: string;
  connections: number;
  maxDuration: number;
};

const MIB = 1024 * 1024;
/** The sizes offered for a provider that serves any size, and for a custom URL. */
export const ANY_SIZES = [100 * MIB, 1024 * MIB, 10 * 1024 * MIB];
const SAB_SIZES: SabTestSize[] = ["MB_100", "GB_1", "GB_10"];
export const MAX_DURATION = 900;

export function defaultForm(consumer: Pick<Consumer, "kind" | "cap">): TestForm {
  return {
    kind: consumer.kind === "RSS" ? "BANDWIDTH" : "USENET",
    providerId: "cloudflare",
    server: "",
    customUrl: "",
    bytes: null,
    streams: 4,
    nzb: "MB_100",
    nzbUrl: "",
    connections: consumer.cap,
    maxDuration: 120,
  };
}

export const sizeLabel = (bytes: number) => {
  const { value, unit } = splitBytes(bytes);
  return `${value} ${unit}`;
};
const rateLabel = (bytesPerSecond: number) => {
  const { value, unit } = splitSpeed(bytesPerSecond);
  return `${value} ${unit}`;
};
export const sabLabel = (t: Translate, size: SabTestSize) => t(`next.networking.speed.sab.${size}`);

/** The sizes one stream may ask a provider for, or a custom URL. */
export function streamSizes(provider: BandwidthProvider | undefined): number[] {
  if (provider && !provider.anySize && provider.fixedSizes.length) {
    return provider.fixedSizes;
  }
  const cap = provider?.maxBytes;
  // A cap below an offered size takes that size's place.
  return cap == null ? ANY_SIZES : [...new Set(ANY_SIZES.map((bytes) => Math.min(bytes, cap)))];
}

/** What each stream asks for: the size chosen, or the first one offered. */
export const formBytes = (form: TestForm, provider: BandwidthProvider | undefined) =>
  form.bytes ?? streamSizes(form.providerId ? provider : undefined)[0]!;

/** Why the form cannot be run as it stands, in one line, or null. */
export function formProblem(
  t: Translate,
  form: TestForm,
  consumer: Pick<Consumer, "kind">,
  provider: BandwidthProvider | undefined,
): string | null {
  if (form.kind === "BANDWIDTH" && form.providerId && provider?.maxBytes != null && formBytes(form, provider) > provider.maxBytes) {
    return t("next.networking.speed.sizeTooLarge", { max: sizeLabel(provider.maxBytes) });
  }
  if (form.kind === "USENET" && consumer.kind === "RSS") {
    return t("next.networking.speed.usenetNeedsServer");
  }
  if (form.kind === "BANDWIDTH" && !form.providerId && !/^https:\/\/\S+$/i.test(form.customUrl.trim())) {
    return t("next.networking.speed.customUrlInvalid");
  }
  if (form.kind === "USENET" && form.nzb === "URL" && !/^https?:\/\/\S+$/i.test(form.nzbUrl.trim())) {
    return t("next.networking.speed.nzbUrlInvalid");
  }
  return null;
}

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
    maxDurationSeconds: Math.max(1, Math.min(MAX_DURATION, Math.round(form.maxDuration))),
  };
  if (route) {
    input.route = routeInput(route);
  }
  if (form.kind === "BANDWIDTH") {
    input.bandwidth = form.providerId
      ? {
          providerId: form.providerId,
          ...(provider?.needsServer && form.server ? { server: form.server } : {}),
          bytes: formBytes(form, provider),
          streams: form.streams,
        }
      : { customUrl: form.customUrl.trim(), bytes: formBytes(form, undefined), streams: form.streams };
  } else {
    input.usenet = {
      ...(form.nzb === "URL" ? { nzbUrl: form.nzbUrl.trim() } : { sabSize: form.nzb }),
      connections: form.connections,
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

  const start = async (input: SpeedTestInput) => {
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

  return { run, setRun, start, cancel, busy, refusal };
}

/** The controls for what to run: the kind of test, its source and size, and how long it may take. */
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
  const sizes = streamSizes(provider);
  const [{ data: listed }] = useQuery<{ bandwidthProviderServers: BandwidthProviderServer[] }>({
    query: BANDWIDTH_PROVIDER_SERVERS_QUERY,
    variables: { providerId: form.providerId, kind: consumer.kind, id: consumer.id },
    pause: form.kind !== "BANDWIDTH" || !provider?.needsServer,
  });
  const servers = provider?.needsServer ? (listed?.bandwidthProviderServers ?? []) : [];
  const pickProvider = (providerId: string) => {
    const next = providers.find((candidate) => candidate.id === providerId);
    const fixed = streamSizes(next);
    set({ providerId, server: "", bytes: form.bytes !== null && fixed.includes(form.bytes) ? form.bytes : null });
  };

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
                onChange={pickProvider}
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
              <ControlLabel label={t("next.networking.speed.url")} className="flex-[1_1_260px]">
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
            <ControlLabel label={t("next.networking.speed.size")}>
              <Select
                label={t("next.networking.speed.size")}
                value={String(formBytes(form, provider))}
                onChange={(bytes) => set({ bytes: Number(bytes) })}
                className="min-w-[120px]"
                options={sizes.map((bytes) => ({ value: String(bytes), label: sizeLabel(bytes) }))}
              />
            </ControlLabel>
            <ControlLabel label={t("next.networking.speed.streams")}>
              <NumberField
                label={t("next.networking.speed.streams")}
                value={form.streams}
                min={1}
                max={32}
                onChange={(streams) => set({ streams })}
              />
            </ControlLabel>
            {/* Each stream asks for the size, so the test moves both multiplied. */}
            <ControlLabel label={t("next.networking.speed.total")}>
              <output
                aria-label={t("next.networking.speed.total")}
                className="flex h-[34px] items-center font-wv-mono text-[13px] text-wv-secondary"
              >
                {sizeLabel(formBytes(form, provider) * form.streams)}
              </output>
            </ControlLabel>
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
                ]}
              />
            </ControlLabel>
            {form.nzb === "URL" ? (
              <ControlLabel label={t("next.networking.speed.nzbUrl")} className="flex-[1_1_260px]">
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
          </>
        )}
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
      </div>
    </div>
  );
}

/** The newest one-second bin's bytes, in all and per leg: the rate right now. */
function lastSecond(run: SpeedTestRun): { total: number; legs: Map<number, number> } {
  const samples = run.result?.samples ?? [];
  const newest = samples.reduce((latest, sample) => Math.max(latest, sample.atMs), -1);
  const legs = new Map<number, number>();
  for (const sample of samples) {
    if (sample.atMs === newest) {
      legs.set(sample.leg, (legs.get(sample.leg) ?? 0) + sample.bytes);
    }
  }
  return { total: [...legs.values()].reduce((sum, bytes) => sum + bytes, 0), legs };
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

/** A running test: the gate's state, the rate now, what it has moved, each leg's rate, and Cancel. */
export function RunProgress({ run, onCancel }: { run: SpeedTestRun; onCancel: () => void }) {
  const t = useTranslate();
  const live = useLiveSpeed();
  const now = lastSecond(run);
  // A Usenet test books into the live download counter, so its rate is the header's.
  const rate = run.kind === "USENET" ? live : now.total;
  const rateSplit = splitSpeed(rate);
  const bytes = splitBytes(run.result?.bytes ?? 0);
  return (
    <section aria-label={t("next.networking.speed.progress")} className="flex flex-col border-b border-wv-hairline">
      <MetricStrip min={132}>
        <MetricCell variant="stat" eyebrow={t("next.networking.speed.gate")} value={phaseLabel(t, run.phase)} valueClassName="text-wv-fg text-[13px]" />
        <MetricCell variant="stat" eyebrow={t("next.networking.speed.rate")} value={rateSplit.value} unit={rateSplit.unit} />
        <MetricCell variant="stat" eyebrow={t("next.networking.speed.bytes")} value={bytes.value} unit={bytes.unit} />
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
      <div className="flex justify-end px-4 py-3 sm:px-6">
        <SecondaryButton icon="close" onClick={onCancel}>
          {t("next.networking.speed.cancel")}
        </SecondaryButton>
      </div>
    </section>
  );
}

/**
 * What a finished run measured: its average with its peak beside it, then the
 * bytes it moved. A Usenet run shows what came off the wire and what that
 * decoded to side by side, at the same weight; a bandwidth run has only the first.
 */
export function RunResult({ run }: { run: SpeedTestRun }) {
  const t = useTranslate();
  const result = run.result;
  const failed = run.status === "FAILED" || run.status === "CANCELLED";
  const figure = (split: { value: string; unit: string }) => ({ value: split.value, unit: split.unit });
  return (
    <section aria-label={t("next.networking.speed.result")} className="flex flex-col border-b border-wv-hairline">
      {failed ? (
        <p role={run.status === "FAILED" ? "alert" : "status"} className="px-4 py-3 text-[12.5px] text-wv-error-text sm:px-6">
          {run.error ? `${t(`next.networking.speed.status.${run.status}`)} · ${run.error}` : t(`next.networking.speed.status.${run.status}`)}
        </p>
      ) : null}
      {result ? (
        <MetricStrip min={140}>
          <MetricCell variant="stat" eyebrow={t("next.networking.speed.average")} {...figure(splitSpeed(result.averageBps))} />
          <MetricCell variant="stat" eyebrow={t("next.networking.speed.peak")} {...figure(splitSpeed(result.peakBps))} />
          <MetricCell variant="stat" eyebrow={t("next.networking.speed.downloaded")} {...figure(splitBytes(result.bytes))} />
          {run.kind === "USENET" ? (
            <MetricCell
              variant="stat"
              eyebrow={t("next.networking.speed.decoded")}
              {...figure(splitBytes(result.usenet?.decodedBytes ?? 0))}
            />
          ) : null}
        </MetricStrip>
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
  const problem = formProblem(t, form, consumer, provider);
  const running = run?.status === "RUNNING";
  return (
    <div className="flex min-w-0 flex-col">
      <div className="flex flex-wrap items-center gap-3 border-b border-wv-hairline px-4 py-3 sm:px-6">
        <PrimaryButton
          icon="go"
          disabled={busy || running || problem !== null}
          onClick={() => void start(speedTestInput(consumer, route, overrides, form, provider))}
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
