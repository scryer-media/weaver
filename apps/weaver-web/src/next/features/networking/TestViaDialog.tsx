import { Fragment, useEffect, useRef, useState } from "react";
import { useMutation, useQuery } from "urql";
import { NETWORKING_QUERY, TEST_VIA } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import { routeInput, routeProblem, type NetworkRoute } from "@/lib/networking";
import { proxyLabels } from "@/lib/proxies";
import { proxyTestSentence } from "@/lib/proxy-fault";
import { refusalMessage } from "@/lib/refusal";
import { cn } from "@/lib/utils";
import {
  draftRoute,
  resolutionParts,
  routeChoices,
  testSummary,
  type PathTestResult,
  type ResolutionParts,
  type TestSubject,
} from "@/lib/test-via";
import { SectionHeader } from "../../components/chrome";
import { Dialog } from "../../components/Dialog";
import { PrimaryButton, SecondaryButton } from "../../components/controls";
import { Icon } from "../../components/icons";
import { FieldRows } from "../../pages/settings/framework";
import type { NetworkingData } from "./data";
import { RouteEditor } from "./RouteEditor";
import { StatusMark } from "./presentation";

const DEFAULT_PORT = 443;

/** A connect time to the precision it is worth: tenths under ten milliseconds. */
const millis = (value: number) => `${value < 10 ? value.toFixed(1) : Math.round(value)} ms`;

/**
 * Test a proxy, a pool, or a route alone through a route drafted for the test.
 *
 * Nothing dials outside a route, so the test goes through one drafted here,
 * and the draft is never saved. The daemon tries every path through it, with
 * the proxy or the pool on top; a pool names the member it picked. A proxy
 * cannot carry itself, so the draft is not offered the subject's proxies.
 */
export function TestViaDialog({
  subject,
  egressId,
  onTested,
  onClose,
}: {
  /** What goes on top of the route; null tests the route alone, which then needs a host. */
  subject: TestSubject;
  /** The egress the draft starts on, else the first enabled one. */
  egressId?: number;
  /** Hears each run in one sentence: its refusal, or what its paths came to. */
  onTested?: (summary: string) => void;
  onClose: () => void;
}) {
  const t = useTranslate();
  const [{ data, error: loadError }] = useQuery<NetworkingData>({
    query: NETWORKING_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [{ fetching: running }, testVia] = useMutation<{ testVia: PathTestResult[] }>(TEST_VIA);
  // Null until the draft is first changed: it starts on the default route.
  const [edited, setEdited] = useState<NetworkRoute | null>(null);
  const [host, setHost] = useState("");
  const [port, setPort] = useState(DEFAULT_PORT);
  const [destinationOpen, setDestinationOpen] = useState(false);
  const [results, setResults] = useState<PathTestResult[] | null>(null);
  const [refusal, setRefusal] = useState<string | null>(null);
  const outcomeRef = useRef<HTMLDivElement>(null);

  // A run's outcome lands below the draft, so it is brought into view.
  useEffect(() => {
    if (results !== null || refusal !== null) {
      outcomeRef.current?.scrollIntoView({ block: "nearest" });
    }
  }, [results, refusal]);

  const route = edited ?? (data ? draftRoute(data.egressInterfaces, egressId) : null);
  const problem = data && route ? routeProblem(route, { egresses: data.egressInterfaces, pools: data.proxyPools }) : null;
  const choices = data ? routeChoices(data.proxyProfiles, data.proxyPools, subject) : null;
  // A destination folded away is not tested, whatever was typed into it.
  const target = (subject === null || destinationOpen) && host.trim() ? { host: host.trim(), port } : null;

  const egressName = (id: number) => data?.egressInterfaces.find((egress) => egress.id === id)?.name ?? String(id);
  const proxyName = (id: number) =>
    data?.proxyProfiles.find((profile) => profile.id === id)?.name ?? t("next.networking.proxyNumber", { id });
  const poolName = (id: number) => data?.proxyPools.find((pool) => pool.id === id)?.name ?? String(id);

  const subjectNote = (() => {
    if (!data || subject === null) {
      return undefined;
    }
    const tested =
      "proxy" in subject
        ? data.proxyProfiles.find((profile) => profile.id === subject.proxy)
        : data.proxyPools.find((pool) => pool.id === subject.pool);
    return tested ? `${tested.name} · ${proxyLabels[tested.kind]}` : undefined;
  })();

  const run = async () => {
    if (!route) {
      return;
    }
    setResults(null);
    setRefusal(null);
    const result = await testVia({ route: routeInput(route), subject, target });
    if (result.error) {
      const sentence = refusalMessage(t, result.error);
      setRefusal(sentence);
      onTested?.(sentence);
      return;
    }
    const paths = result.data?.testVia ?? [];
    setResults(paths);
    const summary = testSummary(t, paths);
    if (summary !== null) {
      onTested?.(summary);
    }
  };

  return (
    <Dialog
      open
      title={t("next.networking.testVia.title")}
      note={subjectNote}
      width={720}
      onDismiss={onClose}
      footer={
        <>
          <SecondaryButton onClick={onClose}>{t("next.networking.close")}</SecondaryButton>
          <PrimaryButton
            icon="test"
            disabled={!route || running || problem !== null || (subject === null && target === null)}
            onClick={() => void run()}
          >
            {running ? t("next.networking.testVia.running") : t("next.networking.testVia.run")}
          </PrimaryButton>
        </>
      }
    >
      {loadError && !data ? (
        <p role="alert" className="px-4 py-4 text-[12.5px] text-wv-error-text sm:px-6">
          {refusalMessage(t, loadError)}
        </p>
      ) : null}
      {!data && !loadError ? (
        <p role="status" className="px-4 py-4 text-[12.5px] text-wv-muted sm:px-6">
          {t("next.common.loading")}
        </p>
      ) : null}
      {data && route && choices ? (
        <>
          <SectionHeader label={t("next.networking.testVia.route")} sticky={false} />
          <div className="flex-none px-4 py-5 sm:px-6">
            <RouteEditor
              value={route}
              onChange={setEdited}
              egresses={data.egressInterfaces}
              profiles={choices.profiles}
              pools={choices.pools}
              kind="OUTBOUND"
            />
          </div>
          {/* A route alone needs somewhere to connect; a proxy or pool is tested without one. */}
          {subject === null ? (
            <SectionHeader label={t("next.networking.testVia.destination")} sticky={false} />
          ) : (
            <button
              type="button"
              aria-expanded={destinationOpen}
              onClick={() => setDestinationOpen((open) => !open)}
              className="flex h-8 flex-none items-center gap-2 border-b border-wv-hairline px-4 text-left font-wv-mono text-[11px] text-wv-muted hover:text-wv-fg sm:px-6"
            >
              <Icon name="expand" size={12} className={cn("flex-none transition-transform", destinationOpen && "rotate-90")} />
              {t("next.networking.testVia.optional")}
            </button>
          )}
          {subject === null || destinationOpen ? (
            <FieldRows
              fields={[
                {
                  id: "host",
                  label: t("next.networking.testVia.host"),
                  control: { kind: "text", value: host, onChange: setHost },
                },
                {
                  id: "port",
                  label: t("next.networking.testVia.port"),
                  control: { kind: "number", value: port, min: 1, max: 65535, onChange: setPort },
                },
              ]}
            />
          ) : null}
          <div ref={outcomeRef} className="flex flex-none flex-col">
            {refusal === null ? null : (
              <p role="alert" className="px-4 py-4 text-[12.5px] leading-[1.5] text-wv-error-text sm:px-6">
                {refusal}
              </p>
            )}
            {results === null ? null : (
              <>
                <SectionHeader label={t("next.networking.testVia.paths")} count={results.length} sticky={false} />
                <ul aria-label={t("next.networking.testVia.paths")} className="flex flex-col">
                  {results.map((result, index) => (
                    <PathRow
                      key={index}
                      result={result}
                      egressName={egressName}
                      proxyName={proxyName}
                      poolName={poolName}
                    />
                  ))}
                </ul>
              </>
            )}
          </div>
        </>
      ) : null}
    </Dialog>
  );
}

/**
 * One path the test tried: its hops by name, how it went, and what the daemon
 * said of it. A pool's hop names the member it picked.
 */
function PathRow({
  result,
  egressName,
  proxyName,
  poolName,
}: {
  result: PathTestResult;
  egressName: (id: number) => string;
  proxyName: (id: number) => string;
  poolName: (id: number) => string;
}) {
  const t = useTranslate();
  const hops: { key: string; name: string; picked?: string; failing: boolean }[] = [
    { key: "egress", name: egressName(result.egressId), failing: false },
    ...result.carrierIds.map((id, position) => ({
      key: `carrier-${position}`,
      name: proxyName(id),
      failing: !result.success && id === result.failingProxyId,
    })),
    ...(result.poolId !== null
      ? [
          {
            key: "subject",
            name: poolName(result.poolId),
            picked: result.proxyId === null ? undefined : proxyName(result.proxyId),
            failing: false,
          },
        ]
      : result.proxyId === null
        ? []
        : [{ key: "subject", name: proxyName(result.proxyId), failing: !result.success && result.proxyId === result.failingProxyId }]),
  ];
  const label = result.success
    ? t("next.networking.testVia.passed")
    : result.failingProxyId === null
      ? t("next.networking.testVia.failed")
      : t("next.networking.testVia.failedAt", { name: proxyName(result.failingProxyId) });
  const detail = [
    ...(result.connectMillis > 0 ? [millis(result.connectMillis)] : []),
    ...(result.sourceAddress ? [t("next.networking.testVia.source", { address: result.sourceAddress })] : []),
  ].join(" · ");
  return (
    <li className="flex min-w-0 flex-col gap-[6px] border-b border-wv-hairline px-4 py-3 sm:px-6">
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-x-4 gap-y-1">
        <span className="min-w-0 font-wv-mono text-[12px] leading-[1.5] break-words text-wv-fg">
          {hops.map((hop, position) => (
            <Fragment key={hop.key}>
              {position > 0 ? <span className="text-wv-muted">{" > "}</span> : null}
              <span className={hop.failing ? "text-wv-error-text" : undefined}>{hop.name}</span>
              {hop.picked === undefined ? null : (
                <>
                  {" ("}
                  <strong className="font-medium">{hop.picked}</strong>
                  {")"}
                </>
              )}
            </Fragment>
          ))}
        </span>
        <StatusMark state={result.success ? "UP" : "DOWN"} label={label} detail={detail || undefined} />
      </div>
      <p className="text-[12.5px] leading-[1.5] text-pretty text-wv-secondary">{proxyTestSentence(t, result)}</p>
      {result.resolution ? (
        <ResolutionLine
          parts={resolutionParts(t, result.resolution, { egress: egressName(result.egressId), proxy: proxyName })}
        />
      ) : null}
    </li>
  );
}

/** How the path's host was resolved, the connected address marked among several. */
function ResolutionLine({ parts }: { parts: ResolutionParts }) {
  return (
    <p
      title={parts.detail ?? undefined}
      className="font-wv-mono text-[11px] leading-[1.5] break-words text-wv-muted"
    >
      {parts.host}
      {parts.addresses.length > 0 ? " → " : null}
      {parts.addresses.map((part, index) => (
        <Fragment key={part.address}>
          {index > 0 ? ", " : null}
          {part.connected ? <strong className="font-medium text-wv-fg">{part.address}</strong> : part.address}
        </Fragment>
      ))}
      {parts.said ? ` · ${parts.said}` : null}
    </p>
  );
}
