import { useCallback, useEffect, useRef, useState } from "react";
import { useClient } from "urql";
import {
  CANCEL_SCRIPT_TEST_MUTATION,
  SCRIPT_TEST_RUN_QUERY,
  TEST_SCRIPT_INSTANCE_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { Dialog } from "../../../components/Dialog";
import { ScriptStatusMark, TruncatedTag } from "../../../components/JobScriptResults";
import { SectionHeader, Square } from "../../../components/chrome";
import { SecondaryButton } from "../../../components/controls";
import { EM_DASH } from "../../../data/format";
import { WV } from "../../../data/palette";
import { formatTimeout, type ScriptInstance, type ScriptKind } from "../../../data/script-instances";
import { formatRunDuration, triggerLabel } from "../../../data/script-runs";
import { FieldRows, type FieldSpec } from "../framework";

/**
 * A test of one instance: the script run once, as the instance is saved,
 * against a download that does not exist.
 *
 * The daemon makes up what a real trigger would supply and reports what the
 * script printed and asked for. Nothing it asks for is applied. The run lives
 * only as long as it is watched, so closing the dialog ends one still running.
 */

interface ScriptTestRun {
  id: string;
  instanceId: string;
  instanceName: string;
  script: string;
  event: string;
  kind: ScriptKind;
  adapter: "SABNZBD" | "NZBGET";
  startedAtEpochMs: number;
  /** The run is ended after this long. */
  timeoutSeconds: number;
  running: boolean;
  /** Set once the script has ended. */
  status: string | null;
  exitCode: number | null;
  durationMs: number | null;
  errorMessage: string | null;
  /** What the script has printed so far. */
  log: string;
  logTruncated: boolean;
  /** The variables made up for the run; the instance's own inputs are left out. */
  inputs: { name: string; value: string }[];
  arguments: string[];
  /** What the script asked for. None of it is applied. */
  commands: string[];
  commandsTruncated: boolean;
}

/** How long a running test is left before it is read again. */
const POLL_MS = 1000;

const BAND = "flex-none border-b border-wv-hairline px-4 py-3 sm:px-6";
const LINE = "border-b border-wv-hairline px-4 py-2 font-wv-mono text-[12px] leading-[1.5] break-all sm:px-6";
const NOTE = "text-[12.5px] leading-[1.5] text-pretty text-wv-muted";

export function ScriptTestDialog({ instance, onClose }: { instance: ScriptInstance; onClose: () => void }) {
  const t = useTranslate();
  const client = useClient();
  const [run, setRun] = useState<ScriptTestRun | null>(null);
  const [starting, setStarting] = useState(true);
  const [cancelling, setCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Counts the reads that failed, so the next one is still scheduled.
  const [missed, setMissed] = useState(0);
  const started = useRef(false);
  const closed = useRef(false);
  const latest = useRef<ScriptTestRun | null>(null);
  latest.current = run;

  const cancel = useCallback(
    (id: string) => client.mutation<{ cancelScriptTest: boolean }>(CANCEL_SCRIPT_TEST_MUTATION, { id }).toPromise(),
    [client],
  );

  const start = useCallback(async () => {
    setStarting(true);
    setError(null);
    setRun(null);
    const result = await client
      .mutation<{ testScriptInstance: ScriptTestRun }>(TEST_SCRIPT_INSTANCE_MUTATION, { id: instance.id })
      .toPromise();
    const begun = result.data?.testScriptInstance ?? null;
    if (closed.current) {
      // The dialog went away while the run was being started; nobody is left to watch it.
      if (begun?.running) {
        void cancel(begun.id);
      }
      return;
    }
    setStarting(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setRun(begun);
  }, [cancel, client, instance.id]);

  // The run starts with the dialog, once: a remount in development must not start a second.
  useEffect(() => {
    closed.current = false;
    if (!started.current) {
      started.current = true;
      void start();
    }
    return () => {
      closed.current = true;
      if (latest.current?.running) {
        void cancel(latest.current.id);
      }
    };
  }, [cancel, start]);

  // A running test is read again until the daemon says it has ended.
  useEffect(() => {
    if (!run?.running) {
      return;
    }
    let live = true;
    const timer = window.setTimeout(() => {
      void client
        .query<{ scriptTestRun: ScriptTestRun | null }>(
          SCRIPT_TEST_RUN_QUERY,
          { id: run.id },
          { requestPolicy: "network-only" },
        )
        .toPromise()
        .then((result) => {
          if (!live) {
            return;
          }
          if (result.error) {
            setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
            setMissed((count) => count + 1);
            return;
          }
          const next = result.data?.scriptTestRun ?? null;
          if (next === null) {
            // The daemon no longer keeps the run, so there is nothing more to read.
            setRun({ ...run, running: false });
            setError(t("next.postProcessing.testGone"));
            return;
          }
          setError(null);
          setRun(next);
        });
    }, POLL_MS);
    return () => {
      live = false;
      window.clearTimeout(timer);
    };
  }, [client, missed, run, t]);

  const cancelRun = async () => {
    if (!run) {
      return;
    }
    setCancelling(true);
    const result = await cancel(run.id);
    setCancelling(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
    }
  };

  const running = run?.running === true;
  const fields: FieldSpec[] = run
    ? [
        {
          id: "status",
          label: t("table.status"),
          control: {
            kind: "custom",
            control: running ? (
              <span className="flex flex-none items-center gap-[7px]">
                <Square color={WV.info} />
                <span className="font-wv-mono text-[11px] text-wv-secondary">
                  {t("next.postProcessing.testRunning")}
                </span>
              </span>
            ) : run.status === null ? (
              // A run the daemon stopped keeping has no outcome to show.
              <span className="font-wv-mono text-[12px] text-wv-muted">{EM_DASH}</span>
            ) : (
              <ScriptStatusMark status={run.status} />
            ),
          },
        },
        {
          id: "trigger",
          label: t("next.scriptRuns.trigger"),
          control: { kind: "static", value: triggerLabel(t, run.event) },
        },
        {
          id: "timeout",
          label: t("next.postProcessing.timeout"),
          control: { kind: "static", value: formatTimeout(run.timeoutSeconds) },
        },
        {
          id: "exitCode",
          label: t("next.scriptRuns.exitCode"),
          control: { kind: "static", value: run.exitCode ?? EM_DASH },
        },
        {
          id: "duration",
          label: t("next.scriptRuns.duration"),
          control: {
            kind: "static",
            value: run.durationMs === null ? EM_DASH : formatRunDuration(run.durationMs),
          },
        },
        {
          id: "adapter",
          label: t("next.postProcessing.adapter"),
          control: { kind: "static", value: run.adapter === "SABNZBD" ? "SABnzbd" : "NZBGet" },
        },
      ]
    : [];

  return (
    <Dialog
      open
      title={t("next.postProcessing.testTitle", { name: instance.name })}
      note={instance.script}
      width={720}
      onDismiss={onClose}
      footer={
        <>
          {running ? (
            <SecondaryButton icon="stopScripts" disabled={cancelling} onClick={() => void cancelRun()}>
              {t("next.postProcessing.cancelTest")}
            </SecondaryButton>
          ) : (
            <SecondaryButton icon="test" disabled={starting} onClick={() => void start()}>
              {t("next.postProcessing.testAgain")}
            </SecondaryButton>
          )}
          <SecondaryButton onClick={onClose}>{t("next.networking.close")}</SecondaryButton>
        </>
      }
    >
      <div className={`${BAND} ${NOTE}`}>{t("next.postProcessing.testNote")}</div>
      {starting ? <div className={`${BAND} ${NOTE}`}>{t("next.postProcessing.testStarting")}</div> : null}
      {error ? (
        <div role="alert" className={`${BAND} text-[12.5px] leading-[1.5] text-wv-error-text`}>
          {error}
        </div>
      ) : null}

      {run ? (
        <>
          <FieldRows fields={fields} />
          {run.errorMessage ? (
            <div className={`${BAND} text-[12.5px] leading-[1.5] text-wv-error-text`}>{run.errorMessage}</div>
          ) : null}

          <section aria-label={t("next.postProcessing.testInputs")} className="flex flex-none flex-col">
            <SectionHeader
              label={t("next.postProcessing.testInputs")}
              count={run.inputs.length}
              note={t("next.postProcessing.testInputsNote")}
              sticky={false}
            />
            {run.inputs.length === 0 ? (
              <div className={`${BAND} ${NOTE}`}>{t("next.job.none")}</div>
            ) : (
              run.inputs.map((input) => (
                <div
                  key={input.name}
                  className="grid grid-cols-[minmax(0,0.9fr)_minmax(0,1.6fr)] gap-5 border-b border-wv-hairline px-4 py-2 font-wv-mono text-[12px] leading-[1.5] sm:px-6"
                >
                  <span className="min-w-0 break-all text-wv-secondary">{input.name}</span>
                  <span className="min-w-0 break-all text-wv-fg">{input.value}</span>
                </div>
              ))
            )}
          </section>

          {run.arguments.length > 0 ? (
            <section aria-label={t("next.postProcessing.testArguments")} className="flex flex-none flex-col">
              <SectionHeader label={t("next.postProcessing.testArguments")} count={run.arguments.length} sticky={false} />
              {run.arguments.map((argument, index) => (
                <div key={index} className={`${LINE} text-wv-fg`}>
                  {argument}
                </div>
              ))}
            </section>
          ) : null}

          <section aria-label={t("next.postProcessing.testLog")} className="flex flex-none flex-col">
            <SectionHeader label={t("next.postProcessing.testLog")} sticky={false} />
            <div className={`${BAND} flex flex-col gap-2`}>
              {run.log ? (
                <pre className="max-h-80 overflow-auto bg-wv-input px-3 py-2 font-wv-mono text-[12px] leading-[1.55] break-words whitespace-pre-wrap text-wv-fg">
                  {run.log}
                </pre>
              ) : (
                <span className={NOTE}>
                  {t(running ? "next.postProcessing.testLogWaiting" : "next.postProcessing.testLogEmpty")}
                </span>
              )}
              {run.logTruncated ? (
                <span className="flex flex-wrap items-center gap-x-[10px] gap-y-1">
                  <TruncatedTag />
                  <span className="font-wv-mono text-[11px] text-wv-muted">{t("next.job.scriptOutputTruncated")}</span>
                </span>
              ) : null}
            </div>
          </section>

          {run.commands.length > 0 ? (
            <section aria-label={t("next.postProcessing.testCommands")} className="flex flex-none flex-col">
              <SectionHeader
                label={t("next.postProcessing.testCommands")}
                count={run.commands.length}
                note={t("next.postProcessing.testCommandsNote")}
                sticky={false}
              />
              {run.commands.map((command, index) => (
                <div key={index} className={`${LINE} text-wv-fg`}>
                  {command}
                </div>
              ))}
              {run.commandsTruncated ? (
                <div className={`${BAND} font-wv-mono text-[11px] text-wv-muted`}>
                  {t("next.postProcessing.testCommandsTruncated")}
                </div>
              ) : null}
            </section>
          ) : null}
        </>
      ) : null}
    </Dialog>
  );
}
