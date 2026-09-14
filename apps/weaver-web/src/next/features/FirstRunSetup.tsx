import { useEffect, useRef, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import {
  ADD_CATEGORY_MUTATION,
  ADD_SERVER_MUTATION,
  BEGIN_FIRST_RUN_SETUP_MUTATION,
  CATEGORIES_QUERY,
  FINISH_FIRST_RUN_SETUP_MUTATION,
  FIRST_RUN_SETUP_QUERY,
  REMOVE_CATEGORY_MUTATION,
  SERVERS_QUERY,
  SETTINGS_QUERY,
  SYSTEM_INFO_QUERY,
  TEST_CONNECTION_MUTATION,
  UPDATE_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { BrandLockup } from "@/lib/brand";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { LoadingMark } from "@/lib/loading-mark";
import { cn } from "@/lib/utils";
import { Eyebrow, Square } from "../components/chrome";
import { NumberField, PrimaryButton, SecondaryButton, TextField, Toggle } from "../components/controls";
import { Icon } from "../components/icons";
import { StorageMounts, type StorageVolume } from "../components/storage";
import { formatLatency } from "../data/format";
import { WV } from "../data/palette";
import { PathField } from "./DirectoryBrowserDialog";

interface FirstRunSetupState {
  firstRunSetup: { pending: boolean };
}

interface SavedServer {
  id: number;
  host: string;
  port: number;
  tls: boolean;
  connections: number;
}

interface TestResult {
  success: boolean;
  message: string;
  latencyMs: number | null;
  supportsPipelining?: boolean;
  adoptableTlsNameMismatchCertificate: {
    derBase64: string;
    sha256Fingerprint: string;
  } | null;
}

interface Category {
  id: number;
  name: string;
  destDir: string | null;
}

interface FolderSettings {
  settings: { dataDir: string; intermediateDir: string; completeDir: string };
}

interface ProviderForm {
  host: string;
  port: number;
  tls: boolean;
  username: string;
  password: string;
  connections: number;
  certificate: { derBase64: string; fingerprint: string } | null;
}

const NEW_PROVIDER: ProviderForm = {
  host: "",
  port: 563,
  tls: true,
  username: "",
  password: "",
  connections: 20,
  certificate: null,
};

type Step = "provider" | "folders" | "done";
const STEPS: readonly Step[] = ["provider", "folders", "done"];

function normalizeHost(host: string): string {
  return host
    .trim()
    .replace(/^(nntps?|https?):\/\//i, "")
    .replace(/\/+$/, "");
}

function errorMessage(error: { graphQLErrors: { message: string }[]; message: string }): string {
  return error.graphQLErrors[0]?.message ?? error.message;
}

/**
 * Walks a new install through its first provider and its folders before the
 * Next interface opens.
 *
 * The server decides whether the walk is owed, so it shows once per install
 * rather than once per browser, and an install that already had a provider
 * when it upgraded never sees it. The decision is taken once per page load: a
 * refetch never pulls the interface out from under someone. If the question
 * cannot be answered the interface opens as usual.
 */
export function FirstRunGate({ children }: { children: ReactNode }) {
  const [decision, setDecision] = useState<"pending" | "wizard" | "app">("pending");
  const [{ data, error, fetching }] = useQuery<FirstRunSetupState>({
    query: FIRST_RUN_SETUP_QUERY,
    requestPolicy: "network-only",
  });

  useEffect(() => {
    if (fetching || (!data && !error)) {
      return;
    }
    setDecision((current) => {
      if (current !== "pending") {
        return current;
      }
      return !error && data?.firstRunSetup.pending ? "wizard" : "app";
    });
  }, [data, error, fetching]);

  if (decision === "pending") {
    return (
      <div className="flex h-dvh items-center justify-center bg-wv-app" aria-hidden="true">
        <LoadingMark className="h-10" reveal />
      </div>
    );
  }
  if (decision === "wizard") {
    return <FirstRunWizard onDone={() => setDecision("app")} />;
  }
  return <>{children}</>;
}

function FirstRunWizard({ onDone }: { onDone: () => void }) {
  const t = useTranslate();
  const [step, setStep] = useState<Step>("provider");
  const [finishing, setFinishing] = useState(false);
  const [finishError, setFinishError] = useState<string | null>(null);
  const [, beginSetup] = useMutation(BEGIN_FIRST_RUN_SETUP_MUTATION);
  const [, finishSetup] = useMutation(FINISH_FIRST_RUN_SETUP_MUTATION);
  const began = useRef(false);

  // Adding the provider would otherwise end the owed walk on the next reload.
  useEffect(() => {
    if (!began.current) {
      began.current = true;
      void beginSetup({});
    }
  }, [beginSetup]);

  const finish = async () => {
    setFinishing(true);
    setFinishError(null);
    const result = await finishSetup({});
    if (result.error) {
      setFinishing(false);
      setFinishError(t("next.firstRun.finishFailed", { message: errorMessage(result.error) }));
      return;
    }
    onDone();
  };

  const index = STEPS.indexOf(step);
  const labels: Record<Step, string> = {
    provider: t("next.firstRun.stepProvider"),
    folders: t("next.firstRun.stepFolders"),
    done: t("next.firstRun.stepDone"),
  };

  return (
    // The Next interface locks page scrolling, so the walk scrolls itself.
    <div className="h-dvh overflow-y-auto bg-wv-app text-wv-fg">
      <div className="mx-auto flex w-full max-w-[600px] flex-col gap-8 px-4 py-10 sm:px-6 sm:py-16">
        <BrandLockup className="h-[26px] w-auto self-center text-wv-strong" />

        <main className="flex flex-col border border-wv-control bg-wv-chrome shadow-wv-menu">
          <div className="flex flex-col gap-4 border-b border-wv-hairline px-5 pt-5 pb-4 sm:px-7">
            <div className="flex items-center gap-3">
              <Eyebrow className="min-w-0 truncate">{t("next.firstRun.title")}</Eyebrow>
              <span className="hidden flex-none font-wv-mono text-[11px] whitespace-nowrap text-wv-note sm:inline">
                {t("next.firstRun.step", { step: index + 1, total: STEPS.length })}
              </span>
              {step === "done" ? null : (
                <button
                  type="button"
                  onClick={() => void finish()}
                  disabled={finishing}
                  className="ml-auto flex-none cursor-pointer text-[12px] whitespace-nowrap text-wv-muted hover:text-wv-fg disabled:cursor-default"
                >
                  {t("next.firstRun.skipSetup")}
                </button>
              )}
            </div>
            <ol className="grid grid-cols-3 gap-1.5">
              {STEPS.map((entry, position) => (
                <li
                  key={entry}
                  aria-current={entry === step ? "step" : undefined}
                  className="flex flex-col gap-1.5"
                >
                  <span className={cn("h-[3px]", position <= index ? "bg-wv-accent" : "bg-wv-control")} />
                  <span
                    className={cn(
                      "font-wv-title text-[10.5px] font-semibold uppercase tracking-[0.12em]",
                      entry === step ? "text-wv-fg" : "text-wv-faint",
                    )}
                  >
                    {labels[entry]}
                  </span>
                </li>
              ))}
            </ol>
          </div>

          {step === "provider" ? (
            <ProviderStep onContinue={() => setStep("folders")} />
          ) : step === "folders" ? (
            <FoldersStep onBack={() => setStep("provider")} onContinue={() => setStep("done")} />
          ) : (
            <DoneStep
              onBack={() => setStep("folders")}
              onOpen={() => void finish()}
              finishing={finishing}
            />
          )}

          {finishError ? (
            <div role="alert" className="border-t border-wv-hairline px-5 py-3 text-[12.5px] text-wv-error-text sm:px-7">
              {finishError}
            </div>
          ) : null}
        </main>
      </div>
    </div>
  );
}

/* --------------------------------------------------------------- step shell */

function StepBody({
  title,
  body,
  children,
  footer,
}: {
  title: string;
  body: string;
  children: ReactNode;
  footer: ReactNode;
}) {
  return (
    <>
      <div className="flex flex-col gap-5 px-5 py-6 sm:px-7">
        <div className="flex flex-col gap-1.5">
          <h1 className="font-wv-title text-[19px] font-semibold text-wv-strong">{title}</h1>
          <p className="text-[13px] leading-[1.55] text-wv-muted">{body}</p>
        </div>
        {children}
      </div>
      <div className="flex flex-wrap items-center gap-3 border-t border-wv-hairline px-5 py-4 sm:px-7">
        {footer}
      </div>
    </>
  );
}

function FormField({
  label,
  help,
  children,
  className,
}: {
  label: string;
  help?: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-[7px]", className)}>
      <span className="text-[12px] font-medium text-wv-secondary">{label}</span>
      {children}
      {help ? <span className="text-[11.5px] leading-[1.45] text-wv-muted">{help}</span> : null}
    </div>
  );
}

function ErrorLine({ children }: { children: ReactNode }) {
  return (
    <div role="alert" className="text-[12.5px] leading-[1.45] text-wv-error-text">
      {children}
    </div>
  );
}

/* ------------------------------------------------------------ provider step */

function ProviderStep({ onContinue }: { onContinue: () => void }) {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ servers: SavedServer[] }>({
    query: SERVERS_QUERY,
    requestPolicy: "network-only",
  });
  const [, addServer] = useMutation(ADD_SERVER_MUTATION);
  const [, testConnection] = useMutation(TEST_CONNECTION_MUTATION);
  const [form, setForm] = useState<ProviderForm>(NEW_PROVIDER);
  const [addingAnother, setAddingAnother] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const servers = data?.servers ?? [];
  const loaded = data !== undefined || !fetching;
  const editing = loaded && (servers.length === 0 || addingAnother);

  const patch = (next: Partial<ProviderForm>) => {
    setForm((current) => ({ ...current, ...next }));
    setTestResult(null);
    setError(null);
  };

  const input = () => ({
    host: normalizeHost(form.host),
    port: form.port,
    tls: form.tls,
    username: form.username.trim() || null,
    password: form.password.trim() || null,
    connections: form.connections,
    active: true,
    priority: 0,
    backfill: false,
    retentionDays: 0,
    tlsNameMismatchCertificateDerBase64: form.certificate?.derBase64 ?? null,
  });

  const runTest = async () => {
    if (!normalizeHost(form.host)) {
      setError(t("next.providers.hostRequired"));
      return;
    }
    setTesting(true);
    setTestResult(null);
    setError(null);
    const result = await testConnection({ input: input() });
    setTesting(false);
    setTestResult(
      (result.data?.testConnection as TestResult | undefined) ??
        (result.error
          ? {
              success: false,
              message: errorMessage(result.error),
              latencyMs: null,
              adoptableTlsNameMismatchCertificate: null,
            }
          : null),
    );
  };

  const save = async () => {
    if (!normalizeHost(form.host)) {
      setError(t("next.providers.hostRequired"));
      return;
    }
    setSaving(true);
    setError(null);
    const result = await addServer({ input: input() });
    setSaving(false);
    if (result.error) {
      setError(t("next.firstRun.provider.saveFailed", { message: errorMessage(result.error) }));
      return;
    }
    setForm(NEW_PROVIDER);
    setTestResult(null);
    setAddingAnother(false);
    reexecute({ requestPolicy: "network-only" });
  };

  const certificate = testResult?.adoptableTlsNameMismatchCertificate ?? null;

  return (
    <StepBody
      title={t("next.firstRun.provider.title")}
      body={t("next.firstRun.provider.body")}
      footer={
        editing ? (
          <>
            <SecondaryButton icon="test" onClick={() => void runTest()} disabled={testing || saving}>
              {testing ? t("next.providers.testing") : t("next.providers.test")}
            </SecondaryButton>
            {servers.length > 0 ? (
              <SecondaryButton
                onClick={() => {
                  setAddingAnother(false);
                  setForm(NEW_PROVIDER);
                  setTestResult(null);
                  setError(null);
                }}
              >
                {t("action.cancel")}
              </SecondaryButton>
            ) : (
              <SecondaryButton onClick={onContinue}>{t("next.firstRun.provider.skip")}</SecondaryButton>
            )}
            <PrimaryButton
              icon="add"
              onClick={() => void save()}
              disabled={saving || !normalizeHost(form.host)}
              className="ml-auto"
            >
              {saving ? t("next.firstRun.saving") : t("next.firstRun.provider.save")}
            </PrimaryButton>
          </>
        ) : (
          <>
            <SecondaryButton icon="add" onClick={() => setAddingAnother(true)} disabled={!loaded}>
              {t("next.firstRun.provider.addAnother")}
            </SecondaryButton>
            <PrimaryButton onClick={onContinue} disabled={!loaded} className="ml-auto">
              {t("next.firstRun.continue")}
            </PrimaryButton>
          </>
        )
      }
    >
      {!loaded ? (
        <div role="status" className="flex items-center gap-3 font-wv-mono text-[12px] text-wv-muted">
          <LoadingMark className="h-5" />
        </div>
      ) : null}

      {servers.length > 0 ? (
        <div className="flex flex-col gap-2">
          <Eyebrow tone="rail">{t("next.firstRun.provider.saved")}</Eyebrow>
          <ul className="flex flex-col border border-wv-hairline">
            {servers.map((server) => (
              <li
                key={server.id}
                className="flex items-center gap-3 border-b border-wv-hairline px-3 py-2.5 last:border-b-0"
              >
                <Square color={WV.green} />
                <span className="min-w-0 flex-1 truncate font-wv-mono text-[12px] text-wv-fg">
                  {server.host}:{server.port}
                </span>
                <span className="font-wv-mono text-[11px] text-wv-muted">
                  {server.tls ? "TLS · " : ""}
                  {t("next.providers.threads")} {server.connections}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {editing ? (
        <div className="flex flex-col gap-4">
          <div className="grid grid-cols-[minmax(0,1fr)_110px] gap-3">
            <FormField label={t("next.providers.host")} help={t("next.providers.hostHelp")}>
              <TextField
                label={t("next.providers.host")}
                value={form.host}
                placeholder="news.example.com"
                onChange={(host) => patch({ host, certificate: null })}
                autoComplete="off"
                autoFocus
                className="w-full"
              />
            </FormField>
            <FormField label={t("next.providers.port")}>
              <NumberField
                label={t("next.providers.port")}
                value={form.port}
                min={1}
                max={65535}
                onChange={(port) => patch({ port })}
                className="w-full"
              />
            </FormField>
          </div>

          <div className="flex items-start justify-between gap-4">
            <div className="flex min-w-0 flex-col gap-[7px]">
              <span className="text-[12px] font-medium text-wv-secondary">{t("next.firstRun.provider.tls")}</span>
              <span className="text-[11.5px] leading-[1.45] text-wv-muted">{t("next.firstRun.provider.tlsHint")}</span>
            </div>
            <Toggle
              label={t("next.firstRun.provider.tls")}
              checked={form.tls}
              onChange={(tls) => patch({ tls, port: tls ? 563 : 119 })}
            />
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <FormField label={`${t("next.providers.username")} (${t("next.common.optional")})`}>
              <TextField
                label={t("next.providers.username")}
                value={form.username}
                onChange={(username) => patch({ username })}
                autoComplete="off"
                className="w-full"
              />
            </FormField>
            <FormField label={`${t("next.providers.password")} (${t("next.common.optional")})`}>
              <TextField
                type="password"
                label={t("next.providers.password")}
                value={form.password}
                onChange={(password) => patch({ password })}
                autoComplete="new-password"
                className="w-full"
              />
            </FormField>
          </div>

          <FormField label={t("next.providers.threads")} help={t("next.providers.threadsHelp")}>
            <NumberField
              label={t("next.providers.threads")}
              value={form.connections}
              min={1}
              max={200}
              onChange={(connections) => patch({ connections })}
            />
          </FormField>

          {testResult ? (
            <TestOutcome
              t={t}
              result={testResult}
              trusted={form.certificate !== null}
              onTrust={
                certificate
                  ? () =>
                      setForm((current) => ({
                        ...current,
                        certificate: {
                          derBase64: certificate.derBase64,
                          fingerprint: certificate.sha256Fingerprint,
                        },
                      }))
                  : undefined
              }
            />
          ) : null}
          {form.certificate && !testResult ? (
            <div className="flex items-center gap-2 text-[12px] text-wv-muted">
              <Icon name="trust" size={13} className="flex-none" />
              {t("next.firstRun.provider.certificateTrusted")}
            </div>
          ) : null}
          {error ? <ErrorLine>{error}</ErrorLine> : null}
        </div>
      ) : null}
    </StepBody>
  );
}

function TestOutcome({
  t,
  result,
  trusted,
  onTrust,
}: {
  t: Translate;
  result: TestResult;
  trusted: boolean;
  onTrust?: () => void;
}) {
  const certificate = result.adoptableTlsNameMismatchCertificate;
  return (
    <div role="status" className="flex flex-col gap-1.5 border border-wv-hairline bg-wv-input px-3 py-3">
      <div className="flex items-center gap-[9px] text-[13px]">
        <Square color={result.success ? WV.green : WV.error} />
        <span className={result.success ? "text-wv-fg" : "text-wv-error-text"}>{result.message}</span>
      </div>
      {result.latencyMs === null ? null : (
        <div className="font-wv-mono text-[11.5px] text-wv-muted">
          {formatLatency(result.latencyMs)}
          {result.supportsPipelining ? ` · ${t("next.providers.pipelining")}` : ""}
        </div>
      )}
      {certificate ? (
        <div className="flex flex-col gap-2 pt-1.5">
          <span className="text-[12px] leading-[1.45] text-wv-warn">
            {t("next.firstRun.provider.certMismatch")}
          </span>
          <span className="font-wv-mono text-[11px] break-all text-wv-muted">
            {t("next.firstRun.provider.certFingerprint", { fingerprint: certificate.sha256Fingerprint })}
          </span>
          {trusted ? (
            <span className="flex items-center gap-2 text-[12px] text-wv-fg">
              <Icon name="trust" size={13} className="flex-none" />
              {t("next.firstRun.provider.certificateTrusted")}
            </span>
          ) : (
            <SecondaryButton icon="trust" onClick={onTrust} className="self-start">
              {t("next.providers.trustCert")}
            </SecondaryButton>
          )}
        </div>
      ) : null}
    </div>
  );
}

/* ------------------------------------------------------------- folders step */

function useStorageCheck() {
  const [{ data }, reexecute] = useQuery<{ systemInfo: { configuredStorage: StorageVolume[] } }>({
    query: SYSTEM_INFO_QUERY,
    requestPolicy: "network-only",
  });
  return {
    volumes: data?.systemInfo.configuredStorage ?? null,
    refresh: () => reexecute({ requestPolicy: "network-only" }),
  };
}

function FoldersStep({ onBack, onContinue }: { onBack: () => void; onContinue: () => void }) {
  const t = useTranslate();
  const [{ data: settingsData }] = useQuery<FolderSettings>({
    query: SETTINGS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: categoryData }, reexecuteCategories] = useQuery<{ categories: Category[] }>({
    query: CATEGORIES_QUERY,
    requestPolicy: "network-only",
  });
  const [, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [, addCategory] = useMutation(ADD_CATEGORY_MUTATION);
  const [, removeCategory] = useMutation(REMOVE_CATEGORY_MUTATION);
  const storage = useStorageCheck();

  const [folders, setFolders] = useState<{ intermediateDir: string; completeDir: string } | null>(null);
  const [savingFolders, setSavingFolders] = useState(false);
  const [folderError, setFolderError] = useState<string | null>(null);
  const [categoryName, setCategoryName] = useState("");
  const [categoryDest, setCategoryDest] = useState("");
  const [categoryBusy, setCategoryBusy] = useState(false);
  const [categoryError, setCategoryError] = useState<string | null>(null);

  const settings = settingsData?.settings;
  useEffect(() => {
    if (settings && folders === null) {
      setFolders({ intermediateDir: settings.intermediateDir ?? "", completeDir: settings.completeDir ?? "" });
    }
  }, [settings, folders]);

  const categories = [...(categoryData?.categories ?? [])].sort((left, right) =>
    left.name.localeCompare(right.name),
  );
  const dataDir = settings?.dataDir ?? "";
  const completeDir = folders?.completeDir.trim() || (dataDir ? `${dataDir}/complete` : "");

  const continueStep = async () => {
    if (!folders || !settings) {
      return;
    }
    const intermediateDir = folders.intermediateDir.trim();
    const completed = folders.completeDir.trim();
    if (intermediateDir !== (settings.intermediateDir ?? "") || completed !== (settings.completeDir ?? "")) {
      setSavingFolders(true);
      setFolderError(null);
      const result = await updateSettings({
        input: { intermediateDir: intermediateDir || null, completeDir: completed || null },
      });
      setSavingFolders(false);
      if (result.error) {
        setFolderError(t("next.firstRun.folders.saveFailed", { message: errorMessage(result.error) }));
        return;
      }
    }
    onContinue();
  };

  const add = async () => {
    const name = categoryName.trim();
    if (!name) {
      setCategoryError(t("next.categories.nameRequired"));
      return;
    }
    setCategoryBusy(true);
    setCategoryError(null);
    const result = await addCategory({
      input: { name, destDir: categoryDest.trim() || null, aliases: "" },
    });
    setCategoryBusy(false);
    if (result.error) {
      setCategoryError(t("next.firstRun.categories.saveFailed", { message: errorMessage(result.error) }));
      return;
    }
    setCategoryName("");
    setCategoryDest("");
    reexecuteCategories({ requestPolicy: "network-only" });
    storage.refresh();
  };

  const remove = async (category: Category) => {
    setCategoryBusy(true);
    setCategoryError(null);
    const result = await removeCategory({ id: category.id });
    setCategoryBusy(false);
    if (result.error) {
      setCategoryError(t("next.firstRun.categories.saveFailed", { message: errorMessage(result.error) }));
      return;
    }
    reexecuteCategories({ requestPolicy: "network-only" });
    storage.refresh();
  };

  return (
    <StepBody
      title={t("next.firstRun.folders.title")}
      body={t("next.firstRun.folders.body")}
      footer={
        <>
          <SecondaryButton onClick={onBack}>{t("next.firstRun.back")}</SecondaryButton>
          <PrimaryButton
            onClick={() => void continueStep()}
            disabled={folders === null || savingFolders}
            className="ml-auto"
          >
            {savingFolders ? t("next.firstRun.saving") : t("next.firstRun.continue")}
          </PrimaryButton>
        </>
      }
    >
      {folders === null ? (
        <div role="status" className="flex items-center gap-3">
          <LoadingMark className="h-5" />
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          <FormField label={t("next.general.workingDir")} help={t("next.general.workingDirHelp")}>
            <PathField
              label={t("next.general.workingDir")}
              value={folders.intermediateDir}
              placeholder={dataDir ? `${dataDir}/intermediate` : undefined}
              onChange={(intermediateDir) => setFolders({ ...folders, intermediateDir })}
              className="w-full"
            />
          </FormField>
          <FormField label={t("next.general.completeDir")} help={t("next.general.completeDirHelp")}>
            <PathField
              label={t("next.general.completeDir")}
              value={folders.completeDir}
              placeholder={dataDir ? `${dataDir}/complete` : undefined}
              onChange={(next) => setFolders({ ...folders, completeDir: next })}
              className="w-full"
            />
          </FormField>
          {folderError ? <ErrorLine>{folderError}</ErrorLine> : null}
        </div>
      )}

      {storage.volumes ? <StorageMounts volumes={storage.volumes} /> : null}

      <div className="flex flex-col gap-3 border-t border-wv-hairline pt-5">
        <div className="flex flex-col gap-1.5">
          <Eyebrow tone="rail">{t("next.firstRun.categories.title")}</Eyebrow>
          <p className="text-[12.5px] leading-[1.55] text-wv-muted">{t("next.firstRun.categories.body")}</p>
        </div>

        {categories.length === 0 ? (
          <div className="text-[12px] text-wv-muted">{t("next.categories.empty")}</div>
        ) : (
          <ul className="flex flex-col border border-wv-hairline">
            {categories.map((category) => (
              <li
                key={category.id}
                className="flex items-center gap-3 border-b border-wv-hairline px-3 py-2 last:border-b-0"
              >
                <span className="w-[110px] flex-none truncate text-[13px] font-medium text-wv-fg">
                  {category.name}
                </span>
                <span className="min-w-0 flex-1 truncate font-wv-mono text-[11.5px] text-wv-muted">
                  {category.destDir || `${completeDir}/${category.name}`}
                </span>
                <button
                  type="button"
                  aria-label={t("next.firstRun.categories.remove", { name: category.name })}
                  title={t("next.firstRun.categories.remove", { name: category.name })}
                  disabled={categoryBusy}
                  onClick={() => void remove(category)}
                  className="flex size-7 flex-none cursor-pointer items-center justify-center text-wv-muted hover:text-wv-error-text disabled:cursor-default"
                >
                  <Icon name="remove" size={14} />
                </button>
              </li>
            ))}
          </ul>
        )}

        <div className="grid grid-cols-1 items-start gap-3 sm:grid-cols-[150px_minmax(0,1fr)]">
          <FormField label={t("next.categories.name")}>
            <TextField
              label={t("next.categories.name")}
              value={categoryName}
              placeholder="tv"
              onChange={(next) => {
                setCategoryName(next);
                setCategoryError(null);
              }}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  void add();
                }
              }}
              className="w-full"
            />
          </FormField>
          <FormField
            label={`${t("next.categories.destination")} (${t("next.common.optional")})`}
            help={t("next.categories.destinationHelp", {
              path: `${completeDir || t("next.categories.completedFolder")}/${categoryName.trim() || t("next.categories.namePlaceholder")}`,
            })}
          >
            <PathField
              label={t("next.categories.destination")}
              value={categoryDest}
              onChange={setCategoryDest}
              className="w-full"
            />
          </FormField>
        </div>
        {categoryError ? <ErrorLine>{categoryError}</ErrorLine> : null}
        <SecondaryButton
          icon="add"
          onClick={() => void add()}
          disabled={categoryBusy || !categoryName.trim()}
          className="self-start"
        >
          {t("next.categories.add")}
        </SecondaryButton>
      </div>
    </StepBody>
  );
}

/* ---------------------------------------------------------------- done step */

function DoneStep({
  onBack,
  onOpen,
  finishing,
}: {
  onBack: () => void;
  onOpen: () => void;
  finishing: boolean;
}) {
  const t = useTranslate();
  const [{ data: serverData }] = useQuery<{ servers: SavedServer[] }>({
    query: SERVERS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: settingsData }] = useQuery<FolderSettings>({
    query: SETTINGS_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: categoryData }] = useQuery<{ categories: Category[] }>({
    query: CATEGORIES_QUERY,
    requestPolicy: "network-only",
  });
  const storage = useStorageCheck();

  const servers = serverData?.servers ?? [];
  const settings = settingsData?.settings;
  const categories = [...(categoryData?.categories ?? [])]
    .map((category) => category.name)
    .sort((left, right) => left.localeCompare(right));

  return (
    <StepBody
      title={t("next.firstRun.done.title")}
      body={t("next.firstRun.done.body")}
      footer={
        <>
          <SecondaryButton onClick={onBack} disabled={finishing}>
            {t("next.firstRun.back")}
          </SecondaryButton>
          <PrimaryButton onClick={onOpen} disabled={finishing} className="ml-auto">
            {finishing ? t("next.firstRun.saving") : t("next.firstRun.done.open")}
          </PrimaryButton>
        </>
      }
    >
      <dl className="flex flex-col border border-wv-hairline">
        <SummaryRow label={t("next.firstRun.done.providers")}>
          {serverData === undefined ? null : servers.length === 0 ? (
            <span className="text-wv-warn">{t("next.firstRun.done.noProvider")}</span>
          ) : (
            <span className="font-wv-mono text-[12px]">
              {servers.map((server) => `${server.host}:${server.port}`).join(", ")}
            </span>
          )}
        </SummaryRow>
        <SummaryRow label={t("next.general.workingDir")}>
          <span className="font-wv-mono text-[12px] break-all">
            {settings ? settings.intermediateDir || `${settings.dataDir}/intermediate` : null}
          </span>
        </SummaryRow>
        <SummaryRow label={t("next.general.completeDir")}>
          <span className="font-wv-mono text-[12px] break-all">
            {settings ? settings.completeDir || `${settings.dataDir}/complete` : null}
          </span>
        </SummaryRow>
        <SummaryRow label={t("next.firstRun.categories.title")}>
          {categoryData === undefined ? null : categories.length === 0 ? (
            <span className="text-wv-muted">{t("next.firstRun.done.categoriesNone")}</span>
          ) : (
            categories.join(", ")
          )}
        </SummaryRow>
      </dl>

      {storage.volumes ? <StorageMounts volumes={storage.volumes} /> : null}
    </StepBody>
  );
}

function SummaryRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="grid grid-cols-1 gap-1 border-b border-wv-hairline px-3 py-2.5 last:border-b-0 sm:grid-cols-[150px_minmax(0,1fr)] sm:gap-3">
      <dt className="text-[12px] text-wv-muted">{label}</dt>
      <dd className="min-w-0 text-[13px] text-wv-fg">{children}</dd>
    </div>
  );
}
