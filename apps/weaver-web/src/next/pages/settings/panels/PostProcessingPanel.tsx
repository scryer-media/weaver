import { useMemo, useRef, useState, type KeyboardEvent, type MouseEvent } from "react";
import { useMutation, useQuery, type CombinedError } from "urql";
import { eventScriptDefaults, eventScriptOptions, eventScriptSection, type EventScriptOptions } from "@/next/components/EventScriptSettings";
import {
  DELETE_SCRIPT_INSTANCE_MUTATION,
  DISCOVERED_SCRIPTS_QUERY,
  POST_PROCESSING_SETTINGS_QUERY,
  REAPPLY_SCRIPT_HEADER_MUTATION,
  REORDER_SCRIPT_INSTANCES_MUTATION,
  SCRIPT_INSTANCES_QUERY,
  SET_POST_PROCESSING_SCRIPT_DIRECTORY_MUTATION,
  SET_POST_PROCESSING_SETTINGS_MUTATION,
  SET_UP_SCRIPT_FROM_HEADER_MUTATION,
  UPDATE_SCRIPT_INSTANCE_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { PrimaryButton, SecondaryButton, Toggle } from "../../../components/controls";
import { Icon, type IconName } from "../../../components/icons";
import { Cell } from "../../../components/rows";
import { EM_DASH } from "../../../data/format";
import { WV } from "../../../data/palette";
import {
  categoryScoped,
  formatTimeout,
  groupInstances,
  inputFromInstance,
  reorderedIds,
  triggerTitle,
  type DiscoveredScript,
  type InstanceGroup,
  type ScriptInstance,
} from "../../../data/script-instances";
import { PathField } from "../../../features/DirectoryBrowserDialog";
import { countLabel } from "../../../i18n/labels";
import {
  PanelControls,
  SettingsBlocks,
  useDraft,
  usePanelState,
  usePanelStatus,
  type SettingsBlock,
  type SettingsTableRowModel,
} from "../framework";
import { ScriptInstanceEditor, type InstanceEditorTarget } from "./ScriptInstanceEditor";
import { ScriptTestDialog } from "./ScriptTestDialog";

/**
 * Scripts: what weaver runs on a download and on the events around it.
 *
 * Configuration holds what applies to every script: the execution settings, a
 * draft behind the top bar's Save, and the scripts directory, destructive
 * enough to ask first. Jobs holds the instances, which are what runs: a
 * script wired to one trigger, with the inputs and run policy saved for it. A
 * script's header only offers a starting point for one, in its editor.
 */

/** When the instances that are not narrowed to a category run. */
type GlobalScriptsRun = "ALWAYS" | "ONLY_WITHOUT_CATEGORY_SCRIPTS";

interface PostProcessingSettings extends EventScriptOptions {
  scriptDirectory: string;
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  pythonInterpreter?: string | null;
  powershellInterpreter?: string | null;
  batchInterpreter?: string | null;
  goInterpreter?: string | null;
  unacceptableExtensions: string[];
  strictSecurityRefusesExecution: boolean;
  globalScriptsRun: GlobalScriptsRun;
}

interface ExecutionForm extends EventScriptOptions {
  executionEnabled: boolean;
  globalScriptsRun: GlobalScriptsRun;
  concurrency: number;
  terminationGraceSeconds: number;
  pythonInterpreter: string;
  powershellInterpreter: string;
  batchInterpreter: string;
  goInterpreter: string;
  unacceptableExtensions: string;
}

/** The daemon runs between one and eight scripts at once. */
const CONCURRENCY_MAX = 8;

const DEFAULTS: ExecutionForm = {
  ...eventScriptDefaults,
  executionEnabled: false,
  globalScriptsRun: "ALWAYS",
  concurrency: 4,
  terminationGraceSeconds: 10,
  pythonInterpreter: "",
  powershellInterpreter: "",
  batchInterpreter: "",
  goInterpreter: "",
  unacceptableExtensions: "",
};

function splitExtensions(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function errorText(error: CombinedError): string {
  return error.graphQLErrors[0]?.message ?? error.message;
}

/* ------------------------------------------------------------ configuration */

/** What applies to every script: whether and how they run, and where they are read from. */
export function ScriptConfigurationPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ postProcessingSettings: PostProcessingSettings }>({
    query: POST_PROCESSING_SETTINGS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [saveState, saveSettings] = useMutation(SET_POST_PROCESSING_SETTINGS_MUTATION);
  const [directoryState, saveDirectory] = useMutation(
    SET_POST_PROCESSING_SCRIPT_DIRECTORY_MUTATION,
  );

  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [directory, setDirectory] = useState<string | null>(null);
  const [confirmDirectory, setConfirmDirectory] = useState(false);

  const settings = data?.postProcessingSettings;

  const source = useMemo<ExecutionForm>(
    () =>
      settings
        ? {
            executionEnabled: settings.executionEnabled,
            globalScriptsRun: settings.globalScriptsRun,
            ...eventScriptOptions(settings),
            concurrency: settings.concurrency,
            terminationGraceSeconds: settings.terminationGraceSeconds,
            pythonInterpreter: settings.pythonInterpreter ?? "",
            powershellInterpreter: settings.powershellInterpreter ?? "",
            batchInterpreter: settings.batchInterpreter ?? "",
            goInterpreter: settings.goInterpreter ?? "",
            unacceptableExtensions: settings.unacceptableExtensions.join(", "),
          }
        : DEFAULTS,
    [settings],
  );

  const draft = useDraft<ExecutionForm>(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value ?? source;
  const patch = draft.set;

  usePanelState({
    dirty: draft.dirty,
    busy: saveState.fetching,
    status: error ?? status,
    failed: error !== null,
    revert: () => {
      setError(null);
      draft.revert();
    },
    save: () => {
      setError(null);
      void saveSettings({
        input: {
          ...eventScriptOptions(values),
          executionEnabled: values.executionEnabled,
          globalScriptsRun: values.globalScriptsRun,
          concurrency: Math.min(CONCURRENCY_MAX, Math.max(1, Math.round(values.concurrency || 1))),
          terminationGraceSeconds: Math.max(0, Math.round(values.terminationGraceSeconds || 0)),
          pythonInterpreter: values.pythonInterpreter.trim() || null,
          powershellInterpreter: values.powershellInterpreter.trim() || null,
          batchInterpreter: values.batchInterpreter.trim() || null,
          goInterpreter: values.goInterpreter.trim() || null,
          unacceptableExtensions: splitExtensions(values.unacceptableExtensions),
        },
      }).then((result) => {
        if (result.error) {
          setError(errorText(result.error));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const scriptDirectory = directory ?? settings?.scriptDirectory ?? "";

  const applyDirectory = () => {
    setConfirmDirectory(false);
    setError(null);
    void saveDirectory({ directory: scriptDirectory.trim() }).then((result) => {
      if (result.error) {
        setError(errorText(result.error));
        return;
      }
      setDirectory(null);
      setStatus(t("next.postProcessing.directorySaved"));
      void reexecute({ requestPolicy: "network-only" });
    });
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "section",
      id: "execution",
      title: t("next.postProcessing.execution"),
      note: settings?.strictSecurityRefusesExecution ? t("next.postProcessing.strictOn") : undefined,
      fields: [
        {
          id: "executionEnabled",
          label: t("next.postProcessing.runScripts"),
          help: t("next.postProcessing.runScriptsHelp"),
          control: {
            kind: "toggle",
            value: values.executionEnabled,
            onChange: (next) => patch({ executionEnabled: next }),
          },
        },
        {
          id: "globalScriptsRun",
          label: t("next.postProcessing.globalScriptsRun"),
          help: t("next.postProcessing.globalScriptsRunHelp"),
          control: {
            kind: "select",
            value: values.globalScriptsRun,
            className: "w-[340px] max-w-full",
            options: [
              { value: "ALWAYS", label: t("next.postProcessing.globalAlways") },
              {
                value: "ONLY_WITHOUT_CATEGORY_SCRIPTS",
                label: t("next.postProcessing.globalOnlyWithout"),
              },
            ],
            onChange: (next) => patch({ globalScriptsRun: next as GlobalScriptsRun }),
          },
        },
        {
          id: "concurrency",
          label: t("next.postProcessing.concurrency"),
          help: t("next.postProcessing.concurrencyHelp"),
          control: {
            kind: "number",
            value: values.concurrency,
            min: 1,
            max: CONCURRENCY_MAX,
            onChange: (next) => patch({ concurrency: next }),
          },
        },
        {
          id: "terminationGraceSeconds",
          label: t("next.postProcessing.grace"),
          help: t("next.postProcessing.graceHelp"),
          control: {
            kind: "number",
            value: values.terminationGraceSeconds,
            min: 0,
            max: 600,
            suffix: t("next.general.seconds"),
            onChange: (next) => patch({ terminationGraceSeconds: next }),
          },
        },
        {
          id: "unacceptableExtensions",
          label: t("next.postProcessing.extensions"),
          help: t("next.postProcessing.extensionsHelp"),
          keywords: values.unacceptableExtensions,
          control: {
            kind: "text",
            value: values.unacceptableExtensions,
            placeholder: "exe, bat, scr",
            onChange: (next) => patch({ unacceptableExtensions: next }),
          },
        },
        {
          id: "strictSecurity",
          label: t("next.postProcessing.strictSecurity"),
          help: t("next.postProcessing.strictSecurityHelp"),
          control: {
            kind: "static",
            value: settings?.strictSecurityRefusesExecution
              ? t("next.postProcessing.refuses")
              : t("next.postProcessing.permissive"),
          },
        },
      ],
    },
    eventScriptSection(t, values, patch, source),
    {
      kind: "section",
      id: "interpreters",
      title: t("next.postProcessing.interpreters"),
      note: t("next.postProcessing.interpretersNote"),
      fields: [
        {
          id: "pythonInterpreter",
          label: "Python",
          keywords: values.pythonInterpreter,
          control: {
            kind: "text",
            value: values.pythonInterpreter,
            placeholder: "/usr/bin/python3",
            onChange: (next) => patch({ pythonInterpreter: next }),
          },
        },
        {
          id: "powershellInterpreter",
          label: "PowerShell",
          keywords: values.powershellInterpreter,
          control: {
            kind: "text",
            value: values.powershellInterpreter,
            placeholder: "pwsh",
            onChange: (next) => patch({ powershellInterpreter: next }),
          },
        },
        {
          id: "batchInterpreter",
          label: "Batch",
          keywords: values.batchInterpreter,
          control: {
            kind: "text",
            value: values.batchInterpreter,
            placeholder: "cmd.exe",
            onChange: (next) => patch({ batchInterpreter: next }),
          },
        },
        {
          id: "goInterpreter",
          label: "Go",
          keywords: values.goInterpreter,
          control: {
            kind: "text",
            value: values.goInterpreter,
            placeholder: "/usr/local/go/bin/go",
            onChange: (next) => patch({ goInterpreter: next }),
          },
        },
      ],
    },
    {
      kind: "section",
      id: "directory",
      title: t("next.postProcessing.directory"),
      note: t("next.postProcessing.directoryNote"),
      fields: [
        {
          id: "scriptDirectory",
          label: t("next.postProcessing.directoryLabel"),
          help: t("next.postProcessing.directoryHelp"),
          keywords: scriptDirectory,
          control: {
            kind: "custom",
            control: (
              <div className="flex min-w-0 flex-wrap items-center gap-3">
                <PathField
                  label={t("next.postProcessing.directory")}
                  value={scriptDirectory}
                  className="w-[360px] max-w-full"
                  onChange={setDirectory}
                />
                <SecondaryButton
                  icon="changeFolder"
                  disabled={
                    directoryState.fetching
                    || scriptDirectory.trim() === (settings?.scriptDirectory ?? "")
                  }
                  onClick={() => setConfirmDirectory(true)}
                >
                  {t("next.postProcessing.change")}
                </SecondaryButton>
              </div>
            ),
          },
        },
      ],
    },
  ];

  return (
    <>
      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <ConfirmDialog
        open={confirmDirectory}
        title={t("next.postProcessing.changeDirTitle")}
        note={scriptDirectory}
        busy={directoryState.fetching}
        confirmLabel={t("next.postProcessing.changeDirConfirm")}
        body={t("next.postProcessing.changeDirBody")}
        onConfirm={applyDirectory}
        onDismiss={() => setConfirmDirectory(false)}
      />
    </>
  );
}

/* ------------------------------------------------------------------ scripts */

interface InstancesData {
  postProcessingSettings: { scriptDirectory: string; globalScriptsRun: GlobalScriptsRun };
  scriptInstances: ScriptInstance[];
  categories: { id: number; name: string }[];
}

interface DiscoveredData {
  discoveredScripts: {
    scripts: DiscoveredScript[];
    problems: { name: string; message: string }[];
  };
}

/** What a confirmation is asked for: both change what is saved in an instance for good. */
interface PendingConfirm {
  kind: "delete" | "reapply";
  instance: ScriptInstance;
}

const NO_INSTANCES: ScriptInstance[] = [];
const NO_SCRIPTS: DiscoveredScript[] = [];

/** One icon-only action at the end of a row; its title is its name. */
function RowAction({
  icon,
  title,
  disabled = false,
  onClick,
}: {
  icon: IconName;
  title: string;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <SecondaryButton className="h-7 px-2" title={title} disabled={disabled} onClick={onClick}>
      <Icon name={icon} size={13} />
    </SecondaryButton>
  );
}

/**
 * The instances that run, by what starts them. The scripts themselves are not
 * listed here: one is chosen in the editor of a new instance.
 */
export function ScriptListPanel() {
  const t = useTranslate();
  const [{ data, fetching, error: instancesError }, reloadInstances] = useQuery<InstancesData>({
    query: SCRIPT_INSTANCES_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [{ data: discovered, error: scriptsError }, reloadScripts] = useQuery<DiscoveredData>({
    query: DISCOVERED_SCRIPTS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [, updateInstance] = useMutation(UPDATE_SCRIPT_INSTANCE_MUTATION);
  const [, deleteInstance] = useMutation(DELETE_SCRIPT_INSTANCE_MUTATION);
  const [, reorderInstances] = useMutation<{ reorderScriptInstances: ScriptInstance[] }>(
    REORDER_SCRIPT_INSTANCES_MUTATION,
  );
  const [, setUpFromHeader] = useMutation<{ setUpScriptFromHeader: ScriptInstance[] }>(
    SET_UP_SCRIPT_FROM_HEADER_MUTATION,
  );
  const [, reapplyHeader] = useMutation(REAPPLY_SCRIPT_HEADER_MUTATION);

  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [working, setWorking] = useState(false);
  const [editor, setEditor] = useState<{ key: number; target: InstanceEditorTarget } | null>(null);
  const [confirm, setConfirm] = useState<PendingConfirm | null>(null);
  const [confirmError, setConfirmError] = useState<string | null>(null);
  const [confirmBusy, setConfirmBusy] = useState(false);
  const [testing, setTesting] = useState<ScriptInstance | null>(null);
  // The order the daemon answered a reorder with stands in until the list is
  // read again, so a second move builds on the first rather than on the order
  // last fetched.
  const [reordered, setReordered] = useState<{ base: ScriptInstance[] | undefined; value: ScriptInstance[] } | null>(
    null,
  );

  const fetched = data?.scriptInstances;
  // The list as last read, for an answer that lands after a newer read than the one it was asked under.
  const fetchedRef = useRef(fetched);
  fetchedRef.current = fetched;
  const instances = reordered && reordered.base === fetched ? reordered.value : (fetched ?? NO_INSTANCES);
  const scripts = discovered?.discoveredScripts.scripts ?? NO_SCRIPTS;
  const problems = discovered?.discoveredScripts.problems ?? [];
  const categories = useMemo(
    () => (data?.categories ?? []).map((category) => category.name).sort((left, right) => left.localeCompare(right)),
    [data?.categories],
  );
  const groups = useMemo(() => groupInstances(instances), [instances]);

  const queryError = instancesError ?? scriptsError;
  const shownError = error ?? (queryError ? errorText(queryError) : null);
  usePanelStatus(shownError ?? status, shownError !== null);

  const refetchInstances = () => void reloadInstances({ requestPolicy: "network-only" });

  const openEditor = (target: InstanceEditorTarget) => {
    setError(null);
    setStatus(null);
    setEditor((current) => ({ key: (current?.key ?? 0) + 1, target }));
  };

  const toggle = async (instance: ScriptInstance, enabled: boolean) => {
    setStatus(null);
    const result = await updateInstance({ id: instance.id, input: inputFromInstance(instance, { enabled }) });
    setError(result.error ? errorText(result.error) : null);
    refetchInstances();
  };

  const move = async (instance: ScriptInstance, direction: -1 | 1) => {
    const order = reorderedIds(instances, instance.id, direction);
    if (!order) {
      return;
    }
    setWorking(true);
    setStatus(null);
    const result = await reorderInstances(order);
    setWorking(false);
    if (result.error) {
      setError(errorText(result.error));
    } else {
      setError(null);
      if (result.data) {
        setReordered({ base: fetchedRef.current, value: result.data.reorderScriptInstances });
      }
    }
    refetchInstances();
  };

  /** Resolves to what went wrong, or null once the instances exist. */
  const setUp = async (script: DiscoveredScript): Promise<string | null> => {
    setWorking(true);
    setStatus(null);
    setError(null);
    const result = await setUpFromHeader({ script: script.name });
    setWorking(false);
    if (result.error) {
      return errorText(result.error);
    }
    setStatus(
      countLabel(t, "next.postProcessing.setUpDone", result.data?.setUpScriptFromHeader.length ?? 0, {
        name: script.displayName,
      }),
    );
    refetchInstances();
    return null;
  };

  const ask = (pending: PendingConfirm) => {
    setConfirmError(null);
    setConfirm(pending);
  };

  const confirmed = async () => {
    if (!confirm) {
      return;
    }
    setConfirmBusy(true);
    const result =
      confirm.kind === "delete"
        ? await deleteInstance({ id: confirm.instance.id })
        : await reapplyHeader({ id: confirm.instance.id });
    setConfirmBusy(false);
    if (result.error) {
      setConfirmError(errorText(result.error));
      return;
    }
    setError(null);
    setStatus(
      t(confirm.kind === "delete" ? "next.postProcessing.instanceDeleted" : "next.postProcessing.reapplied", {
        name: confirm.instance.name,
      }),
    );
    setConfirm(null);
    setEditor(null);
    refetchInstances();
  };

  // A control inside a row acts for itself; the row under it must not open as well.
  const own = {
    onClick: (event: MouseEvent) => event.stopPropagation(),
    onKeyDown: (event: KeyboardEvent) => event.stopPropagation(),
  };

  const instanceRow = (group: InstanceGroup, instance: ScriptInstance, index: number): SettingsTableRowModel => {
    const scoped = categoryScoped(instance.trigger);
    const everyCategory = t("next.postProcessing.everyCategory");
    const categoryText = instance.categories.join(", ");
    return {
      id: `i:${instance.id}`,
      searchText: `${instance.name} ${instance.script} ${categoryText} ${instance.scriptProblem ?? ""}`,
      cells: [
        <div key="name" className="flex min-w-0 flex-col gap-1">
          <span className="flex min-w-0 items-center gap-[10px]">
            <Square color={instance.scriptProblem ? WV.error : instance.enabled ? WV.accent : WV.inert} />
            <span className="min-w-0 truncate text-[13px]" title={instance.name}>
              {instance.name}
            </span>
          </span>
          {instance.scriptProblem ? (
            <span className="pl-[17px] text-[11.5px] leading-[1.4] text-wv-error-text">{instance.scriptProblem}</span>
          ) : null}
          {instance.headerDrift ? (
            <span className="pl-[17px] text-[11.5px] leading-[1.4] text-wv-warn">
              {t("next.postProcessing.headerDrift")}
            </span>
          ) : null}
        </div>,
        <Cell key="script" mono className="text-wv-secondary" title={instance.script}>
          {instance.script}
        </Cell>,
        scoped ? (
          <Cell
            key="categories"
            className={instance.categories.length > 0 ? "text-[12.5px] text-wv-secondary" : "text-[12.5px] text-wv-muted"}
            title={categoryText || everyCategory}
          >
            {categoryText || everyCategory}
          </Cell>
        ) : (
          <Cell key="categories" mono className="text-wv-faint">
            {EM_DASH}
          </Cell>
        ),
        <Cell key="runMode" className="text-[12.5px] text-wv-secondary">
          {t(instance.blocking ? "next.postProcessing.blocking" : "next.postProcessing.fireAndForget")}
        </Cell>,
        <Cell key="timeout" mono className="text-wv-muted">
          {instance.timeoutSeconds === null
            ? t("next.postProcessing.timeoutDefault")
            : formatTimeout(instance.timeoutSeconds)}
        </Cell>,
        <span key="enabled" {...own}>
          <Toggle
            size="table"
            checked={instance.enabled}
            label={t("next.postProcessing.enabledFor", { name: instance.name })}
            onChange={(next) => void toggle(instance, next)}
          />
        </span>,
        <span key="actions" className="ml-auto flex items-center gap-1" {...own}>
          {instance.headerDrift ? (
            <RowAction
              icon="reset"
              title={t("next.postProcessing.reapply")}
              onClick={() => ask({ kind: "reapply", instance })}
            />
          ) : null}
          <RowAction
            icon="moveUp"
            title={t("next.postProcessing.moveUp")}
            disabled={working || index === 0}
            onClick={() => void move(instance, -1)}
          />
          <RowAction
            icon="moveDown"
            title={t("next.postProcessing.moveDown")}
            disabled={working || index === group.instances.length - 1}
            onClick={() => void move(instance, 1)}
          />
          <RowAction
            icon="test"
            title={t("next.postProcessing.test")}
            onClick={() => {
              setError(null);
              setStatus(null);
              setTesting(instance);
            }}
          />
          <RowAction
            icon="remove"
            title={t("action.delete")}
            onClick={() => ask({ kind: "delete", instance })}
          />
        </span>,
      ],
    };
  };

  const groupNote = (group: InstanceGroup): string | undefined => {
    if (group.trigger === "SCHEDULER") {
      return t("next.postProcessing.scheduleGroupNote");
    }
    return group.trigger === "FEED" ? t("next.postProcessing.feedGroupNote") : undefined;
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "instances",
      title: t("next.postProcessing.instances"),
      // Which of the two holds is a setting, so the note says the one in force.
      note: t(
        data?.postProcessingSettings.globalScriptsRun === "ONLY_WITHOUT_CATEGORY_SCRIPTS"
          ? "next.postProcessing.instancesNoteOnlyWithout"
          : "next.postProcessing.instancesNote",
      ),
      columns: "minmax(150px, 1.3fr) minmax(100px, 1fr) minmax(84px, 0.9fr) 136px 76px 72px 172px",
      headers: [
        t("next.postProcessing.instanceName"),
        t("next.postProcessing.script"),
        t("next.postProcessing.categories"),
        t("next.postProcessing.runMode"),
        t("next.postProcessing.timeout"),
        t("next.postProcessing.enabled"),
        "",
      ],
      empty: t("next.postProcessing.noInstances"),
      emptyAction: { label: t("next.postProcessing.createInstance"), onClick: () => openEditor({ mode: "new" }) },
      onRowClick: (id) => {
        const instance = instances.find((entry) => `i:${entry.id}` === id);
        if (instance) {
          openEditor({ mode: "edit", instance });
        }
      },
      rows: [],
      groups: groups.map((group) => ({
        id: group.id,
        title: triggerTitle(t, group.trigger, group.queueEvent),
        note: groupNote(group),
        rows: group.instances.map((instance, index) => instanceRow(group, instance, index)),
      })),
    },
  ];

  const confirmBody = (text: string) => (
    <>
      {text}
      {confirmError ? (
        <span role="alert" className="mt-3 block text-[12.5px] text-wv-error-text">
          {confirmError}
        </span>
      ) : null}
    </>
  );

  return (
    <>
      <PanelControls>
        <SecondaryButton
          icon="refresh"
          onClick={() => {
            refetchInstances();
            void reloadScripts({ requestPolicy: "network-only" });
          }}
        >
          {t("action.refresh")}
        </SecondaryButton>
        <PrimaryButton icon="add" onClick={() => openEditor({ mode: "new" })}>
          {t("next.postProcessing.createInstance")}
        </PrimaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      {editor ? (
        <ScriptInstanceEditor
          key={editor.key}
          target={editor.target}
          scripts={scripts}
          problems={problems}
          instances={instances}
          categories={categories}
          onSaved={(saved, problem) => {
            setEditor(null);
            setError(problem ?? null);
            setStatus(saved);
            refetchInstances();
          }}
          onDismiss={() => setEditor(null)}
          onDelete={(instance) => ask({ kind: "delete", instance })}
          onReapply={(instance) => ask({ kind: "reapply", instance })}
          onSetUp={async (script) => {
            const problem = await setUp(script);
            if (problem === null) {
              setEditor(null);
            }
            return problem;
          }}
        />
      ) : null}

      <ConfirmDialog
        open={confirm?.kind === "delete"}
        title={t("next.postProcessing.deleteTitle")}
        note={confirm?.instance.name}
        busy={confirmBusy}
        confirmLabel={t("action.delete")}
        body={confirmBody(t("next.postProcessing.deleteBody"))}
        onConfirm={() => void confirmed()}
        onDismiss={() => setConfirm(null)}
      />
      <ConfirmDialog
        open={confirm?.kind === "reapply"}
        title={t("next.postProcessing.reapply")}
        note={confirm?.instance.name}
        busy={confirmBusy}
        destructive={false}
        confirmLabel={t("next.postProcessing.reapply")}
        body={confirmBody(t("next.postProcessing.reapplyBody"))}
        onConfirm={() => void confirmed()}
        onDismiss={() => setConfirm(null)}
      />

      {testing ? (
        <ScriptTestDialog key={testing.id} instance={testing} onClose={() => setTesting(null)} />
      ) : null}
    </>
  );
}
