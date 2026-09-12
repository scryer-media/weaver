import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  POST_PROCESSING_SETTINGS_QUERY,
  SET_POST_PROCESSING_SCRIPT_DIRECTORY_MUTATION,
  SET_POST_PROCESSING_SETTINGS_MUTATION,
  SET_SCRIPT_LISTS_MUTATION,
  SET_SCRIPT_OPTIONS_MUTATION,
} from "@/graphql/queries";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { NumberField, SecondaryButton, Select, TextField, Toggle } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { WV } from "../../../data/palette";
import {
  PanelControls,
  SettingsBlocks,
  useDraft,
  usePanelState,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * Post-processing: the scripts weaver runs when a download finishes.
 *
 * Three things live here that save in three different ways, which is the
 * daemon's own shape rather than a choice: the execution settings are a draft
 * behind the top bar's Save, the scripts directory is destructive enough to
 * ask first, and the run list writes the moment it is reordered.
 */

const GLOBAL = "__global__";
const MASKED_SECRET = "[REDACTED]";

type OptionType = "STRING" | "INTEGER" | "NUMBER" | "BOOLEAN" | "SECRET";

interface ScriptOption {
  name: string;
  section?: string | null;
  optionType: OptionType;
  displayName?: string | null;
  description: string[];
  select: string[];
  required: boolean;
  defaultValue?: string | null;
  value?: string | null;
}

interface Script {
  name: string;
  displayName: string;
  adapter: "SABNZBD" | "NZBGET";
  version?: string | null;
  options: ScriptOption[];
}

interface ListEntry {
  script: string;
  enabled: boolean;
  timeoutSeconds?: number | null;
}

interface ScriptLists {
  global: ListEntry[];
  categories: { category: string; entries: ListEntry[] }[];
}

interface PostProcessingSettings {
  scriptDirectory: string;
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  pythonInterpreter?: string | null;
  powershellInterpreter?: string | null;
  batchInterpreter?: string | null;
  unacceptableExtensions: string[];
  strictSecurityRefusesExecution: boolean;
  lists: ScriptLists;
}

interface PostProcessingData {
  postProcessingSettings: PostProcessingSettings;
  scripts: { scripts: Script[]; problems: { name: string; message: string }[] };
  categories: { id: number; name: string }[];
}

interface ExecutionForm {
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  pythonInterpreter: string;
  powershellInterpreter: string;
  batchInterpreter: string;
  unacceptableExtensions: string;
}

const DEFAULTS: ExecutionForm = {
  executionEnabled: false,
  concurrency: 1,
  terminationGraceSeconds: 10,
  pythonInterpreter: "",
  powershellInterpreter: "",
  batchInterpreter: "",
  unacceptableExtensions: "",
};

/** The entries that run for one scope, in the order they will run. */
function listFor(lists: ScriptLists, scope: string): ListEntry[] {
  return scope === GLOBAL
    ? lists.global
    : (lists.categories.find((entry) => entry.category === scope)?.entries ?? []);
}

function withList(lists: ScriptLists, scope: string, entries: ListEntry[]): ScriptLists {
  if (scope === GLOBAL) {
    return { ...lists, global: entries };
  }
  const categories = lists.categories.filter((entry) => entry.category !== scope);
  if (entries.length > 0) {
    categories.push({ category: scope, entries });
  }
  categories.sort((left, right) => left.category.localeCompare(right.category));
  return { ...lists, categories };
}

function moved<T>(items: readonly T[], from: number, to: number): T[] {
  if (to < 0 || to >= items.length) {
    return [...items];
  }
  const next = [...items];
  const [entry] = next.splice(from, 1);
  if (entry !== undefined) {
    next.splice(to, 0, entry);
  }
  return next;
}

function splitExtensions(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export function PostProcessingPanel() {
  const [{ data }, reexecute] = useQuery<PostProcessingData>({
    query: POST_PROCESSING_SETTINGS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [saveState, saveSettings] = useMutation(SET_POST_PROCESSING_SETTINGS_MUTATION);
  const [directoryState, saveDirectory] = useMutation(
    SET_POST_PROCESSING_SCRIPT_DIRECTORY_MUTATION,
  );
  const [, saveLists] = useMutation(SET_SCRIPT_LISTS_MUTATION);
  const [, saveOptions] = useMutation(SET_SCRIPT_OPTIONS_MUTATION);

  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [scope, setScope] = useState<string>(GLOBAL);
  const [directory, setDirectory] = useState<string | null>(null);
  const [confirmDirectory, setConfirmDirectory] = useState(false);
  const [optionsScript, setOptionsScript] = useState<string | null>(null);
  const [optionValues, setOptionValues] = useState<Record<string, string>>({});
  const [optionsError, setOptionsError] = useState<string | null>(null);
  const [optionsBusy, setOptionsBusy] = useState(false);

  const settings = data?.postProcessingSettings;
  const scripts = useMemo(() => data?.scripts?.scripts ?? [], [data?.scripts?.scripts]);
  const problems = data?.scripts?.problems ?? [];
  const categories = useMemo(
    () => [...(data?.categories ?? [])].sort((left, right) => left.name.localeCompare(right.name)),
    [data?.categories],
  );

  const source = useMemo<ExecutionForm>(
    () =>
      settings
        ? {
            executionEnabled: settings.executionEnabled,
            concurrency: settings.concurrency,
            terminationGraceSeconds: settings.terminationGraceSeconds,
            pythonInterpreter: settings.pythonInterpreter ?? "",
            powershellInterpreter: settings.powershellInterpreter ?? "",
            batchInterpreter: settings.batchInterpreter ?? "",
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
          executionEnabled: values.executionEnabled,
          concurrency: Math.max(1, Math.round(values.concurrency || 1)),
          terminationGraceSeconds: Math.max(0, Math.round(values.terminationGraceSeconds || 0)),
          pythonInterpreter: values.pythonInterpreter.trim() || null,
          powershellInterpreter: values.powershellInterpreter.trim() || null,
          batchInterpreter: values.batchInterpreter.trim() || null,
          unacceptableExtensions: splitExtensions(values.unacceptableExtensions),
        },
      }).then((result) => {
        if (result.error) {
          setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
          return;
        }
        draft.markSaved();
        setStatus("Saved");
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const scriptDirectory = directory ?? settings?.scriptDirectory ?? "";
  const lists = settings?.lists ?? { global: [], categories: [] };
  const entries = listFor(lists, scope);
  const listed = new Set(entries.map((entry) => entry.script));
  const available = scripts.filter((script) => !listed.has(script.name));
  const byName = useMemo(
    () => new Map(scripts.map((script) => [script.name, script])),
    [scripts],
  );
  const selected = optionsScript ? (byName.get(optionsScript) ?? null) : null;

  const persistLists = (next: ScriptLists) => {
    setError(null);
    void saveLists({
      input: {
        global: next.global.map((entry) => ({
          script: entry.script,
          enabled: entry.enabled,
          timeoutSeconds: entry.timeoutSeconds ?? null,
        })),
        categories: next.categories.map((category) => ({
          category: category.category,
          entries: category.entries.map((entry) => ({
            script: entry.script,
            enabled: entry.enabled,
            timeoutSeconds: entry.timeoutSeconds ?? null,
          })),
        })),
      },
    }).then((result) => {
      if (result.error) {
        setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
        return;
      }
      setStatus("Run list saved");
      void reexecute({ requestPolicy: "network-only" });
    });
  };

  const patchEntries = (next: ListEntry[]) => persistLists(withList(lists, scope, next));

  const applyDirectory = () => {
    setConfirmDirectory(false);
    setError(null);
    void saveDirectory({ directory: scriptDirectory.trim() }).then((result) => {
      if (result.error) {
        setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
        return;
      }
      setDirectory(null);
      setScope(GLOBAL);
      setStatus("Scripts directory saved · assignments and saved options were cleared");
      void reexecute({ requestPolicy: "network-only" });
    });
  };

  const openOptions = (script: Script) => {
    setOptionsError(null);
    setOptionValues(
      Object.fromEntries(
        script.options.map((option) => [option.name, option.value ?? option.defaultValue ?? ""]),
      ),
    );
    setOptionsScript(script.name);
  };

  const persistOptions = async () => {
    if (!selected) {
      return;
    }
    setOptionsBusy(true);
    const result = await saveOptions({
      script: selected.name,
      options: selected.options
        // A masked secret means "keep what is stored", so it is never sent back.
        .filter(
          (option) =>
            !(option.optionType === "SECRET" && optionValues[option.name] === MASKED_SECRET),
        )
        .map((option) => ({
          name: option.name,
          optionType: option.optionType,
          value: optionValues[option.name] ?? "",
        })),
    });
    setOptionsBusy(false);
    if (result.error) {
      setOptionsError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setOptionsScript(null);
    setStatus(`Options for ${selected.displayName} saved`);
    void reexecute({ requestPolicy: "network-only" });
  };

  const optionField = (option: ScriptOption): FieldSpec => {
    const label = option.displayName || option.name;
    const value = optionValues[option.name] ?? "";
    const onChange = (next: string) =>
      setOptionValues((current) => ({ ...current, [option.name]: next }));
    const help = [
      ...option.description,
      option.required ? "Required." : "",
      option.defaultValue ? `Default ${option.defaultValue}.` : "",
    ]
      .filter(Boolean)
      .join(" ");

    if (option.select.length > 0) {
      return {
        id: option.name,
        label,
        help: help || undefined,
        control: {
          kind: "select",
          value,
          options: option.select.map((entry) => ({ value: entry, label: entry })),
          onChange,
        },
      };
    }
    if (option.optionType === "BOOLEAN") {
      return {
        id: option.name,
        label,
        help: help || undefined,
        control: {
          kind: "toggle",
          value: value === "true",
          onChange: (next: boolean) => onChange(next ? "true" : "false"),
        },
      };
    }
    return {
      id: option.name,
      label,
      help: help || undefined,
      control: {
        kind: "text",
        type: option.optionType === "SECRET" ? "password" : "text",
        value,
        onChange,
      },
    };
  };

  const scopeLabel = scope === GLOBAL ? "every download" : `the ${scope} category`;

  const blocks: (SettingsBlock | null)[] = [
    {
      kind: "section",
      id: "execution",
      title: "Execution",
      note: settings?.strictSecurityRefusesExecution ? "strict security is on" : undefined,
      fields: [
        {
          id: "executionEnabled",
          label: "Run scripts",
          help: "With this off, weaver discovers scripts but never executes one.",
          control: {
            kind: "toggle",
            value: values.executionEnabled,
            onChange: (next) => patch({ executionEnabled: next }),
          },
        },
        {
          id: "concurrency",
          label: "Concurrent scripts",
          help: "How many downloads may be in post-processing at once.",
          control: {
            kind: "number",
            value: values.concurrency,
            min: 1,
            max: 32,
            onChange: (next) => patch({ concurrency: next }),
          },
        },
        {
          id: "terminationGraceSeconds",
          label: "Termination grace",
          help: "How long a script has to exit after it is asked to stop.",
          control: {
            kind: "number",
            value: values.terminationGraceSeconds,
            min: 0,
            max: 600,
            suffix: "seconds",
            onChange: (next) => patch({ terminationGraceSeconds: next }),
          },
        },
        {
          id: "unacceptableExtensions",
          label: "Unacceptable extensions",
          help: "Comma-separated. A finished download holding one of these is refused.",
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
          label: "Strict security",
          help: "Set by the daemon's own configuration; when on, anything unsigned is refused.",
          control: {
            kind: "static",
            value: settings?.strictSecurityRefusesExecution ? "refuses execution" : "permissive",
          },
        },
      ],
    },
    {
      kind: "section",
      id: "interpreters",
      title: "Interpreters",
      note: "blank uses whatever is on the daemon's PATH",
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
      ],
    },
    {
      kind: "section",
      id: "directory",
      title: "Scripts directory",
      note: "read from the daemon's own filesystem",
      fields: [
        {
          id: "scriptDirectory",
          label: "Directory",
          help: "Scripts are discovered here. Weaver never uploads, edits or deletes them.",
          keywords: scriptDirectory,
          control: {
            kind: "custom",
            control: (
              <div className="flex min-w-0 flex-wrap items-center gap-3">
                <TextField
                  label="Scripts directory"
                  value={scriptDirectory}
                  className="w-[268px] max-w-full"
                  onChange={setDirectory}
                />
                <SecondaryButton
                  disabled={
                    directoryState.fetching
                    || scriptDirectory.trim() === (settings?.scriptDirectory ?? "")
                  }
                  onClick={() => setConfirmDirectory(true)}
                >
                  Change
                </SecondaryButton>
              </div>
            ),
          },
        },
      ],
    },
    {
      kind: "table",
      id: "run-list",
      title: "Run list",
      note: `runs for ${scopeLabel}`,
      columns: "44px minmax(0, 1fr) 120px 92px 150px",
      headers: ["Order", "Script", "Timeout", "Enabled", ""],
      empty:
        scripts.length === 0
          ? "No scripts were found in the scripts directory."
          : "Nothing runs for this scope yet.",
      footer:
        available.length > 0 ? (
          <Select
            label="Add a script to the run list"
            value=""
            className="min-w-0 sm:min-w-[240px]"
            options={[
              { value: "", label: "Add a script…" },
              ...available.map((script) => ({ value: script.name, label: script.displayName })),
            ]}
            onChange={(next) => {
              if (next) {
                patchEntries([...entries, { script: next, enabled: true, timeoutSeconds: null }]);
              }
            }}
          />
        ) : undefined,
      rows: entries.map((entry, index) => {
        const script = byName.get(entry.script);
        return {
          id: entry.script,
          searchText: `${entry.script} ${script?.displayName ?? ""}`,
          cells: [
            <Cell key="order" mono className="text-wv-faint">
              {index + 1}
            </Cell>,
            <span key="script" className="flex min-w-0 items-center gap-[10px]">
              <Square color={script ? (entry.enabled ? WV.accent : WV.inert) : WV.error} />
              <span className="min-w-0 truncate" title={entry.script}>
                {script?.displayName ?? entry.script}
              </span>
              {script ? null : (
                <span className="flex-none font-wv-mono text-[11px] text-wv-error-text">
                  missing
                </span>
              )}
            </span>,
            <NumberField
              key="timeout"
              label={`Timeout for ${entry.script}`}
              value={entry.timeoutSeconds ?? 0}
              min={0}
              max={86400}
              className="h-7 w-[92px]"
              onChange={(next) => {
                const updated = [...entries];
                updated[index] = { ...entry, timeoutSeconds: next > 0 ? next : null };
                patchEntries(updated);
              }}
            />,
            <Toggle
              key="enabled"
              size="table"
              label={`Run ${entry.script}`}
              checked={entry.enabled}
              onChange={(next) => {
                const updated = [...entries];
                updated[index] = { ...entry, enabled: next };
                patchEntries(updated);
              }}
            />,
            <span key="order-controls" className="flex items-center gap-[6px]">
              <SecondaryButton
                className="h-7 px-2"
                title="Move up"
                disabled={index === 0}
                onClick={() => patchEntries(moved(entries, index, index - 1))}
              >
                &#8593;
              </SecondaryButton>
              <SecondaryButton
                className="h-7 px-2"
                title="Move down"
                disabled={index === entries.length - 1}
                onClick={() => patchEntries(moved(entries, index, index + 1))}
              >
                &#8595;
              </SecondaryButton>
              <SecondaryButton
                className="h-7 px-2"
                onClick={() =>
                  patchEntries(entries.filter((candidate) => candidate.script !== entry.script))
                }
              >
                Remove
              </SecondaryButton>
            </span>,
          ],
        };
      }),
    },
    {
      kind: "table",
      id: "scripts",
      title: "Discovered scripts",
      note: scriptDirectory || undefined,
      columns: "minmax(0, 1fr) 110px 110px 110px",
      headers: ["Script", "Adapter", "Version", "Options"],
      empty: "Nothing here. Put a script in the scripts directory, then reload.",
      onRowClick: (id) => {
        const script = byName.get(id);
        if (script) {
          openOptions(script);
        }
      },
      rows: scripts.map((script) => ({
        id: script.name,
        searchText: `${script.name} ${script.displayName} ${script.adapter}`,
        cells: [
          <Cell key="name" className="text-wv-fg" title={script.name}>
            {script.displayName}
          </Cell>,
          <Cell key="adapter" mono className="text-wv-secondary">
            {script.adapter === "SABNZBD" ? "SABnzbd" : "NZBGet"}
          </Cell>,
          <Cell key="version" mono className="text-wv-muted">
            {script.version || "—"}
          </Cell>,
          <Cell key="options" mono className="text-wv-muted">
            {script.options.length === 0 ? "none" : script.options.length}
          </Cell>,
        ],
      })),
    },
    problems.length > 0
      ? {
          kind: "custom",
          id: "problems",
          title: "Scripts that could not be read",
          note: `${problems.length}`,
          searchText: problems.map((problem) => `${problem.name} ${problem.message}`).join(" "),
          body: (
            <div className="flex flex-col">
              {problems.map((problem) => (
                <div
                  key={problem.name}
                  className="flex flex-wrap items-baseline gap-x-5 gap-y-1 border-b border-wv-hairline px-4 sm:px-6 py-3"
                >
                  <span className="font-wv-mono text-[12.5px] text-wv-fg">{problem.name}</span>
                  <span className="min-w-0 flex-1 text-[12.5px] text-wv-error-text">
                    {problem.message}
                  </span>
                </div>
              ))}
            </div>
          ),
        }
      : null,
  ];

  return (
    <>
      <PanelControls>
        <Select
          label="Which downloads this run list applies to"
          value={scope}
          className="min-w-0 sm:min-w-[180px]"
          options={[
            { value: GLOBAL, label: "Every download" },
            ...categories.map((category) => ({ value: category.name, label: category.name })),
          ]}
          onChange={setScope}
        />
      </PanelControls>

      <SettingsBlocks blocks={blocks} />

      <ConfirmDialog
        open={confirmDirectory}
        title="Change scripts directory"
        note={scriptDirectory}
        busy={directoryState.fetching}
        confirmLabel="Change directory"
        body="Every script assignment and every saved script option is cleared. No file on disk is touched."
        onConfirm={applyDirectory}
        onDismiss={() => setConfirmDirectory(false)}
      />

      <RecordEditor
        open={selected !== null}
        title={selected?.displayName ?? "Script"}
        note={selected?.name}
        width={620}
        error={optionsError}
        busy={optionsBusy}
        saveLabel="Save options"
        saveDisabled={(selected?.options.length ?? 0) === 0}
        sections={
          selected
            ? [{ id: "options", title: "Options", fields: selected.options.map(optionField) }]
            : []
        }
        onSave={() => void persistOptions()}
        onDismiss={() => {
          setOptionsError(null);
          setOptionsScript(null);
        }}
      >
        {selected && selected.options.length === 0 ? (
          <div className="flex-none px-4 sm:px-6 py-5 text-[13px] text-wv-muted">
            {selected.displayName} declares no options.
          </div>
        ) : null}
      </RecordEditor>
    </>
  );
}
