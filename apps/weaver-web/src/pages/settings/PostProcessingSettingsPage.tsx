import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import {
  POST_PROCESSING_SETTINGS_QUERY,
  SET_POST_PROCESSING_SETTINGS_MUTATION,
  SET_SCRIPT_LISTS_MUTATION,
  SET_SCRIPT_OPTIONS_MUTATION,
} from "@/graphql/queries";

const GLOBAL_LIST = "__global__";
const MASKED_SECRET = "[REDACTED]";

type ScriptOption = {
  name: string;
  section?: string | null;
  optionType: "STRING" | "INTEGER" | "NUMBER" | "BOOLEAN" | "SECRET";
  displayName?: string | null;
  description: string[];
  select: string[];
  required: boolean;
  defaultValue?: string | null;
  value?: string | null;
};

type Script = {
  name: string;
  displayName: string;
  adapter: "SABNZBD" | "NZBGET";
  version?: string | null;
  options: ScriptOption[];
};

type ScriptProblem = { name: string; message: string };

type ScriptListEntry = {
  script: string;
  enabled: boolean;
  timeoutSeconds?: number | null;
};

type ScriptLists = {
  global: ScriptListEntry[];
  categories: { category: string; entries: ScriptListEntry[] }[];
};

type Settings = {
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  pythonInterpreter?: string | null;
  powershellInterpreter?: string | null;
  batchInterpreter?: string | null;
  strictSecurityRefusesExecution: boolean;
  lists: ScriptLists;
};

type QueryData = {
  postProcessingSettings: Settings;
  scripts: { scripts: Script[]; problems: ScriptProblem[] };
  categories: { id: number; name: string }[];
};

type SettingsDraft = {
  executionEnabled: boolean;
  concurrency: string;
  terminationGraceSeconds: string;
  pythonInterpreter: string;
  powershellInterpreter: string;
  batchInterpreter: string;
};

function settingsDraft(settings: Settings): SettingsDraft {
  return {
    executionEnabled: settings.executionEnabled,
    concurrency: String(settings.concurrency),
    terminationGraceSeconds: String(settings.terminationGraceSeconds),
    pythonInterpreter: settings.pythonInterpreter ?? "",
    powershellInterpreter: settings.powershellInterpreter ?? "",
    batchInterpreter: settings.batchInterpreter ?? "",
  };
}

/** Entries for `scope`, in the order they will run. */
function listFor(lists: ScriptLists, scope: string): ScriptListEntry[] {
  if (scope === GLOBAL_LIST) return lists.global;
  return lists.categories.find((entry) => entry.category === scope)?.entries ?? [];
}

function withList(lists: ScriptLists, scope: string, entries: ScriptListEntry[]): ScriptLists {
  if (scope === GLOBAL_LIST) return { ...lists, global: entries };
  const categories = lists.categories.filter((entry) => entry.category !== scope);
  if (entries.length > 0) categories.push({ category: scope, entries });
  categories.sort((left, right) => left.category.localeCompare(right.category));
  return { ...lists, categories };
}

function move<T>(items: T[], from: number, to: number): T[] {
  if (to < 0 || to >= items.length) return items;
  const next = [...items];
  const [entry] = next.splice(from, 1);
  next.splice(to, 0, entry);
  return next;
}

export function PostProcessingSettingsPage() {
  const [{ data, fetching, error }, refetch] = useQuery<QueryData>({
    query: POST_PROCESSING_SETTINGS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [, saveSettings] = useMutation(SET_POST_PROCESSING_SETTINGS_MUTATION);
  const [, saveLists] = useMutation(SET_SCRIPT_LISTS_MUTATION);
  const [, saveOptions] = useMutation(SET_SCRIPT_OPTIONS_MUTATION);

  const [notice, setNotice] = useState<string | null>(null);
  const [draft, setDraft] = useState<SettingsDraft | null>(null);
  const [lists, setLists] = useState<ScriptLists | null>(null);
  const [scope, setScope] = useState<string>(GLOBAL_LIST);
  const [optionsScript, setOptionsScript] = useState<string | null>(null);
  const [optionValues, setOptionValues] = useState<Record<string, string>>({});

  const settings = data?.postProcessingSettings;
  const scripts = useMemo(() => data?.scripts.scripts ?? [], [data?.scripts.scripts]);
  const problems = data?.scripts.problems ?? [];

  useEffect(() => {
    if (settings) {
      setDraft(settingsDraft(settings));
      setLists(settings.lists);
    }
  }, [settings]);

  const selected = useMemo(
    () => scripts.find((script) => script.name === optionsScript) ?? null,
    [scripts, optionsScript],
  );

  useEffect(() => {
    if (!selected) {
      setOptionValues({});
      return;
    }
    setOptionValues(
      Object.fromEntries(
        selected.options.map((option) => [
          option.name,
          option.value ?? option.defaultValue ?? "",
        ]),
      ),
    );
  }, [selected]);

  const entries = lists ? listFor(lists, scope) : [];
  const listed = new Set(entries.map((entry) => entry.script));
  const available = scripts.filter((script) => !listed.has(script.name));

  async function persistSettings(next: SettingsDraft) {
    const concurrency = Number.parseInt(next.concurrency, 10);
    const grace = Number.parseInt(next.terminationGraceSeconds, 10);
    if (!Number.isFinite(concurrency) || !Number.isFinite(grace)) {
      setNotice("Concurrency and termination grace must be numbers.");
      return;
    }
    const result = await saveSettings({
      input: {
        executionEnabled: next.executionEnabled,
        concurrency,
        terminationGraceSeconds: grace,
        pythonInterpreter: next.pythonInterpreter.trim() || null,
        powershellInterpreter: next.powershellInterpreter.trim() || null,
        batchInterpreter: next.batchInterpreter.trim() || null,
      },
    });
    setNotice(result.error ? result.error.message : "Post-processing settings saved.");
    if (!result.error) refetch({ requestPolicy: "network-only" });
  }

  async function persistLists(next: ScriptLists) {
    setLists(next);
    const result = await saveLists({
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
    });
    setNotice(result.error ? result.error.message : "Script list saved.");
    if (!result.error) refetch({ requestPolicy: "network-only" });
  }

  async function persistOptions() {
    if (!selected) return;
    const options = selected.options
      // A masked secret means "leave the stored value alone", so it is never
      // sent back as if it were the real one.
      .filter(
        (option) =>
          !(option.optionType === "SECRET" && optionValues[option.name] === MASKED_SECRET),
      )
      .map((option) => ({
        name: option.name,
        optionType: option.optionType,
        value: optionValues[option.name] ?? "",
      }));
    const result = await saveOptions({ script: selected.name, options });
    setNotice(
      result.error ? result.error.message : `Options for ${selected.displayName} saved.`,
    );
    if (!result.error) refetch({ requestPolicy: "network-only" });
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Post-processing"
        description="Run scripts from the data directory's scripts folder when a job finishes."
      />

      {error ? (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm">
          {error.message}
        </div>
      ) : null}
      {notice ? (
        <div className="rounded-md border border-border bg-muted/40 px-4 py-3 text-sm">
          {notice}
        </div>
      ) : null}

      <SectionCard
        title="Scripts"
        description="Every file or manifest package in the scripts folder is listed live. Enabling execution runs the enabled scripts below with Weaver's privileges."
      >
        <div className="space-y-6">
          {settings?.strictSecurityRefusesExecution ? (
            <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm">
              WEAVER_STRICT_SECURITY=1 refuses post-processing script execution.
            </div>
          ) : null}

          <div className="flex items-center justify-between gap-4">
            <div>
              <Label htmlFor="pp-execution">Run post-processing scripts</Label>
              <p className="text-sm text-muted-foreground">
                Off by default. Turning this on means files in the scripts folder run when a
                job finishes.
              </p>
            </div>
            <Switch
              id="pp-execution"
              aria-label="Run post-processing scripts"
              checked={draft?.executionEnabled ?? false}
              disabled={!draft || settings?.strictSecurityRefusesExecution}
              onCheckedChange={(checked) => {
                if (!draft) return;
                const next = { ...draft, executionEnabled: checked };
                setDraft(next);
                void persistSettings(next);
              }}
            />
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="pp-concurrency">Concurrent jobs (1-8)</Label>
              <Input
                id="pp-concurrency"
                value={draft?.concurrency ?? ""}
                onChange={(event) =>
                  setDraft((current) =>
                    current ? { ...current, concurrency: event.target.value } : current,
                  )
                }
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="pp-grace">Termination grace (seconds)</Label>
              <Input
                id="pp-grace"
                value={draft?.terminationGraceSeconds ?? ""}
                onChange={(event) =>
                  setDraft((current) =>
                    current
                      ? { ...current, terminationGraceSeconds: event.target.value }
                      : current,
                  )
                }
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="pp-python">Python interpreter</Label>
              <Input
                id="pp-python"
                placeholder="python3"
                value={draft?.pythonInterpreter ?? ""}
                onChange={(event) =>
                  setDraft((current) =>
                    current ? { ...current, pythonInterpreter: event.target.value } : current,
                  )
                }
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="pp-powershell">PowerShell interpreter</Label>
              <Input
                id="pp-powershell"
                placeholder="pwsh"
                value={draft?.powershellInterpreter ?? ""}
                onChange={(event) =>
                  setDraft((current) =>
                    current
                      ? { ...current, powershellInterpreter: event.target.value }
                      : current,
                  )
                }
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="pp-batch">Batch interpreter</Label>
              <Input
                id="pp-batch"
                placeholder="cmd.exe"
                value={draft?.batchInterpreter ?? ""}
                onChange={(event) =>
                  setDraft((current) =>
                    current ? { ...current, batchInterpreter: event.target.value } : current,
                  )
                }
              />
            </div>
          </div>
          <div>
            <Button
              disabled={!draft || fetching}
              onClick={() => draft && void persistSettings(draft)}
            >
              Save settings
            </Button>
          </div>

          <div className="space-y-3 border-t border-border pt-6">
            <div className="flex flex-wrap items-end justify-between gap-3">
              <div className="space-y-2">
                <Label htmlFor="pp-scope">Script list</Label>
                <Select value={scope} onValueChange={setScope}>
                  <SelectTrigger id="pp-scope" className="w-64" aria-label="Script list">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={GLOBAL_LIST}>Global default</SelectItem>
                    {(data?.categories ?? []).map((category) => (
                      <SelectItem key={category.id} value={category.name}>
                        Category: {category.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <p className="text-sm text-muted-foreground">
                {scope === GLOBAL_LIST
                  ? "Runs for every job without a category override."
                  : `Replaces the global default for ${scope}.`}
              </p>
            </div>

            <ul aria-label="Script list" className="space-y-2">
              {entries.length === 0 ? (
                <li className="text-sm text-muted-foreground">
                  No scripts in this list. Add one below.
                </li>
              ) : null}
              {entries.map((entry, index) => (
                <li
                  key={entry.script}
                  aria-label={`Script ${entry.script}`}
                  className="flex flex-wrap items-center gap-3 rounded-md border border-border px-3 py-2"
                >
                  <span className="font-mono text-sm">{entry.script}</span>
                  <Badge variant="secondary">
                    {scripts.find((script) => script.name === entry.script)?.adapter ??
                      "MISSING"}
                  </Badge>
                  <div className="flex items-center gap-2">
                    <Switch
                      aria-label={`Enable ${entry.script}`}
                      checked={entry.enabled}
                      onCheckedChange={(checked) => {
                        if (!lists) return;
                        const next = [...entries];
                        next[index] = { ...entry, enabled: checked };
                        void persistLists(withList(lists, scope, next));
                      }}
                    />
                    <span className="text-sm text-muted-foreground">Enabled</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Label htmlFor={`pp-timeout-${entry.script}`} className="text-sm">
                      Timeout (s)
                    </Label>
                    <Input
                      id={`pp-timeout-${entry.script}`}
                      className="w-28"
                      placeholder="24h"
                      value={entry.timeoutSeconds ? String(entry.timeoutSeconds) : ""}
                      onChange={(event) => {
                        if (!lists) return;
                        const parsed = Number.parseInt(event.target.value, 10);
                        const next = [...entries];
                        next[index] = {
                          ...entry,
                          timeoutSeconds: Number.isFinite(parsed) && parsed > 0 ? parsed : null,
                        };
                        setLists(withList(lists, scope, next));
                      }}
                      onBlur={() => lists && void persistLists(lists)}
                    />
                  </div>
                  <div className="ml-auto flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={index === 0}
                      onClick={() =>
                        lists && void persistLists(withList(lists, scope, move(entries, index, index - 1)))
                      }
                    >
                      Move up
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={index === entries.length - 1}
                      onClick={() =>
                        lists && void persistLists(withList(lists, scope, move(entries, index, index + 1)))
                      }
                    >
                      Move down
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        lists &&
                        void persistLists(
                          withList(
                            lists,
                            scope,
                            entries.filter((candidate) => candidate.script !== entry.script),
                          ),
                        )
                      }
                    >
                      Remove
                    </Button>
                  </div>
                </li>
              ))}
            </ul>

            <div className="flex flex-wrap items-center gap-2">
              {available.length === 0 ? (
                <span className="text-sm text-muted-foreground">
                  Every discovered script is already in this list.
                </span>
              ) : (
                available.map((script) => (
                  <Button
                    key={script.name}
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      lists &&
                      void persistLists(
                        withList(lists, scope, [
                          ...entries,
                          { script: script.name, enabled: true, timeoutSeconds: null },
                        ]),
                      )
                    }
                  >
                    Add {script.displayName}
                  </Button>
                ))
              )}
            </div>
          </div>

          <div className="space-y-3 border-t border-border pt-6">
            <div className="space-y-2">
              <Label htmlFor="pp-options-script">Script options</Label>
              <Select
                value={optionsScript ?? ""}
                onValueChange={(value) => setOptionsScript(value)}
              >
                <SelectTrigger
                  id="pp-options-script"
                  className="w-64"
                  aria-label="Script options"
                >
                  <SelectValue placeholder="Select a script" />
                </SelectTrigger>
                <SelectContent>
                  {scripts.map((script) => (
                    <SelectItem key={script.name} value={script.name}>
                      {script.displayName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {selected ? (
              selected.options.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  {selected.displayName} declares no options.
                </p>
              ) : (
                <div
                  role="group"
                  aria-label={`Options for ${selected.displayName}`}
                  className="space-y-4"
                >
                  {selected.options.map((option) => (
                    <div key={option.name} className="space-y-2">
                      <Label htmlFor={`pp-option-${option.name}`}>
                        {option.displayName ?? option.name}
                        {option.required ? " *" : ""}
                      </Label>
                      {option.description.length > 0 ? (
                        <p className="text-sm text-muted-foreground">
                          {option.description.join(" ")}
                        </p>
                      ) : null}
                      <Input
                        id={`pp-option-${option.name}`}
                        type={option.optionType === "SECRET" ? "password" : "text"}
                        value={optionValues[option.name] ?? ""}
                        onChange={(event) =>
                          setOptionValues((current) => ({
                            ...current,
                            [option.name]: event.target.value,
                          }))
                        }
                      />
                    </div>
                  ))}
                  <Button onClick={() => void persistOptions()}>Save options</Button>
                </div>
              )
            ) : null}
          </div>

          {problems.length > 0 ? (
            <div
              role="region"
              aria-label="Script problems"
              className="space-y-2 border-t border-border pt-6"
            >
              <h3 className="text-sm font-medium">Scripts that could not be read</h3>
              <ul className="space-y-1">
                {problems.map((problem) => (
                  <li key={problem.name} className="text-sm text-muted-foreground">
                    <span className="font-mono">{problem.name}</span>: {problem.message}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </div>
      </SectionCard>
    </div>
  );
}

export default PostProcessingSettingsPage;
