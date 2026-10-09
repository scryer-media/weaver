import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { nextEn } from "@/next/i18n/en";
import { JobScriptResults } from "@/next/components/JobScriptResults";
import { ScriptConfigurationPanel, ScriptListPanel } from "@/next/pages/settings/panels/PostProcessingPanel";
import { SettingsShellProvider, type PanelFlags } from "@/next/pages/settings/framework";
import "@/next/fonts.css";
import "@/next/theme.css";

const MASKED = "[REDACTED]";
const option = (name: string, optionType: string, extra: Record<string, unknown> = {}) => ({
  name, section: null, optionType, displayName: name, description: [], select: [], required: false,
  defaultValue: null, value: null, ...extra,
});
const scripts = [
  { name: "notify.py", displayName: "Notify", adapter: "NZBGET", version: "1.4", kinds: ["POST_PROCESSING", "QUEUE"],
    queueEvents: ["NZB_ADDED", "NZB_DOWNLOADED"], taskTimes: [], options: [
      option("Label", "STRING", { description: ["Shown in the notification title."], required: true, value: "fixture" }),
      option("Token", "SECRET", { description: ["The service's access token."], value: MASKED }),
      option("Mode", "STRING", { select: ["quiet", "verbose"], defaultValue: "quiet", value: "quiet" }),
      option("Attach", "BOOLEAN", { displayName: "Attach the log", value: "false" }),
    ] },
  { name: "cleanup.sh", displayName: "Cleanup", adapter: "SABNZBD", version: null, kinds: ["POST_PROCESSING"],
    queueEvents: [], taskTimes: [], options: [] },
  { name: "nightly.py", displayName: "Nightly report", adapter: "NZBGET", version: "0.3", kinds: ["SCHEDULER"],
    queueEvents: [], taskTimes: ["04:00", "*:20"], options: [] },
  { name: "intake.py", displayName: "Intake filter", adapter: "NZBGET", version: "2.0", kinds: ["SCAN", "FEED", "QUEUE"],
    queueEvents: [], taskTimes: [], options: [] },
];
const empty = location.search.includes("empty");
const state = {
  settings: {
    scriptDirectory: "/fixture/scripts", executionEnabled: true, concurrency: 2,
    eventScriptConcurrency: 1, eventScriptTimeoutSeconds: 300, fileDownloadedEventInterval: 0,
    scriptOutputCeilingBytes: 1048576, scriptOutputRunsPerJob: 32, scriptOutputRingBytes: 67108864,
    // A size set through the API need not be a whole number of the unit its field shows.
    scriptOutputRunCapBytes: location.search.includes("uneven") ? 2097000 : 2097152, terminationGraceSeconds: 10,
    pythonInterpreter: null as string | null, powershellInterpreter: null as string | null,
    batchInterpreter: null as string | null, unacceptableExtensions: ["exe", "scr"],
    strictSecurityRefusesExecution: false,
    lists: {
      global: empty ? [] : [
        { script: "notify.py", enabled: true, timeoutSeconds: null as number | null },
        { script: "cleanup.sh", enabled: false, timeoutSeconds: 600 },
        { script: "retired.py", enabled: true, timeoutSeconds: null },
      ],
      categories: [] as { category: string; entries: unknown[] }[],
    },
  },
  scripts: {
    scripts: empty ? [] : scripts,
    problems: empty ? [] : [{ name: "broken.py", message: "option 3 declares an unknown type" }],
  },
  categories: [{ id: 1, name: "movies" }, { id: 2, name: "tv" }],
};
const run = (script: string, event: string, status: string, outputTail: string, extra: Record<string, unknown> = {}) => ({
  script, event, adapter: "NZBGET", status, exitCode: 0, durationMs: 120, outputTail, outputId: null,
  outputRetained: false, outputTruncated: false, errorMessage: null, finishedAtEpochMs: 1767225600000, ...extra,
});
const results = empty ? [] : [
  run("notify.py", "queue:NZB_ADDED", "SUCCEEDED", "queued fixture job"),
  run("notify.py", "post_processing", "SUCCEEDED", `token=${MASKED}\nnotified 1 recipient`, { outputId: "run-1", outputRetained: true }),
  run("cleanup.sh", "post_processing", "WARNING", "removed 2 samples", { exitCode: 3, outputTruncated: true }),
  run("retired.py", "post_processing", "FAILED", "", { errorMessage: "script is no longer in the scripts directory" }),
];
function graphql(name: string, variables: Record<string, any>) {
  let mutation = {};
  if (name === "SetPostProcessingSettings") {
    Object.assign(state.settings, variables.input);
    mutation = { setPostProcessingSettings: state.settings };
  } else if (name === "SetPostProcessingScriptDirectory") {
    Object.assign(state.settings, { scriptDirectory: variables.directory, lists: { global: [], categories: [] } });
    mutation = { setPostProcessingScriptDirectory: state.settings };
  } else if (name === "SetScriptLists") {
    state.settings.lists = variables.input;
    mutation = { setScriptLists: state.settings.lists };
  } else if (name === "SetScriptOptions") {
    const script = state.scripts.scripts.find((entry) => entry.name === variables.script)!;
    for (const saved of variables.options) {
      const target = script.options.find((entry) => entry.name === saved.name)!;
      target.value = saved.optionType === "SECRET" ? MASKED : saved.value;
    }
    mutation = { setScriptOptions: script };
  }
  return { data: structuredClone({ ...state, postProcessingSettings: state.settings, postProcessingResults: results,
    scriptOutput: `token=${MASKED}\nresolved 1 recipient\nnotified 1 recipient`,
    browseDirectories: { currentPath: variables.path ?? state.settings.scriptDirectory, parentPath: "/fixture", entries: [] },
    ...mutation }) };
}
const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  const url = new URL(String(request), document.baseURI);
  if (url.pathname !== "/graphql") return originalFetch(request, init);
  const body = JSON.parse(String(init?.body));
  return Response.json(graphql(body.operationName, body.variables));
};
const dictionary = { ...en, ...nextEn };
// The screen under test: a job's script runs, the script list, or the configuration.
const screen = location.search.includes("job") ? <JobScriptResults jobId={1} />
  : location.search.includes("scripts") ? <ScriptListPanel /> : <ScriptConfigurationPanel />;
// The shell's status bar, reduced to the line a panel publishes into it.
const status = document.getElementById("status")!;
const actions = { current: null as { save: () => void; revert: () => void } | null };
const save = document.getElementById("save") as HTMLButtonElement;
save.addEventListener("click", () => actions.current?.save());
const shell = {
  search: new URLSearchParams(location.search).get("search") ?? "", actionsRef: actions,
  setFlags: (flags: PanelFlags) => { status.textContent = flags.status ?? ""; save.disabled = !flags.dirty || flags.busy; },
  controlsHost: document.getElementById("controls"),
};
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key), uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto flex max-w-[1400px] flex-col p-8">{screen}</main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);
