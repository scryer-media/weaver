import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, Provider, fetchExchange } from "urql";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import { nextEn } from "@/next/i18n/en";
import { BackupPanel } from "@/next/pages/settings/panels/BackupPanel";
import { BackupSettingsPage } from "@/pages/settings/BackupSettingsPage";
import { SchedulesPanel } from "@/next/pages/settings/panels/SchedulesPanel";
import { SettingsShellProvider } from "@/next/pages/settings/framework";
import "@/fonts.css";
import "@/globals.css";

const state = {
  schedules: [] as Record<string, unknown>[],
  backups: [] as Record<string, unknown>[],
  backupSettings: { customBackupPath: null as string | null, backupPath: "/fixture/backups" },
  autoBackupSettings: { enabled: location.search.includes("automatic"), dailyTimeLocal: "03:00", autoBackupKeyPresent: location.search.includes("automatic"), nextRunAt: (location.search.includes("automatic") ? "2026-01-03T03:00:00Z" : null) as string | null },
};
let sequence = 0;
let passwordVerified = !location.search.includes("expired");
let verificationRequests = 0;
let acceptedBackupMutations = 0;
let acceptedBackupCreates = 0;
const track = (action: string) => ({ pause_all: "DOWNLOADS", pause_post_processing: "POST_PROCESSING", resume_post_processing: "POST_PROCESSING", set_server_active: "SERVER", set_quota_metering: "QUOTA", scan_watch_folder: "ONE_SHOT", fetch_rss: "ONE_SHOT", prune_history: "ONE_SHOT" })[action] ?? "DOWNLOADS";
function graphql(name: string, variables: Record<string, any>) {
  let mutation = {};
  if (["UpdateBackupSettings", "UpdateAutoBackupSettings", "DeleteBackup", "BackupDownloadToken"].includes(name)) {
    if (!passwordVerified) return { errors: [{ message: "recent password verification required", extensions: { code: "REAUTH_REQUIRED" } }] };
    acceptedBackupMutations += 1;
  }
  if (name === "CreateSchedule" || name === "UpdateSchedule") {
    const input = variables.input;
    if (input.actionType === "set_server_active" && (input.serverId == null || input.serverActive == null)) {
      return { errors: [{ message: "set_server_active requires serverId and serverActive" }] };
    }
    if (input.actionType === "set_quota_metering" && input.quotaMeteringEnabled == null) {
      return { errors: [{ message: "set_quota_metering requires quotaMeteringEnabled" }] };
    }
  }
  if (name === "CreateSchedule") {
    state.schedules.push({ id: String(++sequence), ...variables.input, days: variables.input.days ?? [], times: variables.input.times ?? [], track: track(variables.input.actionType) });
    mutation = { createSchedule: state.schedules };
  } else if (name === "UpdateSchedule") {
    state.schedules = state.schedules.map((rule) => rule.id === variables.id ? { ...rule, ...variables.input, days: variables.input.days ?? [], times: variables.input.times ?? [], track: track(variables.input.actionType) } : rule);
    mutation = { updateSchedule: state.schedules };
  } else if (name === "ToggleSchedule") {
    state.schedules = state.schedules.map((rule) => rule.id === variables.id ? { ...rule, enabled: variables.enabled } : rule);
    mutation = { toggleSchedule: state.schedules };
  } else if (name === "DeleteSchedule") {
    state.schedules = state.schedules.filter((rule) => rule.id !== variables.id); mutation = { deleteSchedule: state.schedules };
  } else if (name === "UpdateBackupSettings") {
    state.backupSettings = { customBackupPath: variables.path, backupPath: variables.path || "/fixture/backups" };
    mutation = { updateBackupSettings: state.backupSettings };
  } else if (name === "UpdateAutoBackupSettings") {
    const input = variables.input;
    if ((input.enabled && !input.setAutoBackupKey && !state.autoBackupSettings.autoBackupKeyPresent) || (input.clearAutoBackupKey && input.enabled)) return { errors: [{ message: "automatic backup key is required while enabled" }] };
    state.autoBackupSettings = { enabled: input.enabled, dailyTimeLocal: input.dailyTimeLocal, autoBackupKeyPresent: input.clearAutoBackupKey ? false : !!input.setAutoBackupKey || state.autoBackupSettings.autoBackupKeyPresent, nextRunAt: input.enabled ? "2026-01-03T03:00:00Z" : null };
    mutation = { updateAutoBackupSettings: state.autoBackupSettings };
  } else if (name === "DeleteBackup") {
    state.backups = state.backups.filter((row) => row.filename !== variables.filename); mutation = { deleteBackup: true };
  } else if (name === "BackupDownloadToken") mutation = { createBackupDownloadToken: "fixture-token" };
  return { data: structuredClone({ ...state, settings: { dataDir: "/fixture" }, servers: [{ id: 1, host: "fixture-provider" }], rssFeeds: [{ id: 1, name: "Fixture feed" }], hardwareProfile: { available: ["balanced"], current: "balanced" }, ...mutation }) };
}
const client = new Client({ url: "/graphql", exchanges: [fetchExchange], preferGetMethod: false });
const originalFetch = window.fetch.bind(window);
window.fetch = async (request, init) => {
  const url = new URL(String(request), document.baseURI);
  if (url.pathname === "/api/auth/csrf") return Response.json({ csrfToken: "fixture-csrf" });
  if (url.pathname === "/api/auth/verify") {
    verificationRequests += 1;
    if (new Headers(init?.headers).get("X-Weaver-Csrf") !== "fixture-csrf") return Response.json({ error: "browser verification required" }, { status: 403 });
    if (JSON.parse(String(init?.body)).password !== "fixture account password") return Response.json({ error: "invalid password" }, { status: 401 });
    passwordVerified = true;
    return Response.json({ ok: true });
  }
  if (url.pathname === "/fixture/auth-state") return Response.json({ verificationRequests, acceptedBackupMutations, acceptedBackupCreates });
  if (url.pathname === "/fixture/complete-automatic-backup") {
    state.backups.push({ filename: "weaver_backup_automatic_fixture.enc", sizeBytes: 2048, createdAt: "2026-01-03T03:00:00Z", sourceWeaverVersion: "0.14.2", trigger: "AUTO", status: "READY", error: null });
    state.autoBackupSettings.nextRunAt = "2026-01-04T03:00:00Z";
    return Response.json({ ok: true });
  }
  if (url.pathname === "/graphql") {
    const body = JSON.parse(String(init?.body));
    return Response.json(graphql(body.operationName, body.variables));
  }
  if (url.pathname === "/api/backup/status") return Response.json({ can_restore: true, busy: false });
  if (url.pathname === "/api/backup/create" || url.pathname === "/api/backup/export") {
    if (url.pathname.endsWith("create")) {
      if (!passwordVerified) return Response.json({ error: "recent password verification required", code: "REAUTH_REQUIRED" }, { status: 428 });
      acceptedBackupCreates += 1;
    }
    const info = { filename: `weaver_backup_fixture_${++sequence}.enc`, sizeBytes: 1024, createdAt: "2026-01-02T03:00:00Z", sourceWeaverVersion: "0.14.2", trigger: "MANUAL", status: "READY", error: null };
    if (url.pathname.endsWith("create")) {
      state.backups.push(info);
      return Response.json({ ...info, status: "Creating" }, { status: 202 });
    }
    return new Response("fixture archive", { headers: { "Content-Disposition": `attachment; filename="${info.filename}"` } });
  }
  if (url.pathname.startsWith("/api/backup/download/")) {
    if (url.searchParams.has("token") || new Headers(init?.headers).get("X-Weaver-Backup-Token") !== "fixture-token") return new Response("invalid token transport", { status: 403 });
    return new Response("fixture archive", { headers: { "Content-Disposition": `attachment; filename="${url.pathname.split("/").pop()}"` } });
  }
  return originalFetch(request, init);
};
const dictionary = { ...en, ...nextEn };
const shell = { search: "", actionsRef: { current: null }, setFlags: () => {}, controlsHost: document.getElementById("controls") };
createRoot(document.getElementById("root")!).render(<StrictMode><Provider value={client}><TranslateContext.Provider value={{ t: (key, values) => Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key), uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {} }}><MemoryRouter><SettingsShellProvider {...shell}><main className="mx-auto max-w-[1400px] p-8">{location.search.includes("schedules") ? <SchedulesPanel /> : location.search.includes("classic") ? <BackupSettingsPage /> : <BackupPanel />}</main></SettingsShellProvider></MemoryRouter></TranslateContext.Provider></Provider></StrictMode>);
