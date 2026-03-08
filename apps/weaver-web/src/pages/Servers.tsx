import { useState } from "react";
import { useQuery, useMutation } from "urql";
import {
  SERVERS_QUERY,
  ADD_SERVER_MUTATION,
  UPDATE_SERVER_MUTATION,
  REMOVE_SERVER_MUTATION,
  TEST_CONNECTION_MUTATION,
} from "@/graphql/queries";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { useTranslate } from "@/lib/context/translate-context";

interface Server {
  id: number;
  host: string;
  port: number;
  tls: boolean;
  username: string | null;
  connections: number;
  active: boolean;
  supportsPipelining: boolean;
}

interface ServerFormData {
  host: string;
  port: number;
  tls: boolean;
  username: string;
  password: string;
  connections: number;
  active: boolean;
}

const defaultForm: ServerFormData = {
  host: "",
  port: 563,
  tls: true,
  username: "",
  password: "",
  connections: 20,
  active: true,
};

export function Servers() {
  const t = useTranslate();
  const [{ data }, reexecute] = useQuery({ query: SERVERS_QUERY });
  const [, addServer] = useMutation(ADD_SERVER_MUTATION);
  const [, updateServer] = useMutation(UPDATE_SERVER_MUTATION);
  const [, removeServer] = useMutation(REMOVE_SERVER_MUTATION);
  const [, testConnection] = useMutation(TEST_CONNECTION_MUTATION);

  const [editingId, setEditingId] = useState<number | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState<ServerFormData>(defaultForm);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string; latencyMs?: number; supportsPipelining?: boolean } | null>(null);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);

  const servers: Server[] = data?.servers ?? [];

  const openAdd = () => {
    setEditingId(null);
    setForm(defaultForm);
    setTestResult(null);
    setShowForm(true);
  };

  const openEdit = (server: Server) => {
    setEditingId(server.id);
    setForm({
      host: server.host,
      port: server.port,
      tls: server.tls,
      username: server.username ?? "",
      password: "",
      connections: server.connections,
      active: server.active,
    });
    setTestResult(null);
    setShowForm(true);
  };

  const closeForm = () => {
    setShowForm(false);
    setEditingId(null);
    setTestResult(null);
  };

  const handleSubmit = async () => {
    const input = {
      host: form.host,
      port: form.port,
      tls: form.tls,
      username: form.username || null,
      password: form.password || null,
      connections: form.connections,
      active: form.active,
    };

    if (editingId != null) {
      await updateServer({ id: editingId, input });
    } else {
      await addServer({ input });
    }
    reexecute();
    closeForm();
  };

  const handleDelete = async (id: number) => {
    await removeServer({ id });
    reexecute();
  };

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    const result = await testConnection({
      input: {
        host: form.host,
        port: form.port,
        tls: form.tls,
        username: form.username || null,
        password: form.password || null,
        connections: form.connections,
        active: form.active,
      },
    });
    setTesting(false);
    if (result.data?.testConnection) {
      setTestResult(result.data.testConnection);
    }
  };

  return (
    <div className="p-4 sm:p-6">
      <div className="mb-4 flex items-center justify-between sm:mb-6">
        <h1 className="text-xl font-bold text-foreground sm:text-2xl">{t("servers.title")}</h1>
        <button
          onClick={openAdd}
          className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 sm:px-4"
        >
          {t("servers.addServer")}
        </button>
      </div>

      <div className="max-w-3xl space-y-4">
        {servers.length === 0 && !showForm && (
          <div className="rounded-lg border border-border bg-card p-6 text-center sm:p-8">
            <p className="text-muted-foreground">{t("servers.empty")}</p>
            <p className="mt-1 text-sm text-muted-foreground">{t("servers.emptyHint")}</p>
          </div>
        )}

        {servers.map((server) => (
          <div
            key={server.id}
            className="flex flex-col gap-3 rounded-lg border border-border bg-card p-4 sm:flex-row sm:items-center sm:justify-between"
          >
            <div className="flex items-center gap-3 sm:gap-4">
              <div
                className={`h-2.5 w-2.5 shrink-0 rounded-full ${server.active ? "bg-green-500" : "bg-muted-foreground/40"}`}
                title={server.active ? "Active" : "Inactive"}
              />
              <div className="min-w-0">
                <div className="truncate font-medium text-card-foreground">
                  {server.host}:{server.port}
                </div>
                <div className="text-xs text-muted-foreground">
                  {server.connections} conn{server.connections !== 1 ? "s" : ""}
                  {server.tls && " \u00b7 TLS"}
                  {server.supportsPipelining && " \u00b7 Pipelining"}
                  {server.username && ` \u00b7 ${server.username}`}
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => openEdit(server)}
                className="rounded-md px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
              >
                Edit
              </button>
              <button
                onClick={() => setDeleteConfirmId(server.id)}
                className="rounded-md px-3 py-1.5 text-xs font-medium text-destructive transition-colors hover:bg-destructive/10"
              >
                {t("action.delete")}
              </button>
            </div>
          </div>
        ))}

        {showForm && (
          <div className="rounded-lg border border-border bg-card p-4 sm:p-5">
            <h2 className="mb-4 text-lg font-semibold text-card-foreground">
              {editingId != null ? t("servers.editServer") : t("servers.addServer")}
            </h2>

            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <div className="sm:col-span-1">
                <label className="mb-1 block text-sm text-muted-foreground">{t("servers.host")}</label>
                <input
                  type="text"
                  value={form.host}
                  onChange={(e) => setForm({ ...form, host: e.target.value })}
                  placeholder="news.example.com"
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("servers.port")}</label>
                <input
                  type="number"
                  value={form.port}
                  onChange={(e) => setForm({ ...form, port: Number(e.target.value) })}
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("servers.username")}</label>
                <input
                  type="text"
                  value={form.username}
                  onChange={(e) => setForm({ ...form, username: e.target.value })}
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("servers.password")}</label>
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  placeholder={editingId != null ? "Leave blank to keep" : ""}
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
              <div>
                <label className="mb-1 block text-sm text-muted-foreground">{t("servers.connections")}</label>
                <input
                  type="number"
                  min={1}
                  max={50}
                  value={form.connections}
                  onChange={(e) => setForm({ ...form, connections: Number(e.target.value) })}
                  className="w-full rounded-md border border-input bg-field px-3 py-2 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
                />
              </div>
              <div className="flex items-end gap-6">
                <label className="flex items-center gap-2 text-sm text-foreground">
                  <input
                    type="checkbox"
                    checked={form.tls}
                    onChange={(e) => setForm({ ...form, tls: e.target.checked })}
                    className="accent-primary"
                  />
                  {t("servers.tls")}
                </label>
                <label className="flex items-center gap-2 text-sm text-foreground">
                  <input
                    type="checkbox"
                    checked={form.active}
                    onChange={(e) => setForm({ ...form, active: e.target.checked })}
                    className="accent-primary"
                  />
                  {t("servers.active")}
                </label>
              </div>
            </div>

            {testResult && (
              <div
                className={`mt-4 rounded-md px-3 py-2 text-sm ${
                  testResult.success
                    ? "bg-green-500/10 text-green-400"
                    : "bg-destructive/10 text-destructive"
                }`}
              >
                {testResult.success
                  ? `${t("servers.testSuccess")} (${testResult.latencyMs}ms${testResult.supportsPipelining ? ", pipelining supported" : ""})`
                  : `${t("servers.testFailed")}: ${testResult.message}`}
              </div>
            )}

            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button
                onClick={handleSubmit}
                disabled={!form.host}
                className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
              >
                {editingId != null ? t("settings.save") : t("servers.addServer")}
              </button>
              <button
                onClick={handleTest}
                disabled={!form.host || testing}
                className="rounded-md border border-border px-4 py-2 text-sm font-medium text-foreground hover:bg-accent disabled:opacity-50"
              >
                {testing ? t("servers.testing") : t("servers.testConnection")}
              </button>
              <button
                onClick={closeForm}
                className="rounded-md px-4 py-2 text-sm text-muted-foreground hover:text-foreground"
              >
                {t("action.cancel")}
              </button>
            </div>
          </div>
        )}
      </div>

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteServer")}
        message={t("confirm.deleteServerMessage")}
        confirmLabel={t("action.delete")}
        onConfirm={() => {
          if (deleteConfirmId != null) handleDelete(deleteConfirmId);
          setDeleteConfirmId(null);
        }}
        onCancel={() => setDeleteConfirmId(null)}
      />
    </div>
  );
}
