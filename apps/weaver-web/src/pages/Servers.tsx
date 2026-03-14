import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ADD_SERVER_MUTATION,
  REMOVE_SERVER_MUTATION,
  SERVERS_QUERY,
  TEST_CONNECTION_MUTATION,
  UPDATE_SERVER_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type Server = {
  id: number;
  host: string;
  port: number;
  tls: boolean;
  username: string | null;
  connections: number;
  active: boolean;
  supportsPipelining: boolean;
  priority: number;
};

type ServerFormValues = {
  host: string;
  port: number;
  tls: boolean;
  username: string;
  password: string;
  connections: number;
  active: boolean;
  priority: number;
};

const defaultForm: ServerFormValues = {
  host: "",
  port: 563,
  tls: true,
  username: "",
  password: "",
  connections: 20,
  active: true,
  priority: 0,
};

export function Servers({ embedded = false }: { embedded?: boolean }) {
  const t = useTranslate();
  const [{ data }] = useQuery({ query: SERVERS_QUERY });
  const [, addServer] = useMutation(ADD_SERVER_MUTATION);
  const [, updateServer] = useMutation(UPDATE_SERVER_MUTATION);
  const [, removeServer] = useMutation(REMOVE_SERVER_MUTATION);
  const [, testConnection] = useMutation(TEST_CONNECTION_MUTATION);

  const [servers, setServers] = useState<Server[]>([]);
  const [editingServer, setEditingServer] = useState<Server | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
    latencyMs?: number;
    supportsPipelining?: boolean;
  } | null>(null);

  useEffect(() => {
    if (data?.servers) {
      setServers(data.servers);
    }
  }, [data?.servers]);

  const groupedServers = useMemo(() => {
    const groups = new Map<number, Server[]>();
    for (const server of servers) {
      const group = groups.get(server.priority) ?? [];
      group.push(server);
      groups.set(server.priority, group);
    }
    return [...groups.entries()].sort(([left], [right]) => left - right);
  }, [servers]);

  const openAdd = () => {
    setEditingServer(null);
    setTestResult(null);
    setShowForm(true);
  };

  const openEdit = (server: Server) => {
    setEditingServer(server);
    setTestResult(null);
    setShowForm(true);
  };

  const closeForm = () => {
    setEditingServer(null);
    setTestResult(null);
    setShowForm(false);
  };

  const handleSave = async (values: ServerFormValues) => {
    const input = {
      host: values.host.trim(),
      port: values.port,
      tls: values.tls,
      username: values.username.trim() || null,
      password: values.password.trim() || null,
      connections: values.connections,
      active: values.active,
      priority: values.priority,
    };

    if (editingServer) {
      const result = await updateServer({ id: editingServer.id, input });
      if (result.data?.updateServer) {
        setServers((current) =>
          current.map((server) =>
            server.id === editingServer.id ? result.data.updateServer : server,
          ),
        );
      }
    } else {
      const result = await addServer({ input });
      if (result.data?.addServer) {
        setServers((current) =>
          [...current, result.data.addServer].sort((left, right) =>
            left.priority - right.priority || left.host.localeCompare(right.host),
          ),
        );
      }
    }

    closeForm();
  };

  const handleDelete = async (id: number) => {
    const result = await removeServer({ id });
    if (result.data?.removeServer) {
      setServers(result.data.removeServer);
    }
    setDeleteConfirmId(null);
  };

  const handleTest = async (values: ServerFormValues) => {
    setTesting(true);
    setTestResult(null);
    const result = await testConnection({
      input: {
        host: values.host.trim(),
        port: values.port,
        tls: values.tls,
        username: values.username.trim() || null,
        password: values.password.trim() || null,
        connections: values.connections,
        active: values.active,
        priority: values.priority,
      },
    });
    setTestResult(result.data?.testConnection ?? null);
    setTesting(false);
  };

  return (
    <div className={embedded ? "space-y-5" : "space-y-6"}>
      <PageHeader
        title={t("servers.title")}
        description={embedded ? t("settings.serversDesc") : t("servers.description")}
        actions={<Button onClick={openAdd}>{t("servers.addServer")}</Button>}
      />

      {showForm ? (
        <ServerFormCard
          initialValues={
            editingServer
              ? {
                  host: editingServer.host,
                  port: editingServer.port,
                  tls: editingServer.tls,
                  username: editingServer.username ?? "",
                  password: "",
                  connections: editingServer.connections,
                  active: editingServer.active,
                  priority: editingServer.priority,
                }
              : defaultForm
          }
          editing={!!editingServer}
          testing={testing}
          testResult={testResult}
          onCancel={closeForm}
          onSave={handleSave}
          onTest={handleTest}
        />
      ) : null}

      {servers.length === 0 && !showForm ? (
        <EmptyState
          title={t("servers.empty")}
          description={t("servers.emptyHint")}
          actionLabel={t("servers.addServer")}
          onAction={openAdd}
        />
      ) : (
        groupedServers.map(([priority, items]) => (
          <Card key={priority}>
            <CardHeader>
              <CardTitle>{t("servers.group")} {priority}</CardTitle>
              <CardDescription>{t("servers.groupDescription")}</CardDescription>
            </CardHeader>
            <CardContent className="px-0 pb-0">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>{t("servers.host")}</TableHead>
                    <TableHead>{t("servers.port")}</TableHead>
                    <TableHead>{t("servers.username")}</TableHead>
                    <TableHead>{t("servers.connections")}</TableHead>
                    <TableHead>{t("servers.tls")}</TableHead>
                    <TableHead>{t("servers.active")}</TableHead>
                    <TableHead>{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((server) => (
                    <TableRow key={server.id}>
                      <TableCell className="font-medium">{server.host}</TableCell>
                      <TableCell>{server.port}</TableCell>
                      <TableCell>{server.username ?? "\u2014"}</TableCell>
                      <TableCell>{server.connections}</TableCell>
                      <TableCell>{server.tls ? t("label.enabled") : t("label.disabled")}</TableCell>
                      <TableCell>{server.active ? t("label.enabled") : t("label.disabled")}</TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-2">
                          <Button variant="outline" size="sm" onClick={() => openEdit(server)}>
                            {t("action.edit")}
                          </Button>
                          <Button variant="destructive" size="sm" onClick={() => setDeleteConfirmId(server.id)}>
                            {t("action.delete")}
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        ))
      )}

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteServer")}
        message={t("confirm.deleteServerMessage")}
        confirmLabel={t("confirm.deleteServerConfirm")}
        cancelLabel={t("confirm.deleteServerDismiss")}
        onConfirm={() => deleteConfirmId != null && void handleDelete(deleteConfirmId)}
        onCancel={() => setDeleteConfirmId(null)}
      />
    </div>
  );
}

function ServerFormCard({
  initialValues,
  editing,
  testing,
  testResult,
  onSave,
  onTest,
  onCancel,
}: {
  initialValues: ServerFormValues;
  editing: boolean;
  testing: boolean;
  testResult: {
    success: boolean;
    message: string;
    latencyMs?: number;
    supportsPipelining?: boolean;
  } | null;
  onSave: (values: ServerFormValues) => Promise<void>;
  onTest: (values: ServerFormValues) => Promise<void>;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>{editing ? t("servers.editServer") : t("servers.addServer")}</CardTitle>
        <CardDescription>{t("settings.serversDesc")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <Field label={t("servers.host")}>
            <Input
              value={values.host}
              placeholder="news.example.com"
              onChange={(event) => setValues((current) => ({ ...current, host: event.target.value }))}
            />
          </Field>
          <Field label={t("servers.port")}>
            <Input
              type="number"
              value={values.port}
              onChange={(event) => setValues((current) => ({ ...current, port: Number(event.target.value) }))}
            />
          </Field>
          <Field label={t("servers.username")}>
            <Input
              value={values.username}
              onChange={(event) => setValues((current) => ({ ...current, username: event.target.value }))}
            />
          </Field>
          <Field label={t("servers.password")}>
            <Input
              type="password"
              value={values.password}
              placeholder={editing ? "Leave blank to keep" : ""}
              onChange={(event) => setValues((current) => ({ ...current, password: event.target.value }))}
            />
          </Field>
          <Field label={t("servers.connections")}>
            <Input
              type="number"
              min={1}
              max={50}
              value={values.connections}
              onChange={(event) => setValues((current) => ({ ...current, connections: Number(event.target.value) }))}
            />
          </Field>
          <Field label={t("servers.group")} description={t("servers.groupDescription")}>
            <Input
              type="number"
              min={0}
              value={values.priority}
              onChange={(event) => setValues((current) => ({ ...current, priority: Number(event.target.value) }))}
            />
          </Field>
        </div>

        <div className="flex flex-wrap gap-6 rounded-2xl border border-border/70 bg-background/70 p-4">
          <ToggleField
            label={t("servers.tls")}
            checked={values.tls}
            onCheckedChange={(checked) => setValues((current) => ({ ...current, tls: checked }))}
          />
          <ToggleField
            label={t("servers.active")}
            checked={values.active}
            onCheckedChange={(checked) => setValues((current) => ({ ...current, active: checked }))}
          />
        </div>

        {testResult ? (
          <div className={testResult.success ? "rounded-2xl border border-emerald-500/30 bg-emerald-500/10 p-4 text-sm text-emerald-700 dark:text-emerald-300" : "rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive"}>
            {testResult.success
              ? `${t("servers.testSuccess")} (${testResult.latencyMs}ms${testResult.supportsPipelining ? ", pipelining supported" : ""})`
              : `${t("servers.testFailed")}: ${testResult.message}`}
          </div>
        ) : null}

        <div className="flex flex-wrap gap-3">
          <Button onClick={() => void onSave(values)} disabled={!values.host.trim()}>
            {editing ? t("settings.save") : t("servers.addServer")}
          </Button>
          <Button variant="outline" onClick={() => void onTest(values)} disabled={!values.host.trim() || testing}>
            {testing ? t("servers.testing") : t("servers.testConnection")}
          </Button>
          <Button variant="ghost" onClick={onCancel}>
            {t("action.cancel")}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function Field({
  label,
  description,
  children,
}: {
  label: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-2">
      <Label>{label}</Label>
      {children}
      {description ? <p className="text-xs text-muted-foreground">{description}</p> : null}
    </div>
  );
}

function ToggleField({
  label,
  checked,
  onCheckedChange,
}: {
  label: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <Label className="gap-3">
      <Checkbox checked={checked} onCheckedChange={(value) => onCheckedChange(value === true)} />
      {label}
    </Label>
  );
}
