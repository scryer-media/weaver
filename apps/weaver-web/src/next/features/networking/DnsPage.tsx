// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback, useRef, useState } from "react";
import { useClient, useMutation, useQuery } from "urql";
import type { DocumentNode } from "graphql";
import {
  DELETE_DNS_PROFILE,
  DELETE_DNS_SERVER_LIST,
  DNS_QUERY,
  SAVE_DNS_PROFILE,
  SAVE_DNS_SERVER_LIST,
  TEST_DNS_SERVERS,
} from "@/graphql/dns";
import { useTranslate } from "@/lib/context/translate-context";
import {
  DNS_TRANSITS,
  SYSTEM_DNS_PROFILE_ID,
  dnsTransitKey,
  dnsProfileName,
  dnsWhoKey,
  listForm,
  listInput,
  listProblem,
  probeRequest,
  problemCode,
  profileForm,
  profileInput,
  profileProblem,
  profileSummary,
  capabilityTable,
  routeCaption,
  serverRowsKey,
  testRefusalRow,
  trustChoices,
  whoChoices,
  withWho,
  type DnsListForm,
  type DnsProfileEdited,
  type DnsProfileForm,
  type DnsProfileKind,
  type DnsNameProfileRow,
  type DnsRouteProfileRow,
  type DnsServerCapability,
  type DnsServerList,
  type DnsServerRow,
  type DnsTestRun,
  type DnsTransit,
  type DnsWho,
} from "@/lib/dns";
import { routeInput, type NetworkRoute } from "@/lib/networking";
import { networkIds, testIds } from "@/next/testids";
import { EmptyState } from "../../components/chrome";
import { ConfirmDialog } from "../../components/ConfirmDialog";
import { Dialog } from "../../components/Dialog";
import { RecordEditor } from "../../components/RecordEditor";
import { PrimaryButton, SecondaryButton, Select } from "../../components/controls";
import { Cell } from "../../components/rows";
import { countLabel } from "../../i18n/labels";
import { DnsCapabilityResults, dnsServerFields } from "../../pages/settings/dns-fields";
import { SettingsBlocks, usePanelStatus, useSearchReport, type SettingsBlock } from "../../pages/settings/framework";
import { Hint } from "./presentation";
import { useNamedRefusal } from "./refusal-names";
import { DraftRouteEditor, useDraftRoute } from "./TestViaDialog";
import { useNextData } from "../../data/next-data";

/** The words each kind of profile is shown with. */
const KIND_WORDS: Record<DnsProfileKind, { title: string; add: string; used: string; deleteBody: string }> = {
  ROUTE: {
    title: "next.dns.profiles.title",
    add: "next.dns.profiles.add",
    used: "next.dns.profiles.routes",
    deleteBody: "next.dns.profiles.deleteBody",
  },
  PROXY_NAME: {
    title: "next.dns.nameProfiles.title",
    add: "next.dns.nameProfiles.add",
    used: "next.dns.profiles.proxies",
    deleteBody: "next.dns.nameProfiles.deleteBody",
  },
};

/** The lists, and the profiles of each kind: System first, then that kind by id. */
type DnsData = { dnsServerLists: DnsServerList[]; nameProfiles: DnsNameProfileRow[]; routeProfiles: DnsRouteProfileRow[] };

/** Runs one change; resolves to its refusal's sentence, or null once it landed and the page was read again. */
type DnsAction = (document: DocumentNode, variables: Record<string, unknown>, onData?: (data: unknown) => void) => Promise<string | null>;

/**
 * Networking -> DNS: saved server lists, and the profiles routes pick from.
 * A route follows one profile, chosen in its own editor. A change here takes
 * effect on the next lookup.
 */
export function DnsPage() {
  const t = useTranslate();
  const client = useClient();
  const [{ data, error, fetching }, refresh] = useQuery<DnsData>({ query: DNS_QUERY, requestPolicy: "cache-and-network" });
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);
  const [editingList, setEditingList] = useState<DnsServerList | "new" | null>(null);
  // A saved profile, or the kind of a new one.
  const [editingProfile, setEditingProfile] = useState<DnsProfileEdited | { kind: DnsProfileKind } | null>(null);
  usePanelStatus(message, failed);
  useSearchReport(0, !data && fetching);

  const refusal = useNamedRefusal();

  const action = useCallback<DnsAction>(
    async (document, variables, onData) => {
      setBusy(true);
      setMessage(null);
      try {
        const result = await client.mutation(document, variables).toPromise();
        const failure = result.error ? await refusal(result.error) : null;
        setFailed(failure !== null);
        setMessage(failure ?? t("next.networking.saved"));
        if (failure === null) {
          onData?.(result.data);
          refresh({ requestPolicy: "network-only" });
        }
        return failure;
      } catch (caught) {
        const failure = caught instanceof Error ? caught.message : t("next.networking.requestFailed");
        setFailed(true);
        setMessage(failure);
        return failure;
      } finally {
        setBusy(false);
      }
    },
    [client, refresh, refusal, t],
  );

  if (error && !data) {
    return (
      <EmptyState
        title={t("next.networking.loadFailed")}
        body={error.message}
        action={
          <SecondaryButton icon="refresh" onClick={() => refresh({ requestPolicy: "network-only" })}>
            {t("next.networking.retry")}
          </SecondaryButton>
        }
      />
    );
  }
  if (!data) {
    return <EmptyState loading title={t("next.common.loading")} body={t("next.networking.loading")} />;
  }

  const lists = data.dnsServerLists;
  // One table per kind, System first in each; the same editor serves both.
  const profileBlock = (kind: DnsProfileKind): SettingsBlock => {
    // Each with its kind, and counted by what uses that kind.
    const shown: (DnsProfileEdited & { builtIn: boolean; used: number })[] =
      kind === "PROXY_NAME"
        ? data.nameProfiles.map((profile) => ({ ...profile, kind, used: profile.usedByProxies }))
        : data.routeProfiles.map((profile) => ({ ...profile, kind, used: profile.usedByRoutes }));
    const words = KIND_WORDS[kind];
    return {
      kind: "table",
      id: kind === "PROXY_NAME" ? "dnsNameProfiles" : "dnsProfiles",
      title: t(words.title),
      note: countLabel(t, "next.dns.profiles.count", shown.length),
      columns: "minmax(0, 1fr) 112px minmax(0, 2fr) 96px",
      headers: [
        t("next.dns.profiles.name"),
        t("next.dns.who"),
        t("next.dns.profiles.setting"),
        t(words.used),
      ],
      empty: t("next.dns.profiles.empty"),
      add: { label: t(words.add), onClick: () => setEditingProfile({ kind }) },
      onRowClick: (id) => {
        const profile = shown.find((candidate) => String(candidate.id) === id);
        // The built-in profile is shown, never edited.
        if (profile && !profile.builtIn) setEditingProfile(profile);
      },
      rows: shown.map((profile) => {
        const summary = profileSummary(t, profile, lists);
        const name = dnsProfileName(t, profile, kind);
        const used =
          kind === "PROXY_NAME"
            ? countLabel(t, "next.dns.profiles.proxyCount", profile.used)
            : countLabel(t, "next.dns.profiles.routeCount", profile.used);
        return {
          id: String(profile.id),
          searchText: `${name} ${summary}`,
          cells: [
            <Cell key="name" title={name}>
              {name}
              {profile.builtIn ? (
                <span className="ml-2 font-wv-mono text-[11px] text-wv-muted">{t("next.dns.profiles.builtIn")}</span>
              ) : null}
            </Cell>,
            <Cell key="who">{t(dnsWhoKey(profile.who, kind))}</Cell>,
            <Cell key="summary" mono className="text-wv-secondary" title={summary}>
              {summary}
            </Cell>,
            <Cell key="used" mono>
              {used}
            </Cell>,
          ],
        };
      }),
    };
  };
  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "dnsLists",
      title: t("next.dns.lists.title"),
      note: countLabel(t, "next.dns.lists.count", lists.length),
      columns: "minmax(0, 1fr) 96px minmax(0, 1.4fr)",
      headers: [t("next.dns.lists.name"), t("next.dns.lists.servers"), t("next.dns.lists.usedBy")],
      empty: t("next.dns.lists.empty"),
      add: { label: t("next.dns.lists.add"), onClick: () => setEditingList("new") },
      onRowClick: (id) => {
        const list = lists.find((candidate) => String(candidate.id) === id);
        if (list) setEditingList(list);
      },
      rows: lists.map((list) => {
        const usedBy = list.usedByProfiles.join(", ");
        return {
          id: String(list.id),
          searchText: `${list.name} ${list.servers.map((server) => server.url ?? server.address).join(" ")} ${usedBy}`,
          cells: [
            <Cell key="name" title={list.name}>
              {list.name}
            </Cell>,
            <Cell key="servers" mono>
              {countLabel(t, "next.dns.lists.serverCount", list.servers.length)}
            </Cell>,
            <Cell key="usedBy" mono className="text-wv-secondary" title={usedBy}>
              {usedBy || "—"}
            </Cell>,
          ],
        };
      }),
    },
    profileBlock("PROXY_NAME"),
    profileBlock("ROUTE"),
  ];

  return (
    <>
      <Hint>{t("next.dns.note")}</Hint>
      <SettingsBlocks blocks={blocks} />
      {editingProfile ? (
        <DnsProfileEditor
          key={"id" in editingProfile ? editingProfile.id : `new-${editingProfile.kind}`}
          profile={"id" in editingProfile ? editingProfile : null}
          kind={editingProfile.kind}
          lists={lists}
          action={action}
          busy={busy}
          onClose={() => setEditingProfile(null)}
        />
      ) : null}
      {editingList ? (
        <DnsListEditor
          key={editingList === "new" ? "new" : editingList.id}
          list={editingList === "new" ? null : editingList}
          action={action}
          busy={busy}
          onClose={() => setEditingList(null)}
        />
      ) : null}
    </>
  );
}

/** One server list in its editor, with a test of what each server supports. */
function DnsListEditor({
  list,
  action,
  busy,
  onClose,
  onSaved,
}: {
  list: DnsServerList | null;
  action: DnsAction;
  busy: boolean;
  onClose: () => void;
  /** Hears the id a save landed on, a new list's included. */
  onSaved?: (id: number) => void;
}) {
  const t = useTranslate();
  const [form, setForm] = useState<DnsListForm>(() => listForm(list));
  const [error, setError] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [picking, setPicking] = useState(false);
  const [run, setRun] = useState<DnsTestRun | null>(null);
  // A refused test: the sentence, and the form row it names when it names one.
  const [testRefusal, setTestRefusal] = useState<{ sentence: string; row: number | null } | null>(null);
  // The latest run's number: an answer to any earlier run is dropped.
  const latestRun = useRef(0);
  // The rows as they stand now, read when an answer lands.
  const rowsNow = useRef(form.servers);
  rowsNow.current = form.servers;
  const [, testServers] = useMutation<{ testDnsServers: DnsServerCapability[] }>(TEST_DNS_SERVERS);
  const refusal = useNamedRefusal();
  const setServers = (change: (current: DnsServerRow[]) => DnsServerRow[]) => {
    setTestRefusal(null);
    setForm((current) => ({ ...current, servers: change(current.servers) }));
  };
  // Results describe the rows they were run on; any change to the rows drops them.
  const table = capabilityTable(run, form.servers);
  const testing = table?.pending ?? false;
  const hasServer = form.servers.some((row) => row.address.trim() !== "");

  // The results stay in this editor alone: never saved, and gone with it.
  const test = async (route: NetworkRoute, caption: string) => {
    setPicking(false);
    setTestRefusal(null);
    const key = serverRowsKey(form.servers);
    const request = probeRequest(form.servers);
    const token = ++latestRun.current;
    setRun({ key, rows: request.rows, route: caption, results: null });
    const result = await testServers({ route: routeInput(route), servers: request.servers });
    if (token !== latestRun.current) return;
    if (result.error) {
      setRun(null);
      // A refusal of one server names it by its place among the servers sent.
      const at = (result.error.graphQLErrors[0]?.extensions?.params as { index?: unknown } | undefined)?.index;
      const sentence = await refusal(result.error);
      const landing = testRefusalRow({ key, rows: request.rows }, at, rowsNow.current);
      if (token !== latestRun.current || landing === null) return;
      setTestRefusal({ sentence, row: landing.row });
      return;
    }
    // A run whose rows were edited meanwhile no longer matches, and stays hidden.
    setRun((current) => (current?.key === key ? { ...current, results: result.data?.testDnsServers ?? [] } : current));
  };

  const save = async () => {
    const problem = listProblem(form);
    setErrorCode(problem ? problemCode(problem) : null);
    if (problem) {
      setError(t(problem.key, problem.values));
      return;
    }
    let saved = form.id;
    const failure = await action(SAVE_DNS_SERVER_LIST, { input: listInput(form) }, (data) => {
      saved = (data as { saveDnsServerList?: { id: number } } | undefined)?.saveDnsServerList?.id ?? saved;
    });
    if (failure) {
      setError(failure);
      return;
    }
    if (saved !== null) onSaved?.(saved);
    onClose();
  };

  const remove = async () => {
    if (form.id === null) return;
    const failure = await action(DELETE_DNS_SERVER_LIST, { id: form.id });
    setDeleting(false);
    if (failure) {
      setError(failure);
    } else {
      onClose();
    }
  };

  return (
    <>
      <RecordEditor
        open
        title={list ? list.name : t("next.dns.lists.add")}
        testId={testIds.dialog("dnsList")}
        error={error}
        errorCode={errorCode}
        busy={busy}
        saveLabel={t("next.dns.lists.save")}
        onSave={() => void save()}
        onDismiss={onClose}
        onDelete={list ? () => setDeleting(true) : undefined}
        deleteLabel={t("next.dns.lists.delete")}
        extraActions={
          <SecondaryButton
            icon="test"
            disabled={!hasServer || picking || testing}
            testId={networkIds.dnsTestCapabilities}
            onClick={() => setPicking(true)}
          >
            {t("next.dns.test.action")}
          </SecondaryButton>
        }
        sections={[
          {
            id: "dnsList",
            title: t("next.dns.lists.section"),
            fields: [
              {
                id: "dnsListName",
                label: t("next.dns.lists.name"),
                control: { kind: "text", mono: false, value: form.name, onChange: (name) => setForm((current) => ({ ...current, name })) },
              },
              ...dnsServerFields(t, form.servers, setServers, testRefusal?.row == null ? undefined : { row: testRefusal.row, sentence: testRefusal.sentence }),
            ],
            body: (
              <>
                {table ? <DnsCapabilityResults t={t} table={table} /> : null}
                {testRefusal === null || testRefusal.row !== null ? null : (
                  <p role="alert" className="flex-none px-4 py-3 text-[12.5px] leading-[1.5] text-wv-error-text sm:px-6">
                    {testRefusal.sentence}
                  </p>
                )}
              </>
            ),
          },
        ]}
      />
      <ConfirmDialog
        open={deleting}
        testId={testIds.dialog("dnsListDelete")}
        title={t("next.dns.lists.delete")}
        note={list?.name}
        busy={busy}
        confirmLabel={t("next.dns.lists.delete")}
        body={t("next.dns.lists.deleteBody", { name: list?.name ?? "" })}
        onConfirm={() => void remove()}
        onDismiss={() => setDeleting(false)}
      />
      {picking ? (
        <DnsCapabilityDialog onRun={(route, caption) => void test(route, caption)} onClose={() => setPicking(false)} />
      ) : null}
    </>
  );
}

/**
 * Which route to test a list's servers through. The route is drafted the way
 * a proxy test drafts one, and never saved. Each server is asked over every
 * wire whatever its own is; the results show in the list's editor.
 */
function DnsCapabilityDialog({
  onRun,
  onClose,
}: {
  onRun: (route: NetworkRoute, caption: string) => void;
  onClose: () => void;
}) {
  const t = useTranslate();
  const draft = useDraftRoute({ subject: null, kind: "OUTBOUND" });
  const run = () => {
    if (!draft.route || !draft.data) return;
    const caption = routeCaption(
      draft.route,
      { egresses: draft.data.egressInterfaces, proxies: draft.data.proxyProfiles, pools: draft.data.proxyPools },
      t("next.networking.route.direct"),
    );
    onRun(draft.route, caption);
  };

  return (
    <Dialog
      open
      testId={testIds.dialog("dnsTest")}
      title={t("next.dns.test.title")}
      width={720}
      onDismiss={onClose}
      footer={
        <>
          <SecondaryButton onClick={onClose}>{t("next.networking.close")}</SecondaryButton>
          <PrimaryButton
            icon="test"
            disabled={!draft.route || draft.problem !== null}
            testId={networkIds.dnsTestRun}
            onClick={run}
          >
            {t("next.networking.testVia.run")}
          </PrimaryButton>
        </>
      }
    >
      <DraftRouteEditor draft={draft} />
    </Dialog>
  );
}

/**
 * One DNS profile in its editor: who answers first, then only what that who allows. A proxy name profile never has the
 * proxy answer, always asks from the egress, and has no fallback.
 */
function DnsProfileEditor({
  profile,
  kind,
  lists,
  action,
  busy,
  onClose,
}: {
  profile: DnsProfileEdited | null;
  kind: DnsProfileKind;
  lists: DnsServerList[];
  action: DnsAction;
  busy: boolean;
  onClose: () => void;
}) {
  const t = useTranslate();
  const { refreshDnsProfiles } = useNextData("refreshDnsProfiles");
  const [form, setForm] = useState<DnsProfileForm>(() => profileForm(profile, lists, kind));
  const [error, setError] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [addingList, setAddingList] = useState(false);
  const patch = (next: Partial<DnsProfileForm>) => setForm((current) => ({ ...current, ...next }));

  const save = async () => {
    const problem = profileProblem(form);
    setErrorCode(problem ? problemCode(problem) : null);
    if (problem) {
      setError(t(problem.key, problem.values));
      return;
    }
    const failure = await action(SAVE_DNS_PROFILE, { input: profileInput(form) });
    refreshDnsProfiles();
    if (failure) {
      setError(failure);
    } else {
      onClose();
    }
  };

  const remove = async () => {
    if (form.id === null) return;
    const failure = await action(DELETE_DNS_PROFILE, { id: form.id });
    refreshDnsProfiles();
    setDeleting(false);
    if (failure) {
      setError(failure);
    } else {
      onClose();
    }
  };

  const proxyName = form.kind === "PROXY_NAME";
  const words = KIND_WORDS[form.kind];
  const trusts = trustChoices(form.who);

  return (
    <>
      <RecordEditor
        open
        title={profile ? profile.name : t(words.add)}
        testId={testIds.dialog("dnsProfile")}
        error={error}
        errorCode={errorCode}
        busy={busy}
        saveLabel={t("next.dns.profiles.save")}
        onSave={() => void save()}
        onDismiss={onClose}
        onDelete={profile && profile.id !== SYSTEM_DNS_PROFILE_ID ? () => setDeleting(true) : undefined}
        deleteLabel={t("next.dns.profiles.delete")}
        sections={[
          {
            id: "dnsProfile",
            title: t("next.dns.profiles.section"),
            fields: [
              {
                id: "dnsProfileName",
                label: t("next.dns.profiles.name"),
                control: { kind: "text", mono: false, value: form.name, onChange: (name) => patch({ name }) },
              },
              {
                id: "dnsWho",
                label: t("next.dns.who"),
                help: t(proxyName ? `next.dns.nameWhoHelp.${form.who}` : `next.dns.whoHelp.${form.who}`),
                control: {
                  kind: "segmented",
                  value: form.who,
                  options: whoChoices(form.kind).map((who) => ({ value: who, label: t(dnsWhoKey(who, form.kind)) })),
                  onChange: (who) => setForm((current) => withWho(current, who as DnsWho)),
                },
              },
              ...(form.who === "WEAVER"
                ? [
                    {
                      id: "dnsList",
                      label: t("next.dns.list"),
                      control: {
                        kind: "custom" as const,
                        control: (
                          <div className="flex w-[268px] max-w-full flex-wrap items-center gap-2">
                            <Select<string>
                              label={t("next.dns.list")}
                              value={form.serverListId === null ? "" : String(form.serverListId)}
                              placeholder={t("next.dns.listChoose")}
                              options={lists.map((list) => ({ value: String(list.id), label: list.name }))}
                              className="min-w-0 flex-1"
                              testId={testIds.field("dnsList")}
                              onChange={(id) => patch({ serverListId: Number(id) })}
                            />
                            <SecondaryButton icon="add" testId={testIds.field("dnsListNew")} onClick={() => setAddingList(true)}>
                              {t("next.dns.listNew")}
                            </SecondaryButton>
                          </div>
                        ),
                      },
                    },
                    ...(proxyName
                      ? []
                      : [
                    {
                      id: "dnsTransit",
                      label: t("next.dns.transit"),
                      help: t("next.dns.transitHelp"),
                      control: {
                        kind: "segmented" as const,
                        value: form.transit,
                        options: DNS_TRANSITS.map((transit) => ({ value: transit, label: t(dnsTransitKey(transit)) })),
                        onChange: (transit: string) => patch({ transit: transit as DnsTransit }),
                      },
                    },
                        ]),
                  ]
                : []),
              ...(form.who === "PROXY"
                ? [
                    {
                      id: "dnsSystemFallback",
                      label: t("next.dns.systemFallback"),
                      help: t("next.dns.systemFallbackHelp"),
                      control: {
                        kind: "toggle" as const,
                        value: form.systemFallback,
                        onChange: (systemFallback: boolean) => patch({ systemFallback }),
                      },
                    },
                  ]
                : []),
              ...(trusts.length > 1
                ? [
                    {
                      id: "dnsTrust",
                      label: t("next.dns.trust"),
                      control: {
                        kind: "toggle" as const,
                        value: form.trust === "VALIDATE",
                        onChange: (validate: boolean) => patch({ trust: validate ? "VALIDATE" : "AS_SENT" }),
                      },
                    },
                  ]
                : []),
            ],
          },
        ]}
      />
      <ConfirmDialog
        open={deleting}
        testId={testIds.dialog("dnsProfileDelete")}
        title={t("next.dns.profiles.delete")}
        note={profile?.name}
        busy={busy}
        confirmLabel={t("next.dns.profiles.delete")}
        body={t(words.deleteBody, { name: profile?.name ?? "" })}
        onConfirm={() => void remove()}
        onDismiss={() => setDeleting(false)}
      />
      {/* A list made from here is chosen for the profile, whose draft stays open beneath. */}
      {addingList ? (
        <DnsListEditor
          list={null}
          action={action}
          busy={busy}
          onClose={() => setAddingList(false)}
          onSaved={(id) => patch({ serverListId: id })}
        />
      ) : null}
    </>
  );
}
