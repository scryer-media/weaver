// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useRef, useState, type ReactNode } from "react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import {
  DELETE_TRACKER_RULE_MUTATION,
  SAVE_TRACKER_RULE_MUTATION,
  TORRENT_ROUTES_QUERY,
  TORRENT_SETTINGS_QUERY,
  TRACKER_HOSTS_QUERY,
  TRACKER_RULES_QUERY,
  UPDATE_TORRENT_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { UNPACK_CHOICES, routeExposureText, routeReasonText, type TorrentRouteReason, unpackChoice, unpackOf, type TorrentExposure } from "@/lib/torrents";
import type { ResolvedBy } from "@/lib/test-via";
import { announceHosts, patternsOf, torrentAnnounces } from "@/lib/tracker-hosts";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { SecondaryButton, Segmented, Select, TextField, Toggle } from "../../../components/controls";
import { RecordEditor } from "../../../components/RecordEditor";
import { Cell } from "../../../components/rows";
import { EM_DASH } from "../../../data/format";
import { SettingsBlocks, useDraft, usePanelState, type FieldSpec, type SettingsBlock } from "../framework";
import {
  GoalControl,
  INHERIT,
  durationText,
  globalUploadCapField,
  leafFrom,
  leafInput,
  seedingAtGlobal,
  seedingFields,
  seedingFrom,
  seedingInherits,
  seedingInput,
  uploadCapField,
  type Leaf,
  type SeedingPolicy,
} from "../seeding";

/**
 * Torrents: the global seeding policy, the tracker rules, and how the
 * torrent session connects.
 *
 * The seeding terms, the upload cap and the connection save through
 * `updateSettings`, each only when it was edited. A tracker rule saves from
 * its own editor; its terms that all inherit save as none.
 */

interface TrackerRule {
  id: number;
  hostPattern: string;
  /** Null while every term inherits from the category and the global settings. */
  seeding: SeedingPolicy | null;
  neverPause: boolean;
  /** Null while the rule leaves it to the category and the global setting. */
  unpack: boolean | null;
  /** Null names the torrents as Weaver. */
  client: TorrentClient | null;
  torrentRoute: boolean;
  /** Each null inherits; a private torrent uses none of the three whatever they say. */
  dht: boolean | null;
  pex: boolean | null;
  lsd: boolean | null;
  /** Null inherits the global encryption. */
  encryption: TorrentEncryption | null;
  /** The upload cap its torrents take in place of their category's and the global one. */
  uploadCap: Leaf<number>;
  forced: boolean;
  /** Whether it holds a certificate for its SSL torrents; the key is never read back. */
  hasSsl: boolean;
  sslSubject: string | null;
  /** Milliseconds since the epoch. */
  sslNotAfter: number | null;
}

type TorrentClient = "WEAVER" | "QBITTORRENT" | "TRANSMISSION";

const TORRENT_CLIENTS: readonly { value: TorrentClient; label: string }[] = [
  { value: "WEAVER", label: "Weaver" },
  { value: "QBITTORRENT", label: "qBittorrent" },
  { value: "TRANSMISSION", label: "Transmission" },
];

interface TrackerRuleForm {
  hostPattern: string;
  seeding: SeedingPolicy;
  neverPause: boolean;
  unpack: boolean | null;
  client: TorrentClient | null;
  torrentRoute: boolean;
  dht: boolean | null;
  pex: boolean | null;
  lsd: boolean | null;
  encryption: TorrentEncryption | null;
  uploadCap: Leaf<number>;
  forced: boolean;
  /** A certificate and key pasted to replace the rule's; both blank keeps it. */
  certificatePem: string;
  privateKeyPem: string;
  /** The rule's certificate is to be cleared. */
  sslCleared: boolean;
}

const NEW_TRACKER_RULE: TrackerRuleForm = {
  hostPattern: "",
  seeding: seedingFrom(null),
  neverPause: false,
  unpack: null,
  client: null,
  torrentRoute: false,
  dht: null,
  pex: null,
  lsd: null,
  encryption: null,
  uploadCap: INHERIT,
  forced: false,
  certificatePem: "",
  privateKeyPem: "",
  sslCleared: false,
};

/** The rule's peer sources beside its trackers, each switched on its own. */
const DISCOVERY_SWITCHES = [
  { key: "dht", label: "next.torrents.trackerRuleDht", keywords: "dht peer discovery private" },
  { key: "pex", label: "next.torrents.trackerRulePex", keywords: "pex peer exchange private" },
  { key: "lsd", label: "next.torrents.trackerRuleLsd", keywords: "lsd local peer discovery private" },
] as const;

/** The certificate a rule's save sends: pasted, cleared, or left out to keep the rule's. */
function sslInput(form: TrackerRuleForm) {
  if (form.certificatePem.trim() !== "" || form.privateKeyPem.trim() !== "") {
    return { ssl: { certificatePem: form.certificatePem, privateKeyPem: form.privateKeyPem } };
  }
  return form.sslCleared ? { ssl: null } : {};
}

/** A rule's goals in one line, as its row shows them. */
function goalsText(t: Translate, policy: SeedingPolicy | null): string {
  if (!policy) {
    return EM_DASH;
  }
  const goal = (leaf: Leaf<number>, label: string, text: (value: number) => string) =>
    leaf.state === "OFF"
      ? `${label} ${t("next.seeding.noGoal")}`
      : leaf.state === "SET" && leaf.value !== null
        ? `${label} ${text(leaf.value)}`
        : null;
  const minutes = (value: number) => durationText(t, value);
  const goals = [
    goal(policy.ratioGoal, t("next.seeding.ratioGoal"), String),
    goal(policy.seedTimeGoalMinutes, t("next.seeding.seedTimeGoal"), minutes),
    goal(policy.inactiveGoalMinutes, t("next.seeding.inactiveGoal"), minutes),
  ].filter((text): text is string => text !== null);
  return goals.length > 0 ? goals.join(" · ") : EM_DASH;
}

type HostSource = "known" | "custom" | "file";

/**
 * A rule's host: one a torrent has announced to, typed, or one a .torrent
 * file's trackers name, read in the browser. Opens on the known hosts when
 * the pattern is one of them, or is still to be picked and some are known.
 */
function HostPicker({
  t,
  value,
  onChange,
  known,
}: {
  t: Translate;
  value: string;
  onChange: (pattern: string) => void;
  known: readonly string[];
}) {
  const [chosen, setChosen] = useState<HostSource | null>(null);
  const [file, setFile] = useState<{ name: string; patterns: string[] } | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const source = chosen ?? ((value === "" ? known.length > 0 : known.includes(value)) ? "known" : "custom");
  const label = t("next.torrents.trackerRuleHost");

  // A list keeps the pattern only when it offers it; the text field keeps it to edit.
  const pick = (next: HostSource) => {
    const list = next === "known" ? known : next === "file" ? (file?.patterns ?? []) : null;
    if (list && !list.includes(value)) {
      onChange("");
    }
    setChosen(next);
  };

  const read = async (picked: File) => {
    const announces = torrentAnnounces(new Uint8Array(await picked.arrayBuffer()));
    const patterns = announces ? patternsOf(announceHosts(announces)) : [];
    setFileError(
      announces === null
        ? t("next.torrents.trackerRuleFileInvalid")
        : patterns.length === 0
          ? t("next.torrents.trackerRuleFileEmpty")
          : null,
    );
    setFile({ name: picked.name, patterns });
    onChange(patterns[0] ?? "");
  };

  return (
    <div className="flex min-w-0 flex-col gap-2">
      <Segmented<HostSource>
        value={source}
        label={label}
        onChange={pick}
        className="self-start"
        options={[
          { value: "known", label: t("next.torrents.trackerRuleHostKnown"), disabled: known.length === 0 },
          { value: "custom", label: t("next.torrents.trackerRuleHostCustom") },
          { value: "file", label: t("next.torrents.trackerRuleHostFile") },
        ]}
      />
      {source === "known" ? (
        <Select
          value={value}
          label={label}
          placeholder={t("next.torrents.trackerRuleHostChoose")}
          onChange={onChange}
          className="font-wv-mono text-[12px]"
          menuClassName="max-h-[320px]"
          options={known.map((pattern) => ({ value: pattern, label: pattern }))}
        />
      ) : source === "custom" ? (
        <TextField value={value} label={label} placeholder="*.tracker.example" onChange={onChange} />
      ) : (
        <>
          <div className="flex h-[34px] min-w-0 items-center gap-3">
            <input
              ref={fileRef}
              type="file"
              accept=".torrent,application/x-bittorrent"
              aria-label={t("next.torrents.trackerRuleHostFile")}
              className="hidden"
              onChange={(event) => {
                const picked = event.target.files?.[0] ?? null;
                // Cleared so the same file can be chosen again.
                event.target.value = "";
                if (picked) {
                  void read(picked);
                }
              }}
            />
            <SecondaryButton icon="chooseFile" onClick={() => fileRef.current?.click()}>
              {t("next.torrents.trackerRuleChooseFile")}
            </SecondaryButton>
            {file ? (
              <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg" title={file.name}>
                {file.name}
              </span>
            ) : null}
          </div>
          {fileError ? <span className="text-[12px] text-wv-error-text">{fileError}</span> : null}
          {file && file.patterns.length > 0 ? (
            <Select
              value={value}
              label={label}
              placeholder={t("next.torrents.trackerRuleHostChoose")}
              onChange={onChange}
              className="font-wv-mono text-[12px]"
              options={file.patterns.map((pattern) => ({ value: pattern, label: pattern }))}
            />
          ) : null}
        </>
      )}
    </div>
  );
}

interface TorrentPortMapping {
  mapper: "UPNP" | "NAT_PMP";
  protocol: "TCP" | "UDP";
  localPort: number;
  /** Null once the mapping failed or was removed. */
  externalPort: number | null;
  gatewayAddress: string | null;
  error: string | null;
}

interface TorrentRoute {
  key: string;
  scopes: { scope: "DEFAULT" | "CATEGORY" | "TRACKER_RULE"; id: number | null; name: string | null }[];
  open: boolean;
  listenAddress: string | null;
  utp: boolean;
  mappings: TorrentPortMapping[];
  externalAddresses: string[];
  /** Why the route's session is held closed, or why its leg is down. */
  reason: TorrentRouteReason | null;
  /** Who resolves the names its sessions look up. */
  resolvedBy: ResolvedBy | null;
  /** What its session shows trackers and peers, while open. */
  exposure: TorrentExposure | null;
}

const MAPPERS: Record<TorrentPortMapping["mapper"], string> = { UPNP: "UPnP", NAT_PMP: "NAT-PMP" };

function mappingText(mapping: TorrentPortMapping): string {
  const what = `${MAPPERS[mapping.mapper]} ${mapping.protocol}`;
  if (mapping.error !== null) {
    return `${what}: ${mapping.error}`;
  }
  if (mapping.externalPort === null) {
    return `${what}: ${EM_DASH}`;
  }
  const gateway = mapping.gatewayAddress === null ? "" : ` ${mapping.gatewayAddress}`;
  return `${what} ${mapping.externalPort}${gateway}`;
}

/** Each torrent route, where its session listens, and how its port is mapped. */
function useTorrentRoutes(t: Translate): SettingsBlock {
  const [{ data }] = useQuery<{ torrentRoutes: TorrentRoute[] }>({
    query: TORRENT_ROUTES_QUERY,
    requestPolicy: "cache-and-network",
  });
  const routes = data?.torrentRoutes ?? [];
  const scopeName = (scope: TorrentRoute["scopes"][number]) =>
    scope.scope === "DEFAULT" ? t("next.torrents.routeDefault") : (scope.name ?? `#${scope.id ?? ""}`);
  return {
    kind: "table",
    id: "torrentRoutes",
    title: t("next.torrents.routes"),
    columns: "minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.4fr) minmax(0, 1fr) minmax(0, 1.6fr) minmax(0, 1.2fr)",
    headers: [
      t("next.torrents.routeScopes"),
      t("next.torrents.routeListen"),
      t("next.torrents.routeMapping"),
      t("next.torrents.routeExternal"),
      t("next.torrents.routeExposure"),
      t("next.torrents.routeStatus"),
    ],
    empty: t("next.torrents.routesEmpty"),
    rows: routes.map((route) => {
      const scopes = route.scopes.map(scopeName).join(", ");
      const listen =
        route.listenAddress === null ? EM_DASH : `${route.listenAddress}${route.utp ? " TCP+uTP" : " TCP"}`;
      const mappings = route.mappings.length === 0 ? EM_DASH : route.mappings.map(mappingText).join("; ");
      const external = route.externalAddresses.length === 0 ? EM_DASH : route.externalAddresses.join(", ");
      const exposure = routeExposureText(t, route);
      const status =
        route.reason === null
          ? t(route.open ? "next.torrents.routeOpen" : "next.torrents.routeIdle")
          : routeReasonText(t, route.reason);
      return {
        id: route.key,
        searchText: [scopes, listen, mappings, external, exposure, status].join(" "),
        cells: [
          <Cell key="scopes" title={scopes}>
            {scopes || route.key}
          </Cell>,
          <Cell key="listen" mono title={listen}>
            {listen}
          </Cell>,
          <Cell
            key="mapping"
            mono
            title={mappings}
            className={route.mappings.some((mapping) => mapping.error !== null) ? "text-wv-error-text" : undefined}
          >
            {mappings}
          </Cell>,
          <Cell key="external" mono title={external}>
            {external}
          </Cell>,
          <Cell key="exposure" mono title={exposure}>
            {exposure}
          </Cell>,
          <Cell
            key="status"
            title={status}
            className={route.reason !== null ? "text-wv-error-text" : "text-wv-muted"}
          >
            {status}
          </Cell>,
        ],
      };
    }),
  };
}

/** The tracker rules' table, and the editor and confirmation it opens. */
function useTrackerRules(t: Translate): { block: SettingsBlock; overlays: ReactNode } {
  const [{ data }, reexecute] = useQuery<{ trackerRules: TrackerRule[] }>({ query: TRACKER_RULES_QUERY });
  const [, saveRule] = useMutation(SAVE_TRACKER_RULE_MUTATION);
  const [, deleteRule] = useMutation(DELETE_TRACKER_RULE_MUTATION);
  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const [form, setForm] = useState<TrackerRuleForm>(NEW_TRACKER_RULE);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<TrackerRule | null>(null);
  // Counts the editor's openings, so each one starts its host picker afresh.
  const [opening, setOpening] = useState(0);
  const [{ data: hosts }] = useQuery<{ trackerHosts: string[] }>({
    query: TRACKER_HOSTS_QUERY,
    pause: editingId === null,
    requestPolicy: "cache-and-network",
  });
  const known = useMemo(() => patternsOf(hosts?.trackerHosts ?? []), [hosts?.trackerHosts]);

  const rules = useMemo(() => data?.trackerRules ?? [], [data?.trackerRules]);
  const editing = typeof editingId === "number" ? rules.find((rule) => rule.id === editingId) : null;

  const open = (rule: TrackerRule | null) => {
    setError(null);
    const start = rule
      ? {
          hostPattern: rule.hostPattern,
          seeding: seedingFrom(rule.seeding),
          neverPause: rule.neverPause,
          unpack: rule.unpack,
          client: rule.client,
          torrentRoute: rule.torrentRoute,
          dht: rule.dht,
          pex: rule.pex,
          lsd: rule.lsd,
          encryption: rule.encryption,
          uploadCap: leafFrom(rule.uploadCap),
          forced: rule.forced,
          certificatePem: "",
          privateKeyPem: "",
          sslCleared: false,
        }
      : NEW_TRACKER_RULE;
    setForm(start);
    setOpening((count) => count + 1);
    setEditingId(rule ? rule.id : "new");
  };

  const save = async () => {
    setBusy(true);
    const result = await saveRule({
      id: editingId === "new" ? null : editingId,
      input: {
        hostPattern: form.hostPattern.trim(),
        seeding: seedingInherits(form.seeding) ? null : seedingInput(form.seeding),
        neverPause: form.neverPause,
        unpack: form.unpack,
        client: form.client,
        torrentRoute: form.torrentRoute,
        dht: form.dht,
        pex: form.pex,
        lsd: form.lsd,
        encryption: form.encryption,
        uploadCap: leafInput(form.uploadCap),
        forced: form.forced,
        ...sslInput(form),
      },
    });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const remove = async () => {
    if (!confirmDelete) {
      return;
    }
    setBusy(true);
    await deleteRule({ id: confirmDelete.id });
    setBusy(false);
    setConfirmDelete(null);
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const block: SettingsBlock = {
    kind: "table",
    id: "trackerRules",
    title: t("next.torrents.trackerRules"),
    columns: "minmax(0, 1fr) minmax(0, 1.6fr) minmax(0, 0.6fr)",
    headers: [t("next.torrents.trackerRuleHost"), t("next.torrents.trackerRuleGoals"), t("next.torrents.trackerRuleNeverPause")],
    centered: [2],
    empty: t("next.torrents.trackerRulesEmpty"),
    add: { label: t("next.torrents.trackerRuleAdd"), onClick: () => open(null) },
    onRowClick: (id) => {
      const rule = rules.find((entry) => String(entry.id) === id);
      if (rule) {
        open(rule);
      }
    },
    rows: rules.map((rule) => ({
      id: String(rule.id),
      searchText: rule.hostPattern,
      cells: [
        <Cell key="host" mono>
          {rule.hostPattern}
        </Cell>,
        <Cell key="goals" className="text-wv-secondary">
          {goalsText(t, rule.seeding)}
        </Cell>,
        <Cell key="neverPause" mono className="text-wv-muted">
          {t(rule.neverPause ? "next.common.on" : "next.common.off")}
        </Cell>,
      ],
    })),
  };

  const overlays = (
    <>
      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? t("next.torrents.trackerRuleAdd") : (editing?.hostPattern ?? t("next.torrents.trackerRule"))}
        note={editingId === "new" ? t("next.torrents.trackerRuleNewNote") : `#${editing?.id ?? ""}`}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmDelete(editing) : undefined}
        deleteLabel={t("next.rss.removeRule")}
        sections={[
          {
            id: "trackerRule",
            title: t("next.torrents.trackerRule"),
            fields: [
              {
                id: "hostPattern",
                label: t("next.torrents.trackerRuleHost"),
                keywords: "tracker host announce",
                control: {
                  kind: "custom",
                  control: (
                    <HostPicker
                      key={opening}
                      t={t}
                      value={form.hostPattern}
                      known={known}
                      onChange={(hostPattern) => setForm((current) => ({ ...current, hostPattern }))}
                    />
                  ),
                },
              },
              {
                id: "neverPause",
                label: t("next.torrents.trackerRuleNeverPause"),
                keywords: "schedule pause seed",
                control: {
                  kind: "toggle",
                  value: form.neverPause,
                  onChange: (neverPause) => setForm((current) => ({ ...current, neverPause })),
                },
              },
              {
                id: "unpack",
                label: t("next.torrents.unpack"),
                keywords: "unpack extract archive",
                control: {
                  kind: "segmented",
                  value: unpackChoice(form.unpack),
                  options: UNPACK_CHOICES.map((option) => ({ ...option, label: t(option.label) })),
                  onChange: (choice) => setForm((current) => ({ ...current, unpack: unpackOf(choice) })),
                },
              },
              {
                id: "client",
                label: t("next.torrents.trackerRuleClient"),
                keywords: "client peer id user agent",
                control: {
                  kind: "select",
                  value: form.client ?? "WEAVER",
                  options: TORRENT_CLIENTS,
                  onChange: (client) =>
                    setForm((current) => ({
                      ...current,
                      client: client === "WEAVER" ? null : (client as TorrentClient),
                    })),
                },
              },
              {
                id: "torrentRoute",
                label: t("next.torrents.trackerRuleTorrentRoute"),
                keywords: "torrent route networking",
                control: {
                  kind: "custom",
                  // Off, the rule's torrents ride their category's route or the default torrent
                  // route; on, the rule owns a route, edited under Networking's routes.
                  control: (
                    <div className="flex items-center gap-[10px]">
                      <Toggle
                        checked={form.torrentRoute}
                        label={t("next.torrents.trackerRuleTorrentRoute")}
                        onChange={(torrentRoute) => setForm((current) => ({ ...current, torrentRoute }))}
                      />
                      {form.torrentRoute && typeof editingId === "number" ? (
                        <Link
                          to={`/settings/networking/routes?consumer=torrent:tracker:${editingId}`}
                          className="font-wv-mono text-[11.5px] text-wv-accent hover:text-wv-accent-hover"
                        >
                          {t("next.torrents.trackerRuleRouteOwn")}
                        </Link>
                      ) : form.torrentRoute ? (
                        <span className="font-wv-mono text-[11.5px] text-wv-muted">{t("next.torrents.trackerRuleRouteOwn")}</span>
                      ) : null}
                    </div>
                  ),
                },
              },
              ...DISCOVERY_SWITCHES.map(({ key, label, keywords }) => ({
                id: key,
                label: t(label),
                keywords,
                control: {
                  kind: "segmented" as const,
                  value: unpackChoice(form[key]),
                  options: UNPACK_CHOICES.map((option) => ({ ...option, label: t(option.label) })),
                  onChange: (choice: string) => setForm((current) => ({ ...current, [key]: unpackOf(choice) })),
                },
              })),
              {
                id: "encryption",
                label: t("next.torrents.encryption"),
                keywords: "encryption mse pe",
                control: {
                  kind: "segmented",
                  value: form.encryption ?? "inherit",
                  options: [
                    { value: "inherit", label: t("next.torrents.unpackInherit") },
                    ...ENCRYPTIONS.map((option) => ({ ...option, label: t(option.label) })),
                  ],
                  onChange: (encryption) =>
                    setForm((current) => ({
                      ...current,
                      encryption: encryption === "inherit" ? null : (encryption as TorrentEncryption),
                    })),
                },
              },
              uploadCapField(t, form.uploadCap, (uploadCap) => setForm((current) => ({ ...current, uploadCap })), "trackerRule"),
              {
                id: "forced",
                label: t("next.torrents.trackerRuleForced"),
                keywords: "forced queue start",
                control: {
                  kind: "toggle",
                  value: form.forced,
                  onChange: (forced) => setForm((current) => ({ ...current, forced })),
                },
              },
              ...(editing?.hasSsl && !form.sslCleared
                ? [
                    {
                      id: "sslHeld",
                      label: t("next.torrents.trackerRuleCertificate"),
                      keywords: "ssl certificate tls",
                      control: {
                        kind: "custom" as const,
                        control: (
                          <div className="flex min-w-0 items-center gap-[10px]">
                            <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">
                              {[
                                editing.sslSubject,
                                editing.sslNotAfter === null
                                  ? null
                                  : new Date(editing.sslNotAfter).toLocaleDateString(),
                              ]
                                .filter((part) => part)
                                .join(" · ")}
                            </span>
                            <SecondaryButton
                              onClick={() =>
                                setForm((current) => ({
                                  ...current,
                                  certificatePem: "",
                                  privateKeyPem: "",
                                  sslCleared: true,
                                }))
                              }
                            >
                              {t("next.torrents.trackerRuleCertificateClear")}
                            </SecondaryButton>
                          </div>
                        ),
                      },
                    },
                  ]
                : ([] as FieldSpec[])),
              {
                id: "certificatePem",
                label: t("next.torrents.trackerRuleCertificatePem"),
                keywords: "ssl certificate tls pem",
                control: {
                  kind: "textarea",
                  rows: 4,
                  placeholder: "-----BEGIN CERTIFICATE-----",
                  value: form.certificatePem,
                  onChange: (certificatePem) => setForm((current) => ({ ...current, certificatePem })),
                },
              },
              {
                id: "privateKeyPem",
                label: t("next.torrents.trackerRulePrivateKey"),
                keywords: "ssl private key tls pem",
                control: {
                  kind: "textarea",
                  rows: 4,
                  secret: true,
                  placeholder: "-----BEGIN PRIVATE KEY-----",
                  value: form.privateKeyPem,
                  onChange: (privateKeyPem) => setForm((current) => ({ ...current, privateKeyPem })),
                },
              },
              ...seedingFields(t, form.seeding, (seeding) => setForm((current) => ({ ...current, seeding })), {
                inherits: true,
                idPrefix: "trackerRuleSeeding",
              }),
            ],
          },
        ]}
      />

      <ConfirmDialog
        open={confirmDelete !== null}
        title={t("next.rss.removeRule")}
        note={confirmDelete?.hostPattern}
        busy={busy}
        confirmLabel={t("next.rss.removeRule")}
        body={t("next.torrents.trackerRuleRemoveBody")}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmDelete(null)}
      />
    </>
  );

  return { block, overlays };
}

type TorrentEncryption = "PREFER" | "REQUIRE";
type TorrentPeerProtocol = "TCP" | "UTP" | "BOTH";

interface TorrentConnection {
  enabled: boolean;
  basePort: number;
  announcePort: number | null;
  upnp: boolean;
  encryption: TorrentEncryption;
  maxActiveDownloads: number;
  maxPeersTotal: number;
  unpack: boolean;
  connectionsLimit: number | null;
  unchokeSlotsLimit: number | null;
  connectionSpeed: number;
  allowMultipleConnectionsPerIp: boolean;
  peerProtocol: TorrentPeerProtocol;
  dhtBootstrapNodes: string;
  blocklistEnabled: boolean;
  blocklist: string | null;
  stopTrackerTimeoutSecs: number;
  bannedIps: string[];
  blockPrivilegedPorts: boolean;
  filterTrackers: boolean;
  sslEnabled: boolean;
  /** Null while the sessions choose one at random and keep it. */
  sslPort: number | null;
}

interface TorrentForm {
  seeding: SeedingPolicy;
  /** Each torrent's upload cap in bytes per second; null is none. */
  uploadCap: number | null;
  torrent: TorrentConnection;
}

const DEFAULT_CONNECTION: TorrentConnection = {
  enabled: false,
  basePort: 6881,
  announcePort: null,
  upnp: false,
  encryption: "PREFER",
  maxActiveDownloads: 3,
  maxPeersTotal: 100,
  unpack: true,
  connectionsLimit: 200,
  unchokeSlotsLimit: 8,
  connectionSpeed: 30,
  allowMultipleConnectionsPerIp: false,
  peerProtocol: "TCP",
  dhtBootstrapNodes: "dht.transmissionbt.com:6881,dht.libtorrent.org:25401",
  blocklistEnabled: false,
  blocklist: null,
  stopTrackerTimeoutSecs: 5,
  bannedIps: [],
  blockPrivilegedPorts: false,
  filterTrackers: false,
  sslEnabled: false,
  sslPort: null,
};

/** The SSL port an operator's own starts from. */
const DEFAULT_SSL_PORT = 4433;

const PEER_PROTOCOLS: { value: TorrentPeerProtocol; label: string }[] = [
  { value: "TCP", label: "next.torrents.peerProtocolTcp" },
  { value: "UTP", label: "next.torrents.peerProtocolUtp" },
  { value: "BOTH", label: "next.torrents.peerProtocolBoth" },
];

/** Option labels are translation keys, resolved when the panel renders. */
const ENCRYPTIONS: { value: TorrentEncryption; label: string }[] = [
  { value: "PREFER", label: "next.torrents.encryptionPrefer" },
  { value: "REQUIRE", label: "next.torrents.encryptionRequire" },
];

const validPort = (port: number | null) => port === null || (Number.isInteger(port) && port >= 1 && port <= 65535);

/** The connection fields that differ from `saved`, as `TorrentSettingsInput` takes them. */
function connectionChanges(form: TorrentConnection, saved: TorrentConnection): Partial<TorrentConnection> {
  return Object.fromEntries(
    (Object.keys(form) as (keyof TorrentConnection)[])
      .filter((key) => form[key] !== saved[key])
      .map((key) => [key, form[key]]),
  ) as Partial<TorrentConnection>;
}

export function TorrentsPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{
    settings: { seeding: SeedingPolicy | null; uploadCap: number | null; torrent: TorrentConnection | null };
  }>({ query: TORRENT_SETTINGS_QUERY });
  const [updateState, updateSettings] = useMutation(UPDATE_TORRENT_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const trackerRules = useTrackerRules(t);
  const torrentRoutes = useTorrentRoutes(t);

  const source = useMemo<TorrentForm | null>(() => {
    const settings = data?.settings;
    return settings
      ? {
          seeding: seedingAtGlobal(seedingFrom(settings.seeding)),
          uploadCap: settings.uploadCap ?? null,
          torrent: { ...DEFAULT_CONNECTION, ...(settings.torrent ?? {}) },
        }
      : null;
  }, [data?.settings]);

  const draft = useDraft<TorrentForm>(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value;

  usePanelState({
    dirty: draft.dirty,
    busy: updateState.fetching,
    status: error ?? status,
    failed: error !== null,
    revert: () => {
      setError(null);
      draft.revert();
    },
    save: () => {
      if (!values || !source) {
        return;
      }
      if (
        !validPort(values.torrent.basePort) ||
        !validPort(values.torrent.announcePort) ||
        !validPort(values.torrent.sslPort)
      ) {
        setError(t("next.torrents.portInvalid"));
        return;
      }
      const seeding = seedingInput(values.seeding);
      const seedingChanged = JSON.stringify(seeding) !== JSON.stringify(seedingInput(source.seeding));
      const torrent = connectionChanges(values.torrent, source.torrent);
      setError(null);
      void updateSettings({
        input: {
          ...(seedingChanged ? { seeding } : {}),
          ...(values.uploadCap !== source.uploadCap ? { uploadCap: values.uploadCap } : {}),
          ...(Object.keys(torrent).length > 0 ? { torrent } : {}),
        },
      }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.torrents.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const patchTorrent = (next: Partial<TorrentConnection>) =>
    values && draft.set({ torrent: { ...values.torrent, ...next } });

  const blocks: SettingsBlock[] = values
    ? [
        {
          kind: "section",
          id: "seeding",
          title: t("next.torrents.seeding"),
          fields: [
            ...seedingFields(t, values.seeding, (seeding) => draft.set({ seeding }), { inherits: false }),
            globalUploadCapField(t, values.uploadCap, (uploadCap) => draft.set({ uploadCap })),
          ],
        },
        trackerRules.block,
        {
          kind: "section",
          id: "connection",
          title: t("next.torrents.connection"),
          fields: [
            {
              id: "enabled",
              label: t("next.torrents.enabled"),
              control: { kind: "toggle", value: values.torrent.enabled, onChange: (enabled) => patchTorrent({ enabled }) },
            },
            {
              id: "peerProtocol",
              label: t("next.torrents.peerProtocol"),
              keywords: "tcp utp protocol",
              control: {
                kind: "segmented",
                value: values.torrent.peerProtocol,
                options: PEER_PROTOCOLS.map((option) => ({ ...option, label: t(option.label) })),
                onChange: (peerProtocol) => patchTorrent({ peerProtocol: peerProtocol as TorrentPeerProtocol }),
              },
            },
            {
              id: "basePort",
              label: t("next.torrents.basePort"),
              keywords: "port listen",
              control: {
                kind: "number",
                value: values.torrent.basePort,
                min: 1,
                max: 65535,
                onChange: (basePort) => patchTorrent({ basePort: Math.round(basePort) }),
              },
            },
            {
              id: "announcePort",
              label: t("next.torrents.announcePort"),
              keywords: "port announce tracker",
              control: {
                kind: "custom",
                control: (
                  <GoalControl
                    label={t("next.torrents.announcePort")}
                    value={values.torrent.announcePort}
                    fallback={values.torrent.basePort}
                    onChange={(announcePort) =>
                      patchTorrent({ announcePort: announcePort === null ? null : Math.round(announcePort) })
                    }
                  />
                ),
              },
            },
            {
              id: "sslEnabled",
              label: t("next.torrents.sslEnabled"),
              keywords: "ssl tls torrents listen",
              control: {
                kind: "toggle",
                value: values.torrent.sslEnabled,
                onChange: (sslEnabled) => patchTorrent({ sslEnabled }),
              },
            },
            {
              id: "sslPort",
              label: t("next.torrents.sslPort"),
              keywords: "ssl tls port listen",
              control: {
                kind: "custom",
                control: (
                  <GoalControl
                    label={t("next.torrents.sslPort")}
                    value={values.torrent.sslPort}
                    fallback={DEFAULT_SSL_PORT}
                    offLabel={t("next.torrents.sslPortRandom")}
                    onChange={(sslPort) => patchTorrent({ sslPort: sslPort === null ? null : Math.round(sslPort) })}
                  />
                ),
              },
            },
            {
              id: "upnp",
              label: t("next.torrents.upnp"),
              keywords: "upnp port mapping",
              control: { kind: "toggle", value: values.torrent.upnp, onChange: (upnp) => patchTorrent({ upnp }) },
            },
            {
              id: "connectionsLimit",
              label: t("next.torrents.connectionsLimit"),
              keywords: "peers connections limit",
              control: {
                kind: "custom",
                control: (
                  <GoalControl
                    label={t("next.torrents.connectionsLimit")}
                    value={values.torrent.connectionsLimit}
                    fallback={DEFAULT_CONNECTION.connectionsLimit ?? 1}
                    offLabel={t("next.networking.unlimited")}
                    onChange={(next) =>
                      patchTorrent({ connectionsLimit: next === null ? null : Math.max(1, Math.round(next)) })
                    }
                  />
                ),
              },
            },
            {
              id: "maxPeersTotal",
              label: t("next.torrents.maxPeers"),
              keywords: "peers connections",
              control: {
                kind: "number",
                value: values.torrent.maxPeersTotal,
                min: 0,
                onChange: (next) => patchTorrent({ maxPeersTotal: Math.max(0, Math.round(next)) }),
              },
            },
            {
              id: "unchokeSlotsLimit",
              label: t("next.torrents.unchokeSlots"),
              keywords: "upload slots unchoke",
              control: {
                kind: "custom",
                control: (
                  <GoalControl
                    label={t("next.torrents.unchokeSlots")}
                    value={values.torrent.unchokeSlotsLimit}
                    fallback={DEFAULT_CONNECTION.unchokeSlotsLimit ?? 1}
                    offLabel={t("next.networking.unlimited")}
                    onChange={(next) =>
                      patchTorrent({ unchokeSlotsLimit: next === null ? null : Math.max(1, Math.round(next)) })
                    }
                  />
                ),
              },
            },
            {
              id: "blocklistEnabled",
              label: t("next.torrents.blocklistEnabled"),
              keywords: "ip filter blocklist",
              control: {
                kind: "toggle",
                value: values.torrent.blocklistEnabled,
                onChange: (blocklistEnabled) => patchTorrent({ blocklistEnabled }),
              },
            },
            {
              id: "blocklist",
              label: t("next.torrents.blocklist"),
              keywords: "ip filter blocklist path url",
              control: {
                kind: "text",
                mono: true,
                value: values.torrent.blocklist ?? "",
                onChange: (next) => patchTorrent({ blocklist: next.trim() === "" ? null : next }),
              },
            },
            {
              id: "filterTrackers",
              label: t("next.torrents.filterTrackers"),
              keywords: "ip filter trackers",
              control: {
                kind: "toggle",
                value: values.torrent.filterTrackers,
                onChange: (filterTrackers) => patchTorrent({ filterTrackers }),
              },
            },
            {
              id: "bannedIps",
              label: t("next.torrents.bannedIps"),
              keywords: "ip filter ban banned addresses",
              control: {
                kind: "textarea",
                rows: 4,
                value: values.torrent.bannedIps.join("\n"),
                onChange: (next) => patchTorrent({ bannedIps: next.split("\n") }),
              },
            },
          ],
        },
        torrentRoutes,
        {
          kind: "section",
          id: "bittorrent",
          title: t("next.torrents.bittorrent"),
          fields: [
            {
              id: "encryption",
              label: t("next.torrents.encryption"),
              control: {
                kind: "segmented",
                value: values.torrent.encryption,
                options: ENCRYPTIONS.map((option) => ({ ...option, label: t(option.label) })),
                onChange: (encryption) => patchTorrent({ encryption: encryption as TorrentEncryption }),
              },
            },
            {
              id: "maxActiveDownloads",
              label: t("next.torrents.maxActiveDownloads"),
              control: {
                kind: "number",
                value: values.torrent.maxActiveDownloads,
                min: 0,
                onChange: (next) => patchTorrent({ maxActiveDownloads: Math.max(0, Math.round(next)) }),
              },
            },
            {
              id: "unpack",
              label: t("next.torrents.unpack"),
              keywords: "unpack extract archive",
              control: { kind: "toggle", value: values.torrent.unpack, onChange: (unpack) => patchTorrent({ unpack }) },
            },
          ],
        },
        {
          kind: "section",
          id: "advanced",
          title: t("next.torrents.advanced"),
          fields: [
            {
              id: "connectionSpeed",
              label: t("next.torrents.connectionSpeed"),
              keywords: "connection speed outgoing",
              control: {
                kind: "number",
                value: values.torrent.connectionSpeed,
                min: 1,
                onChange: (next) => patchTorrent({ connectionSpeed: Math.max(1, Math.round(next)) }),
              },
            },
            {
              id: "allowMultipleConnectionsPerIp",
              label: t("next.torrents.multipleConnectionsPerIp"),
              keywords: "ip connections",
              control: {
                kind: "toggle",
                value: values.torrent.allowMultipleConnectionsPerIp,
                onChange: (allowMultipleConnectionsPerIp) => patchTorrent({ allowMultipleConnectionsPerIp }),
              },
            },
            {
              id: "blockPrivilegedPorts",
              label: t("next.torrents.blockPrivilegedPorts"),
              keywords: "privileged ports peers",
              control: {
                kind: "toggle",
                value: values.torrent.blockPrivilegedPorts,
                onChange: (blockPrivilegedPorts) => patchTorrent({ blockPrivilegedPorts }),
              },
            },
            {
              id: "stopTrackerTimeoutSecs",
              label: t("next.torrents.stopTrackerTimeout"),
              keywords: "tracker stopped timeout",
              control: {
                kind: "number",
                value: values.torrent.stopTrackerTimeoutSecs,
                min: 0,
                suffix: t("next.general.seconds"),
                onChange: (next) => patchTorrent({ stopTrackerTimeoutSecs: Math.max(0, Math.round(next)) }),
              },
            },
            {
              id: "dhtBootstrapNodes",
              label: t("next.torrents.dhtBootstrapNodes"),
              keywords: "dht bootstrap nodes",
              control: {
                kind: "text",
                mono: true,
                value: values.torrent.dhtBootstrapNodes,
                onChange: (dhtBootstrapNodes) => patchTorrent({ dhtBootstrapNodes }),
              },
            },
          ],
        },
      ]
    : [];

  return (
    <>
      <SettingsBlocks blocks={blocks} loading={fetching && !data} />
      {trackerRules.overlays}
    </>
  );
}
