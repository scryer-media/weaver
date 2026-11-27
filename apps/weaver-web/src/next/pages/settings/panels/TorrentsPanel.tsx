// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useMemo, useRef, useState, type ReactNode } from "react";
import { Globe, Lock } from "lucide-react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import {
  APPLY_TRACKER_FILE_MUTATION,
  DELETE_TRACKER_RULE_MUTATION,
  PREVIEW_TRACKER_FILE_MUTATION,
  SAVE_TRACKER_RULE_MUTATION,
  TORRENT_ROUTES_QUERY,
  TORRENT_SETTINGS_QUERY,
  TRACKER_HOSTS_QUERY,
  TRACKER_RULES_QUERY,
  UNBIND_TRACKER_RULE_MUTATION,
  UPDATE_TORRENT_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import {
  UNPACK_CHOICES,
  routeExposureText,
  routeReasonText,
  type TorrentRouteReason,
  unpackChoice,
  unpackOf,
  type TorrentExposure,
} from "@/lib/torrents";
import type { ResolvedBy } from "@/lib/test-via";
import {
  announceHosts,
  domainPattern,
  patternParts,
  patternsOf,
  torrentTrackers,
  trackerHost,
  typedPattern,
} from "@/lib/tracker-hosts";
import { cn, toBase64 } from "@/lib/utils";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { Dialog } from "../../../components/Dialog";
import {
  asksHowTorrentsEnd,
  stopNowInput,
  torrentSettingsDisclosed,
  torrentSwitchWaits,
  windDownStoppable,
  type TorrentDisableMode,
} from "../../../data/torrents-shown";
import { testIds } from "@/next/testids";
import {
  CheckBox,
  PrimaryButton,
  SecondaryButton,
  Segmented,
  Select,
  TextField,
  Toggle,
} from "../../../components/controls";
import { RecordEditor } from "../../../components/RecordEditor";
import { Cell } from "../../../components/rows";
import { EM_DASH } from "../../../data/format";
import { useNextData } from "../../../data/next-data";
import {
  SettingsBlocks,
  useDraft,
  usePanelStatus,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";
import {
  GoalControl,
  INHERIT,
  durationText,
  globalUploadCapField,
  leafFrom,
  leafInput,
  leafValue,
  seedingAtGlobal,
  seedingFields,
  seedingFrom,
  seedingInherits,
  seedingEffectiveText,
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
  /** What the operator calls the tracker. */
  name: string;
  /** The hosts it covers: a host, or `*.` and a domain for it and every host under it. */
  hostPatterns: string[];
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
  /** Each null inherits the global value; the rule governs its torrents' dials only. */
  peerTcp: boolean | null;
  peerUtp: boolean | null;
  preferDownloading: PeerTransport | null;
  preferSeeding: PeerTransport | null;
  /** The upload cap its torrents take in place of their category's and the global one. */
  uploadCap: Leaf<number>;
  forced: boolean;
  /** Whether it holds a certificate for its SSL torrents; the key is never read back. */
  hasSsl: boolean;
  sslSubject: string | null;
  /** Milliseconds since the epoch. */
  sslNotAfter: number | null;
  /** The tracker file that filled it; null when none binds it. */
  bound: TrackerBinding | null;
  /** How many archive passwords its tracker file gave it; the values are never read back. */
  archivePasswordCount: number;
}

interface TrackerBinding {
  name: string;
  revision: string;
  /** Milliseconds since the epoch. */
  importedAt: number;
  /**
   * The fields the file locked: dht, pex, lsd, encryption, peer_tcp, peer_utp,
   * prefer_downloading, prefer_seeding, ssl.
   */
  locked: string[];
  /** The hosts the file covers, which the rule keeps while bound. */
  hostPatterns: string[];
}

/** What importing a tracker file would do; it holds nothing secret. */
interface TrackerFilePreview {
  name: string;
  revision: string;
  hostPatterns: string[];
  /** Null makes a new rule. */
  ruleId: number | null;
  ruleName: string | null;
  changes: {
    field: string;
    locked: boolean;
    old: string | null;
    new: string | null;
  }[];
  hasSsl: boolean;
  sslSubject: string | null;
  sslNotAfter: number | null;
  /** Null when the file names none. */
  archivePasswordCount: number | null;
  fingerprint: string;
}

type TorrentClient = "WEAVER" | "QBITTORRENT";

const TORRENT_CLIENTS: readonly { value: TorrentClient; label: string }[] = [
  { value: "WEAVER", label: "Weaver" },
  { value: "QBITTORRENT", label: "qBittorrent" },
];

interface TrackerRuleForm {
  name: string;
  hostPatterns: string[];
  seeding: SeedingPolicy;
  neverPause: boolean;
  unpack: boolean | null;
  client: TorrentClient | null;
  torrentRoute: boolean;
  dht: boolean | null;
  pex: boolean | null;
  lsd: boolean | null;
  encryption: TorrentEncryption | null;
  peerTcp: boolean | null;
  peerUtp: boolean | null;
  preferDownloading: PeerTransport | null;
  preferSeeding: PeerTransport | null;
  uploadCap: Leaf<number>;
  forced: boolean;
  /** A certificate and key pasted to replace the rule's; both blank keeps it. */
  certificatePem: string;
  privateKeyPem: string;
  /** The rule's certificate is to be cleared. */
  sslCleared: boolean;
  /** The rule's archive passwords are to be cleared. */
  passwordsCleared: boolean;
}

const NEW_TRACKER_RULE: TrackerRuleForm = {
  name: "",
  hostPatterns: [],
  seeding: seedingFrom(null),
  neverPause: false,
  unpack: null,
  client: null,
  torrentRoute: false,
  dht: null,
  pex: null,
  lsd: null,
  encryption: null,
  peerTcp: null,
  peerUtp: null,
  preferDownloading: null,
  preferSeeding: null,
  uploadCap: INHERIT,
  forced: false,
  certificatePem: "",
  privateKeyPem: "",
  sslCleared: false,
  passwordsCleared: false,
};

/** The rule's peer sources beside its trackers, each switched on its own. */
const DISCOVERY_SWITCHES = [
  {
    key: "dht",
    label: "next.torrents.trackerRuleDht",
    keywords: "dht peer discovery private",
  },
  {
    key: "pex",
    label: "next.torrents.trackerRulePex",
    keywords: "pex peer exchange private",
  },
  {
    key: "lsd",
    label: "next.torrents.trackerRuleLsd",
    keywords: "lsd local peer discovery private",
  },
] as const;

/** A rule's tracker as private or public: two buttons, one pressed. */
function TrackerTypePicker({
  label,
  privateLabel,
  publicLabel,
  value,
  onChange,
}: {
  label: string;
  privateLabel: string;
  publicLabel: string;
  /** Null while neither is picked. */
  value: "private" | "public" | null;
  onChange: (isPrivate: boolean) => void;
}) {
  const choices = [
    { value: true, text: privateLabel, Icon: Lock },
    { value: false, text: publicLabel, Icon: Globe },
  ];
  return (
    <div
      role="radiogroup"
      aria-label={label}
      className="grid w-full max-w-[420px] grid-cols-2 gap-2"
    >
      {choices.map(({ value: choice, text, Icon }) => {
        const active = value !== null && choice === (value === "private");
        return (
          <button
            key={text}
            type="button"
            role="radio"
            aria-checked={active}
            onClick={() => onChange(choice)}
            className={cn(
              "flex cursor-pointer flex-col items-center gap-2 border px-4 py-4 text-center",
              active
                ? "border-wv-accent bg-wv-segment-active"
                : "border-wv-control bg-wv-input hover:border-wv-control-focus",
            )}
          >
            <Icon
              aria-hidden="true"
              strokeWidth={1.5}
              className={cn(
                "h-6 w-6",
                active ? "text-wv-accent" : "text-wv-muted",
              )}
            />
            <span
              className={cn(
                "font-wv-title text-[13.5px] font-semibold",
                active ? "text-wv-strong" : "text-wv-fg",
              )}
            >
              {text}
            </span>
          </button>
        );
      })}
    </div>
  );
}

/** Whether a rule sets anything the editor keeps behind its advanced settings. */
function hasAdvanced(rule: TrackerRule): boolean {
  return (
    rule.peerTcp !== null ||
    rule.peerUtp !== null ||
    rule.preferDownloading !== null ||
    rule.preferSeeding !== null ||
    JSON.stringify(leafFrom(rule.uploadCap)) !== JSON.stringify(INHERIT) ||
    rule.forced ||
    rule.hasSsl
  );
}

/** The certificate a rule's save sends: pasted, cleared, or left out to keep the rule's. */
function sslInput(form: TrackerRuleForm) {
  if (form.certificatePem.trim() !== "" || form.privateKeyPem.trim() !== "") {
    return {
      ssl: {
        certificatePem: form.certificatePem,
        privateKeyPem: form.privateKeyPem,
      },
    };
  }
  return form.sslCleared ? { ssl: null } : {};
}

/**
 * A rule's goals in one line, as its row shows them: each goal in effect,
 * the ones it inherits from the global settings dimmed.
 */
function goalsCell(
  t: Translate,
  policy: SeedingPolicy | null,
  global: SeedingPolicy,
): ReactNode {
  const minutes = (value: number) => durationText(t, value);
  const goal = (
    own: Leaf<number> | undefined,
    from: Leaf<number>,
    label: string,
    text: (value: number) => string,
  ) => {
    const inherited = own === undefined || own.state === "INHERIT";
    const leaf = inherited ? from : own;
    const value =
      leaf.state === "SET" && leaf.value !== null
        ? text(leaf.value)
        : t("next.seeding.noGoal");
    return { text: `${label} ${value}`, inherited };
  };
  const goals = [
    goal(
      policy?.ratioGoal,
      global.ratioGoal,
      t("next.seeding.ratioGoal"),
      (value) => value.toFixed(2),
    ),
    goal(
      policy?.seedTimeGoalMinutes,
      global.seedTimeGoalMinutes,
      t("next.seeding.seedTimeGoal"),
      minutes,
    ),
    goal(
      policy?.inactiveGoalMinutes,
      global.inactiveGoalMinutes,
      t("next.seeding.inactiveGoal"),
      minutes,
    ),
  ];
  return goals.map((entry, index) => (
    <span key={entry.text}>
      {index > 0 ? " · " : null}
      <span
        className={entry.inherited ? "text-wv-muted" : undefined}
        title={
          entry.inherited
            ? t("next.torrents.trackerRuleGoalInherited")
            : undefined
        }
      >
        {entry.text}
      </span>
    </span>
  ));
}

type HostSource = "known" | "custom" | "file";

/**
 * A rule's hosts, each a row to tick: ones a torrent has announced to, typed,
 * or the trackers a .torrent file names, read in the browser. A typed domain
 * covers every host under it. A file replaces the list with its trackers: a
 * private one ticks every tracker it names, a public one its main tracker
 * only. The hosts a bound tracker file covers stay ticked.
 */
function HostPicker({
  t,
  value,
  onChange,
  known,
  locked,
  lockedBy,
}: {
  t: Translate;
  value: readonly string[];
  onChange: (patterns: string[]) => void;
  known: readonly string[];
  locked: readonly string[];
  lockedBy: string | null;
}) {
  const [source, setSource] = useState<HostSource>(
    known.length > 0 ? "known" : "custom",
  );
  // Rows offered but not ticked, kept so a host unticked can be ticked again.
  const [offered, setOffered] = useState<string[]>([]);
  const [typed, setTyped] = useState("");
  const [file, setFile] = useState<{ name: string; private: boolean } | null>(
    null,
  );
  const [fileError, setFileError] = useState<string | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const rows = [...new Set([...value, ...offered])];

  const tick = (pattern: string, on: boolean) => {
    onChange(
      on
        ? [...new Set([...value, pattern])]
        : value.filter((kept) => kept !== pattern),
    );
  };
  const add = (patterns: readonly string[], ticked: readonly string[]) => {
    setOffered((current) => [...new Set([...current, ...patterns])]);
    onChange([...new Set([...value, ...ticked])]);
  };

  const read = async (picked: File) => {
    const trackers = torrentTrackers(
      new Uint8Array(await picked.arrayBuffer()),
    );
    const mainHost = trackers?.main ? trackerHost(trackers.main) : null;
    const main = mainHost ? domainPattern(mainHost) : null;
    const patterns = trackers
      ? [
          ...new Set([
            ...(main ? [main] : []),
            ...announceHosts(trackers.all).map(domainPattern),
          ]),
        ]
      : [];
    setFileError(
      trackers === null
        ? t("next.torrents.trackerRuleFileInvalid")
        : patterns.length === 0
          ? t("next.torrents.trackerRuleFileEmpty")
          : null,
    );
    setFile(trackers ? { name: picked.name, private: trackers.private } : null);
    // A file chosen replaces the list with its trackers, so a second file
    // never adds to the first; the hosts a bound tracker file locks stay.
    setOffered(patterns);
    onChange([
      ...new Set([
        ...locked,
        ...(trackers?.private ? patterns : main ? [main] : []),
      ]),
    ]);
  };

  const addTyped = () => {
    const pattern = typedPattern(typed);
    if (pattern !== "") {
      add([pattern], [pattern]);
      setTyped("");
    }
  };

  const label = t("next.torrents.trackerRuleHosts");
  return (
    <div className="flex min-w-0 flex-col gap-2">
      {rows.length > 0 ? (
        <div
          className="flex min-w-0 flex-col border border-wv-control"
          data-testid="tracker-rule-hosts"
        >
          {rows.map((pattern) => {
            const { host, under } = patternParts(pattern);
            const fixed = locked.includes(pattern);
            const ticked = value.includes(pattern);
            return (
              <div
                key={pattern}
                data-testid={`tracker-rule-host-${pattern}`}
                onClick={() => (fixed ? undefined : tick(pattern, !ticked))}
                className={cn(
                  "flex min-w-0 items-center gap-3 border-b border-wv-hairline px-3 py-[7px] last:border-b-0",
                  fixed
                    ? "cursor-default"
                    : "cursor-pointer hover:bg-wv-cell-hover",
                )}
              >
                <CheckBox
                  checked={ticked}
                  disabled={fixed}
                  label={host}
                  onChange={(on) => tick(pattern, on)}
                />
                <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">
                  {host}
                </span>
                <span className="min-w-0 truncate text-[12px] text-wv-muted">
                  {fixed && lockedBy
                    ? t("next.torrents.trackerRuleHostLocked", {
                        name: lockedBy,
                      })
                    : t(
                        under
                          ? "next.torrents.trackerRuleHostUnder"
                          : "next.torrents.trackerRuleHostOnly",
                      )}
                </span>
              </div>
            );
          })}
        </div>
      ) : null}
      <Segmented<HostSource>
        value={source}
        label={label}
        onChange={setSource}
        className="self-start"
        options={[
          {
            value: "known",
            label: t("next.torrents.trackerRuleHostKnown"),
            disabled: known.length === 0,
          },
          { value: "custom", label: t("next.torrents.trackerRuleHostCustom") },
          { value: "file", label: t("next.torrents.trackerRuleHostFile") },
        ]}
      />
      {source === "known" ? (
        <Select
          value=""
          label={label}
          placeholder={t("next.torrents.trackerRuleHostChoose")}
          onChange={(pattern) => add([pattern], [pattern])}
          className="font-wv-mono text-[12px]"
          menuClassName="max-h-[320px]"
          options={known
            .filter((pattern) => !value.includes(pattern))
            .map((pattern) => ({ value: pattern, label: pattern }))}
        />
      ) : source === "custom" ? (
        <div className="flex min-w-0 items-center gap-2">
          <TextField
            value={typed}
            label={label}
            placeholder="tracker.example"
            onChange={setTyped}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                addTyped();
              }
            }}
            className="min-w-0 flex-1"
          />
          <SecondaryButton
            onClick={addTyped}
            disabled={typedPattern(typed) === ""}
          >
            {t("next.torrents.trackerRuleHostAdd")}
          </SecondaryButton>
        </div>
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
            <SecondaryButton
              icon="chooseFile"
              onClick={() => fileRef.current?.click()}
            >
              {t("next.torrents.trackerRuleChooseFile")}
            </SecondaryButton>
            {file ? (
              <span
                className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg"
                title={file.name}
              >
                {file.name}
              </span>
            ) : null}
          </div>
          {fileError ? (
            <span className="text-[12px] text-wv-error-text">{fileError}</span>
          ) : null}
          {file && !fileError ? (
            <span className="text-[12px] leading-[1.45] text-pretty text-wv-muted">
              {t(
                file.private
                  ? "next.torrents.trackerRuleFilePrivate"
                  : "next.torrents.trackerRuleFilePublic",
              )}
            </span>
          ) : null}
        </>
      )}
      <span className="text-[12px] leading-[1.45] text-pretty text-wv-muted">
        {t(
          value.length === 0
            ? "next.torrents.trackerRuleHostsNone"
            : "next.torrents.trackerRuleCovers",
        )}
      </span>
    </div>
  );
}

/** A rule's hosts as its row shows them: the first, and how many more. */
function hostsSummary(t: Translate, patterns: readonly string[]): string {
  const first = patterns[0] ? patternParts(patterns[0]).host : EM_DASH;
  return patterns.length > 1
    ? t("next.torrents.trackerRuleHostsMore", {
        host: first,
        count: patterns.length - 1,
      })
    : first;
}

/** A preview's field names, as the editor labels them. */
const CHANGE_LABELS: Record<string, string> = {
  dht: "next.torrents.trackerRuleDht",
  pex: "next.torrents.trackerRulePex",
  lsd: "next.torrents.trackerRuleLsd",
  encryption: "next.torrents.encryption",
  peer_tcp: "next.torrents.peerTcp",
  peer_utp: "next.torrents.peerUtp",
  prefer_downloading: "next.torrents.preferDownloading",
  prefer_seeding: "next.torrents.preferSeeding",
  "seeding.ratio_goal": "next.seeding.ratioGoal",
  "seeding.seed_time_goal_minutes": "next.seeding.seedTimeGoal",
  "seeding.inactive_goal_minutes": "next.seeding.inactiveGoal",
  "seeding.mode": "next.seeding.mode",
  "seeding.action": "next.seeding.action",
};

const CHANGE_VALUES: Record<string, string> = {
  true: "next.common.on",
  false: "next.common.off",
  prefer: "next.torrents.encryptionPrefer",
  require: "next.torrents.encryptionRequire",
  disabled: "next.torrents.encryptionDisabled",
  any: "next.seeding.modeAny",
  all: "next.seeding.modeAll",
  stop: "next.seeding.actionStop",
  remove: "next.seeding.actionRemove",
  remove_with_data: "next.seeding.actionRemoveWithData",
};

function changeValue(
  t: Translate,
  field: string,
  value: string | null,
): string {
  if (value === null) {
    return EM_DASH;
  }
  if (field.endsWith("_minutes")) {
    return durationText(t, Number(value));
  }
  if (field.startsWith("prefer_")) {
    return value === "utp" ? "uTP" : "TCP";
  }
  const key = CHANGE_VALUES[value];
  return key ? t(key) : value;
}

const sslText = (subject: string | null, notAfter: number | null) =>
  [subject, notAfter === null ? null : new Date(notAfter).toLocaleDateString()]
    .filter((part) => part)
    .join(" · ");

/**
 * Import a tracker file: pick it and its password, preview what it would do,
 * then apply that preview or cancel. Nothing is kept until Apply.
 */
function TrackerFileImport({
  t,
  open,
  onDismiss,
  onApplied,
}: {
  t: Translate;
  open: boolean;
  onDismiss: () => void;
  onApplied: () => void;
}) {
  const [, previewFile] = useMutation(PREVIEW_TRACKER_FILE_MUTATION);
  const [, applyFile] = useMutation(APPLY_TRACKER_FILE_MUTATION);
  const [file, setFile] = useState<{ name: string; data: string } | null>(null);
  const [password, setPassword] = useState("");
  const [summary, setSummary] = useState<TrackerFilePreview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  const variables = () => ({
    file: file?.data ?? "",
    password: password === "" ? null : password,
  });

  const run = async () => {
    if (!file) {
      return;
    }
    setBusy(true);
    setError(null);
    if (summary === null) {
      const result = await previewFile(variables());
      setBusy(false);
      if (result.error) {
        setError(
          result.error.graphQLErrors[0]?.message ?? result.error.message,
        );
        return;
      }
      setSummary(result.data.previewTrackerFile as TrackerFilePreview);
      return;
    }
    const result = await applyFile({
      ...variables(),
      fingerprint: summary.fingerprint,
    });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      setSummary(null);
      return;
    }
    onApplied();
  };

  return (
    <RecordEditor
      open={open}
      title={t("next.torrents.trackerFileImport")}
      error={error}
      busy={busy}
      saveLabel={
        summary === null
          ? t("next.torrents.trackerFilePreview")
          : t("next.torrents.trackerFileApply")
      }
      saveDisabled={file === null}
      onSave={() => void run()}
      onDismiss={onDismiss}
      sections={[
        {
          id: "trackerFile",
          title: t("next.torrents.trackerFile"),
          fields: [
            {
              id: "trackerFileChoose",
              label: t("next.torrents.trackerFile"),
              control: {
                kind: "custom",
                control: (
                  <div className="flex h-[34px] min-w-0 items-center gap-3">
                    <input
                      ref={fileRef}
                      type="file"
                      accept=".toml,.7z,.zip,.rar"
                      aria-label={t("next.torrents.trackerFile")}
                      className="hidden"
                      onChange={(event) => {
                        const picked = event.target.files?.[0] ?? null;
                        // Cleared so the same file can be chosen again.
                        event.target.value = "";
                        if (picked) {
                          void picked.arrayBuffer().then((bytes) => {
                            setFile({
                              name: picked.name,
                              data: toBase64(new Uint8Array(bytes)),
                            });
                            setSummary(null);
                            setError(null);
                          });
                        }
                      }}
                    />
                    <SecondaryButton
                      icon="chooseFile"
                      onClick={() => fileRef.current?.click()}
                    >
                      {t("next.torrents.trackerRuleChooseFile")}
                    </SecondaryButton>
                    {file ? (
                      <span
                        className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg"
                        title={file.name}
                      >
                        {file.name}
                      </span>
                    ) : null}
                  </div>
                ),
              },
            },
            {
              id: "trackerFilePassword",
              label: t("next.torrents.trackerFilePassword"),
              control: {
                kind: "text",
                type: "password",
                secret: true,
                value: password,
                onChange: (next) => {
                  setPassword(next);
                  setSummary(null);
                },
              },
            },
          ],
        },
      ]}
    >
      {summary ? (
        <div className="grid flex-none grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)_auto] gap-x-4 gap-y-[6px] border-t border-wv-hairline px-4 py-4 text-[12.5px] sm:px-6">
          <span className="text-wv-secondary">
            {t("next.torrents.trackerFile")}
          </span>
          <span className="col-span-2 min-w-0 truncate font-wv-mono text-wv-fg">
            {summary.name} · {summary.revision}
          </span>
          <span className="text-wv-secondary">
            {t("next.torrents.trackerRuleHosts")}
          </span>
          <span
            className="min-w-0 truncate font-wv-mono text-wv-fg"
            title={summary.hostPatterns.join(", ")}
          >
            {hostsSummary(t, summary.hostPatterns)}
          </span>
          <span className="min-w-0 truncate text-wv-muted">
            {summary.ruleName ?? t("next.torrents.trackerFileNewRule")}
          </span>
          {summary.changes.map((change) => (
            <div key={change.field} className="contents">
              <span className="text-wv-secondary">
                {CHANGE_LABELS[change.field]
                  ? t(CHANGE_LABELS[change.field])
                  : change.field}
              </span>
              <span className="min-w-0 truncate font-wv-mono text-wv-fg">
                {changeValue(t, change.field, change.old)} →{" "}
                {changeValue(t, change.field, change.new)}
              </span>
              <span className="font-wv-mono text-wv-muted">
                {t(
                  change.locked
                    ? "next.torrents.trackerFileLocked"
                    : "next.torrents.trackerFileRecommended",
                )}
              </span>
            </div>
          ))}
          {summary.hasSsl ? (
            <>
              <span className="text-wv-secondary">
                {t("next.torrents.trackerRuleCertificate")}
              </span>
              <span className="min-w-0 truncate font-wv-mono text-wv-fg">
                {sslText(summary.sslSubject, summary.sslNotAfter)}
              </span>
              <span className="font-wv-mono text-wv-muted">
                {t("next.torrents.trackerFileLocked")}
              </span>
            </>
          ) : null}
          {summary.archivePasswordCount === null ? null : (
            <>
              <span className="text-wv-secondary">
                {t("next.torrents.trackerFileArchivePasswords")}
              </span>
              <span className="col-span-2 font-wv-mono text-wv-fg">
                {summary.archivePasswordCount}
              </span>
            </>
          )}
        </div>
      ) : null}
    </RecordEditor>
  );
}

/** A locked field's control, disabled, with the tracker that locked it beside it. */
function Locked({ by, children }: { by: string; children: ReactNode }) {
  return (
    <div className="flex min-w-0 items-center gap-[10px]">
      {children}
      <span className="min-w-0 truncate font-wv-mono text-[11.5px] text-wv-muted">
        {by}
      </span>
    </div>
  );
}

interface TorrentPortMapping {
  mapper: "UPNP" | "NAT_PMP" | "PCP_IPV6";
  protocol: "TCP" | "UDP";
  localPort: number;
  /** Null once the mapping failed or was removed. */
  externalPort: number | null;
  gatewayAddress: string | null;
  error: string | null;
}

interface TorrentRoute {
  key: string;
  scopes: {
    scope: "DEFAULT" | "CATEGORY" | "TRACKER_RULE";
    name: string | null;
  }[];
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

const MAPPERS: Record<TorrentPortMapping["mapper"], string> = {
  UPNP: "UPnP",
  NAT_PMP: "NAT-PMP",
  PCP_IPV6: "PCP (IPv6)",
};

function mappingText(mapping: TorrentPortMapping): string {
  const what = `${MAPPERS[mapping.mapper]} ${mapping.protocol}`;
  if (mapping.error !== null) {
    return `${what}: ${mapping.error}`;
  }
  if (mapping.externalPort === null) {
    return `${what}: ${EM_DASH}`;
  }
  const gateway =
    mapping.gatewayAddress === null ? "" : ` ${mapping.gatewayAddress}`;
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
    scope.scope === "DEFAULT"
      ? t("next.torrents.routeDefault")
      : (scope.name ?? EM_DASH);
  return {
    kind: "table",
    id: "torrentRoutes",
    title: t("next.torrents.routes"),
    columns:
      "minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.4fr) minmax(0, 1fr) minmax(0, 1.6fr) minmax(0, 1.2fr)",
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
        route.listenAddress === null
          ? EM_DASH
          : `${route.listenAddress}${route.utp ? " TCP+uTP" : " TCP"}`;
      const mappings =
        route.mappings.length === 0
          ? EM_DASH
          : route.mappings.map(mappingText).join("; ");
      const external =
        route.externalAddresses.length === 0
          ? EM_DASH
          : route.externalAddresses.join(", ");
      const exposure = routeExposureText(t, route);
      const status =
        route.reason === null
          ? t(
              route.open
                ? "next.torrents.routeOpen"
                : "next.torrents.routeIdle",
            )
          : routeReasonText(t, route.reason);
      return {
        id: route.key,
        searchText: [scopes, listen, mappings, external, exposure, status].join(
          " ",
        ),
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
            className={
              route.mappings.some((mapping) => mapping.error !== null)
                ? "text-wv-error-text"
                : undefined
            }
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
            className={
              route.reason !== null ? "text-wv-error-text" : "text-wv-muted"
            }
          >
            {status}
          </Cell>,
        ],
      };
    }),
  };
}

/** The tracker rules' table, and the editor and confirmation it opens. */
/**
 * The tracker rules table and editor. `global` is the saved connection
 * settings, which a rule's transports resolve over.
 */
function useTrackerRules(
  t: Translate,
  global: TorrentConnection,
  globalSeeding: SeedingPolicy,
  globalUploadCap: number | null,
): { block: SettingsBlock; overlays: ReactNode } {
  const [{ data }, reexecute] = useQuery<{ trackerRules: TrackerRule[] }>({
    query: TRACKER_RULES_QUERY,
  });
  const [, saveRule] = useMutation(SAVE_TRACKER_RULE_MUTATION);
  const [, deleteRule] = useMutation(DELETE_TRACKER_RULE_MUTATION);
  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const [form, setForm] = useState<TrackerRuleForm>(NEW_TRACKER_RULE);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<TrackerRule | null>(null);
  const [confirmUnbind, setConfirmUnbind] = useState<TrackerRule | null>(null);
  const [, unbindRule] = useMutation(UNBIND_TRACKER_RULE_MUTATION);
  // Counts the import's openings, so each one starts empty; zero is closed.
  const [importing, setImporting] = useState(0);
  const [importOpen, setImportOpen] = useState(false);
  // Counts the editor's openings, so each one starts its host picker afresh.
  const [opening, setOpening] = useState(0);
  // Whether the editor shows its advanced settings.
  const [advanced, setAdvanced] = useState(false);
  // Private or public, picked first: a new rule's other fields wait for it.
  const [trackerType, setTrackerType] = useState<"private" | "public" | null>(
    null,
  );
  const [{ data: hosts }] = useQuery<{ trackerHosts: string[] }>({
    query: TRACKER_HOSTS_QUERY,
    pause: editingId === null,
    requestPolicy: "cache-and-network",
  });
  const known = useMemo(
    () => patternsOf(hosts?.trackerHosts ?? []),
    [hosts?.trackerHosts],
  );

  const rules = useMemo(() => data?.trackerRules ?? [], [data?.trackerRules]);
  const editing =
    typeof editingId === "number"
      ? rules.find((rule) => rule.id === editingId)
      : null;
  // The tracker that locked `field` on the rule being edited.
  const lockedBy = (field: string) =>
    editing?.bound?.locked.includes(field) ? editing.bound.name : null;
  // A private tracker's rule turns every other peer source off.
  const privateTracker = trackerType === "private";

  const open = (rule: TrackerRule | null) => {
    setError(null);
    const start = rule
      ? {
          name: rule.name,
          hostPatterns: rule.hostPatterns,
          seeding: seedingFrom(rule.seeding),
          neverPause: rule.neverPause,
          unpack: rule.unpack,
          client: rule.client,
          torrentRoute: rule.torrentRoute,
          dht: rule.dht,
          pex: rule.pex,
          lsd: rule.lsd,
          encryption: rule.encryption,
          peerTcp: rule.peerTcp,
          peerUtp: rule.peerUtp,
          preferDownloading: rule.preferDownloading,
          preferSeeding: rule.preferSeeding,
          uploadCap: leafFrom(rule.uploadCap),
          forced: rule.forced,
          certificatePem: "",
          privateKeyPem: "",
          sslCleared: false,
          passwordsCleared: false,
        }
      : NEW_TRACKER_RULE;
    setForm(start);
    setAdvanced(rule !== null && hasAdvanced(rule));
    setTrackerType(
      rule === null
        ? null
        : rule.dht === false && rule.pex === false && rule.lsd === false
          ? "private"
          : "public",
    );
    setOpening((count) => count + 1);
    setEditingId(rule ? rule.id : "new");
  };

  const save = async () => {
    setBusy(true);
    const result = await saveRule({
      id: editingId === "new" ? null : editingId,
      input: {
        name: form.name.trim(),
        hostPatterns: form.hostPatterns,
        seeding: seedingInherits(form.seeding)
          ? null
          : seedingInput(form.seeding),
        neverPause: form.neverPause,
        unpack: form.unpack,
        client: form.client,
        torrentRoute: form.torrentRoute,
        dht: form.dht,
        pex: form.pex,
        lsd: form.lsd,
        encryption: form.encryption,
        peerTcp: form.peerTcp,
        peerUtp: form.peerUtp,
        preferDownloading: form.preferDownloading,
        preferSeeding: form.preferSeeding,
        uploadCap: leafInput(form.uploadCap),
        forced: form.forced,
        ...sslInput(form),
        clearArchivePasswords: form.passwordsCleared,
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

  const unbind = async () => {
    if (!confirmUnbind) {
      return;
    }
    setBusy(true);
    const result = await unbindRule({ id: confirmUnbind.id });
    setBusy(false);
    setConfirmUnbind(null);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
    }
    void reexecute({ requestPolicy: "network-only" });
  };

  const lockable = (
    field: string,
    value: string,
    options: readonly { value: string; label: string }[],
    spec: FieldSpec,
  ): FieldSpec => {
    const by = lockedBy(field);
    return by === null
      ? spec
      : {
          ...spec,
          control: {
            kind: "custom",
            control: (
              <Locked by={by}>
                <Segmented
                  value={value}
                  label={spec.label}
                  onChange={() => undefined}
                  options={options.map((option) => ({
                    ...option,
                    disabled: true,
                  }))}
                />
              </Locked>
            ),
          },
        };
  };

  // What each field left to inherit gives the rule's torrents now, by field id.
  const effective = (() => {
    const onOff = (on: boolean) => t(on ? "next.common.on" : "next.common.off");
    const transport = (prefer: PeerTransport) =>
      PEER_TRANSPORTS.find((option) => option.value === prefer)?.label ??
      prefer;
    const only = onlyTransport(
      form.peerTcp ?? global.peerTcp,
      form.peerUtp ?? global.peerUtp,
    );
    const seedingText = seedingEffectiveText(
      t,
      globalSeeding,
      globalUploadCap,
      "trackerRuleSeeding",
      "trackerRule",
    );
    const inherits = (leaf: Leaf<unknown>) => leaf.state === "INHERIT";
    const setsGoal = [
      form.seeding.ratioGoal,
      form.seeding.seedTimeGoalMinutes,
      form.seeding.inactiveGoalMinutes,
    ].some((leaf) => leaf.state === "SET");
    const ruleGoalsOnly = setsGoal && leafValue(form.seeding.mode) !== "ALL";
    const encryption = ENCRYPTIONS.find(
      (option) => option.value === global.encryption,
    );
    const entries: [string, boolean, string][] = [
      ["unpack", form.unpack === null, onOff(global.unpack)],
      ...DISCOVERY_SWITCHES.map(({ key }): [string, boolean, string] => [
        key,
        form[key] === null,
        onOff(true),
      ]),
      [
        "encryption",
        form.encryption === null,
        encryption ? t(encryption.label) : global.encryption,
      ],
      ["peerTcp", form.peerTcp === null, onOff(global.peerTcp)],
      ["peerUtp", form.peerUtp === null, onOff(global.peerUtp)],
      [
        "preferDownloading",
        form.preferDownloading === null && only === null,
        transport(global.preferDownloading),
      ],
      [
        "preferSeeding",
        form.preferSeeding === null && only === null,
        transport(global.preferSeeding),
      ],
      [
        "trackerRule-uploadCap",
        inherits(form.uploadCap),
        seedingText["trackerRule-uploadCap"],
      ],
      // A goal left to inherit comes from below only under the rule's own
      // All; under Any, once the rule sets a goal, it is off.
      ...(
        ["ratioGoal", "seedTimeGoalMinutes", "inactiveGoalMinutes"] as const
      ).map((key): [string, boolean, string] => [
        `trackerRuleSeeding-${key}`,
        inherits(form.seeding[key]),
        ruleGoalsOnly
          ? t("next.seeding.noGoal")
          : seedingText[`trackerRuleSeeding-${key}`],
      ]),
      [
        "trackerRuleSeeding-action",
        inherits(form.seeding.action),
        seedingText["trackerRuleSeeding-action"],
      ],
    ];
    return new Map(
      entries
        .filter(([, inheriting]) => inheriting)
        .map(([id, , value]) => [
          id,
          t("next.torrents.trackerRuleEffective", { value }),
        ]),
    );
  })();
  // Each field left to inherit says, under its control, what it gives.
  const withEffective = (field: FieldSpec): FieldSpec => {
    const under = effective.get(field.id);
    return under === undefined ? field : { ...field, under };
  };

  const block: SettingsBlock = {
    kind: "table",
    id: "trackerRules",
    title: t("next.torrents.trackerRules"),
    columns:
      "minmax(0, 0.8fr) minmax(0, 1fr) minmax(0, 1.6fr) minmax(0, 0.6fr)",
    headers: [
      t("next.torrents.trackerRuleName"),
      t("next.torrents.trackerRuleHosts"),
      t("next.torrents.trackerRuleGoals"),
      t("next.torrents.trackerRuleNeverPause"),
    ],
    centered: [3],
    empty: t("next.torrents.trackerRulesEmpty"),
    add: {
      label: t("next.torrents.trackerRuleAdd"),
      onClick: () => open(null),
    },
    footer: (
      <SecondaryButton
        icon="chooseFile"
        onClick={() => {
          setImporting((count) => count + 1);
          setImportOpen(true);
        }}
      >
        {t("next.torrents.trackerFileImport")}
      </SecondaryButton>
    ),
    onRowClick: (id) => {
      const rule = rules.find((entry) => String(entry.id) === id);
      if (rule) {
        open(rule);
      }
    },
    rows: rules.map((rule) => ({
      id: String(rule.id),
      searchText: [rule.name, ...rule.hostPatterns].join(" "),
      cells: [
        <Cell key="name">{rule.name}</Cell>,
        <Cell key="hosts" mono className="text-wv-secondary">
          <span title={rule.hostPatterns.join(", ")}>
            {hostsSummary(t, rule.hostPatterns)}
          </span>
        </Cell>,
        <Cell key="goals" className="text-wv-secondary">
          {goalsCell(t, rule.seeding, globalSeeding)}
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
        title={
          editingId === "new"
            ? t("next.torrents.trackerRuleAdd")
            : (editing?.name ?? t("next.torrents.trackerRule"))
        }
        note={
          editingId === "new"
            ? t("next.torrents.trackerRuleNewNote")
            : `#${editing?.id ?? ""}`
        }
        error={error}
        busy={busy}
        onSave={() => void save()}
        saveDisabled={trackerType === null}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmDelete(editing) : undefined}
        deleteLabel={t("next.rss.removeRule")}
        sections={[
          {
            id: "trackerRule",
            title: t("next.torrents.trackerRule"),
            fields: [
              {
                id: "trackerType",
                label: t("next.torrents.trackerRuleType"),
                keywords: "private public tracker dht pex lsd",
                wide: true,
                control: {
                  kind: "custom",
                  // Not saved: read from the peer sources it sets.
                  control: (
                    <TrackerTypePicker
                      label={t("next.torrents.trackerRuleType")}
                      privateLabel={t("next.torrents.trackerRulePrivate")}
                      publicLabel={t("next.torrents.trackerRulePublic")}
                      value={trackerType}
                      onChange={(isPrivate) => {
                        setTrackerType(isPrivate ? "private" : "public");
                        setForm((current) => {
                          const next = { ...current };
                          for (const { key } of DISCOVERY_SWITCHES) {
                            if (lockedBy(key) === null) {
                              next[key] = isPrivate ? false : null;
                            }
                          }
                          return next;
                        });
                      }}
                    />
                  ),
                },
              },
            ],
            // Every other field slides open as one once the tracker's type is picked.
            folded: {
              open: trackerType !== null,
              fields: (
                [
                  ...(editing?.bound
                    ? [
                        {
                          id: "trackerFile",
                          label: t("next.torrents.trackerFile"),
                          keywords: "tracker file bound unbind",
                          control: {
                            kind: "custom" as const,
                            control: (
                              <div className="flex min-w-0 items-center gap-[10px]">
                                <span className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg">
                                  {editing.bound.name} ·{" "}
                                  {editing.bound.revision}
                                </span>
                                <SecondaryButton
                                  onClick={() => setConfirmUnbind(editing)}
                                >
                                  {t("next.torrents.trackerFileUnbind")}
                                </SecondaryButton>
                              </div>
                            ),
                          },
                        },
                      ]
                    : ([] as FieldSpec[])),
                  {
                    id: "name",
                    label: t("next.torrents.trackerRuleName"),
                    keywords: "tracker name label",
                    control: {
                      kind: "custom",
                      control: (
                        <TextField
                          value={form.name}
                          label={t("next.torrents.trackerRuleName")}
                          placeholder="IPT"
                          mono={false}
                          onChange={(name) =>
                            setForm((current) => ({ ...current, name }))
                          }
                        />
                      ),
                    },
                  },
                  {
                    id: "hostPatterns",
                    label: t("next.torrents.trackerRuleHosts"),
                    help: t("next.torrents.trackerRuleHostsNote"),
                    keywords: "tracker host domain announce",
                    stacked: true,
                    control: {
                      kind: "custom",
                      control: (
                        <HostPicker
                          key={opening}
                          t={t}
                          value={form.hostPatterns}
                          known={known}
                          locked={editing?.bound?.hostPatterns ?? []}
                          lockedBy={editing?.bound?.name ?? null}
                          onChange={(hostPatterns) =>
                            setForm((current) => ({
                              ...current,
                              hostPatterns,
                              // A new rule is named after its first host until a name is typed.
                              name:
                                current.name.trim() === "" && hostPatterns[0]
                                  ? patternParts(hostPatterns[0]).host
                                  : current.name,
                            }))
                          }
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
                      onChange: (neverPause) =>
                        setForm((current) => ({ ...current, neverPause })),
                    },
                  },
                  {
                    id: "unpack",
                    label: t("next.torrents.unpack"),
                    keywords: "unpack extract archive",
                    control: {
                      kind: "segmented",
                      value: unpackChoice(form.unpack),
                      options: UNPACK_CHOICES.map((option) => ({
                        ...option,
                        label: t(option.label),
                      })),
                      onChange: (choice) =>
                        setForm((current) => ({
                          ...current,
                          unpack: unpackOf(choice),
                        })),
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
                          client:
                            client === "WEAVER"
                              ? null
                              : (client as TorrentClient),
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
                            onChange={(torrentRoute) =>
                              setForm((current) => ({
                                ...current,
                                torrentRoute,
                              }))
                            }
                          />
                          {form.torrentRoute &&
                          typeof editingId === "number" ? (
                            <Link
                              to={`/settings/networking/routes?consumer=torrent:tracker:${editingId}`}
                              className="font-wv-mono text-[11.5px] text-wv-accent hover:text-wv-accent-hover"
                            >
                              {t("next.torrents.trackerRuleRouteOwn")}
                            </Link>
                          ) : form.torrentRoute ? (
                            <span className="font-wv-mono text-[11.5px] text-wv-muted">
                              {t("next.torrents.trackerRuleRouteOwn")}
                            </span>
                          ) : null}
                        </div>
                      ),
                    },
                  },
                  ...DISCOVERY_SWITCHES.map(({ key, label, keywords }) => {
                    const options = UNPACK_CHOICES.map((option) => ({
                      ...option,
                      label: t(option.label),
                    }));
                    return {
                      ...lockable(key, unpackChoice(form[key]), options, {
                        id: key,
                        label: t(label),
                        keywords,
                        control: {
                          kind: "segmented" as const,
                          value: unpackChoice(form[key]),
                          options,
                          onChange: (choice: string) =>
                            setForm((current) => ({
                              ...current,
                              [key]: unpackOf(choice),
                            })),
                        },
                      }),
                      // A private tracker's torrents use no other peer source.
                      collapsed: privateTracker,
                    };
                  }),
                  ((options) =>
                    lockable(
                      "encryption",
                      form.encryption ?? "inherit",
                      options,
                      {
                        id: "encryption",
                        label: t("next.torrents.encryption"),
                        keywords: "encryption mse pe",
                        control: {
                          kind: "segmented",
                          value: form.encryption ?? "inherit",
                          options,
                          onChange: (encryption) =>
                            setForm((current) => ({
                              ...current,
                              encryption:
                                encryption === "inherit"
                                  ? null
                                  : (encryption as TorrentEncryption),
                            })),
                        },
                      },
                    ))([
                    {
                      value: "inherit",
                      label: t("next.torrents.unpackInherit"),
                    },
                    ...ENCRYPTIONS.map((option) => ({
                      ...option,
                      label: t(option.label),
                    })),
                  ]),
                  ...(editing &&
                  editing.archivePasswordCount > 0 &&
                  !form.passwordsCleared
                    ? [
                        {
                          id: "archivePasswords",
                          label: t("next.torrents.trackerFileArchivePasswords"),
                          keywords: "archive passwords tracker",
                          control: {
                            kind: "custom" as const,
                            control: (
                              <div className="flex min-w-0 items-center gap-[10px]">
                                <span className="font-wv-mono text-[12px] text-wv-fg">
                                  {editing.archivePasswordCount}
                                </span>
                                <SecondaryButton
                                  onClick={() =>
                                    setForm((current) => ({
                                      ...current,
                                      passwordsCleared: true,
                                    }))
                                  }
                                >
                                  {t(
                                    "next.torrents.trackerRuleCertificateClear",
                                  )}
                                </SecondaryButton>
                              </div>
                            ),
                          },
                        },
                      ]
                    : ([] as FieldSpec[])),
                  ...seedingFields(
                    t,
                    form.seeding,
                    (seeding) =>
                      setForm((current) => ({ ...current, seeding })),
                    {
                      inherits: true,
                      idPrefix: "trackerRuleSeeding",
                    },
                  ),
                  {
                    id: "advanced",
                    label: t("next.torrents.trackerRuleAdvanced"),
                    keywords:
                      "advanced transport tcp utp prefer upload speed limit forced ssl certificate",
                    control: {
                      kind: "toggle",
                      value: advanced,
                      onChange: setAdvanced,
                    },
                  },
                  // Shown behind the toggle above.
                  ...(
                    [
                      // Each transport field inherits the global one while unset. A
                      // preference is kept as set; while the resolved transports leave
                      // one off, it shows locked to the one left on.
                      ...TRANSPORT_SWITCHES.map(
                        ({ key, field, label, keywords }) => {
                          const options = UNPACK_CHOICES.map((option) => ({
                            ...option,
                            label: t(option.label),
                          }));
                          return lockable(
                            field,
                            unpackChoice(form[key]),
                            options,
                            {
                              id: key,
                              label: t(label),
                              keywords,
                              control: {
                                kind: "segmented" as const,
                                value: unpackChoice(form[key]),
                                options,
                                onChange: (choice: string) =>
                                  setForm((current) => ({
                                    ...current,
                                    [key]: unpackOf(choice),
                                  })),
                              },
                            },
                          );
                        },
                      ),
                      ...TRANSPORT_PREFERENCES.map(
                        ({ key, field, label, keywords }) => {
                          const only = onlyTransport(
                            form.peerTcp ?? global.peerTcp,
                            form.peerUtp ?? global.peerUtp,
                          );
                          const options = [
                            {
                              value: "inherit",
                              label: t("next.torrents.unpackInherit"),
                            },
                            ...PEER_TRANSPORTS,
                          ].map((option) => ({
                            ...option,
                            disabled: only !== null,
                          }));
                          const value = only ?? form[key] ?? "inherit";
                          return lockable(field, value, options, {
                            id: key,
                            label: t(label),
                            keywords,
                            control: {
                              kind: "segmented" as const,
                              value,
                              options,
                              onChange: (prefer: string) =>
                                setForm((current) => ({
                                  ...current,
                                  [key]:
                                    prefer === "inherit"
                                      ? null
                                      : (prefer as PeerTransport),
                                })),
                            },
                          });
                        },
                      ),
                      uploadCapField(
                        t,
                        form.uploadCap,
                        (uploadCap) =>
                          setForm((current) => ({ ...current, uploadCap })),
                        "trackerRule",
                      ),
                      {
                        id: "forced",
                        label: t("next.torrents.trackerRuleForced"),
                        keywords: "forced queue start",
                        control: {
                          kind: "toggle",
                          value: form.forced,
                          onChange: (forced) =>
                            setForm((current) => ({ ...current, forced })),
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
                                      {sslText(
                                        editing.sslSubject,
                                        editing.sslNotAfter,
                                      )}
                                    </span>
                                    {lockedBy("ssl") !== null ? (
                                      <span className="min-w-0 truncate font-wv-mono text-[11.5px] text-wv-muted">
                                        {lockedBy("ssl")}
                                      </span>
                                    ) : (
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
                                        {t(
                                          "next.torrents.trackerRuleCertificateClear",
                                        )}
                                      </SecondaryButton>
                                    )}
                                  </div>
                                ),
                              },
                            },
                          ]
                        : ([] as FieldSpec[])),
                      ...(lockedBy("ssl") !== null
                        ? ([] as FieldSpec[])
                        : ([
                            {
                              id: "certificatePem",
                              label: t(
                                "next.torrents.trackerRuleCertificatePem",
                              ),
                              keywords: "ssl certificate tls pem",
                              control: {
                                kind: "textarea",
                                rows: 4,
                                placeholder: "-----BEGIN CERTIFICATE-----",
                                value: form.certificatePem,
                                onChange: (certificatePem) =>
                                  setForm((current) => ({
                                    ...current,
                                    certificatePem,
                                  })),
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
                                onChange: (privateKeyPem) =>
                                  setForm((current) => ({
                                    ...current,
                                    privateKeyPem,
                                  })),
                              },
                            },
                          ] as FieldSpec[])),
                    ] as FieldSpec[]
                  ).map((field) => ({ ...field, collapsed: !advanced })),
                ] as FieldSpec[]
              ).map(withEffective),
            },
          },
        ]}
      />

      <ConfirmDialog
        open={confirmUnbind !== null}
        title={t("next.torrents.trackerFileUnbind")}
        note={confirmUnbind?.bound?.name}
        busy={busy}
        confirmLabel={t("next.torrents.trackerFileUnbind")}
        body={t("next.torrents.trackerFileUnbindBody")}
        onConfirm={() => void unbind()}
        onDismiss={() => setConfirmUnbind(null)}
      />

      <TrackerFileImport
        key={importing}
        t={t}
        open={importOpen}
        onDismiss={() => setImportOpen(false)}
        onApplied={() => {
          setImportOpen(false);
          void reexecute({ requestPolicy: "network-only" });
        }}
      />

      <ConfirmDialog
        open={confirmDelete !== null}
        title={t("next.rss.removeRule")}
        note={confirmDelete?.name}
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

type TorrentEncryption = "PREFER" | "REQUIRE" | "DISABLED";
type PeerTransport = "TCP" | "UTP";

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
  peerTcp: boolean;
  peerUtp: boolean;
  /** Each kept as set; used as the transport left on while the other is off. */
  preferDownloading: PeerTransport;
  preferSeeding: PeerTransport;
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
  unpack: false,
  connectionsLimit: 200,
  unchokeSlotsLimit: 8,
  connectionSpeed: 30,
  allowMultipleConnectionsPerIp: false,
  peerTcp: true,
  peerUtp: true,
  preferDownloading: "TCP",
  preferSeeding: "UTP",
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

/** The transport switches; one off locks both preferences to the other. */
const TRANSPORT_SWITCHES = [
  {
    key: "peerTcp",
    field: "peer_tcp",
    label: "next.torrents.peerTcp",
    keywords: "tcp transport peers",
  },
  {
    key: "peerUtp",
    field: "peer_utp",
    label: "next.torrents.peerUtp",
    keywords: "utp transport peers udp",
  },
] as const;

const TRANSPORT_PREFERENCES = [
  {
    key: "preferDownloading",
    field: "prefer_downloading",
    label: "next.torrents.preferDownloading",
    help: "next.torrents.preferDownloadingHelp",
    keywords: "tcp utp transport order dial downloading",
  },
  {
    key: "preferSeeding",
    field: "prefer_seeding",
    label: "next.torrents.preferSeeding",
    help: "next.torrents.preferSeedingHelp",
    keywords: "tcp utp transport order dial seeding",
  },
] as const;

const PEER_TRANSPORTS: { value: PeerTransport; label: string }[] = [
  { value: "TCP", label: "TCP" },
  { value: "UTP", label: "uTP" },
];

/**
 * The one transport on, which both preferences are used as while the other
 * is off; null while both are on and the preferences are used as kept.
 */
const onlyTransport = (tcp: boolean, utp: boolean): PeerTransport | null =>
  tcp && !utp ? "TCP" : utp && !tcp ? "UTP" : null;

/** Option labels are translation keys, resolved when the panel renders. */
const ENCRYPTIONS: { value: TorrentEncryption; label: string }[] = [
  { value: "PREFER", label: "next.torrents.encryptionPrefer" },
  { value: "REQUIRE", label: "next.torrents.encryptionRequire" },
  { value: "DISABLED", label: "next.torrents.encryptionDisabled" },
];

const validPort = (port: number | null) =>
  port === null || (Number.isInteger(port) && port >= 1 && port <= 65535);

/** The connection fields that differ from `saved`, as `TorrentSettingsInput` takes them. */
function connectionChanges(
  form: TorrentConnection,
  saved: TorrentConnection,
): Partial<TorrentConnection> {
  return Object.fromEntries(
    (Object.keys(form) as (keyof TorrentConnection)[])
      .filter((key) => form[key] !== saved[key])
      .map((key) => [key, form[key]]),
  ) as Partial<TorrentConnection>;
}

export function TorrentsPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{
    settings: {
      seeding: SeedingPolicy | null;
      uploadCap: number | null;
      torrent: TorrentConnection | null;
    };
  }>({ query: TORRENT_SETTINGS_QUERY });
  const [updateState, updateSettings] = useMutation(
    UPDATE_TORRENT_SETTINGS_MUTATION,
  );
  // Whether torrents still run, so turning them off asks how they end; every
  // other screen follows the same reading, pushed when the save lands.
  const { torrentPresence } = useNextData("torrentPresence");
  // Turning off asks first; with torrents running, how they end.
  const [asking, setAsking] = useState(false);
  const [finish, setFinish] = useState({ downloads: true, seeding: true });
  // Neither finished: every torrent stops now, after a second ask.
  const [forcing, setForcing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const savedConnection = useMemo<TorrentConnection>(
    () => ({ ...DEFAULT_CONNECTION, ...(data?.settings?.torrent ?? {}) }),
    [data?.settings?.torrent],
  );
  const savedSeeding = useMemo(
    () => seedingAtGlobal(seedingFrom(data?.settings?.seeding ?? null)),
    [data?.settings?.seeding],
  );
  const trackerRules = useTrackerRules(
    t,
    savedConnection,
    savedSeeding,
    data?.settings?.uploadCap ?? null,
  );
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

  const draft = useDraft<TorrentForm>(source, () => setError(null));
  const values = draft.value;
  // Each change saves as it is made; a typed field when it is left or Enter
  // is pressed. Saves run one at a time, each sending what differs from the
  // last one stored, so a quick run of changes lands in order.
  const queue = useRef<Promise<void>>(Promise.resolve());
  const stored = useRef<TorrentForm | null>(null);
  const latest = useRef<TorrentForm | null>(null);
  const [saving, setSaving] = useState(0);

  const save = (form: TorrentForm) => {
    if (
      !validPort(form.torrent.basePort) ||
      !validPort(form.torrent.announcePort) ||
      !validPort(form.torrent.sslPort)
    ) {
      setError(t("next.torrents.portInvalid"));
      return;
    }
    latest.current = form;
    setSaving((count) => count + 1);
    queue.current = queue.current.then(async () => {
      const base = stored.current ?? source;
      if (!base) {
        setSaving((count) => count - 1);
        return;
      }
      const seeding = seedingInput(form.seeding);
      const seedingChanged =
        JSON.stringify(seeding) !== JSON.stringify(seedingInput(base.seeding));
      const torrent = connectionChanges(form.torrent, base.torrent);
      const input = {
        ...(seedingChanged ? { seeding } : {}),
        ...(form.uploadCap !== base.uploadCap
          ? { uploadCap: form.uploadCap }
          : {}),
        ...(Object.keys(torrent).length > 0 ? { torrent } : {}),
      };
      const result =
        Object.keys(input).length > 0 ? await updateSettings({ input }) : null;
      setSaving((count) => count - 1);
      if (result && (result.error || !result.data?.updateSettings)) {
        setError(result.error?.message ?? t("next.torrents.saveFailed"));
        return;
      }
      stored.current = form;
      setError(null);
      // Nothing newer waits: the form shows the daemon's copy again.
      if (latest.current === form) {
        draft.markSaved();
        stored.current = null;
        if (result) {
          void reexecute({ requestPolicy: "network-only" });
        }
      }
    });
  };

  usePanelStatus(
    error ?? (saving > 0 ? t("settings.saving") : null),
    error !== null,
  );

  /** A change saved at once. */
  const change = (patch: Partial<TorrentForm>) => {
    if (!values) {
      return;
    }
    draft.set(patch);
    save({ ...values, ...patch });
  };
  const patchTorrent = (next: Partial<TorrentConnection>) =>
    values && change({ torrent: { ...values.torrent, ...next } });
  // A typed field: shown as typed, saved when it is left.
  const typeTorrent = (next: Partial<TorrentConnection>) =>
    values && draft.set({ torrent: { ...values.torrent, ...next } });
  const commitTyped = () => values && save(values);

  // Turning torrents on or off saves at once, after a confirm.
  const [confirmEnable, setConfirmEnable] = useState(false);
  const saveEnabled = (enabled: boolean, whenDisabled?: TorrentDisableMode) => {
    setError(null);
    void updateSettings({
      input: {
        torrent:
          whenDisabled === undefined ? { enabled } : { enabled, whenDisabled },
      },
    }).then((result) => {
      setConfirmEnable(false);
      setAsking(false);
      setForcing(false);
      if (result.error || !result.data?.updateSettings) {
        setError(result.error?.message ?? t("next.torrents.saveFailed"));
        return;
      }
      if (values) {
        draft.set({ torrent: { ...values.torrent, enabled } });
      }
      if (saving === 0) {
        draft.markSaved();
      }
      void reexecute({ requestPolicy: "network-only" });
    });
  };
  // With torrents running, the boxes say how they end; with none, there is
  // nothing to choose.
  const asksHow = asksHowTorrentsEnd(true, false, torrentPresence);
  const askOff = () => {
    setFinish({ downloads: true, seeding: true });
    setAsking(true);
  };
  const turnOff = () => {
    if (!asksHow) {
      saveEnabled(false);
    } else if (finish.downloads || finish.seeding) {
      saveEnabled(false, disableMode(finish));
    } else {
      setForcing(true);
    }
  };

  // A wind-down under way, cut short: every torrent stops now.
  const stopNow = () => {
    setError(null);
    void updateSettings({ input: stopNowInput() }).then((result) => {
      if (result.error || !result.data?.updateSettings) {
        setError(result.error?.message ?? t("next.torrents.saveFailed"));
        return;
      }
      void reexecute({ requestPolicy: "network-only" });
    });
  };

  // The switch discloses the rest: off, it shows alone, torrent jobs left or not.
  const enabledBlock: SettingsBlock | null = values
    ? {
        kind: "section",
        id: "enabled",
        title: t("next.settings.panel.torrents"),
        note: windDownStoppable(values.torrent.enabled, torrentPresence)
          ? t("next.torrents.windingDown")
          : undefined,
        fields: [
          {
            id: "enabled",
            label: t("next.torrents.enabled"),
            control: {
              kind: "toggle",
              value: values.torrent.enabled,
              // Turning off asks how running torrents end, which waits on the server's answer.
              disabled: torrentSwitchWaits(
                values.torrent.enabled,
                torrentPresence,
              ),
              onChange: (enabled) =>
                enabled ? setConfirmEnable(true) : askOff(),
            },
          },
          ...(windDownStoppable(values.torrent.enabled, torrentPresence)
            ? [
                {
                  id: "stopNow",
                  label: t("next.torrents.offStopNow"),
                  // Letting every torrent finish never ends one that is forced or has no seeding limit.
                  help: t(
                    torrentPresence?.windDown === "FINISH_ALL" ||
                      torrentPresence?.windDown === "FINISH_SEEDING"
                      ? "next.torrents.finishAllForced"
                      : "next.torrents.offStopNowNote",
                  ),
                  control: {
                    kind: "custom" as const,
                    control: (
                      <SecondaryButton
                        onClick={stopNow}
                        disabled={updateState.fetching}
                        testId={testIds.field("stopNow")}
                      >
                        {t("next.torrents.stopNow")}
                      </SecondaryButton>
                    ),
                  },
                },
              ]
            : []),
        ],
      }
    : null;
  const blocks: SettingsBlock[] =
    !values || !enabledBlock
      ? []
      : torrentSettingsDisclosed<SettingsBlock>(
          values.torrent.enabled,
          enabledBlock,
          [
            {
              kind: "section",
              id: "seeding",
              title: t("next.torrents.seeding"),
              fields: [
                ...seedingFields(
                  t,
                  values.seeding,
                  (seeding) => change({ seeding }),
                  { inherits: false },
                ),
                globalUploadCapField(t, values.uploadCap, (uploadCap) =>
                  change({ uploadCap }),
                ),
              ],
            },
            trackerRules.block,
            {
              kind: "section",
              id: "connection",
              title: t("next.torrents.connection"),
              fields: [
                // One transport stays on. The preferences are kept as set; while
                // one is off they show locked to the other, as the dials use them.
                ...TRANSPORT_SWITCHES.map(({ key, label, keywords }) => {
                  const otherOn =
                    key === "peerTcp"
                      ? values.torrent.peerUtp
                      : values.torrent.peerTcp;
                  return {
                    id: key,
                    label: t(label),
                    keywords,
                    help: t(`${label}Help`),
                    control: {
                      kind: "toggle" as const,
                      value: values.torrent[key],
                      disabled: values.torrent[key] && !otherOn,
                      onChange: (on: boolean) => patchTorrent({ [key]: on }),
                    },
                  };
                }),
                ...TRANSPORT_PREFERENCES.map(
                  ({ key, label, help, keywords }) => {
                    const only = onlyTransport(
                      values.torrent.peerTcp,
                      values.torrent.peerUtp,
                    );
                    return {
                      id: key,
                      label: t(label),
                      keywords,
                      help: t(help),
                      control: {
                        kind: "segmented" as const,
                        value: only ?? values.torrent[key],
                        options: PEER_TRANSPORTS.map((option) => ({
                          ...option,
                          disabled: only !== null,
                        })),
                        onChange: (prefer: string) =>
                          patchTorrent({ [key]: prefer as PeerTransport }),
                      },
                    };
                  },
                ),
                {
                  id: "basePort",
                  label: t("next.torrents.basePort"),
                  keywords: "port listen",
                  control: {
                    kind: "number",
                    value: values.torrent.basePort,
                    min: 1,
                    max: 65535,
                    onChange: (basePort) =>
                      patchTorrent({ basePort: Math.round(basePort) }),
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
                          patchTorrent({
                            announcePort:
                              announcePort === null
                                ? null
                                : Math.round(announcePort),
                          })
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
                        onChange={(sslPort) =>
                          patchTorrent({
                            sslPort:
                              sslPort === null ? null : Math.round(sslPort),
                          })
                        }
                      />
                    ),
                  },
                },
                {
                  id: "upnp",
                  label: t("next.torrents.upnp"),
                  keywords: "upnp port mapping",
                  control: {
                    kind: "toggle",
                    value: values.torrent.upnp,
                    onChange: (upnp) => patchTorrent({ upnp }),
                  },
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
                          patchTorrent({
                            connectionsLimit:
                              next === null
                                ? null
                                : Math.max(1, Math.round(next)),
                          })
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
                    onChange: (next) =>
                      patchTorrent({
                        maxPeersTotal: Math.max(0, Math.round(next)),
                      }),
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
                          patchTorrent({
                            unchokeSlotsLimit:
                              next === null
                                ? null
                                : Math.max(1, Math.round(next)),
                          })
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
                    onChange: (blocklistEnabled) =>
                      patchTorrent({ blocklistEnabled }),
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
                    onChange: (next) =>
                      typeTorrent({
                        blocklist: next.trim() === "" ? null : next,
                      }),
                    onCommit: commitTyped,
                  },
                },
                {
                  id: "filterTrackers",
                  label: t("next.torrents.filterTrackers"),
                  keywords: "ip filter trackers",
                  control: {
                    kind: "toggle",
                    value: values.torrent.filterTrackers,
                    onChange: (filterTrackers) =>
                      patchTorrent({ filterTrackers }),
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
                    onChange: (next) =>
                      typeTorrent({ bannedIps: next.split("\n") }),
                    onCommit: commitTyped,
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
                    options: ENCRYPTIONS.map((option) => ({
                      ...option,
                      label: t(option.label),
                    })),
                    onChange: (encryption) =>
                      patchTorrent({
                        encryption: encryption as TorrentEncryption,
                      }),
                  },
                },
                {
                  id: "maxActiveDownloads",
                  label: t("next.torrents.maxActiveDownloads"),
                  control: {
                    kind: "number",
                    value: values.torrent.maxActiveDownloads,
                    min: 0,
                    onChange: (next) =>
                      patchTorrent({
                        maxActiveDownloads: Math.max(0, Math.round(next)),
                      }),
                  },
                },
                {
                  id: "unpack",
                  label: t("next.torrents.unpack"),
                  keywords: "unpack extract archive",
                  control: {
                    kind: "toggle",
                    value: values.torrent.unpack,
                    onChange: (unpack) => patchTorrent({ unpack }),
                  },
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
                    onChange: (next) =>
                      patchTorrent({
                        connectionSpeed: Math.max(1, Math.round(next)),
                      }),
                  },
                },
                {
                  id: "allowMultipleConnectionsPerIp",
                  label: t("next.torrents.multipleConnectionsPerIp"),
                  keywords: "ip connections",
                  control: {
                    kind: "toggle",
                    value: values.torrent.allowMultipleConnectionsPerIp,
                    onChange: (allowMultipleConnectionsPerIp) =>
                      patchTorrent({ allowMultipleConnectionsPerIp }),
                  },
                },
                {
                  id: "blockPrivilegedPorts",
                  label: t("next.torrents.blockPrivilegedPorts"),
                  keywords: "privileged ports peers",
                  control: {
                    kind: "toggle",
                    value: values.torrent.blockPrivilegedPorts,
                    onChange: (blockPrivilegedPorts) =>
                      patchTorrent({ blockPrivilegedPorts }),
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
                    onChange: (next) =>
                      patchTorrent({
                        stopTrackerTimeoutSecs: Math.max(0, Math.round(next)),
                      }),
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
                    onChange: (dhtBootstrapNodes) =>
                      typeTorrent({ dhtBootstrapNodes }),
                    onCommit: commitTyped,
                  },
                },
              ],
            },
          ],
        );

  return (
    <>
      <SettingsBlocks blocks={blocks} loading={fetching && !data} />
      {values &&
      source &&
      !values.torrent.enabled &&
      !source.torrent.enabled ? (
        <div className="flex justify-center px-4 py-16">
          <PrimaryButton
            icon="resume"
            onClick={() => setConfirmEnable(true)}
            disabled={updateState.fetching}
            className="h-[52px] px-10 text-[15px]"
            testId={testIds.field("enableTorrents")}
          >
            {t("next.torrents.enableButton")}
          </PrimaryButton>
        </div>
      ) : null}
      <ConfirmDialog
        open={confirmEnable}
        title={t("next.torrents.enableButton")}
        body={t("next.torrents.enableBody")}
        confirmLabel={t("next.torrents.enableButton")}
        destructive={false}
        busy={updateState.fetching}
        onConfirm={() => saveEnabled(true)}
        onDismiss={() => setConfirmEnable(false)}
        testId={testIds.dialog("enable-torrents")}
      />
      {trackerRules.overlays}
      <TorrentsOffDialog
        open={asking && !forcing}
        asksHow={asksHow}
        finish={finish}
        onFinish={setFinish}
        busy={updateState.fetching}
        onConfirm={turnOff}
        onDismiss={() => setAsking(false)}
      />
      <ConfirmDialog
        open={forcing}
        title={t("next.torrents.offForceTitle")}
        body={t("next.torrents.offForceBody")}
        confirmLabel={t("next.torrents.offTitle")}
        solid
        busy={updateState.fetching}
        onConfirm={() => saveEnabled(false, "STOP_NOW")}
        onDismiss={() => setForcing(false)}
        testId={testIds.dialog("torrents-off-force")}
      />
    </>
  );
}

const TORRENTS_OFF_DIALOG = "torrents-off";

type Finish = { downloads: boolean; seeding: boolean };

/** How the torrents still running end, from the two boxes; neither is `STOP_NOW`. */
function disableMode(finish: Finish): TorrentDisableMode {
  if (finish.downloads && finish.seeding) {
    return "FINISH_ALL";
  }
  if (finish.downloads) {
    return "FINISH_DOWNLOADS";
  }
  return finish.seeding ? "FINISH_SEEDING" : "STOP_NOW";
}

/**
 * Asks before torrents are turned off and, while some run, how they end:
 * each box on lets that part finish.
 */
function TorrentsOffDialog({
  open,
  asksHow,
  finish,
  onFinish,
  busy,
  onConfirm,
  onDismiss,
}: {
  open: boolean;
  asksHow: boolean;
  finish: Finish;
  onFinish: (next: Finish) => void;
  busy: boolean;
  onConfirm: () => void;
  onDismiss: () => void;
}) {
  const t = useTranslate();
  const boxes: { key: keyof Finish; label: string; note: string }[] = [
    {
      key: "downloads",
      label: "next.torrents.offFinishDownloadsBox",
      note: finish.downloads
        ? "next.torrents.offFinishDownloadsOn"
        : "next.torrents.offFinishDownloadsOff",
    },
    {
      key: "seeding",
      label: "next.torrents.offFinishSeedingBox",
      note: finish.seeding
        ? "next.torrents.finishAllForced"
        : "next.torrents.offFinishSeedingOff",
    },
  ];
  return (
    <Dialog
      open={open}
      title={t("next.torrents.offTitle")}
      onDismiss={onDismiss}
      testId={testIds.dialog(TORRENTS_OFF_DIALOG)}
      footer={
        <>
          <SecondaryButton onClick={onDismiss} testId={testIds.confirmDismiss}>
            {t("action.cancel")}
          </SecondaryButton>
          <PrimaryButton
            onClick={onConfirm}
            disabled={busy}
            testId={testIds.confirmAccept}
          >
            {t("next.torrents.offTitle")}
          </PrimaryButton>
        </>
      }
    >
      <div className="flex flex-col gap-3 px-4 py-5 sm:px-6">
        <p className="text-[13px] leading-[1.55] text-pretty text-wv-secondary">
          {t(asksHow ? "next.torrents.offBody" : "next.torrents.offIdleBody")}
        </p>
        {asksHow
          ? boxes.map((box) => {
              const toggle = () =>
                onFinish({ ...finish, [box.key]: !finish[box.key] });
              return (
                <div
                  key={box.key}
                  onClick={toggle}
                  className="flex cursor-pointer items-start gap-3 border border-wv-control bg-wv-button px-4 py-3 hover:border-wv-control-hover-strong"
                >
                  <CheckBox
                    checked={finish[box.key]}
                    onChange={toggle}
                    label={t(box.label)}
                    className="mt-[2px]"
                    testId={testIds.option(TORRENTS_OFF_DIALOG, box.key)}
                  />
                  <div className="flex flex-col gap-1">
                    <span className="text-[13px] font-medium text-wv-strong">
                      {t(box.label)}
                    </span>
                    <span
                      data-testid={testIds.optionNote(
                        TORRENTS_OFF_DIALOG,
                        box.key,
                      )}
                      className="text-[12px] text-wv-muted"
                    >
                      {t(box.note)}
                    </span>
                  </div>
                </div>
              );
            })
          : null}
      </div>
    </Dialog>
  );
}
