import { useMemo, useRef, useState } from "react";
import { useMutation, useQuery, type CombinedError } from "urql";
import {
  ARCHIVE_PASSWORD_SETTINGS_QUERY,
  HARDWARE_PROFILE_QUERY,
  SET_HARDWARE_PROFILE_MUTATION,
  SET_UNACCEPTABLE_EXTENSIONS_MUTATION,
  SETTINGS_QUERY,
  UNACCEPTABLE_EXTENSIONS_QUERY,
  UPDATE_ARCHIVE_PASSWORD_SETTINGS_MUTATION,
  UPDATE_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { useLanguageSettings, useTranslate } from "@/lib/context/translate-context";
import { AVAILABLE_LANGUAGES } from "@/lib/i18n";
import {
  DEFAULT_DUPLICATE_POLICY,
  DUPLICATE_ACTIONS,
  normalizeDuplicatePolicy,
  type DuplicateAction,
  type DuplicatePolicy,
} from "@/next/features/duplicates/duplicate-policy";
import { useUpdateCheck } from "@/next/features/updates/use-update-check";
import { Leaf, Rocket, Scale } from "lucide-react";
import {
  initialProfile,
  offersProfileChoice,
  profileName,
  scheduledProfileNotice,
  type HardwareProfileName,
  type HardwareProfileSettings,
} from "../../../data/hardware-profiles";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { SecondaryButton, Select, TextArea } from "../../../components/controls";
import { SettingsBlocks, usePanelStatus, type FieldSpec, type SettingsBlock } from "../framework";

/**
 * General: everything the daemon keeps in `GeneralSettings`, plus the
 * browser-local language preference.
 *
 * Every field saves on its own, so the panel never feeds the top bar's Save
 * and Revert. A toggle or a select saves as it changes; a typed field saves
 * when it is left or Enter is pressed in it, and only if that changes
 * anything. Each save sends only the mutation its field belongs to.
 */

interface GeneralSettings {
  dataDir: string;
  intermediateDir: string;
  completeDir: string;
  cleanupAfterExtract: boolean;
  maxRetries: number;
  propagationDelaySecs: number;
  enableSrrdbLookup: boolean;
  duplicatePolicy: DuplicatePolicy;
}

interface ArchivePasswordSettings {
  hasPasswords: boolean;
  passwordFile: string | null;
}

/** What each field holds, one key per field: the duplicate rules sit beside the rest. */
interface Form extends DuplicatePolicy {
  intermediateDir: string;
  completeDir: string;
  cleanupAfterExtract: boolean;
  maxRetries: number;
  propagationDelaySecs: number;
  enableSrrdbLookup: boolean;
  /** A replacement list as typed. The stored list never comes back, so it starts blank. */
  archivePasswords: string;
  archivePasswordFile: string;
  /** The unwanted extension list, comma-separated. */
  unacceptableExtensions: string;
}

type FieldKey = keyof Form;

function splitExtensions(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

/** A field's value as the daemon would keep it, to tell whether a commit changes anything. */
function comparable(key: FieldKey, value: unknown): unknown {
  if (key === "unacceptableExtensions") {
    return splitExtensions(String(value)).join(",");
  }
  return typeof value === "string" ? value.trim() : value;
}

/** The `updateSettings` input that stores one field and leaves the rest alone. */
function settingsInput(key: FieldKey, value: unknown): Record<string, unknown> {
  if (key in DEFAULT_DUPLICATE_POLICY) {
    return { duplicatePolicy: { [key]: value } };
  }
  if (key === "intermediateDir" || key === "completeDir") {
    return { [key]: String(value).trim() || null };
  }
  return { [key]: value };
}

function without<T extends object>(record: T, key: keyof T): T {
  if (!(key in record)) {
    return record;
  }
  const next = { ...record };
  delete next[key];
  return next;
}

/**
 * The daemon's copy of a setting: as last read, or as a save last answered.
 * An answer stands in until the setting is read again.
 */
function useAnswered<T>(read: T | undefined): [T | undefined, (answer: T) => void] {
  const [answered, setAnswered] = useState<{ read: T | undefined; value: T } | null>(null);
  // The read an answer lands on, which may be newer than the one it was asked under.
  const readRef = useRef(read);
  readRef.current = read;
  return [
    answered !== null && answered.read === read ? answered.value : read,
    (value: T) => setAnswered({ read: readRef.current, value }),
  ];
}

const DUPLICATE_ACTION_LABEL: Record<DuplicateAction, string> = {
  ACCEPT: "next.general.duplicateAction.accept",
  WARN: "next.general.duplicateAction.warn",
  PAUSE: "next.general.duplicateAction.pause",
  BLOCK: "next.general.duplicateAction.block",
};

/** Translation keys, resolved when the panel renders. */
const DUPLICATE_FIELDS: { key: keyof DuplicatePolicy; label: string; help: string }[] = [
  {
    key: "strictActiveOrSuccess",
    label: "next.general.duplicate.strictActive",
    help: "next.general.duplicate.strictActiveHelp",
  },
  {
    key: "strictFailedOrCancelled",
    label: "next.general.duplicate.strictFailed",
    help: "next.general.duplicate.strictFailedHelp",
  },
  {
    key: "articleLayoutActiveOrSuccess",
    label: "next.general.duplicate.layoutActive",
    help: "next.general.duplicate.layoutActiveHelp",
  },
  {
    key: "articleLayoutFailedOrCancelled",
    label: "next.general.duplicate.layoutFailed",
    help: "next.general.duplicate.layoutFailedHelp",
  },
  {
    key: "articleSet",
    label: "next.general.duplicate.articleSet",
    help: "next.general.duplicate.articleSetHelp",
  },
  {
    key: "normalizedName",
    label: "next.general.duplicate.normalizedName",
    help: "next.general.duplicate.normalizedNameHelp",
  },
];

export function GeneralPanel() {
  const t = useTranslate();
  const { uiLanguage, setLanguagePreference } = useLanguageSettings();
  const [{ data, fetching }] = useQuery<{ settings: GeneralSettings }>({ query: SETTINGS_QUERY });
  const [{ data: profileData }] = useQuery<{ hardwareProfile: HardwareProfileSettings }>({
    query: HARDWARE_PROFILE_QUERY,
  });
  const profileSettings = profileData?.hardwareProfile ?? null;
  const [{ data: archiveData }] = useQuery<{
    archivePasswordSettings: ArchivePasswordSettings;
  }>({ query: ARCHIVE_PASSWORD_SETTINGS_QUERY });
  const [{ data: extensionsData }] = useQuery<{
    postProcessingSettings: { unacceptableExtensions: string[] };
  }>({ query: UNACCEPTABLE_EXTENSIONS_QUERY });
  const [, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [, updateArchivePasswords] = useMutation(UPDATE_ARCHIVE_PASSWORD_SETTINGS_MUTATION);
  const [, setExtensions] = useMutation(SET_UNACCEPTABLE_EXTENSIONS_MUTATION);
  // Each save answers with the setting as stored, so nothing is read again.
  const [settings, answerSettings] = useAnswered(data?.settings);
  const [archive, answerArchive] = useAnswered(archiveData?.archivePasswordSettings);
  const [extensions, answerExtensions] = useAnswered(extensionsData?.postProcessingSettings.unacceptableExtensions);

  // What is typed or chosen and not stored yet: still being typed, on its way,
  // or refused. A refused value stays in its field beside the daemon's reason.
  const [edits, setEdits] = useState<Partial<Form>>({});
  const editsRef = useRef(edits);
  editsRef.current = edits;
  // What each field holds once the saves still on their way land.
  const pending = useRef<Partial<Form>>({});
  // Saves run one at a time in the order they were made, so the last change
  // to a field is the one the daemon keeps.
  const queue = useRef<Promise<void>>(Promise.resolve());
  const [saving, setSaving] = useState(0);
  const [failure, setFailure] = useState<{ key: FieldKey; message: string } | null>(null);
  // The password box stays shut until asked for, so clicking through settings on a shared screen shows none.
  const [editingPasswords, setEditingPasswords] = useState(false);
  // The stored list never comes back, so clearing it is asked first.
  const [confirmClear, setConfirmClear] = useState(false);

  usePanelStatus(failure?.message ?? (saving > 0 ? t("settings.saving") : null), failure !== null);

  const stored = useMemo<Form | null>(
    () =>
      settings && archive && extensions
        ? {
            ...normalizeDuplicatePolicy(settings.duplicatePolicy),
            intermediateDir: settings.intermediateDir,
            completeDir: settings.completeDir,
            cleanupAfterExtract: settings.cleanupAfterExtract,
            maxRetries: settings.maxRetries,
            propagationDelaySecs: settings.propagationDelaySecs,
            enableSrrdbLookup: settings.enableSrrdbLookup,
            archivePasswords: "",
            archivePasswordFile: archive.passwordFile ?? "",
            unacceptableExtensions: extensions.join(", "),
          }
        : null,
    [settings, archive, extensions],
  );
  const values = stored ? { ...stored, ...edits } : null;
  const dataDir = settings?.dataDir ?? "";
  // Whether the daemon holds a password list; the list itself never comes back.
  const hasPasswords = archive?.hasPasswords ?? false;

  const refusal = (error: CombinedError | undefined) =>
    error ? (error.graphQLErrors[0]?.message ?? error.message) : t("next.settings.saveFailed");

  const sendSettings = async (input: Record<string, unknown>) => {
    const result = await updateSettings({ input });
    const answer = result.data?.updateSettings as GeneralSettings | undefined;
    if (result.error || !answer) {
      return refusal(result.error);
    }
    answerSettings(answer);
    return null;
  };

  // A list given replaces the stored one, and an empty list removes it; a
  // file left out keeps the stored file, and null forgets it.
  const sendArchive = async (variables: { passwords?: string[]; passwordFile?: string | null }) => {
    const result = await updateArchivePasswords(variables);
    const answer = result.data?.updateArchivePasswordSettings as ArchivePasswordSettings | undefined;
    if (result.error || !answer) {
      return refusal(result.error);
    }
    answerArchive(answer);
    return null;
  };

  const sendExtensions = async (list: string[]) => {
    const result = await setExtensions({ extensions: list });
    const answer = result.data?.setUnacceptableExtensions as string[] | undefined;
    if (result.error || !answer) {
      return refusal(result.error);
    }
    answerExtensions(answer);
    return null;
  };

  const forget = (key: FieldKey) => setFailure((current) => (current?.key === key ? null : current));

  /** Queue one field's save; `send` answers with the daemon's refusal, or null once stored. */
  const run = <K extends FieldKey>(key: K, value: Form[K], send: () => Promise<string | null>, onStored?: () => void) => {
    pending.current = { ...pending.current, [key]: value };
    setSaving((count) => count + 1);
    forget(key);
    queue.current = queue.current.then(async () => {
      let refused: string | null;
      try {
        refused = await send();
      } catch (error) {
        refused = error instanceof Error ? error.message : String(error);
      }
      if (pending.current[key] === value) {
        pending.current = without(pending.current, key);
      }
      setSaving((count) => count - 1);
      if (refused !== null) {
        setFailure({ key, message: refused });
        return;
      }
      forget(key);
      onStored?.();
      // Stored: the field shows the daemon's copy again, unless it changed meanwhile.
      setEdits((current) => (current[key] === value ? without(current, key) : current));
    });
  };

  const edit = <K extends FieldKey>(key: K, value: Form[K]) => setEdits((current) => ({ ...current, [key]: value }));

  /** A field's new value, stored unless the field already holds it or is about to. */
  const commit = <K extends FieldKey>(key: K, value: Form[K]) => {
    if (!stored) {
      return;
    }
    const onItsWay = key in pending.current;
    if (comparable(key, value) === comparable(key, onItsWay ? pending.current[key] : stored[key])) {
      if (!onItsWay) {
        setEdits((current) => without(current, key));
        forget(key);
      }
      return;
    }
    edit(key, value);
    run(key, value, () =>
      key === "unacceptableExtensions"
        ? sendExtensions(splitExtensions(String(value)))
        : key === "archivePasswordFile"
          ? sendArchive({ passwordFile: String(value).trim() || null })
          : sendSettings(settingsInput(key, value)),
    );
  };

  // A blank list keeps what is stored; the box shuts once a list is stored.
  const commitPasswords = (typed: string) => {
    if (typed === "") {
      setEdits((current) => without(current, "archivePasswords"));
      forget("archivePasswords");
      setEditingPasswords(false);
      return;
    }
    if (pending.current.archivePasswords === typed) {
      return;
    }
    run(
      "archivePasswords",
      typed,
      () => sendArchive({ passwords: typed.split(/\r?\n/) }),
      () => {
        if (editsRef.current.archivePasswords === typed) {
          setEditingPasswords(false);
        }
      },
    );
  };

  const clearPasswords = () =>
    run(
      "archivePasswords",
      "",
      () => sendArchive({ passwords: [] }),
      () => {
        setEdits((current) => without(current, "archivePasswords"));
        setEditingPasswords(false);
      },
    );

  const interfaceFields: FieldSpec[] = [
    {
      id: "language",
      label: t("next.general.language"),
      help: t("next.general.languageHelp"),
      keywords: "locale translation",
      control: {
        kind: "select",
        value: uiLanguage,
        options: AVAILABLE_LANGUAGES.map((language) => ({
          value: language.code,
          label: language.label,
        })),
        onChange: setLanguagePreference,
      },
    },
  ];

  const blocks: (SettingsBlock | null)[] = [
    { kind: "section", id: "interface", title: t("next.general.interface"), fields: interfaceFields },
    {
      kind: "section",
      id: "updates",
      title: t("next.general.updates"),
      fields: [
        {
          id: "checkForUpdates",
          label: t("next.general.checkForUpdates"),
          help: t("next.general.checkForUpdatesHelp"),
          keywords: "update upgrade release version",
          control: { kind: "custom", control: <UpdateCheck /> },
        },
      ],
    },
    values
      ? {
          kind: "section",
          id: "downloads",
          title: t("next.general.downloads"),
          fields: [
            {
              id: "maxRetries",
              label: t("next.general.retries"),
              help: t("next.general.retriesHelp"),
              control: {
                kind: "number",
                value: values.maxRetries,
                min: 0,
                max: 20,
                onChange: (next) => commit("maxRetries", next),
                suffix: t("next.general.attempts"),
              },
            },
            {
              id: "propagationDelaySecs",
              label: t("next.general.propagationDelay"),
              help: t("next.general.propagationDelayHelp"),
              keywords: "wait posting delay seconds",
              control: {
                kind: "number",
                value: values.propagationDelaySecs,
                min: 0,
                step: 60,
                onChange: (next) => commit("propagationDelaySecs", next),
                suffix: t("next.general.seconds"),
              },
            },
            {
              id: "srrdb",
              label: t("next.general.srrdb"),
              help: t("next.general.srrdbHelp"),
              keywords: "obfuscated rename",
              control: {
                kind: "toggle",
                value: values.enableSrrdbLookup,
                onChange: (next) => commit("enableSrrdbLookup", next),
              },
            },
            {
              id: "unacceptableExtensions",
              label: t("next.general.unacceptableExtensions"),
              help: t("next.general.unacceptableExtensionsHelp"),
              keywords: values.unacceptableExtensions,
              control: {
                kind: "text",
                value: values.unacceptableExtensions,
                placeholder: "exe, bat, scr",
                className: "w-[420px] max-w-full",
                onChange: (next) => edit("unacceptableExtensions", next),
                onCommit: (typed) => commit("unacceptableExtensions", typed),
              },
            },
          ],
        }
      : null,
    values
      ? {
          kind: "section",
          id: "archivePasswords",
          title: t("next.archivePasswords.title"),
          fields: [
            {
              id: "archivePasswords",
              label: t("next.archivePasswords.list"),
              keywords: "archive password rar 7z zip encrypted",
              control: {
                kind: "custom",
                control: (
                  <div className="flex items-start justify-end gap-2">
                    {editingPasswords ? (
                      <TextArea
                        label={t("next.archivePasswords.list")}
                        value={values.archivePasswords}
                        rows={3}
                        secret
                        placeholder={hasPasswords ? "••••••••" : undefined}
                        className="w-[190px] max-w-full"
                        onChange={(next) => edit("archivePasswords", next)}
                        onBlur={() => commitPasswords(values.archivePasswords)}
                      />
                    ) : (
                      <SecondaryButton icon="password" onClick={() => setEditingPasswords(true)}>
                        {t("next.archivePasswords.edit")}
                      </SecondaryButton>
                    )}
                    {hasPasswords ? (
                      <SecondaryButton onClick={() => setConfirmClear(true)}>{t("next.common.clear")}</SecondaryButton>
                    ) : null}
                  </div>
                ),
              },
            },
            {
              id: "archivePasswordFile",
              label: t("next.archivePasswords.file"),
              keywords: `${values.archivePasswordFile} archive password file`,
              control: {
                kind: "path",
                pick: "file",
                browseFrom: dataDir,
                value: values.archivePasswordFile,
                placeholder: `${dataDir}/archive-passwords.txt`,
                onChange: (next) => edit("archivePasswordFile", next),
                onCommit: (path) => commit("archivePasswordFile", path),
              },
            },
          ],
        }
      : null,
    profileSettings && offersProfileChoice(profileSettings)
      ? {
          kind: "section",
          id: "performance",
          title: t("next.performance.title"),
          fields: [
            {
              id: "hardwareProfile",
              label: t("next.performance.profile"),
              help: t("next.performance.timing"),
              keywords: "performance profile hardware memory threads efficient balanced",
              control: { kind: "custom", control: <PerformanceProfile settings={profileSettings} /> },
            },
          ],
        }
      : null,
    values
      ? {
          kind: "section",
          id: "storage",
          title: t("next.general.storage"),
          note: t("next.general.storageNote"),
          fields: [
            {
              id: "dataDir",
              label: t("next.settings.dataDirectory"),
              help: t("next.general.dataDirHelp"),
              keywords: dataDir,
              control: { kind: "static", value: dataDir },
            },
            {
              id: "intermediateDir",
              label: t("next.general.workingDir"),
              help: t("next.general.workingDirHelp"),
              keywords: `${values.intermediateDir} incomplete temporary`,
              control: {
                kind: "path",
                value: values.intermediateDir,
                placeholder: `${dataDir}/intermediate`,
                onChange: (next) => edit("intermediateDir", next),
                onCommit: (path) => commit("intermediateDir", path),
              },
            },
            {
              id: "completeDir",
              label: t("next.general.completeDir"),
              help: t("next.general.completeDirHelp"),
              keywords: `${values.completeDir} destination`,
              control: {
                kind: "path",
                value: values.completeDir,
                placeholder: `${dataDir}/complete`,
                onChange: (next) => edit("completeDir", next),
                onCommit: (path) => commit("completeDir", path),
              },
            },
            {
              id: "cleanupAfterExtract",
              label: t("next.general.cleanup"),
              help: t("next.general.cleanupHelp"),
              keywords: "delete rar par2 housekeeping",
              control: {
                kind: "toggle",
                value: values.cleanupAfterExtract,
                onChange: (next) => commit("cleanupAfterExtract", next),
              },
            },
          ],
        }
      : null,
    values
      ? {
          kind: "section",
          id: "duplicates",
          title: t("next.general.duplicates"),
          note: t("next.general.duplicatesNote"),
          fields: DUPLICATE_FIELDS.map(({ key, label, help }) => ({
            id: key,
            label: t(label),
            help: t(help),
            keywords: "duplicate",
            control: {
              kind: "select" as const,
              value: values[key],
              options: DUPLICATE_ACTIONS.map((action) => ({
                value: action,
                label: t(DUPLICATE_ACTION_LABEL[action]),
              })),
              onChange: (next: string) => commit(key, next as DuplicateAction),
            },
          })),
        }
      : null,
  ];

  return (
    <>
      <SettingsBlocks blocks={blocks} loading={fetching && !data} />
      <ConfirmDialog
        open={confirmClear}
        title={t("next.archivePasswords.clearTitle")}
        body={t("next.archivePasswords.clearBody")}
        confirmLabel={t("next.common.clear")}
        onConfirm={() => {
          setConfirmClear(false);
          clearPasswords();
        }}
        onDismiss={() => setConfirmClear(false)}
      />
    </>
  );
}

/**
 * The hardware profile, saved the moment a profile is picked.
 *
 * The daemon validates the pick against the machine it is running on, and a
 * refusal belongs beside the select.
 */
function PerformanceProfile({ settings }: { settings: HardwareProfileSettings }) {
  const t = useTranslate();
  const [state, setProfile] = useMutation(SET_HARDWARE_PROFILE_MUTATION);
  const [chosen, setChosen] = useState<HardwareProfileName | null>(null);
  const [error, setError] = useState<string | null>(null);

  // The mutation answers with the whole setting, so a save is visible without
  // asking the daemon again.
  const saved = (state.data?.setHardwareProfile as HardwareProfileSettings | undefined) ?? settings;
  const value = chosen ?? initialProfile(saved);
  const scheduledNotice = scheduledProfileNotice(t, saved);

  const pick = (next: HardwareProfileName) => {
    setChosen(next);
    setError(null);
    void setProfile({ profile: next }).then((result) => {
      if (result.error) {
        setChosen(null);
        setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      }
    });
  };

  return (
    <div className="flex min-w-0 flex-col items-end gap-2">
      <Select
        label={t("next.performance.profile")}
        options={saved.options.map((option) => {
          const Icon = { EFFICIENT: Leaf, BALANCED: Scale, PERFORMANCE: Rocket }[option.profile];
          return {
            value: option.profile,
            label: profileName(t, option.profile),
            icon: <Icon aria-hidden="true" size={16} strokeWidth={1.5} className="flex-none text-wv-muted" />,
          };
        })}
        value={value}
        onChange={pick}
        disabled={state.fetching}
      />
      {saved.selected === null ? (
        <p className="text-[11.5px] text-wv-dim">{t("next.performance.notConfirmed")}</p>
      ) : null}
      {scheduledNotice ? <p className="text-[11.5px] text-wv-dim">{scheduledNotice}</p> : null}
      {error ? (
        <p role="alert" className="text-[12.5px] text-wv-error-text">
          {error}
        </p>
      ) : null}
    </div>
  );
}

/** A button that asks the release checker to look now, and what it last found. */
function UpdateCheck() {
  const t = useTranslate();
  const { busy, summary, failed, check } = useUpdateCheck();
  return (
    <div className="flex min-w-0 max-w-full flex-col items-end gap-2">
      <SecondaryButton icon="refresh" disabled={busy} onClick={check}>
        {t("next.general.checkNow")}
      </SecondaryButton>
      <span role="status" className={`max-w-full text-right text-[12.5px] whitespace-normal [overflow-wrap:anywhere] ${failed ? "text-wv-error" : "text-wv-dim"}`}>
        {summary}
      </span>
    </div>
  );
}
