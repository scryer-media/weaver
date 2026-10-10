import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  ARCHIVE_PASSWORD_SETTINGS_QUERY,
  HARDWARE_PROFILE_QUERY,
  SET_HARDWARE_PROFILE_MUTATION,
  SETTINGS_QUERY,
  UPDATE_ARCHIVE_PASSWORD_SETTINGS_MUTATION,
  UPDATE_SETTINGS_MUTATION,
} from "@/graphql/queries";
import { useLanguageSettings, useTranslate } from "@/lib/context/translate-context";
import { AVAILABLE_LANGUAGES } from "@/lib/i18n";
import {
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
import { SecondaryButton, Select, TextArea } from "../../../components/controls";
import {
  SettingsBlocks,
  useDraft,
  usePanelState,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * General: everything the daemon keeps in `GeneralSettings`, plus the
 * browser-local language preference.
 *
 * The daemon's fields are one draft saved by the top bar's Save; the language
 * applies the moment it changes, so it never enters the draft.
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
  /** Whether the daemon holds a password list; the list itself never comes back. */
  hasArchivePasswords: boolean;
  /** A replacement list, blank to keep what is stored, null to remove it on save. */
  archivePasswords: string | null;
  archivePasswordFile: string;
}

interface ArchivePasswordSettings {
  hasPasswords: boolean;
  passwordFile: string | null;
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
  const [{ data, fetching }, reexecute] = useQuery<{ settings: GeneralSettings }>({ query: SETTINGS_QUERY });
  const [{ data: profileData }] = useQuery<{ hardwareProfile: HardwareProfileSettings }>({
    query: HARDWARE_PROFILE_QUERY,
  });
  const profileSettings = profileData?.hardwareProfile ?? null;
  const [{ data: archiveData }, reexecuteArchive] = useQuery<{
    archivePasswordSettings: ArchivePasswordSettings;
  }>({ query: ARCHIVE_PASSWORD_SETTINGS_QUERY });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [archiveState, updateArchivePasswords] = useMutation(UPDATE_ARCHIVE_PASSWORD_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const source = useMemo<GeneralSettings | null>(() => {
    const settings = data?.settings;
    const archive = archiveData?.archivePasswordSettings;
    return settings && archive
      ? {
          ...settings,
          duplicatePolicy: normalizeDuplicatePolicy(settings.duplicatePolicy),
          hasArchivePasswords: archive.hasPasswords,
          archivePasswords: "",
          archivePasswordFile: archive.passwordFile ?? "",
        }
      : null;
  }, [data?.settings, archiveData?.archivePasswordSettings]);

  const draft = useDraft(source, () => {
    setStatus(null);
    setError(null);
  });
  const values = draft.value;

  usePanelState({
    dirty: draft.dirty,
    busy: updateState.fetching || archiveState.fetching,
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
      setError(null);
      // The password list only travels when it changed: blank keeps what is
      // stored, and the daemon never echoes it back to compare against.
      const archiveChanged =
        values.archivePasswords !== "" ||
        values.archivePasswordFile.trim() !== source.archivePasswordFile;
      void Promise.all([
        updateSettings({
          input: {
            intermediateDir: values.intermediateDir.trim() || null,
            completeDir: values.completeDir.trim() || null,
            cleanupAfterExtract: values.cleanupAfterExtract,
            maxRetries: values.maxRetries,
            propagationDelaySecs: values.propagationDelaySecs,
            enableSrrdbLookup: values.enableSrrdbLookup,
            duplicatePolicy: values.duplicatePolicy,
          },
        }),
        archiveChanged
          ? updateArchivePasswords({
              ...(values.archivePasswords === ""
                ? {}
                : { passwords: values.archivePasswords === null ? [] : values.archivePasswords.split(/\r?\n/) }),
              passwordFile: values.archivePasswordFile.trim() || null,
            })
          : null,
      ]).then(([general, archive]) => {
        if (general.error || !general.data?.updateSettings) {
          setError(general.error?.message ?? t("next.settings.saveFailed"));
          return;
        }
        if (archive && (archive.error || !archive.data?.updateArchivePasswordSettings)) {
          setError(archive.error?.message ?? t("next.settings.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
        if (archive) {
          void reexecuteArchive({ requestPolicy: "network-only" });
        }
      });
    },
  });

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
                onChange: (next) => draft.set({ maxRetries: next }),
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
                onChange: (next) => draft.set({ propagationDelaySecs: next }),
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
                onChange: (next) => draft.set({ enableSrrdbLookup: next }),
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
                    <TextArea
                      label={t("next.archivePasswords.list")}
                      value={values.archivePasswords ?? ""}
                      rows={3}
                      secret
                      placeholder={
                        values.archivePasswords === null
                          ? t("next.archivePasswords.cleared")
                          : values.hasArchivePasswords
                            ? "••••••••"
                            : undefined
                      }
                      className="w-[190px] max-w-full"
                      onChange={(next) => draft.set({ archivePasswords: next })}
                    />
                    {values.hasArchivePasswords ? (
                      <SecondaryButton
                        onClick={() =>
                          draft.set({ archivePasswords: values.archivePasswords === null ? "" : null })
                        }
                      >
                        {t(values.archivePasswords === null ? "next.archivePasswords.keep" : "next.common.clear")}
                      </SecondaryButton>
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
                kind: "text",
                value: values.archivePasswordFile,
                placeholder: `${values.dataDir}/archive-passwords.txt`,
                onChange: (next) => draft.set({ archivePasswordFile: next }),
              },
            },
          ],
        }
      : null,
    profileSettings && offersProfileChoice(profileSettings)
      ? {
          kind: "custom",
          id: "performance",
          title: t("next.performance.title"),
          note: t("next.performance.body"),
          searchText: "performance profile hardware memory threads efficient balanced",
          body: <PerformanceProfile settings={profileSettings} />,
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
              keywords: values.dataDir,
              control: { kind: "static", value: values.dataDir },
            },
            {
              id: "intermediateDir",
              label: t("next.general.workingDir"),
              help: t("next.general.workingDirHelp"),
              keywords: `${values.intermediateDir} incomplete temporary`,
              control: {
                kind: "path",
                value: values.intermediateDir,
                placeholder: `${values.dataDir}/intermediate`,
                onChange: (next) => draft.set({ intermediateDir: next }),
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
                placeholder: `${values.dataDir}/complete`,
                onChange: (next) => draft.set({ completeDir: next }),
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
                onChange: (next) => draft.set({ cleanupAfterExtract: next }),
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
              value: values.duplicatePolicy[key],
              options: DUPLICATE_ACTIONS.map((action) => ({
                value: action,
                label: t(DUPLICATE_ACTION_LABEL[action]),
              })),
              onChange: (next: string) =>
                draft.set({
                  duplicatePolicy: {
                    ...values.duplicatePolicy,
                    [key]: next as DuplicateAction,
                  },
                }),
            },
          })),
        }
      : null,
  ];

  return <SettingsBlocks blocks={blocks} loading={fetching && !data} />;
}

/**
 * The hardware profile, saved the moment a profile is picked.
 *
 * It is not part of the panel's draft: the daemon validates the pick against
 * the machine it is running on, and a refusal belongs beside the select rather
 * than in the top bar's Save.
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
    <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6">
      <span className="text-[13px] font-semibold text-wv-fg">{t("next.performance.profile")}</span>
      <div className="ml-auto flex min-w-0 flex-col items-end gap-2">
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
      <p className="text-[11.5px] text-wv-dim">{t("next.performance.timing")}</p>
      {error ? (
        <p role="alert" className="text-[12.5px] text-wv-error-text">
          {error}
        </p>
      ) : null}
      </div>
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
