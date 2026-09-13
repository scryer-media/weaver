import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import { useLanguageSettings, useTranslate } from "@/lib/context/translate-context";
import { AVAILABLE_LANGUAGES } from "@/lib/i18n";
import { setUiVariant } from "@/lib/ui-variant";
import {
  DUPLICATE_ACTIONS,
  normalizeDuplicatePolicy,
  type DuplicateAction,
  type DuplicatePolicy,
} from "@/features/duplicates/duplicate-policy";
import {
  SettingsBlocks,
  useDraft,
  usePanelState,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * General: everything the daemon keeps in `GeneralSettings`, plus the two
 * browser-local preferences (language and which interface this browser uses).
 *
 * The daemon's fields are one draft saved by the top bar's Save; the two local
 * preferences apply the moment they change, so they never enter the draft.
 */

interface GeneralSettings {
  dataDir: string;
  intermediateDir: string;
  completeDir: string;
  cleanupAfterExtract: boolean;
  maxRetries: number;
  propagationDelaySecs: number;
  ipReplacementTrialExtraConnections: number;
  enableSrrdbLookup: boolean;
  duplicatePolicy: DuplicatePolicy;
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
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const source = useMemo<GeneralSettings | null>(() => {
    const settings = data?.settings;
    return settings
      ? { ...settings, duplicatePolicy: normalizeDuplicatePolicy(settings.duplicatePolicy) }
      : null;
  }, [data?.settings]);

  const draft = useDraft(source, () => {
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
      if (!values) {
        return;
      }
      setError(null);
      void updateSettings({
        input: {
          intermediateDir: values.intermediateDir.trim() || null,
          completeDir: values.completeDir.trim() || null,
          cleanupAfterExtract: values.cleanupAfterExtract,
          maxRetries: values.maxRetries,
          propagationDelaySecs: values.propagationDelaySecs,
          ipReplacementTrialExtraConnections: values.ipReplacementTrialExtraConnections,
          enableSrrdbLookup: values.enableSrrdbLookup,
          duplicatePolicy: values.duplicatePolicy,
        },
      }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.settings.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
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
    {
      id: "ui-variant",
      label: t("next.general.newInterface"),
      help: t("next.general.newInterfaceHelp"),
      keywords: "classic theme appearance layout",
      control: {
        kind: "toggle",
        value: true,
        onChange: (next) => {
          if (!next) {
            setUiVariant("classic");
          }
        },
      },
    },
  ];

  const blocks: (SettingsBlock | null)[] = [
    { kind: "section", id: "interface", title: t("next.general.interface"), fields: interfaceFields },
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
              id: "ipReplacement",
              label: t("next.general.trialConnection"),
              help: t("next.general.trialConnectionHelp"),
              keywords: "ip replacement trial connections",
              control: {
                kind: "toggle",
                value: values.ipReplacementTrialExtraConnections > 0,
                onChange: (next) =>
                  draft.set({ ipReplacementTrialExtraConnections: next ? 1 : 0 }),
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
