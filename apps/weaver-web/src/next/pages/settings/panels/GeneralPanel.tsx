import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import { useLanguageSettings } from "@/lib/context/translate-context";
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
  ACCEPT: "Accept",
  WARN: "Warn",
  PAUSE: "Pause",
  BLOCK: "Block",
};

const DUPLICATE_OPTIONS = DUPLICATE_ACTIONS.map((action) => ({
  value: action,
  label: DUPLICATE_ACTION_LABEL[action],
}));

const DUPLICATE_FIELDS: { key: keyof DuplicatePolicy; label: string; help: string }[] = [
  {
    key: "strictActiveOrSuccess",
    label: "Same release, already here",
    help: "An identical NZB that is downloading now or finished successfully.",
  },
  {
    key: "strictFailedOrCancelled",
    label: "Same release, previously failed",
    help: "An identical NZB whose earlier attempt failed or was cancelled.",
  },
  {
    key: "articleLayoutActiveOrSuccess",
    label: "Same article layout, already here",
    help: "A different NZB that posts the same articles as a live or finished job.",
  },
  {
    key: "articleLayoutFailedOrCancelled",
    label: "Same article layout, previously failed",
    help: "A different NZB that posts the same articles as a failed attempt.",
  },
  {
    key: "articleSet",
    label: "Overlapping article set",
    help: "A partial overlap with something weaver has already seen.",
  },
  {
    key: "normalizedName",
    label: "Same normalised name",
    help: "A release whose name matches once casing, spacing and tags are stripped.",
  },
];

export function GeneralPanel() {
  const { uiLanguage, setLanguagePreference } = useLanguageSettings();
  const [{ data }, reexecute] = useQuery<{ settings: GeneralSettings }>({ query: SETTINGS_QUERY });
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
          setError(result.error?.message ?? "Could not save these settings.");
          return;
        }
        draft.markSaved();
        setStatus("Saved");
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const interfaceFields: FieldSpec[] = [
    {
      id: "language",
      label: "Language",
      help: "Display language for the Weaver interface. Applies to this browser only.",
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
      label: "New interface",
      help: "Turning this off returns this browser to the classic interface and reloads the page.",
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
    { kind: "section", id: "interface", title: "Interface", fields: interfaceFields },
    values
      ? {
          kind: "section",
          id: "downloads",
          title: "Downloads",
          fields: [
            {
              id: "maxRetries",
              label: "Article retries",
              help: "How many times a failed article is tried again before weaver gives up on it.",
              control: {
                kind: "number",
                value: values.maxRetries,
                min: 0,
                max: 20,
                onChange: (next) => draft.set({ maxRetries: next }),
                suffix: "attempts",
              },
            },
            {
              id: "propagationDelaySecs",
              label: "Propagation delay",
              help: "Hold a new NZB this long before starting, so every article has reached the servers.",
              keywords: "wait posting delay seconds",
              control: {
                kind: "number",
                value: values.propagationDelaySecs,
                min: 0,
                step: 60,
                onChange: (next) => draft.set({ propagationDelaySecs: next }),
                suffix: "seconds",
              },
            },
            {
              id: "ipReplacement",
              label: "Extra connection while an address is on trial",
              help: "Open one additional connection to a provider address weaver is still assessing.",
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
              label: "SRRDB release lookup",
              help: "For obfuscated archive members, send only their CRC32 checksum to the public SRRDB index to recover a release name.",
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
          title: "Storage",
          note: "paths the daemon writes to",
          fields: [
            {
              id: "dataDir",
              label: "Data directory",
              help: "Set when the daemon starts; the database and the default folders live here.",
              keywords: values.dataDir,
              control: { kind: "static", value: values.dataDir },
            },
            {
              id: "intermediateDir",
              label: "Working directory",
              help: "Where downloads are assembled before post-processing. Blank uses the data directory.",
              keywords: `${values.intermediateDir} incomplete temporary`,
              control: {
                kind: "text",
                value: values.intermediateDir,
                placeholder: `${values.dataDir}/intermediate`,
                onChange: (next) => draft.set({ intermediateDir: next }),
              },
            },
            {
              id: "completeDir",
              label: "Completed directory",
              help: "Where finished downloads land. Categories are relative to this folder.",
              keywords: `${values.completeDir} destination`,
              control: {
                kind: "text",
                value: values.completeDir,
                placeholder: `${values.dataDir}/complete`,
                onChange: (next) => draft.set({ completeDir: next }),
              },
            },
            {
              id: "cleanupAfterExtract",
              label: "Clean up after unpacking",
              help: "Delete archives and repair files once a release has been unpacked successfully.",
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
          title: "Duplicates",
          note: "what happens when a new NZB matches something weaver has seen",
          fields: DUPLICATE_FIELDS.map(({ key, label, help }) => ({
            id: key,
            label,
            help,
            keywords: "duplicate",
            control: {
              kind: "select" as const,
              value: values.duplicatePolicy[key],
              options: DUPLICATE_OPTIONS,
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

  return <SettingsBlocks blocks={blocks} />;
}
