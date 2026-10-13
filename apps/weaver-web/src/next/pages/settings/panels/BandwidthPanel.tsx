import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { egressQuotaTable, useEgressQuotas } from "../../../features/networking/egress-quota";
import { SettingsBlocks, useDraft, usePanelState, type FieldSpec, type SettingsBlock } from "../framework";

/**
 * Bandwidth: the global download ceiling, and how much of each egress's
 * download quota is spent.
 *
 * A quota belongs to the egress it meters and is set in that egress's editor;
 * this panel lists every egress, System first, with the counters weaver
 * enforces.
 */

const MIB = 1024 ** 2;

interface BandwidthDraft {
  maxDownloadSpeed: number;
}

export function BandwidthPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ settings: { maxDownloadSpeed: number } }>({
    query: SETTINGS_QUERY,
  });
  const [updateState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const quotas = useEgressQuotas(t);

  const source = useMemo<BandwidthDraft | null>(() => {
    const settings = data?.settings;
    return settings ? { maxDownloadSpeed: settings.maxDownloadSpeed ?? 0 } : null;
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
      void updateSettings({ input: { maxDownloadSpeed: values.maxDownloadSpeed } }).then((result) => {
        if (result.error || !result.data?.updateSettings) {
          setError(result.error?.message ?? t("next.bandwidth.saveFailed"));
          return;
        }
        draft.markSaved();
        setStatus(t("next.settings.saved"));
        void reexecute({ requestPolicy: "network-only" });
      });
    },
  });

  const limitFields: FieldSpec[] = values
    ? [
        {
          id: "maxDownloadSpeed",
          label: t("next.bandwidth.ceiling"),
          help: t("next.bandwidth.ceilingHelp"),
          keywords: "speed limit throttle rate",
          // The same field as the speed-limit dialog, so the ceiling reads and
          // edits identically wherever it is set.
          control: {
            kind: "number",
            value: Math.round((values.maxDownloadSpeed / MIB) * 10) / 10,
            min: 0,
            onChange: (next) => draft.set({ maxDownloadSpeed: Math.round(Math.max(0, next) * MIB) }),
            suffix: "MB/s",
          },
        },
      ]
    : [];

  const blocks: (SettingsBlock | null)[] = [
    values ? { kind: "section", id: "limits", title: t("next.bandwidth.limits"), fields: limitFields } : null,
    quotas.loading
      ? null
      : {
          ...egressQuotaTable(t, quotas.rows),
          // A failed read says so inside the list rather than showing zeroes.
          ...(quotas.error ? { note: <span className="text-wv-error-text">{quotas.error}</span> } : {}),
        },
  ];

  return <SettingsBlocks blocks={blocks} loading={fetching && !data} />;
}
