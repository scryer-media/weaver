import { useState } from "react";
import { useMutation } from "urql";
import { UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { NumberField, PrimaryButton, SecondaryButton } from "@/next/components/controls";
import { Dialog } from "@/next/components/Dialog";
import { FormRow } from "@/next/components/rows";
import { formatRate } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";

const MIB = 1024 * 1024;

/**
 * The download ceiling dialog, opened from the queue header and the rail's
 * Throughput gauge.
 *
 * It writes the same `maxDownloadSpeed` setting as Settings → Bandwidth, so the
 * two never disagree and a limit set here outlives a restart. The ceiling it
 * starts from is the one the live metrics stream carries, so every opener sees
 * a change within a tick without refetching the settings.
 */
export function useSpeedLimitDialog() {
  const t = useTranslate();
  const { speedLimit: ceiling, downloadBlock } = useNextData();
  const [saveState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [open, setOpen] = useState(false);
  const [megabytes, setMegabytes] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const scheduled = downloadBlock.scheduledSpeedLimit;

  const openDialog = () => {
    setMegabytes(Math.round((ceiling / MIB) * 10) / 10);
    setError(null);
    setOpen(true);
  };

  const save = () => {
    const bytes = Math.round(Math.max(0, megabytes) * MIB);
    setError(null);
    void updateSettings({ input: { maxDownloadSpeed: bytes } }).then((result) => {
      if (result.error || !result.data?.updateSettings) {
        setError(result.error?.message ?? t("next.speedLimit.saveFailed"));
        return;
      }
      setOpen(false);
    });
  };

  const dialog = (
    <Dialog
      open={open}
      title={t("next.speedLimit.title")}
      width={580}
      note={t("next.speedLimit.note")}
      onDismiss={() => setOpen(false)}
      footer={
        <>
          <SecondaryButton onClick={() => setOpen(false)}>{t("action.cancel")}</SecondaryButton>
          <PrimaryButton disabled={saveState.fetching} onClick={save}>
            {saveState.fetching ? t("settings.saving") : t("action.apply")}
          </PrimaryButton>
        </>
      }
    >
      <FormRow label={t("next.speedLimit.ceiling")} help={t("next.speedLimit.ceilingHelp")}>
        <NumberField
          label={t("next.speedLimit.ceilingAria")}
          value={megabytes}
          onChange={setMegabytes}
          min={0}
          suffix="MB/s"
        />
      </FormRow>
      {scheduled > 0 ? (
        <div className="border-b border-wv-hairline px-4 py-3 text-[12.5px] text-wv-warn sm:px-6">
          {t("next.speedLimit.scheduleHolding", { rate: formatRate(scheduled) })}
        </div>
      ) : null}
      {error === null ? null : (
        <div className="px-4 py-3 text-[12.5px] text-wv-error-text sm:px-6">{error}</div>
      )}
    </Dialog>
  );

  return { openDialog, dialog };
}

/**
 * The limit actually in force: a schedule's limit while one holds, otherwise
 * the configured ceiling. Zero is unlimited.
 */
export function useSpeedLimitInForce() {
  const { speedLimit, downloadBlock } = useNextData();
  const scheduled = downloadBlock.scheduledSpeedLimit;
  return { inForce: scheduled > 0 ? scheduled : speedLimit, bySchedule: scheduled > 0 };
}

/** The queue header's button: the limit in force, opening the dialog. */
export function SpeedLimitControl() {
  const t = useTranslate();
  const { inForce, bySchedule } = useSpeedLimitInForce();
  const { openDialog, dialog } = useSpeedLimitDialog();
  return (
    <>
      <SecondaryButton
        icon="bandwidth"
        title={bySchedule ? t("next.speedLimit.bySchedule") : t("next.speedLimit.buttonTitle")}
        onClick={openDialog}
      >
        {inForce > 0 ? formatRate(inForce) : t("settings.unlimited")}
      </SecondaryButton>
      {dialog}
    </>
  );
}
