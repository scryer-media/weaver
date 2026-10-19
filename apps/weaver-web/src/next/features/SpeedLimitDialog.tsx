// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useMutation, useQuery } from "urql";
import { BANDWIDTH_QUERY, SET_BANDWIDTH } from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { Bandwidth } from "@/lib/networking";
import { PrimaryButton, SecondaryButton } from "@/next/components/controls";
import { Dialog } from "@/next/components/Dialog";
import { FormRow } from "@/next/components/rows";
import { formatRate } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { LimitControl } from "@/next/features/networking/limits";

/**
 * The download ceiling dialog, opened from the queue header and the rail's
 * Throughput gauge.
 *
 * It writes the same global download limit as Settings → Networking →
 * Egress, through `setBandwidth`, so the two never disagree and a limit set
 * here outlives a restart. `setBandwidth` replaces both directions, so the
 * upload limit is read when the dialog opens and sent back as it was. The
 * download limit it
 * starts from is the one the live metrics stream carries, so every opener sees
 * a change within a tick without refetching the settings.
 */
export function useSpeedLimitDialog() {
  const t = useTranslate();
  const { speedLimit: ceiling, downloadBlock } = useNextData("speedLimit", "downloadBlock");
  const [saveState, setBandwidth] = useMutation(SET_BANDWIDTH);
  const [open, setOpen] = useState(false);
  const [limit, setLimit] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [{ data: bandwidth }] = useQuery<{ networking: { bandwidth: Bandwidth } }>({
    query: BANDWIDTH_QUERY,
    requestPolicy: "network-only",
    pause: !open,
  });
  const upload = bandwidth?.networking?.bandwidth;

  const scheduled = downloadBlock.scheduledSpeedLimit;

  const openDialog = () => {
    setLimit(ceiling);
    setError(null);
    setOpen(true);
  };

  const save = () => {
    if (!upload) {
      return;
    }
    setError(null);
    void setBandwidth({ input: { downloadBps: limit, uploadBps: upload.uploadBps ?? null } }).then((result) => {
      if (result.error || !result.data?.setBandwidth) {
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
          <PrimaryButton disabled={saveState.fetching || !upload} onClick={save}>
            {saveState.fetching ? t("settings.saving") : t("action.apply")}
          </PrimaryButton>
        </>
      }
    >
      <FormRow label={t("next.speedLimit.ceiling")}>
        <LimitControl
          key={String(open)}
          label={t("next.speedLimit.ceilingAria")}
          value={limit}
          onChange={setLimit}
          suffix="MB/s"
        />
      </FormRow>
      {scheduled !== null ? (
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
 * the configured limit. Null is unlimited; zero lets nothing through.
 */
export function useSpeedLimitInForce() {
  const { speedLimit, downloadBlock } = useNextData("speedLimit", "downloadBlock");
  const scheduled = downloadBlock.scheduledSpeedLimit;
  return { inForce: scheduled ?? speedLimit, bySchedule: scheduled !== null };
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
        {inForce !== null ? formatRate(inForce) : t("settings.unlimited")}
      </SecondaryButton>
      {dialog}
    </>
  );
}
