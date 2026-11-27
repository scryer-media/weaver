// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  BANDWIDTH_QUERY,
  SET_BANDWIDTH,
  SET_EGRESS_QUOTA,
} from "@/graphql/networking";
import { useTranslate } from "@/lib/context/translate-context";
import type { Bandwidth, BandwidthSplit } from "@/lib/networking";
import { SectionHeader } from "@/next/components/chrome";
import { PrimaryButton, SecondaryButton } from "@/next/components/controls";
import { Dialog } from "@/next/components/Dialog";
import { FormRow } from "@/next/components/rows";
import { formatRate } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { useTorrentsShown } from "@/next/data/use-torrents-shown";
import {
  egressQuotaChanged,
  egressQuotaDraft,
  egressQuotaFields,
  egressQuotaInput,
  egressQuotaProblem,
  useEgressQuotas,
  type EgressQuotaDraft,
} from "@/next/features/networking/egress-quota";
import { LimitControl, SplitControl } from "@/next/features/networking/limits";
import { FieldRows, Fold } from "@/next/pages/settings/framework";

/**
 * The speed limit dialog, opened from the queue header and the rail's
 * Throughput gauge.
 *
 * It writes the same global limits as Settings → Networking → Egress,
 * through `setBandwidth`, so the two never disagree and a limit set here
 * outlives a restart. While torrents are on it holds every limit that page
 * does: download, the split of it between usenet and torrents, and upload.
 * `setBandwidth` replaces both directions, so with torrents off the upload
 * limit is read when the dialog opens and sent back as it was, and the split
 * is left out, which keeps it. The download limit it starts from is the one
 * the live metrics stream carries, so every opener sees a change within a
 * tick without refetching the settings.
 *
 * Below the speeds an "Adjust bandwidth quotas" button slides open each
 * egress's quota, one egress at a time behind a picker, with the same fields
 * and `setEgressQuota` as the egress editor. It starts shut each time the
 * dialog opens. Every egress edited since then is saved on Apply.
 */
export function useSpeedLimitDialog() {
  const t = useTranslate();
  const { speedLimit: ceiling, downloadBlock } = useNextData(
    "speedLimit",
    "downloadBlock",
  );
  const [saveState, setBandwidth] = useMutation(SET_BANDWIDTH);
  const [open, setOpen] = useState(false);
  const [limit, setLimit] = useState<number | null>(null);
  // Changes to what was read when the dialog opened; undefined is untouched.
  const [uploadEdit, setUploadEdit] = useState<number | null | undefined>(
    undefined,
  );
  const [splitEdit, setSplitEdit] = useState<BandwidthSplit | null | undefined>(
    undefined,
  );
  const torrentsOn = useTorrentsShown();
  const [error, setError] = useState<string | null>(null);
  const [{ data: bandwidth }] = useQuery<{
    networking: { bandwidth: Bandwidth };
  }>({
    query: BANDWIDTH_QUERY,
    requestPolicy: "network-only",
    pause: !open,
  });
  const saved = bandwidth?.networking?.bandwidth;
  const uploadBps =
    uploadEdit !== undefined ? uploadEdit : (saved?.uploadBps ?? null);
  const split = splitEdit !== undefined ? splitEdit : (saved?.split ?? null);

  const scheduled = downloadBlock.scheduledSpeedLimit;

  // Quotas are per egress: one is shown at a time, and every egress edited
  // since the dialog opened is saved on Apply.
  const { rows: egresses } = useEgressQuotas(t, !open);
  const [, setEgressQuota] = useMutation(SET_EGRESS_QUOTA);
  const [quotaEgressId, setQuotaEgressId] = useState(0);
  const [quotaEdits, setQuotaEdits] = useState<
    Record<number, EgressQuotaDraft>
  >({});
  const [savingQuotas, setSavingQuotas] = useState(false);
  // The quotas sit shut behind their header; a refused quota opens them.
  const [quotaOpen, setQuotaOpen] = useState(false);
  const quotaEgress =
    egresses.find((egress) => egress.id === quotaEgressId) ?? egresses[0];
  const quotaDraft = quotaEgress
    ? (quotaEdits[quotaEgress.id] ?? egressQuotaDraft(quotaEgress.quota))
    : null;

  const openDialog = () => {
    setLimit(ceiling);
    setUploadEdit(undefined);
    setSplitEdit(undefined);
    setQuotaEdits({});
    setQuotaOpen(false);
    setError(null);
    setOpen(true);
  };

  const save = async () => {
    if (!saved) {
      return;
    }
    setError(null);
    const edited = egresses.filter(
      (egress) => quotaEdits[egress.id] !== undefined,
    );
    const changed = edited.filter((egress) =>
      egressQuotaChanged(quotaEdits[egress.id]!, egress.quota),
    );
    // A cap switched on with nothing typed saves as no cap, so it reads as unchanged: check every edit.
    const invalid = edited.find(
      (egress) => egressQuotaProblem(t, quotaEdits[egress.id]!) !== null,
    );
    if (invalid) {
      setQuotaEgressId(invalid.id);
      setQuotaOpen(true);
      setError(egressQuotaProblem(t, quotaEdits[invalid.id]!));
      return;
    }
    const input = {
      downloadBps: limit,
      uploadBps,
      ...(torrentsOn ? { split } : {}),
    };
    const result = await setBandwidth({ input });
    if (result.error || !result.data?.setBandwidth) {
      setError(result.error?.message ?? t("next.speedLimit.saveFailed"));
      return;
    }
    setSavingQuotas(true);
    try {
      for (const egress of changed) {
        const quota = await setEgressQuota({
          id: egress.id,
          input: egressQuotaInput(quotaEdits[egress.id]!),
        });
        if (quota.error || !quota.data?.setEgressQuota) {
          setQuotaEgressId(egress.id);
          setError(quota.error?.message ?? t("next.speedLimit.saveFailed"));
          return;
        }
      }
    } finally {
      setSavingQuotas(false);
    }
    setOpen(false);
  };

  // Only torrents upload: with them off the upload caps are hidden and saved as they were.
  const quotaFields =
    quotaEgress && quotaDraft
      ? egressQuotaFields(
          t,
          quotaDraft,
          (next) =>
            setQuotaEdits((edits) => ({ ...edits, [quotaEgress.id]: next })),
          {
            down: quotaEgress.quotaUsage?.down.usedBytes,
            up: quotaEgress.quotaUsage?.up.usedBytes,
          },
        ).filter(
          (field) =>
            torrentsOn ||
            !["quota-uploadSoft", "quota-trickle", "quota-uploadHard"].includes(
              field.id,
            ),
        )
      : [];
  const busy = saveState.fetching || savingQuotas;

  const dialog = (
    <Dialog
      open={open}
      title={t("next.speedLimit.title")}
      width={580}
      onDismiss={() => setOpen(false)}
      footer={
        <>
          <SecondaryButton onClick={() => setOpen(false)}>
            {t("action.cancel")}
          </SecondaryButton>
          <PrimaryButton disabled={busy || !saved} onClick={() => void save()}>
            {busy ? t("settings.saving") : t("action.apply")}
          </PrimaryButton>
        </>
      }
    >
      <SectionHeader label={t("next.speedLimit.speed")} sticky={false} />
      <FormRow label={t("next.speedLimit.ceiling")}>
        <LimitControl
          key={String(open)}
          label={t("next.speedLimit.ceilingAria")}
          value={limit}
          onChange={setLimit}
          suffix="MB/s"
        />
      </FormRow>
      {/* Only torrents upload, and the split shares the download limit with them: both show with torrents. */}
      {torrentsOn && saved ? (
        <>
          <FormRow
            label={t("next.bandwidth.split")}
            help={
              limit === null
                ? t("next.bandwidth.needsDownloadLimit")
                : undefined
            }
          >
            <SplitControl
              value={split}
              needsDownloadLimit={limit === null}
              onChange={setSplitEdit}
            />
          </FormRow>
          <FormRow label={t("next.speedLimit.upload")}>
            <LimitControl
              key={String(open)}
              label={t("next.speedLimit.uploadAria")}
              value={uploadBps}
              onChange={setUploadEdit}
              suffix="MB/s"
            />
          </FormRow>
        </>
      ) : null}
      {quotaEgress ? (
        <>
          <div className="flex justify-center border-b border-wv-hairline px-4 py-[14px] sm:px-6">
            <SecondaryButton
              expanded={quotaOpen}
              onClick={() => setQuotaOpen((was) => !was)}
            >
              {t("next.speedLimit.adjustQuotas")}
            </SecondaryButton>
          </div>
          <Fold open={quotaOpen}>
            <FieldRows
              fields={[
                {
                  id: "quota-egress",
                  label: t("next.networking.egress.section"),
                  control: {
                    kind: "select",
                    value: String(quotaEgress.id),
                    options: egresses.map((egress) => ({
                      value: String(egress.id),
                      label: egress.name,
                    })),
                    onChange: (id) => setQuotaEgressId(Number(id)),
                  },
                },
                ...quotaFields,
              ]}
            />
          </Fold>
        </>
      ) : null}
      {scheduled !== null ? (
        <div className="border-b border-wv-hairline px-4 py-3 text-[12.5px] text-wv-warn sm:px-6">
          {t("next.speedLimit.scheduleHolding", {
            rate: formatRate(scheduled),
          })}
        </div>
      ) : null}
      {error === null ? null : (
        <div className="px-4 py-3 text-[12.5px] text-wv-error-text sm:px-6">
          {error}
        </div>
      )}
    </Dialog>
  );

  return { openDialog, dialog };
}

/**
 * The limit the daemon holds the wire to now: the tighter of the configured
 * limit and a schedule's. Null is unlimited; zero lets nothing through.
 */
export function useSpeedLimitInForce() {
  const { downloadBlock } = useNextData("downloadBlock");
  return {
    inForce: downloadBlock.speedLimitInForce,
    bySchedule: downloadBlock.speedLimitBySchedule,
  };
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
        // A limit in force reads as a warning: yellow outline, not a fill.
        className={
          inForce !== null
            ? "border-wv-warn text-wv-warn hover:border-wv-warn-hover"
            : undefined
        }
        title={
          bySchedule
            ? t("next.speedLimit.bySchedule")
            : t("next.speedLimit.buttonTitle")
        }
        onClick={openDialog}
      >
        {inForce !== null ? formatRate(inForce) : t("settings.unlimited")}
      </SecondaryButton>
      {dialog}
    </>
  );
}
