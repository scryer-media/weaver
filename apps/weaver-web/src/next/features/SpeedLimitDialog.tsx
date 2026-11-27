// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  BANDWIDTH_QUERY,
  EGRESS_LIMITS_QUERY,
  SET_BANDWIDTH,
  SET_EGRESS_QUOTA,
  UPDATE_EGRESS,
} from "@/graphql/networking";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import type { Bandwidth, BandwidthSplit, Egress } from "@/lib/networking";
import { SectionHeader } from "@/next/components/chrome";
import { PrimaryButton, SecondaryButton } from "@/next/components/controls";
import { Dialog } from "@/next/components/Dialog";
import { FormRow } from "@/next/components/rows";
import { formatRate } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";
import { useTorrentsShown } from "@/next/data/use-torrents-shown";
import { egressInput } from "@/next/features/networking/data";
import {
  egressQuotaChanged,
  egressQuotaDraft,
  egressQuotaFields,
  egressQuotaInput,
  egressQuotaProblem,
  type EgressQuotaDraft,
} from "@/next/features/networking/egress-quota";
import { LimitControl, SplitControl } from "@/next/features/networking/limits";
import {
  FieldRows,
  Fold,
  type FieldSpec,
} from "@/next/pages/settings/framework";

/** The egress weaver falls back to; it always exists, so it is always listed. */
const SYSTEM_EGRESS_ID = 0;

/** An egress as the dialog reads it: what its update sends back, and its caps. */
type EgressLimits = Pick<
  Egress,
  | "id"
  | "name"
  | "bindingKind"
  | "interfaceName"
  | "sourceAddress"
  | "enabled"
  | "downloadBps"
  | "uploadBps"
  | "quota"
  | "quotaUsage"
>;

/** One egress's own speed limits as edited. */
type EgressRates = Pick<Egress, "downloadBps" | "uploadBps">;

/** Every egress with its own limits and caps, System first, read only while the dialog is open. */
function useEgressLimits(t: Translate, open: boolean): EgressLimits[] {
  const [{ data }] = useQuery<{ egressInterfaces: EgressLimits[] }>({
    query: EGRESS_LIMITS_QUERY,
    requestPolicy: "cache-and-network",
    pause: !open,
  });
  const listed = data?.egressInterfaces;
  if (!listed) {
    return [];
  }
  const rows = listed.some((egress) => egress.id === SYSTEM_EGRESS_ID)
    ? listed
    : [
        {
          id: SYSTEM_EGRESS_ID,
          name: t("next.networking.binding.system"),
          bindingKind: "SYSTEM" as const,
          interfaceName: null,
          sourceAddress: null,
          enabled: true,
          downloadBps: null,
          uploadBps: null,
          quota: undefined,
          quotaUsage: null,
        },
        ...listed,
      ];
  return [...rows].sort((left, right) => left.id - right.id);
}

/** The upload caps: only torrents upload, so with them off these are hidden and saved as they were. */
const UPLOAD_CAP_FIELDS = [
  "quota-uploadSoft",
  "quota-trickle",
  "quota-uploadHard",
];

/**
 * The speed limit dialog, opened from the queue header and the rail's
 * Throughput gauge.
 *
 * Its global limits are the same ones Settings → Networking → Egress holds,
 * written through `setBandwidth`, so the two never disagree and a limit set
 * here outlives a restart. While torrents are on it holds every global limit
 * that page does: download, the split of it between usenet and torrents, and
 * upload. `setBandwidth` replaces both directions, so with torrents off the
 * upload limit is read when the dialog opens and sent back as it was, and
 * the split is left out, which keeps it. The download limit it starts from
 * is the one the live metrics stream carries, so every opener sees a change
 * within a tick without refetching the settings.
 *
 * Below them a button slides open what each egress holds of its own, with the
 * same fields and mutations as the egress editor. With more than one egress
 * it reads "Per-egress settings" and opens a picker, that egress's own speed
 * limits and its caps. With only System there is nothing to pick and its
 * speed limits would repeat the global ones, so it reads "Adjust bandwidth
 * caps" and opens the caps alone. It starts shut each time the dialog opens,
 * and goes once clicked. Every egress edited since then is saved on Apply.
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

  // One egress is shown at a time; every egress edited since the dialog opened is saved on Apply.
  const egresses = useEgressLimits(t, open);
  const perEgress = egresses.length > 1;
  const [, updateEgress] = useMutation(UPDATE_EGRESS);
  const [, setEgressQuota] = useMutation(SET_EGRESS_QUOTA);
  const [egressId, setEgressId] = useState(SYSTEM_EGRESS_ID);
  const [quotaEdits, setQuotaEdits] = useState<
    Record<number, EgressQuotaDraft>
  >({});
  const [rateEdits, setRateEdits] = useState<Record<number, EgressRates>>({});
  const [savingEgresses, setSavingEgresses] = useState(false);
  // Shut until asked for; nothing below can be edited, or refused, before.
  const [egressOpen, setEgressOpen] = useState(false);
  const egress =
    egresses.find((candidate) => candidate.id === egressId) ?? egresses[0];
  const quotaDraft = egress
    ? (quotaEdits[egress.id] ?? egressQuotaDraft(egress.quota))
    : null;
  const rates: EgressRates | null = egress
    ? (rateEdits[egress.id] ?? {
        downloadBps: egress.downloadBps,
        uploadBps: egress.uploadBps,
      })
    : null;

  const openDialog = () => {
    setLimit(ceiling);
    setUploadEdit(undefined);
    setSplitEdit(undefined);
    setQuotaEdits({});
    setRateEdits({});
    setEgressOpen(false);
    setError(null);
    setOpen(true);
  };

  const save = async () => {
    if (!saved) {
      return;
    }
    setError(null);
    // A cap switched on with nothing typed saves as no cap, so it reads as unchanged: check every edit.
    const invalid = egresses.find(
      (candidate) =>
        quotaEdits[candidate.id] !== undefined &&
        egressQuotaProblem(t, quotaEdits[candidate.id]!) !== null,
    );
    if (invalid) {
      setEgressId(invalid.id);
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
    setSavingEgresses(true);
    try {
      for (const candidate of egresses) {
        const rate = rateEdits[candidate.id];
        const quota = quotaEdits[candidate.id];
        const failed = async (
          answer: Promise<{ error?: { message: string }; data?: unknown }>,
        ) => {
          const { error: failure, data } = await answer;
          if (failure || !data) {
            setEgressId(candidate.id);
            setError(failure?.message ?? t("next.speedLimit.saveFailed"));
            return true;
          }
          return false;
        };
        if (
          rate &&
          (rate.downloadBps !== candidate.downloadBps ||
            rate.uploadBps !== candidate.uploadBps) &&
          (await failed(
            updateEgress({
              id: candidate.id,
              input: egressInput({ ...candidate, ...rate }),
            }),
          ))
        ) {
          return;
        }
        if (
          quota &&
          egressQuotaChanged(quota, candidate.quota) &&
          (await failed(
            setEgressQuota({
              id: candidate.id,
              input: egressQuotaInput(quota),
            }),
          ))
        ) {
          return;
        }
      }
    } finally {
      setSavingEgresses(false);
    }
    setOpen(false);
  };

  const patchRates = (next: Partial<EgressRates>) => {
    if (egress && rates) {
      setRateEdits((edits) => ({
        ...edits,
        [egress.id]: { ...rates, ...next },
      }));
    }
  };
  const rateField = (
    id: "downloadLimit" | "uploadLimit",
    label: string,
    value: number | null,
    onChange: (next: number | null) => void,
  ): FieldSpec => ({
    id,
    label,
    control: {
      kind: "custom",
      control: (
        <LimitControl
          // A fresh control per egress, so a rate switched off comes back as that egress's.
          key={`${egress?.id}-${id}`}
          label={label}
          fieldId={id}
          value={value}
          onChange={onChange}
          suffix="MB/s"
        />
      ),
    },
  });
  const egressFields: FieldSpec[] =
    egress && quotaDraft && rates
      ? [
          ...(perEgress
            ? [
                {
                  id: "egress",
                  label: t("next.networking.egress.section"),
                  control: {
                    kind: "select" as const,
                    value: String(egress.id),
                    options: egresses.map((candidate) => ({
                      value: String(candidate.id),
                      label: candidate.name,
                    })),
                    onChange: (id: string) => setEgressId(Number(id)),
                  },
                },
                rateField(
                  "downloadLimit",
                  t("next.networking.egress.downloadLimit"),
                  rates.downloadBps,
                  (downloadBps) => patchRates({ downloadBps }),
                ),
                ...(torrentsOn
                  ? [
                      rateField(
                        "uploadLimit",
                        t("next.networking.egress.uploadLimit"),
                        rates.uploadBps,
                        (next) => patchRates({ uploadBps: next }),
                      ),
                    ]
                  : []),
              ]
            : []),
          ...egressQuotaFields(
            t,
            quotaDraft,
            (next) =>
              setQuotaEdits((edits) => ({ ...edits, [egress.id]: next })),
            {
              down: egress.quotaUsage?.down.usedBytes,
              up: egress.quotaUsage?.up.usedBytes,
            },
          ).filter(
            (field) => torrentsOn || !UPLOAD_CAP_FIELDS.includes(field.id),
          ),
        ]
      : [];
  const busy = saveState.fetching || savingEgresses;

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
      <SectionHeader label={t("next.speedLimit.global")} sticky={false} />
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
      {egress ? (
        <>
          {/* The button slides away as the settings slide open in its place. */}
          <Fold open={!egressOpen}>
            <div className="flex justify-center border-b border-wv-hairline px-4 py-[14px] sm:px-6">
              <SecondaryButton
                expanded={egressOpen}
                onClick={() => setEgressOpen(true)}
              >
                {perEgress
                  ? t("next.speedLimit.perEgress")
                  : t("next.speedLimit.adjustCaps")}
              </SecondaryButton>
            </div>
          </Fold>
          <Fold open={egressOpen}>
            {perEgress ? (
              <SectionHeader
                label={t("next.speedLimit.perEgress")}
                sticky={false}
              />
            ) : null}
            <FieldRows fields={egressFields} />
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
