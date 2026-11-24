// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import type { BandwidthSplit } from "@/lib/networking";
import { NumberField, Toggle } from "../../components/controls";
import { testIds } from "../../testids";
import { MIB } from "./presentation";

/**
 * A rate limit that is either off or a number of bytes per second. Off is
 * null, never 0: a limit of 0 lets nothing through. The switch says which,
 * and the rate shows only while the limit is on. A rate switched off comes
 * back as it was when it is switched on again.
 */
export function LimitControl({
  label,
  value,
  onChange,
  unit = MIB,
  suffix = "MiB/s",
  fieldId,
}: {
  label: string;
  value: number | null;
  onChange: (next: number | null) => void;
  /** The settings field this control draws: its switch is `field-<id>`, its rate `field-<id>-value`. */
  fieldId?: string;
  /** Bytes in one step of the rate as it is typed. */
  unit?: number;
  suffix?: string;
}) {
  const [last, setLast] = useState(value ?? 10 * unit);
  return (
    <div className="flex items-center gap-[10px]">
      {value !== null ? (
        <NumberField
          label={label}
          value={Math.round((value / unit) * 10) / 10}
          min={0}
          step={0.1}
          precision={1}
          suffix={suffix}
          testId={fieldId === undefined ? undefined : testIds.fieldValue(fieldId)}
          onChange={(next) => {
            const bytes = Math.round(Math.max(0, next) * unit);
            setLast(bytes);
            onChange(bytes);
          }}
        />
      ) : null}
      <Toggle
        checked={value !== null}
        label={label}
        onChange={(on) => onChange(on ? last : null)}
        testId={fieldId === undefined ? undefined : testIds.field(fieldId)}
      />
    </div>
  );
}

/**
 * How the global download limit is shared: usenet's percentage, torrents the
 * rest, or no split. Without a download limit the split stays editable; its
 * shares take effect once a download limit is set. The row it sits in says so,
 * under its label, so the controls keep to one line.
 */
export function SplitControl({
  value,
  onChange,
  needsDownloadLimit = false,
}: {
  value: BandwidthSplit | null;
  onChange: (next: BandwidthSplit | null) => void;
  /**
   * No global download limit is set, so there is nothing to share: a split
   * that is off cannot be turned on. One already saved may still be turned off.
   */
  needsDownloadLimit?: boolean;
}) {
  const t = useTranslate();
  const [last, setLast] = useState(value?.usenetPct ?? 50);
  const label = t("next.bandwidth.split");
  return (
    <div className="flex flex-wrap items-center justify-end gap-[10px]">
      {value !== null ? (
        <>
          {/* The share typed is usenet's; torrents take the rest, said after it. */}
          <span className="font-wv-mono text-[11.5px] text-wv-secondary">{t("next.bandwidth.splitUsenet")}</span>
          <NumberField
            label={t("next.bandwidth.splitUsenet")}
            value={value.usenetPct}
            min={1}
            max={99}
            narrow
            suffix="%"
            onChange={(next) => {
              setLast(next);
              onChange({ usenetPct: next });
            }}
          />
          <span className="font-wv-mono text-[11.5px] text-wv-secondary">
            {t("next.bandwidth.splitTorrents", { pct: 100 - value.usenetPct })}
          </span>
        </>
      ) : null}
      <Toggle
        checked={value !== null}
        label={label}
        disabled={needsDownloadLimit && value === null}
        onChange={(on) => onChange(on ? { usenetPct: last } : null)}
      />
    </div>
  );
}
