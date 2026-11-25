// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { SecondaryButton } from "../components/controls";
import { Icon } from "../components/icons";
import { Menu, MenuItem } from "../components/Menu";
import {
  copySupportReport,
  downloadSupportReportJson,
  useJobSupportReport,
  type SupportReport,
} from "../data/support-report";

// Outside the component, so the component itself compiles: what each action reports.

async function copyReport(
  t: Translate,
  fetchReport: () => Promise<SupportReport>,
): Promise<string> {
  try {
    const report = await fetchReport();
    return (await copySupportReport(report))
      ? t("next.support.copied")
      : t("next.support.copyBlocked");
  } catch {
    return t("next.support.failed");
  }
}

async function downloadReport(
  t: Translate,
  fetchReport: () => Promise<SupportReport>,
  jobId: number,
): Promise<string> {
  try {
    downloadSupportReportJson(await fetchReport(), jobId);
    return t("next.support.saved");
  } catch {
    return t("next.support.failed");
  }
}

/**
 * One "Support report" button for one job, in any state, whose menu copies
 * the report or downloads it as JSON.
 *
 * The outcome is reported through `onReport` so each surface shows it where
 * it already shows the result of its other actions.
 */
export function SupportReportActions({
  jobId,
  onReport,
  size = "default",
  className,
}: {
  jobId: number;
  onReport: (message: string) => void;
  size?: "default" | "compact";
  className?: string;
}) {
  const t = useTranslate();
  const { busy, fetchReport } = useJobSupportReport(jobId);
  const [open, setOpen] = useState(false);

  const copy = async () => {
    onReport(await copyReport(t, fetchReport));
  };

  const download = async () => {
    onReport(await downloadReport(t, fetchReport, jobId));
  };

  return (
    <div className="relative flex">
      <SecondaryButton
        icon="logs"
        size={size}
        className={className}
        disabled={busy}
        menuOpen={open}
        onClick={() => setOpen((previous) => !previous)}
      >
        {t("next.support.report")}
        <Icon
          name="dropdown"
          size={12}
          className="-mr-[2px] ml-[6px] flex-none text-wv-muted"
        />
      </SecondaryButton>
      <Menu
        open={open}
        onDismiss={() => setOpen(false)}
        label={t("next.support.report")}
        className="top-[calc(100%+4px)] left-0 w-max min-w-full"
      >
        <MenuItem
          action
          onSelect={() => {
            setOpen(false);
            void copy();
          }}
        >
          {t("next.support.copy")}
        </MenuItem>
        <MenuItem
          action
          onSelect={() => {
            setOpen(false);
            void download();
          }}
        >
          {t("next.support.downloadJson")}
        </MenuItem>
      </Menu>
    </div>
  );
}
