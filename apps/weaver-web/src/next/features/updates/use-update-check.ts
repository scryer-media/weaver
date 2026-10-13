import { useEffect, useState } from "react";
import { useMutation, useQuery, useSubscription } from "urql";
import {
  CHECK_FOR_UPDATES_MUTATION,
  UPDATE_STATUS_QUERY,
  UPDATE_STATUS_SUBSCRIPTION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import type { UpdateStatus } from "./update-notification";

/**
 * The release checker's last answer as one line of text, and `check` to ask it
 * again now instead of at the next scheduled check.
 */
export function useUpdateCheck(): {
  busy: boolean;
  summary: string;
  failed: boolean;
  check: () => void;
} {
  const t = useTranslate();
  const [{ data }] = useQuery<{ updateStatus: UpdateStatus }>({ query: UPDATE_STATUS_QUERY });
  const [{ data: live }] = useSubscription<{ updateStatusUpdates: UpdateStatus }>({
    query: UPDATE_STATUS_SUBSCRIPTION,
  });
  const [checkState, checkForUpdates] = useMutation<{ checkForUpdates: UpdateStatus }>(
    CHECK_FOR_UPDATES_MUTATION,
  );
  const [checked, setChecked] = useState<UpdateStatus | null>(null);
  const [failure, setFailure] = useState<string | null>(null);

  // A manual check's answer only stands until the checker says something newer.
  const liveStatus = live?.updateStatusUpdates;
  useEffect(() => {
    if (liveStatus) {
      setChecked(null);
      setFailure(null);
    }
  }, [liveStatus]);

  const status = checked ?? liveStatus ?? data?.updateStatus ?? null;
  const busy = checkState.fetching || status?.checking === true;

  const check = () => {
    setFailure(null);
    void checkForUpdates({}).then((result) => {
      if (result.error || !result.data?.checkForUpdates) {
        setFailure(result.error?.message ?? t("next.general.checkFailed"));
        return;
      }
      setChecked(result.data.checkForUpdates);
    });
  };

  const error = failure ?? status?.lastError ?? null;
  let summary: string;
  if (busy) {
    summary = t("next.general.checking");
  } else if (error) {
    summary = error;
  } else if (status?.updateAvailable && status.latestVersion) {
    summary = t("next.general.updateFound", { version: status.latestVersion });
  } else if (status?.lastCheckedAtEpochMs != null) {
    summary = t("next.general.upToDate", {
      time: new Date(status.lastCheckedAtEpochMs).toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }),
    });
  } else {
    summary = t("next.general.notCheckedYet");
  }

  return { busy, summary, failed: !busy && error !== null, check };
}
