import { useEffect, useRef, useState } from "react";

export const STATUS_CHANGE_DEBOUNCE_MS = 3_000;

export function useDebouncedStatus(status: string, enabled = true): string {
  const [visibleStatus, setVisibleStatus] = useState(status);

  useEffect(() => {
    if (!enabled || status === visibleStatus) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      setVisibleStatus(status);
    }, STATUS_CHANGE_DEBOUNCE_MS);
    return () => window.clearTimeout(timeoutId);
  }, [enabled, status, visibleStatus]);

  return enabled ? visibleStatus : status;
}

function statusListKey(statuses: readonly string[]): string {
  return JSON.stringify(statuses);
}

export function useDebouncedStatuses(statuses: readonly string[]): string[] {
  const latestStatusesRef = useRef(statuses);
  latestStatusesRef.current = statuses;

  const nextKey = statusListKey(statuses);
  const [visibleStatuses, setVisibleStatuses] = useState(() => [...statuses]);
  const visibleKey = statusListKey(visibleStatuses);

  useEffect(() => {
    if (nextKey === visibleKey) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      setVisibleStatuses([...latestStatusesRef.current]);
    }, STATUS_CHANGE_DEBOUNCE_MS);
    return () => window.clearTimeout(timeoutId);
  }, [nextKey, visibleKey]);

  return visibleStatuses;
}
