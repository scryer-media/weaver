import { useEffect, useState } from "react";
import { startOfDay } from "./format";

/**
 * Reading the clock during render is not a pure thing to do, and these screens
 * stay open for days — a tab left on Completed overnight would otherwise still
 * be calling last night "today". Both hooks below hold the time in state and
 * move it on a timer, so what the screen shows is a value React knows about.
 */

/** A clock that ticks while `enabled`, for anything measured against "now". */
export function useNow(intervalMs: number, enabled = true): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!enabled) {
      return;
    }
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(timer);
  }, [enabled, intervalMs]);
  return now;
}

/** The midnight the app currently calls today — the key a day group is built on. */
export function useStartOfToday(): number {
  const [midnight, setMidnight] = useState(() => startOfDay(Date.now()));
  useEffect(() => {
    const timer = window.setInterval(() => {
      setMidnight((current) => {
        const next = startOfDay(Date.now());
        return next === current ? current : next;
      });
    }, 60_000);
    return () => window.clearInterval(timer);
  }, []);
  return midnight;
}
