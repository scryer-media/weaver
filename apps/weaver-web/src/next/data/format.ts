/**
 * Value formatting for the Next UI.
 *
 * Big metrics in this design are always a number and a unit set in different
 * sizes and colours, so the formatters return the two halves rather than one
 * string. Everything else returns display text ready for a mono span.
 */

export interface SplitValue {
  value: string;
  unit: string;
}

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"] as const;

/** Binary-scaled size, split for the metric strip: `22.2` + `GB`. */
export function splitBytes(bytes: number): SplitValue {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return { value: "0", unit: "B" };
  }
  // A fractional byte count gives a negative logarithm, so the floor has to be
  // clamped at both ends: an unclamped -1 indexes off the front of the unit
  // table and prints `undefined`. Chart axes hit this whenever a series is all
  // zeroes and the ceiling lands on 1 B/s.
  const index = Math.min(
    Math.max(Math.floor(Math.log(bytes) / Math.log(1024)), 0),
    BYTE_UNITS.length - 1,
  );
  const scaled = bytes / 1024 ** index;
  return {
    value: scaled.toFixed(index === 0 ? 0 : scaled >= 100 ? 0 : 1),
    unit: BYTE_UNITS[index],
  };
}

export function formatSize(bytes: number): string {
  const { value, unit } = splitBytes(bytes);
  return `${value} ${unit}`;
}

export function splitSpeed(bytesPerSecond: number): SplitValue {
  const { value, unit } = splitBytes(bytesPerSecond);
  return { value, unit: `${unit}/s` };
}

export function formatRate(bytesPerSecond: number): string {
  const { value, unit } = splitSpeed(bytesPerSecond);
  return `${value} ${unit}`;
}

/** Counts in this design are grouped and never abbreviated: `31,408`. */
export function formatCount(value: number): string {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return Math.round(value).toLocaleString("en-US");
}

/** Chart axis labels compress instead: `40k`, `1.2M`. */
export function formatCompactCount(value: number): string {
  if (!Number.isFinite(value)) {
    return "0";
  }
  const magnitude = Math.abs(value);
  if (magnitude >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(magnitude >= 10_000_000 ? 0 : 1)}M`;
  }
  if (magnitude >= 1_000) {
    return `${(value / 1_000).toFixed(magnitude >= 10_000 ? 0 : 1)}k`;
  }
  return magnitude >= 10 ? value.toFixed(0) : value.toFixed(magnitude >= 1 ? 1 : 2);
}

export function formatPerSecond(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "0.00 /s";
  }
  return `${value >= 100 ? value.toFixed(0) : value.toFixed(2)} /s`;
}

export const EM_DASH = "—";

/** `14h 48m`, `11m`, `45s`. Zero or unknown reads as an em dash. */
export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return EM_DASH;
  }
  const total = Math.ceil(seconds);
  if (total < 60) {
    return `${total}s`;
  }
  const minutes = Math.floor(total / 60) % 60;
  const hours = Math.floor(total / 3600) % 24;
  const days = Math.floor(total / 86400);
  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

/** Uptime is the one duration shown as a metric, so it splits off its tail. */
export function splitUptime(seconds: number): SplitValue {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return { value: EM_DASH, unit: "" };
  }
  const total = Math.floor(seconds);
  const minutes = Math.floor(total / 60) % 60;
  const hours = Math.floor(total / 3600) % 24;
  const days = Math.floor(total / 86400);
  if (days > 0) {
    return { value: `${days}d ${hours}h`, unit: `${minutes}m` };
  }
  if (hours > 0) {
    return { value: `${hours}h ${minutes}m`, unit: `${total % 60}s` };
  }
  return { value: `${minutes}m`, unit: `${total % 60}s` };
}

/**
 * How long a finished job took, as history says it: `1h 08m`, `31m`, `36s`.
 *
 * Minutes are padded once hours are present so a column of them lines up, and
 * anything under a minute stays in seconds — a two-minute job reading `0h 02m`
 * would be noise.
 */
export function formatElapsed(milliseconds: number | null | undefined): string {
  if (milliseconds == null || !Number.isFinite(milliseconds) || milliseconds < 0) {
    return EM_DASH;
  }
  const total = Math.round(milliseconds / 1000);
  if (total < 60) {
    return `${total}s`;
  }
  const minutes = Math.floor(total / 60) % 60;
  const hours = Math.floor(total / 3600);
  return hours > 0 ? `${hours}h ${String(minutes).padStart(2, "0")}m` : `${minutes}m`;
}

/**
 * One span of a waterfall: `0.4s`, `34.4s`, `2m 51s`.
 *
 * Sub-minute spans keep a decimal because the whole point of the timeline is
 * that a 0.4s move and a 34s download sit on the same axis.
 */
export function formatSpan(milliseconds: number | null | undefined): string {
  if (milliseconds == null || !Number.isFinite(milliseconds) || milliseconds < 0) {
    return EM_DASH;
  }
  if (milliseconds < 60_000) {
    return `${(milliseconds / 1000).toFixed(1)}s`;
  }
  const total = Math.round(milliseconds / 1000);
  const seconds = total % 60;
  const minutes = Math.floor(total / 60) % 60;
  const hours = Math.floor(total / 3600);
  return hours > 0 ? `${hours}h ${minutes}m` : `${minutes}m ${String(seconds).padStart(2, "0")}s`;
}

/** Wall-clock including seconds — what a job's own events are stamped with. */
export function formatClockSeconds(epochMs: number | null | undefined): string {
  if (epochMs == null || !Number.isFinite(epochMs)) {
    return EM_DASH;
  }
  return new Date(epochMs).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

/**
 * The label above a day group: `TODAY`, `YESTERDAY`, then `TUE 9 SEP` and,
 * once the year turns over, `TUE 9 SEP 2025`.
 */
export function formatDayLabel(epochMs: number): string {
  const day = new Date(epochMs);
  const today = new Date();
  const startOfToday = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();
  const startOfDay = new Date(day.getFullYear(), day.getMonth(), day.getDate()).getTime();
  const daysBack = Math.round((startOfToday - startOfDay) / 86_400_000);
  if (daysBack === 0) {
    return "Today";
  }
  if (daysBack === 1) {
    return "Yesterday";
  }
  return day.toLocaleDateString([], {
    weekday: "short",
    day: "numeric",
    month: "short",
    ...(day.getFullYear() === today.getFullYear() ? {} : { year: "numeric" }),
  });
}

/** The midnight a timestamp belongs to — the key a day group is built on. */
export function startOfDay(epochMs: number): number {
  const day = new Date(epochMs);
  return new Date(day.getFullYear(), day.getMonth(), day.getDate()).getTime();
}

export function formatPercent(fraction: number): string {
  if (!Number.isFinite(fraction)) {
    return "0%";
  }
  return `${Math.round(Math.max(0, Math.min(1, fraction)) * 100)}%`;
}

export function formatClock(epochMs: number | null | undefined): string {
  if (epochMs == null || !Number.isFinite(epochMs)) {
    return EM_DASH;
  }
  return new Date(epochMs).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export function formatDate(epochMs: number | null | undefined): string {
  if (epochMs == null || !Number.isFinite(epochMs)) {
    return EM_DASH;
  }
  return new Date(epochMs).toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

export function formatLatency(milliseconds: number | null | undefined): string {
  if (milliseconds == null || !Number.isFinite(milliseconds) || milliseconds <= 0) {
    return EM_DASH;
  }
  return milliseconds >= 100
    ? `${Math.round(milliseconds)} ms`
    : `${milliseconds.toFixed(milliseconds < 10 ? 1 : 0)} ms`;
}
