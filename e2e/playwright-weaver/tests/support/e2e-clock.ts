import fs from "node:fs";
import { expect } from "../helpers";

/**
 * Weaver's deterministic clock. With WEAVER_E2E_MODE=1 Weaver reads its "now"
 * from this RFC 3339 file on every call and polls schedules every 250 ms, so
 * a test moves time by rewriting the file and then waits for the observable
 * effect. The harness creates the file before Weaver starts.
 */
export function clockFile(): string {
  const file = process.env.E2E_WEAVER_CLOCK_FILE;
  expect(file, "release harness must mount the deterministic Weaver clock").toBeTruthy();
  return file!;
}

export function readClock(): Date {
  return new Date(fs.readFileSync(clockFile(), "utf8").trim());
}

/** Set the clock to an RFC 3339 instant (offsets allowed: the instant is what counts). */
export function setClock(instant: string | Date): void {
  const value = typeof instant === "string" ? instant : instant.toISOString();
  expect(Number.isNaN(new Date(value).getTime()), `valid RFC 3339 instant ${value}`).toBe(false);
  const file = clockFile();
  const owner = fs.statSync(file);
  const pending = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(pending, `${value}\n`, { mode: 0o600 });
  fs.chownSync(pending, owner.uid, owner.gid);
  fs.chmodSync(pending, owner.mode & 0o777);
  fs.renameSync(pending, file);
}

export function advanceClock(milliseconds: number): Date {
  const next = new Date(readClock().getTime() + milliseconds);
  setClock(next);
  return next;
}
