export type DuplicateSubmissionStatus =
  | "ACCEPTED"
  | "WARNED"
  | "PAUSED"
  | "PARKED"
  | "BLOCKED"
  | "IDEMPOTENT_REPLAY"
  | "IDEMPOTENCY_CONFLICT"
  | "FORCE_ACCEPTED";

const OUTCOME_KEYS: Record<DuplicateSubmissionStatus, string> = {
  ACCEPTED: "upload.outcomeAccepted",
  WARNED: "upload.outcomeWarned",
  PAUSED: "upload.outcomePaused",
  PARKED: "upload.outcomeParked",
  BLOCKED: "upload.outcomeBlocked",
  IDEMPOTENT_REPLAY: "upload.outcomeIdempotentReplay",
  IDEMPOTENCY_CONFLICT: "upload.outcomeIdempotencyConflict",
  FORCE_ACCEPTED: "upload.outcomeForceAccepted",
};

export function submissionOutcomeI18nKey(status: string | null | undefined): string {
  return OUTCOME_KEYS[status as DuplicateSubmissionStatus] ?? "upload.outcomeAccepted";
}

export function submissionStatusIsDurable(status: string | null | undefined): boolean {
  return matchesSubmissionStatus(status, [
    "ACCEPTED",
    "WARNED",
    "PAUSED",
    "PARKED",
    "IDEMPOTENT_REPLAY",
    "FORCE_ACCEPTED",
  ]);
}

export function semanticStateI18nKey(state: string | null | undefined): string {
  return `duplicate.semanticState.${normalizeEnumValue(state)}`;
}

export function duplicateLifecycleI18nKey(lifecycle: string | null | undefined): string {
  return `duplicate.lifecycle.${normalizeEnumValue(lifecycle)}`;
}

export function duplicateTerminalCauseI18nKey(cause: string | null | undefined): string {
  return `duplicate.terminalCause.${normalizeEnumValue(cause)}`;
}

export function duplicatePromotionStateI18nKey(state: string | null | undefined): string {
  return `duplicate.promotionState.${normalizeEnumValue(state)}`;
}

export function duplicateActionI18nKey(action: string | null | undefined): string {
  return `duplicate.action.${normalizeEnumValue(action)}`;
}

export function duplicateFingerprintKindI18nKey(kind: string | null | undefined): string {
  return `duplicate.match.${normalizeEnumValue(kind)}`;
}

function matchesSubmissionStatus(
  status: string | null | undefined,
  candidates: DuplicateSubmissionStatus[],
): boolean {
  return candidates.includes(status as DuplicateSubmissionStatus);
}

function normalizeEnumValue(value: string | null | undefined): string {
  return (value ?? "UNKNOWN").toLowerCase().replaceAll("_", "-");
}
