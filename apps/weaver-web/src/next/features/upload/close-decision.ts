/**
 * Whether the upload dialog should dismiss itself after a submit round.
 *
 * Submitted entries are RETAINED in the list after a submit (so the
 * queue-duplicate "accepted with warning" outcome can render on the row), so
 * the dialog must close on the terminal *state* of the entries rather than on
 * an empty list — the old `entries.length === 0` check silently stopped firing
 * once retention landed, leaving the dialog stuck open on every submit.
 *
 * Close only when there is at least one entry and every entry reached the
 * successful terminal state (`"submitted"`, which includes a duplicate that was
 * accepted-with-warning). If anything is still `"failed"` or was retained as
 * `"staged"` (e.g. a policy-blocked duplicate awaiting force-accept), keep the
 * dialog open so the user can act on it.
 */
export function shouldCloseAfterSubmit(statuses: readonly string[]): boolean {
  return statuses.length > 0 && statuses.every((status) => status === "submitted");
}
