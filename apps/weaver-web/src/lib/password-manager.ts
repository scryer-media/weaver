/**
 * The markers password managers read to leave a field alone, one per vendor.
 *
 * For secrets that belong to something other than the Weaver login: provider,
 * proxy and feed credentials, API keys, archive and backup passwords. Left to
 * themselves, managers offer to fill these with the Weaver login and to save
 * whatever is typed as a new Weaver password.
 */
export const PASSWORD_MANAGER_MARKERS = {
  "data-1p-ignore": "true",
  "data-lpignore": "true",
  "data-bwignore": "true",
  "data-protonpass-ignore": "true",
  "data-form-type": "other",
} as const;

/** The markers plus `autoComplete="off"`, for a field that sets no autocomplete of its own. */
export const PASSWORD_MANAGER_IGNORE = { autoComplete: "off", ...PASSWORD_MANAGER_MARKERS } as const;

/**
 * Whether a field should carry {@link PASSWORD_MANAGER_IGNORE}: when it says so
 * itself, or when it is a password field that is not the Weaver login. The
 * login fields name themselves with `current-password` or `new-password`, and
 * those stay open to password managers.
 */
export function ignoredByPasswordManagers(options: {
  secret?: boolean;
  type?: string;
  autoComplete?: string;
}): boolean {
  if (options.secret !== undefined) {
    return options.secret;
  }
  return (
    options.type === "password" &&
    options.autoComplete !== "current-password" &&
    options.autoComplete !== "new-password"
  );
}
