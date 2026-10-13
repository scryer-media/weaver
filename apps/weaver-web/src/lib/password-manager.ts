import { createContext, useContext } from "react";

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
 * True under a screen none of whose fields is the Weaver login, so every one
 * of them is kept from password managers without saying so itself.
 */
export const PasswordManagersKeptOut = createContext(false);

/**
 * Whether a field should carry {@link PASSWORD_MANAGER_IGNORE}: when it says so
 * itself, when its whole screen is kept from password managers, or when it is a
 * password field that is not the Weaver login. The login fields name themselves
 * with `current-password` or `new-password`, and those stay open to password
 * managers wherever they are.
 */
export function ignoredByPasswordManagers(options: {
  secret?: boolean;
  type?: string;
  autoComplete?: string;
  /** The field's whole screen is kept from password managers. */
  everyField?: boolean;
}): boolean {
  if (options.secret !== undefined) {
    return options.secret;
  }
  if (options.autoComplete === "current-password" || options.autoComplete === "new-password") {
    return false;
  }
  return options.everyField === true || options.type === "password";
}

/** The attributes that keep password managers off a field, or none when it is open to them. */
export function usePasswordManagerIgnore(options: {
  secret?: boolean;
  type?: string;
  autoComplete?: string;
}): typeof PASSWORD_MANAGER_IGNORE | null {
  const everyField = useContext(PasswordManagersKeptOut);
  return ignoredByPasswordManagers({ ...options, everyField }) ? PASSWORD_MANAGER_IGNORE : null;
}
