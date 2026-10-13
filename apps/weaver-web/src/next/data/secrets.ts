/**
 * Named secrets: a value kept encrypted under a name, for script inputs to
 * link. The daemon never sends a value back, so nothing here holds one beyond
 * what is being typed.
 */

/** A secret as an input that links it shows it. */
export interface SecretRef {
  id: string;
  name: string;
}

export interface Secret {
  id: string;
  name: string;
  createdAt: string;
  updatedAt: string;
  /** The instances that link it. */
  usedBy: SecretRef[];
}

/** What the secret editor holds. The value is write-only: it starts blank. */
export interface SecretForm {
  name: string;
  value: string;
}

export const NEW_SECRET: SecretForm = { name: "", value: "" };

/** The longest name the daemon accepts, in bytes. */
const MAX_NAME_BYTES = 128;

export type SecretChange =
  | { ok: true; name: string | null; value: string | null }
  | { ok: false; problem: string };

/**
 * What to send for the form, or why it cannot be sent, as a translation key.
 *
 * A new secret needs a name and a value. A saved one sends only what changed:
 * its name when it was renamed, and a value only when one was typed, since a
 * blank value field keeps the saved one.
 */
export function secretChange(form: SecretForm, editing: Secret | null): SecretChange {
  const name = form.name.trim();
  if (name === "" || new TextEncoder().encode(name).length > MAX_NAME_BYTES) {
    return { ok: false, problem: "next.secrets.nameInvalid" };
  }
  if (editing === null) {
    return form.value === ""
      ? { ok: false, problem: "next.secrets.valueRequired" }
      : { ok: true, name, value: form.value };
  }
  return {
    ok: true,
    name: name === editing.name ? null : name,
    value: form.value === "" ? null : form.value,
  };
}

/** The instances that link a secret, by name, for a table cell. */
export function usedByText(secret: Secret): string {
  return secret.usedBy.map((usage) => usage.name).join(", ");
}

/** Secrets in the order every list and picker draws them: by name, ignoring case. */
export function sortedSecrets(secrets: readonly Secret[]): Secret[] {
  return [...secrets].sort((left, right) =>
    left.name.localeCompare(right.name, undefined, { sensitivity: "base" }) || left.id.localeCompare(right.id),
  );
}
