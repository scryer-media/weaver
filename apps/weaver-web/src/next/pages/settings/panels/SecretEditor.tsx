import { useRef, useState } from "react";
import { useMutation, type CombinedError } from "urql";
import {
  CREATE_SECRET_MUTATION,
  DELETE_SECRET_MUTATION,
  UPDATE_SECRET_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { NEW_SECRET, secretChange, type Secret, type SecretForm } from "../../../data/secrets";
import {
  needsPasswordCheck,
  usePasswordCheck,
  type CheckedOutcome,
} from "../../../features/PasswordCheckDialog";

/**
 * The editor of one named secret: its name, and a value that can be replaced
 * but never read back. Opened from the Secrets table, and from a script
 * instance's secret input to create one in place.
 *
 * Adding a secret needs no recent password check. Changing or deleting one
 * does on an install that requires sign-in, so when the daemon refuses for
 * that reason the password is asked for and the same change runs again.
 */

export type SecretEditorTarget = { mode: "new" } | { mode: "edit"; secret: Secret };

export function SecretEditor({
  target,
  onSaved,
  onDeleted,
  onDismiss,
}: {
  target: SecretEditorTarget;
  /** The secret as the daemon saved it. */
  onSaved: (secret: Secret) => void;
  /** Offered only where a secret can be deleted from. */
  onDeleted?: (secret: Secret) => void;
  onDismiss: () => void;
}) {
  const t = useTranslate();
  const [, createSecret] = useMutation(CREATE_SECRET_MUTATION);
  const [, updateSecret] = useMutation(UPDATE_SECRET_MUTATION);
  const [, deleteSecret] = useMutation(DELETE_SECRET_MUTATION);
  const editing = target.mode === "edit" ? target.secret : null;
  const [form, setForm] = useState<SecretForm>(() => (editing ? { name: editing.name, value: "" } : NEW_SECRET));
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  // A refused delete is said inside the question, as an instance's is.
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const passwordCheck = usePasswordCheck();
  // Asked once per press of Save or Delete: a change refused again after the
  // password was checked says why instead of asking in a loop.
  const asked = useRef(false);
  const askForPassword = (refusal: CombinedError | undefined) => {
    if (asked.current || !needsPasswordCheck(refusal)) {
      return false;
    }
    asked.current = true;
    return true;
  };

  const patch = (next: Partial<SecretForm>) => {
    setError(null);
    setForm((current) => ({ ...current, ...next }));
  };

  const save = async (): Promise<CheckedOutcome> => {
    const change = secretChange(form, editing);
    if (!change.ok) {
      setError(t(change.problem));
      return "done";
    }
    if (editing && change.name === null && change.value === null) {
      onSaved(editing);
      return "done";
    }
    setBusy(true);
    setError(null);
    const result = editing
      ? await updateSecret({ id: editing.id, name: change.name, value: change.value })
      : await createSecret({ name: change.name, value: change.value });
    setBusy(false);
    if (askForPassword(result.error)) {
      return "password";
    }
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return "done";
    }
    const saved = (editing ? result.data?.updateSecret : result.data?.createSecret) as Secret | undefined;
    if (saved) {
      onSaved(saved);
    }
    return "done";
  };

  const remove = async (): Promise<CheckedOutcome> => {
    if (!editing) {
      return "done";
    }
    setBusy(true);
    setDeleteError(null);
    const result = await deleteSecret({ id: editing.id });
    setBusy(false);
    if (askForPassword(result.error)) {
      return "password";
    }
    if (result.error) {
      // Refused while an instance links it; the message names them.
      setDeleteError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return "done";
    }
    setConfirmDelete(false);
    onDeleted?.(editing);
    return "done";
  };

  const checked = (attempt: () => Promise<CheckedOutcome>) => {
    asked.current = false;
    void passwordCheck.run(attempt);
  };

  return (
    <>
      <RecordEditor
        open
        title={editing ? editing.name : t("next.secrets.add")}
        note={editing ? t("next.secrets.editNote") : t("next.secrets.newNote")}
        error={error}
        busy={busy}
        onSave={() => checked(save)}
        onDismiss={onDismiss}
        onDelete={
          editing && onDeleted
            ? () => {
                setDeleteError(null);
                setConfirmDelete(true);
              }
            : undefined
        }
        sections={[
          {
            id: "secret",
            title: t("next.secrets.secret"),
            fields: [
              {
                id: "name",
                label: t("next.secrets.name"),
                help: t("next.secrets.nameHelp"),
                control: {
                  kind: "text",
                  mono: false,
                  value: form.name,
                  onChange: (next) => patch({ name: next }),
                },
              },
              {
                id: "value",
                label: t("next.secrets.value"),
                help: t(editing ? "next.secrets.valueKeepHelp" : "next.secrets.valueHelp"),
                control: {
                  kind: "text",
                  type: "password",
                  secret: true,
                  value: form.value,
                  placeholder: editing ? t("next.secrets.valueSaved") : undefined,
                  onChange: (next) => patch({ value: next }),
                },
              },
            ],
          },
        ]}
      />
      <ConfirmDialog
        open={confirmDelete}
        title={t("next.secrets.delete")}
        note={editing?.name}
        busy={busy}
        confirmLabel={t("action.delete")}
        body={
          <>
            {t("next.secrets.deleteBody", { name: editing?.name ?? "" })}
            {deleteError ? (
              <span role="alert" className="mt-3 block text-[12.5px] text-wv-error-text">
                {deleteError}
              </span>
            ) : null}
          </>
        }
        onConfirm={() => checked(remove)}
        onDismiss={() => setConfirmDelete(false)}
      />
      {/* Last, so it opens over the editor and the question it interrupts. */}
      {passwordCheck.dialog}
    </>
  );
}
