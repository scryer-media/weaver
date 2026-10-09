import { useState } from "react";
import { useMutation } from "urql";
import {
  CREATE_SECRET_MUTATION,
  DELETE_SECRET_MUTATION,
  UPDATE_SECRET_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { NEW_SECRET, secretChange, type Secret, type SecretForm } from "../../../data/secrets";

/**
 * The editor of one named secret: its name, and a value that can be replaced
 * but never read back. Opened from the Secrets table, and from a script
 * instance's secret input to create one in place.
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

  const patch = (next: Partial<SecretForm>) => {
    setError(null);
    setForm((current) => ({ ...current, ...next }));
  };

  const save = async () => {
    const change = secretChange(form, editing);
    if (!change.ok) {
      setError(t(change.problem));
      return;
    }
    if (editing && change.name === null && change.value === null) {
      onSaved(editing);
      return;
    }
    setBusy(true);
    const result = editing
      ? await updateSecret({ id: editing.id, name: change.name, value: change.value })
      : await createSecret({ name: change.name, value: change.value });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    const saved = (editing ? result.data?.updateSecret : result.data?.createSecret) as Secret | undefined;
    if (saved) {
      onSaved(saved);
    }
  };

  const remove = async () => {
    if (!editing) {
      return;
    }
    setBusy(true);
    const result = await deleteSecret({ id: editing.id });
    setBusy(false);
    setConfirmDelete(false);
    if (result.error) {
      // Refused while an instance links it; the message names them.
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    onDeleted?.(editing);
  };

  return (
    <>
      <RecordEditor
        open
        title={editing ? editing.name : t("next.secrets.add")}
        note={editing ? t("next.secrets.editNote") : t("next.secrets.newNote")}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={onDismiss}
        onDelete={editing && onDeleted ? () => setConfirmDelete(true) : undefined}
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
        body={t("next.secrets.deleteBody", { name: editing?.name ?? "" })}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmDelete(false)}
      />
    </>
  );
}
