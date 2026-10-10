import { useState } from "react";
import { useQuery } from "urql";
import { useTranslate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "./ConfirmDialog";

export function ArchivePassword({ id }: { id: number }) {
  const t = useTranslate();
  const [visible, setVisible] = useState(false);
  const [{ data, fetching, error }] = useQuery<{ validatedArchivePassword: string | null }>({
    query: `query ValidatedArchivePassword($id: Int!) { validatedArchivePassword(id: $id) }`,
    variables: { id },
    pause: !visible,
    requestPolicy: "network-only",
  });
  return (
    <span className="flex items-center gap-2 text-[12px]" onClick={(event) => event.stopPropagation()}>
      <span className="font-wv-mono">{visible ? (fetching ? "…" : error ? t("next.archivePasswords.loadFailed") : data?.validatedArchivePassword ?? "—") : "••••••••"}</span>
      <button type="button" className="text-wv-accent" onClick={() => setVisible(!visible)}>
        {t(visible ? "next.archivePasswords.hide" : "next.archivePasswords.show")}
      </button>
    </span>
  );
}

export function ArchivePasswordDialog({ title, busy, onConfirm, onDismiss }: {
  title: string;
  busy: boolean;
  onConfirm: (password: string | undefined) => void;
  onDismiss: () => void;
}) {
  const t = useTranslate();
  const [password, setPassword] = useState("");
  return <ConfirmDialog open title={title} busy={busy} destructive={false}
    confirmLabel={title} onDismiss={onDismiss} onConfirm={() => onConfirm(password || undefined)}
    body={<label className="block">{t("next.archivePasswords.password")} <span className="text-wv-dim">({t("next.common.optional")})</span>
      <input type="password" autoComplete="new-password" value={password} onChange={(event) => setPassword(event.target.value)}
        className="mt-2 w-full rounded border border-wv-control bg-wv-input p-2" />
    </label>} />;
}
