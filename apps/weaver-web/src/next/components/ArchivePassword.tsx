import { useState } from "react";
import { useQuery } from "urql";
import { VALIDATED_ARCHIVE_PASSWORD_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { RecordEditor } from "./RecordEditor";

/**
 * The password that opened a job's archive, masked until asked for. The
 * daemon only hands it over on request, so nothing is fetched while it is
 * hidden.
 */
export function ArchivePassword({ id }: { id: number }) {
  const t = useTranslate();
  const [visible, setVisible] = useState(false);
  const [{ data, fetching, error }] = useQuery<{ validatedArchivePassword: string | null }>({
    query: VALIDATED_ARCHIVE_PASSWORD_QUERY,
    variables: { id },
    pause: !visible,
    requestPolicy: "network-only",
  });
  const shown = fetching
    ? t("next.common.loading")
    : error
      ? t("next.archivePasswords.loadFailed")
      : (data?.validatedArchivePassword ?? "—");
  return (
    <span className="inline-flex min-w-0 items-baseline gap-3">
      <span className={cn("min-w-0 break-all", visible && error && "text-wv-error-text")}>
        {visible ? shown : "••••••••"}
      </span>
      <button
        type="button"
        onClick={() => setVisible(!visible)}
        className="flex-none cursor-pointer font-wv-mono text-[10.5px] tracking-[0.06em] text-wv-muted uppercase hover:text-wv-fg"
      >
        {t(visible ? "next.archivePasswords.hide" : "next.archivePasswords.show")}
      </button>
    </span>
  );
}

/**
 * Redownload or reprocess with a password to try first. Left blank, the job
 * falls back to the passwords its NZB and the settings name.
 */
export function ArchivePasswordDialog({
  title,
  busy,
  onConfirm,
  onDismiss,
}: {
  title: string;
  busy: boolean;
  onConfirm: (password: string | undefined) => void;
  onDismiss: () => void;
}) {
  const t = useTranslate();
  const [password, setPassword] = useState("");
  return (
    <RecordEditor
      open
      title={title}
      busy={busy}
      width={440}
      saveLabel={title}
      onSave={() => onConfirm(password || undefined)}
      onDismiss={onDismiss}
      sections={[
        {
          id: "password",
          title: t("next.archivePasswords.title"),
          fields: [
            {
              id: "password",
              label: t("next.archivePasswords.password"),
              control: {
                kind: "text",
                type: "password",
                secret: true,
                value: password,
                onChange: setPassword,
              },
            },
          ],
        },
      ]}
    />
  );
}
