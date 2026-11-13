// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { RecordEditor } from "./RecordEditor";

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
