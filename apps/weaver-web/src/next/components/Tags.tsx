// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo, useState } from "react";
import { useMutation } from "urql";
import { ADD_JOB_TAGS_MUTATION, REMOVE_JOB_TAGS_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { splitTags } from "@/next/data/tags";
import { jobIds } from "@/next/testids";
import { SecondaryButton, TextField } from "./controls";
import { Icon } from "./icons";

const CHIP =
  "inline-flex h-[18px] max-w-[160px] flex-none items-center gap-1 border border-wv-control px-[6px] font-wv-mono text-[10.5px] leading-none text-wv-tertiary";

/** A job's tags as chips; nothing when it has none. */
export const TagChips = memo(function TagChips({
  tags,
  className,
}: {
  tags: readonly string[] | undefined;
  className?: string;
}) {
  if (!tags || tags.length === 0) return null;
  return (
    <span data-testid={jobIds.jobTags} className={cn("flex min-w-0 flex-wrap items-center gap-1", className)}>
      {tags.map((tag) => (
        <span key={tag} title={tag} className={CHIP}>
          <span className="truncate">{tag}</span>
        </span>
      ))}
    </span>
  );
});

/** A job's tags with a way to add and take them off. */
export function TagEditor({ jobId, tags }: { jobId: number; tags: readonly string[] }) {
  const t = useTranslate();
  const [text, setText] = useState("");
  const [{ fetching: adding }, addTags] = useMutation(ADD_JOB_TAGS_MUTATION);
  const [{ fetching: removing }, removeTags] = useMutation(REMOVE_JOB_TAGS_MUTATION);
  const busy = adding || removing;
  const wanted = splitTags(text);
  const add = async () => {
    if (wanted.length === 0) return;
    const result = await addTags({ ids: [jobId], tags: wanted });
    if (!result.error) setText("");
  };
  return (
    <div data-testid={jobIds.tagEditor} className="flex flex-col gap-2">
      {tags.length === 0 ? null : (
        <span className="flex flex-wrap items-center gap-1">
          {tags.map((tag) => {
            const label = t("next.tags.remove", { tag });
            return (
              <span key={tag} title={tag} className={CHIP}>
                <span className="truncate">{tag}</span>
                <button
                  type="button"
                  aria-label={label}
                  title={label}
                  disabled={busy}
                  onClick={() => void removeTags({ ids: [jobId], tags: [tag] })}
                  className="flex-none text-wv-muted hover:text-wv-error disabled:opacity-50"
                >
                  <Icon name="close" size={10} />
                </button>
              </span>
            );
          })}
        </span>
      )}
      <span className="flex items-center gap-2">
        <TextField
          label={t("next.tags.title")}
          placeholder={t("next.tags.placeholder")}
          value={text}
          onChange={setText}
          onKeyDown={(event) => {
            if (event.key === "Enter") void add();
          }}
          testId={jobIds.tagInput}
          className="min-w-0 flex-1"
        />
        <SecondaryButton icon="add" size="compact" disabled={busy || wanted.length === 0} onClick={() => void add()}>
          {t("next.tags.add")}
        </SecondaryButton>
      </span>
    </div>
  );
}
