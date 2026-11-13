// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback } from "react";
import { useMutation } from "urql";
import { REMOVE_HISTORY_FILES_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import {
  requestFilesRemoval,
  type FilesRemovalAcceptResult,
  type FilesRemovalRequest,
} from "./history-files";

/**
 * The one way the screens ask for files to be removed: the History bulk bar,
 * a History row and the job page all send through this.
 */
export function useFilesRemoval(): (ids: readonly number[]) => Promise<FilesRemovalRequest> {
  const t = useTranslate();
  const [, removeHistoryFiles] = useMutation<{
    removeHistoryFiles: { results: FilesRemovalAcceptResult[] };
  }>(REMOVE_HISTORY_FILES_MUTATION);
  return useCallback(
    (ids) =>
      requestFilesRemoval(ids, async (chunk) => {
        const result = await removeHistoryFiles({ ids: chunk });
        const answer = result.data?.removeHistoryFiles;
        if (result.error || !answer) {
          return {
            error:
              result.error?.graphQLErrors[0]?.message ??
              result.error?.message ??
              t("next.filesRemoval.notAccepted"),
          };
        }
        return { results: answer.results };
      }),
    [removeHistoryFiles, t],
  );
}
