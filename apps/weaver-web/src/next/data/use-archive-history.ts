// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useCallback } from "react";
import { useMutation } from "urql";
import { ARCHIVE_HISTORY_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import {
  requestArchiveHistory,
  type ArchiveHistoryAcceptResult,
  type ArchiveHistoryRequest,
} from "./history-archive";

/**
 * The one way the screens ask for entries to be archived: the History bulk
 * bar, a History row and the job page all send through this.
 */
export function useArchiveHistory(): (ids: readonly number[]) => Promise<ArchiveHistoryRequest> {
  const t = useTranslate();
  const [, archiveHistory] = useMutation<{
    archiveHistory: { results: ArchiveHistoryAcceptResult[] };
  }>(ARCHIVE_HISTORY_MUTATION);
  return useCallback(
    (ids) =>
      requestArchiveHistory(ids, async (chunk) => {
        const result = await archiveHistory({ ids: chunk });
        const answer = result.data?.archiveHistory;
        if (result.error || !answer) {
          return {
            error:
              result.error?.graphQLErrors[0]?.message ??
              result.error?.message ??
              t("next.historyArchive.notAccepted"),
          };
        }
        return { results: answer.results };
      }),
    [archiveHistory, t],
  );
}
