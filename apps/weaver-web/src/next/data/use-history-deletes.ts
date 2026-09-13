import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  ACCEPT_HISTORY_DELETE_MUTATION,
  HISTORY_DELETE_OPERATIONS_QUERY,
} from "@/graphql/queries";
import {
  deleteProgress,
  settleDeleteLocks,
  withDeleteLocks,
  type DeleteLock,
  type HistoryDeleteOperation,
  type RowDeleteOperation,
} from "./history-deletes";

/** How often active delete operations are asked for their progress. */
const POLL_MS = 1000;

const NO_LOCKS: ReadonlyMap<number, DeleteLock> = new Map();

/**
 * History deletes the way the classic History screen runs them.
 *
 * Accepting a delete locks the rows it covers straight away; while any
 * operation is active its progress is polled every second; and when the last
 * one drains, `onDrained` refetches whatever the screen shows.
 */
export function useHistoryDeletes<T extends { id: number; deleteOperation: RowDeleteOperation | null }>({
  rows,
  onDrained,
}: {
  rows: readonly T[];
  onDrained: () => void;
}) {
  const [locks, setLocks] = useState(NO_LOCKS);
  // The target count of an acceptance the operation list has not caught up with.
  const [pendingTargets, setPendingTargets] = useState(0);
  const [awaitingRefresh, setAwaitingRefresh] = useState<"no" | "requested" | "started">("no");

  const [{ fetching: accepting }, acceptHistoryDelete] = useMutation<{
    acceptHistoryDelete: { operationId: number; totalTargets: number };
  }>(ACCEPT_HISTORY_DELETE_MUTATION);
  const [{ data, fetching }, reexecute] = useQuery<{
    historyDeleteOperations: HistoryDeleteOperation[];
  }>({ query: HISTORY_DELETE_OPERATIONS_QUERY, variables: { activeOnly: true } });

  const operations = useMemo(() => data?.historyDeleteOperations ?? [], [data?.historyDeleteOperations]);
  const active = operations.length > 0 || pendingTargets > 0;
  const progress = useMemo(
    () => deleteProgress(operations, pendingTargets || locks.size),
    [locks.size, operations, pendingTargets],
  );

  const pollOperations = useCallback(() => {
    void reexecute({ requestPolicy: "network-only" });
  }, [reexecute]);

  const fetchingRef = useRef(fetching);
  useEffect(() => {
    fetchingRef.current = fetching;
  }, [fetching]);

  // Rows the server now reports on, or that left the page, drop their stand-in.
  const settled = settleDeleteLocks(locks, rows);
  if (settled !== locks) {
    setLocks(settled);
  }

  // The acceptance's own count stands in until a refresh that started after it finishes.
  useEffect(() => {
    if (awaitingRefresh === "requested" && fetching) {
      setAwaitingRefresh("started");
    } else if (awaitingRefresh === "started" && !fetching) {
      setAwaitingRefresh("no");
      setPendingTargets(0);
    }
  }, [awaitingRefresh, fetching]);

  useEffect(() => {
    if (!active) {
      return;
    }
    const id = window.setInterval(() => {
      if (!fetchingRef.current) {
        pollOperations();
      }
    }, POLL_MS);
    return () => window.clearInterval(id);
  }, [active, pollOperations]);

  const wasActive = useRef(active);
  useEffect(() => {
    if (wasActive.current && !active) {
      pollOperations();
      onDrained();
    }
    wasActive.current = active;
  }, [active, onDrained, pollOperations]);

  /** Hand these rows to a delete operation; resolves to an error message, or null once accepted. */
  const accept = useCallback(
    async (ids: readonly number[], deleteFiles: boolean): Promise<string | null> => {
      const result = await acceptHistoryDelete({ input: { mode: "IDS", ids: [...ids], deleteFiles } });
      const acceptance = result.data?.acceptHistoryDelete;
      if (result.error || !acceptance) {
        return result.error?.message ?? "The delete was not accepted.";
      }
      setPendingTargets(acceptance.totalTargets);
      setAwaitingRefresh("requested");
      setLocks((current) => {
        const next = new Map(current);
        for (const id of ids) {
          next.set(id, { operationId: acceptance.operationId, deleteFiles });
        }
        return next;
      });
      pollOperations();
      return null;
    },
    [acceptHistoryDelete, pollOperations],
  );

  const lockedRows = useMemo(() => withDeleteLocks(rows, settled), [rows, settled]);

  return {
    rows: lockedRows,
    active,
    progress,
    accepting,
    accept,
    /** Ask the operation list again, for a screen that learned something changed. */
    refresh: pollOperations,
  };
}
