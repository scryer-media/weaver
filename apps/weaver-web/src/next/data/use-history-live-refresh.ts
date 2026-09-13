import { useCallback, useEffect, useRef } from "react";
import { useSubscription } from "urql";
import { useGraphqlConnectionState } from "@/graphql/client";
import { HISTORY_FACADE_EVENTS_SUBSCRIPTION } from "@/graphql/queries";

/** Several jobs finishing together are one refetch, not one each. */
const COALESCE_MS = 400;

interface QueueEvent {
  kind: string;
  itemId: number | null;
  state: string | null;
}

/**
 * Keeps a history screen current the way the classic History screen does.
 *
 * History is paged on the server and never held in the client, so there is no
 * row to patch: a job reaching an outcome, or leaving, is a reason to read the
 * page again. So is a reconnect, which may have missed any number of those, and
 * a subscription error, after which no more will arrive. Removals are ignored
 * while a delete is running, because that delete is their cause and it
 * refreshes the page itself once it drains.
 */
export function useHistoryLiveRefresh({
  refresh,
  deletesActive,
}: {
  refresh: () => void;
  deletesActive: boolean;
}) {
  const connection = useGraphqlConnectionState();
  const timer = useRef<number | null>(null);
  const refreshRef = useRef(refresh);
  refreshRef.current = refresh;

  const schedule = useCallback(() => {
    if (timer.current !== null) {
      return;
    }
    timer.current = window.setTimeout(() => {
      timer.current = null;
      refreshRef.current();
    }, COALESCE_MS);
  }, []);

  useEffect(
    () => () => {
      if (timer.current !== null) {
        window.clearTimeout(timer.current);
      }
    },
    [],
  );

  const deletesActiveRef = useRef(deletesActive);
  deletesActiveRef.current = deletesActive;

  const handle = useCallback(
    (previous: unknown, response: { queueEvents: QueueEvent }) => {
      const event = response.queueEvents;
      if (event.itemId == null) {
        return previous;
      }
      if (event.kind === "ITEM_REMOVED" && deletesActiveRef.current) {
        return previous;
      }
      if (
        event.kind === "ITEM_COMPLETED"
        || (event.kind === "ITEM_STATE_CHANGED" && event.state === "FAILED")
        || event.kind === "ITEM_REMOVED"
      ) {
        schedule();
      }
      return previous;
    },
    [schedule],
  );

  const [{ error }] = useSubscription({ query: HISTORY_FACADE_EVENTS_SUBSCRIPTION }, handle);

  const lastConnectedAt = useRef<number | null | undefined>(undefined);
  useEffect(() => {
    if (connection.status !== "connected" || connection.lastConnectedAt === null) {
      return;
    }
    if (lastConnectedAt.current === undefined) {
      lastConnectedAt.current = connection.lastConnectedAt;
      return;
    }
    if (lastConnectedAt.current === connection.lastConnectedAt) {
      return;
    }
    lastConnectedAt.current = connection.lastConnectedAt;
    schedule();
  }, [connection.lastConnectedAt, connection.status, schedule]);

  const lastError = useRef<string | null>(null);
  useEffect(() => {
    if (!error) {
      lastError.current = null;
      return;
    }
    if (lastError.current === error.message) {
      return;
    }
    lastError.current = error.message;
    schedule();
  }, [error, schedule]);
}
