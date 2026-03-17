import { useEffect, useRef, useState } from "react";
import { type AnyVariables, type DocumentInput, useClient } from "urql";
import { refreshSessionToken } from "@/graphql/client";

const INITIAL_POLL_DELAY_MS = 0;
const POLL_INTERVAL_MS = 2_000;

interface UseReconnectPollingOptions<Data, Variables extends AnyVariables> {
  enabled: boolean;
  query: DocumentInput<Data, Variables>;
  variables?: Variables;
  onData: (data: Data) => void;
}

interface UseReconnectPollingResult {
  isPolling: boolean;
}

export function useReconnectPolling<Data, Variables extends AnyVariables = AnyVariables>({
  enabled,
  query,
  variables,
  onData,
}: UseReconnectPollingOptions<Data, Variables>): UseReconnectPollingResult {
  const client = useClient();
  const [isPolling, setIsPolling] = useState(false);
  const timerRef = useRef<number | null>(null);
  const onDataRef = useRef(onData);

  useEffect(() => {
    onDataRef.current = onData;
  });

  useEffect(() => {
    if (!enabled) {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
        timerRef.current = null;
      }
      setIsPolling(false);
      return;
    }

    let cancelled = false;

    const scheduleNext = (delayMs: number) => {
      if (cancelled) {
        return;
      }

      timerRef.current = window.setTimeout(() => {
        void runPoll();
      }, delayMs);
    };

    const runPoll = async () => {
      if (cancelled) {
        return;
      }

      setIsPolling(true);

      // After a backend restart the session token is stale — refresh it
      // from the server's index HTML before attempting the query.
      await refreshSessionToken();

      try {
        const result = await client
          .query<Data, Variables>(query, (variables ?? {}) as Variables, {
            requestPolicy: "network-only",
          })
          .toPromise();

        if (cancelled) {
          return;
        }

        if (result.data) {
          onDataRef.current(result.data);
          scheduleNext(POLL_INTERVAL_MS);
          return;
        }
      } catch {
        // Keep the polling cadence steady during disconnects.
      }

      if (cancelled) {
        return;
      }

      scheduleNext(POLL_INTERVAL_MS);
    };

    scheduleNext(INITIAL_POLL_DELAY_MS);

    return () => {
      cancelled = true;
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
        timerRef.current = null;
      }
      setIsPolling(false);
    };
  }, [client, enabled, query, variables]);

  return { isPolling };
}
