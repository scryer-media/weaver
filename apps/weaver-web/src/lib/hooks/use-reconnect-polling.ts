import { useEffect, useRef, useState } from "react";
import { type AnyVariables, type DocumentInput, useClient } from "urql";

const RETRY_DELAYS_MS = [0, 1_000, 2_000, 5_000, 10_000, 30_000] as const;
const SUCCESS_POLL_INTERVAL_MS = 5_000;

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
  const failureCountRef = useRef(0);
  const onDataRef = useRef(onData);

  onDataRef.current = onData;

  useEffect(() => {
    if (!enabled) {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
        timerRef.current = null;
      }
      failureCountRef.current = 0;
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
          failureCountRef.current = 0;
          onDataRef.current(result.data);
          scheduleNext(SUCCESS_POLL_INTERVAL_MS);
          return;
        }
      } catch {
        // Treat transport failures the same as GraphQL errors for retry scheduling.
      }

      if (cancelled) {
        return;
      }

      const delayMs =
        RETRY_DELAYS_MS[Math.min(failureCountRef.current, RETRY_DELAYS_MS.length - 1)];
      failureCountRef.current += 1;
      scheduleNext(delayMs);
    };

    scheduleNext(RETRY_DELAYS_MS[0]);

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
