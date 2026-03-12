import { useSyncExternalStore } from "react";
import { Client, fetchExchange, subscriptionExchange } from "urql";
import { createClient as createWSClient } from "graphql-ws";

const graphqlUrl = "/graphql";
const WS_KEEP_ALIVE_MS = 10_000;
const WS_PONG_TIMEOUT_MS = 5_000;

export type GraphqlConnectionStatus = "connecting" | "connected" | "disconnected";

interface GraphqlConnectionSnapshot {
  status: GraphqlConnectionStatus;
  reconnectAttempt: number;
  disconnectedAt: number | null;
  lastConnectedAt: number | null;
  closeCode: number | null;
  closeReason: string | null;
}

const connectionListeners = new Set<() => void>();

let connectionSnapshot: GraphqlConnectionSnapshot = {
  status: "connecting",
  reconnectAttempt: 0,
  disconnectedAt: null,
  lastConnectedAt: null,
  closeCode: null,
  closeReason: null,
};

let pongTimeoutId: number | null = null;

function getCloseEventMetadata(event: unknown) {
  if (typeof event !== "object" || event === null) {
    return {
      code: null,
      reason: null,
    };
  }

  const closeEvent = event as { code?: unknown; reason?: unknown };
  return {
    code: typeof closeEvent.code === "number" ? closeEvent.code : null,
    reason: typeof closeEvent.reason === "string" ? closeEvent.reason : null,
  };
}

function emitConnectionSnapshot() {
  connectionListeners.forEach((listener) => listener());
}

function updateConnectionSnapshot(
  update: Partial<GraphqlConnectionSnapshot> | ((current: GraphqlConnectionSnapshot) => GraphqlConnectionSnapshot),
) {
  const nextSnapshot =
    typeof update === "function" ? update(connectionSnapshot) : { ...connectionSnapshot, ...update };

  const hasChanged =
    nextSnapshot.status !== connectionSnapshot.status ||
    nextSnapshot.reconnectAttempt !== connectionSnapshot.reconnectAttempt ||
    nextSnapshot.disconnectedAt !== connectionSnapshot.disconnectedAt ||
    nextSnapshot.lastConnectedAt !== connectionSnapshot.lastConnectedAt ||
    nextSnapshot.closeCode !== connectionSnapshot.closeCode ||
    nextSnapshot.closeReason !== connectionSnapshot.closeReason;

  if (!hasChanged) {
    return;
  }

  connectionSnapshot = nextSnapshot;
  emitConnectionSnapshot();
}

function schedulePongTimeout() {
  if (pongTimeoutId !== null) {
    window.clearTimeout(pongTimeoutId);
  }

  pongTimeoutId = window.setTimeout(() => {
    pongTimeoutId = null;
    updateConnectionSnapshot((current) => ({
      ...current,
      status: "disconnected",
      disconnectedAt: current.disconnectedAt ?? Date.now(),
      closeCode: 4499,
      closeReason: "WebSocket heartbeat timed out",
    }));
    wsClient.terminate();
  }, WS_PONG_TIMEOUT_MS);
}

function clearPongTimeout() {
  if (pongTimeoutId === null) {
    return;
  }

  window.clearTimeout(pongTimeoutId);
  pongTimeoutId = null;
}

// Resolve WebSocket URL from the current page location.
function wsUrl(): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${graphqlUrl}/ws`;
}

const wsClient = createWSClient({
  url: wsUrl(),
  keepAlive: WS_KEEP_ALIVE_MS,
  // Keep the socket alive briefly across React StrictMode unmount/remount
  // cycles so subscriptions don't get killed and re-created.
  lazyCloseTimeout: 3_000,
  retryAttempts: Number.POSITIVE_INFINITY,
  retryWait: async (retries) =>
    new Promise((resolve) =>
      setTimeout(resolve, retries === 0 ? 0 : Math.min(1_000 * 2 ** (retries - 1), 30_000)),
    ),
  on: {
    connecting: (isRetry) => {
      updateConnectionSnapshot((current) => ({
        ...current,
        status: current.lastConnectedAt === null && !isRetry ? "connecting" : "disconnected",
        reconnectAttempt:
          current.lastConnectedAt === null && !isRetry ? 0 : current.reconnectAttempt + 1,
        disconnectedAt:
          current.lastConnectedAt === null && !isRetry
            ? null
            : current.disconnectedAt ?? Date.now(),
      }));
    },
    connected: () => {
      clearPongTimeout();
      updateConnectionSnapshot({
        status: "connected",
        reconnectAttempt: 0,
        disconnectedAt: null,
        lastConnectedAt: Date.now(),
        closeCode: null,
        closeReason: null,
      });
    },
    ping: (received) => {
      if (!received) {
        schedulePongTimeout();
      }
    },
    pong: (received) => {
      if (received) {
        clearPongTimeout();
      }
    },
    closed: (event) => {
      clearPongTimeout();
      const { code, reason } = getCloseEventMetadata(event);
      updateConnectionSnapshot((current) => ({
        ...current,
        status: "disconnected",
        disconnectedAt: current.disconnectedAt ?? Date.now(),
        closeCode: code,
        closeReason: reason,
      }));
    },
  },
});

function subscribeToConnectionSnapshot(listener: () => void) {
  connectionListeners.add(listener);
  return () => {
    connectionListeners.delete(listener);
  };
}

export function getGraphqlConnectionSnapshot(): GraphqlConnectionSnapshot {
  return connectionSnapshot;
}

export function useGraphqlConnectionState(): GraphqlConnectionSnapshot {
  return useSyncExternalStore(
    subscribeToConnectionSnapshot,
    getGraphqlConnectionSnapshot,
    getGraphqlConnectionSnapshot,
  );
}

export const client = new Client({
  url: graphqlUrl,
  preferGetMethod: false,
  requestPolicy: "network-only",
  exchanges: [
    subscriptionExchange({
      forwardSubscription(request) {
        const input = { ...request, query: request.query || "" };
        return {
          subscribe(sink) {
            const unsubscribe = wsClient.subscribe(input, sink);
            return { unsubscribe };
          },
        };
      },
    }),
    fetchExchange,
  ],
});
