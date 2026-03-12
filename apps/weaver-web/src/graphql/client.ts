import { useSyncExternalStore } from "react";
import { Client, fetchExchange, subscriptionExchange } from "urql";
import {
  type Client as GraphqlWsClient,
  createClient as createWSClient,
} from "graphql-ws";

const graphqlUrl = "/graphql";
const WS_KEEP_ALIVE_MS = 10_000;
const WS_PONG_TIMEOUT_MS = 5_000;
const CLIENT_RESTART_THROTTLE_MS = 2_000;

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
const graphqlClientListeners = new Set<() => void>();

interface GraphqlClientResources {
  transportId: number;
  client: Client;
  wsClient: GraphqlWsClient;
}

let connectionSnapshot: GraphqlConnectionSnapshot = {
  status: "connecting",
  reconnectAttempt: 0,
  disconnectedAt: null,
  lastConnectedAt: null,
  closeCode: null,
  closeReason: null,
};

let pongTimeoutId: number | null = null;
let nextTransportId = 0;
let lastClientRestartAt = 0;
let currentResources = createGraphqlClientResources();

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

function emitGraphqlClientSnapshot() {
  graphqlClientListeners.forEach((listener) => listener());
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

function isCurrentTransport(transportId: number) {
  return currentResources.transportId === transportId;
}

function schedulePongTimeout(transportId: number, wsClient: GraphqlWsClient) {
  if (!isCurrentTransport(transportId)) {
    return;
  }

  clearPongTimeout(transportId);
  pongTimeoutId = window.setTimeout(() => {
    if (!isCurrentTransport(transportId)) {
      return;
    }

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

function clearPongTimeout(transportId?: number) {
  if (transportId != null && !isCurrentTransport(transportId)) {
    return;
  }

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

function createGraphqlClientResources(): GraphqlClientResources {
  const transportId = ++nextTransportId;
  const wsClient = createTrackedWsClient(transportId);

  return {
    transportId,
    wsClient,
    client: new Client({
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
    }),
  };
}

function createTrackedWsClient(transportId: number): GraphqlWsClient {
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
        if (!isCurrentTransport(transportId)) {
          return;
        }

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
        if (!isCurrentTransport(transportId)) {
          return;
        }

        clearPongTimeout(transportId);
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
          schedulePongTimeout(transportId, wsClient);
        }
      },
      pong: (received) => {
        if (received) {
          clearPongTimeout(transportId);
        }
      },
      closed: (event) => {
        if (!isCurrentTransport(transportId)) {
          return;
        }

        clearPongTimeout(transportId);
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

  return wsClient;
}

function subscribeToConnectionSnapshot(listener: () => void) {
  connectionListeners.add(listener);
  return () => {
    connectionListeners.delete(listener);
  };
}

export function getGraphqlConnectionSnapshot(): GraphqlConnectionSnapshot {
  return connectionSnapshot;
}

function subscribeToGraphqlClient(listener: () => void) {
  graphqlClientListeners.add(listener);
  return () => {
    graphqlClientListeners.delete(listener);
  };
}

export function getGraphqlClient(): Client {
  return currentResources.client;
}

export function useGraphqlConnectionState(): GraphqlConnectionSnapshot {
  return useSyncExternalStore(
    subscribeToConnectionSnapshot,
    getGraphqlConnectionSnapshot,
    getGraphqlConnectionSnapshot,
  );
}

export function useGraphqlClient(): Client {
  return useSyncExternalStore(subscribeToGraphqlClient, getGraphqlClient, getGraphqlClient);
}

export function requestGraphqlClientRestart() {
  const now = Date.now();
  if (now - lastClientRestartAt < CLIENT_RESTART_THROTTLE_MS) {
    return;
  }

  lastClientRestartAt = now;
  const previousResources = currentResources;
  clearPongTimeout();
  currentResources = createGraphqlClientResources();
  emitGraphqlClientSnapshot();
  void previousResources.wsClient.dispose();
}
