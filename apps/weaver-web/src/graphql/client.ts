import { Client, fetchExchange, subscriptionExchange } from "urql";
import { createClient as createWSClient } from "graphql-ws";

const graphqlUrl = "/graphql";

// Resolve WebSocket URL from the current page location.
function wsUrl(): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${graphqlUrl}/ws`;
}

const wsClient = createWSClient({
  url: wsUrl(),
  // Keep the socket alive briefly across React StrictMode unmount/remount
  // cycles so subscriptions don't get killed and re-created.
  lazyCloseTimeout: 3_000,
  retryAttempts: 5,
  retryWait: async (retries) =>
    new Promise((resolve) =>
      setTimeout(resolve, Math.min(1_000 * 2 ** retries, 30_000)),
    ),
});

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
