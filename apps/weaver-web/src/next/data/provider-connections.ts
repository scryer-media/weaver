import type { ProviderHealth } from "./next-data";

/** One server's open connections, from the live metrics stream. */
export interface ProviderConnections {
  label: string;
  active: number;
  /** Sockets connected to the server right now. */
  open: number;
  /** How many of those are carrying a request. */
  busy: number;
  max: number;
}

/**
 * Server health with the connection counts the metrics stream pushed since.
 *
 * Health is read on a slow beat and carries everything else about a server;
 * the stream carries only the counts, several times a second. A server the
 * stream does not name keeps the counts health gave it.
 */
export function withLiveConnections(
  providers: ProviderHealth[],
  live: ProviderConnections[] | undefined,
): ProviderHealth[] {
  if (!live?.length || providers.length === 0) {
    return providers;
  }
  const byLabel = new Map(live.map((entry) => [entry.label, entry]));
  let changed = false;
  const merged = providers.map((provider) => {
    const entry = byLabel.get(provider.label);
    if (
      !entry ||
      (entry.active === provider.connectionsActive &&
        entry.open === provider.connectionsOpen &&
        entry.busy === provider.connectionsBusy &&
        entry.max === provider.connectionsMax)
    ) {
      return provider;
    }
    changed = true;
    return {
      ...provider,
      connectionsActive: entry.active,
      connectionsOpen: entry.open,
      connectionsBusy: entry.busy,
      connectionsMax: entry.max,
    };
  });
  return changed ? merged : providers;
}
