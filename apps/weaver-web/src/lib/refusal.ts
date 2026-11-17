// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";

/** As much of a failed GraphQL request as the sentence shown for it needs. */
export type FailedRequest = {
  message: string;
  graphQLErrors: readonly { message: string; extensions?: Record<string, unknown> }[];
};

/** Where the sentence for a refusal's code lives in the dictionary. */
export const refusalKey = (code: string) => `next.refusal.${code}`;

/**
 * What to show for a change the daemon refused. The daemon names each refusal
 * with a code, and the sentence is ours, looked up by that code and never by
 * the daemon's wording. The daemon's own message stands in for a code this
 * build has no sentence for, and for a failure that carries none.
 */
export function refusalMessage(t: Translate, error: FailedRequest, names?: RefusalNames): string {
  const refusal = error.graphQLErrors[0];
  if (!refusal) {
    return error.message;
  }
  const code = refusal.extensions?.code;
  if (typeof code === "string") {
    const params = refusal.extensions?.params;
    return codedSentence(t, sentenceKey(code, params), namedParams(t, params, names), refusal.message);
  }
  return refusal.message;
}

const named = (list: unknown) => Array.isArray(list) && list.length > 0;

/** A profile in use names the routes, the proxies, or both that still follow it, each in its own sentence. */
function sentenceKey(code: string, params: unknown): string {
  if (code === "DNS_PROFILE_IN_USE" && typeof params === "object" && params !== null) {
    const { routes, proxies } = params as { routes?: unknown; proxies?: unknown };
    if (named(proxies)) return named(routes) ? "next.dns.profiles.inUseByBoth" : "next.dns.profiles.inUseByProxies";
  }
  return refusalKey(code);
}

/** How a refusal's route keys and proxy ids read: the names the pages show for them. */
export type RefusalNames = { route?: (key: string) => string; proxy?: (id: number) => string | undefined };

/** Whether a refusal names routes or proxies, which read by name only once their names are known. */
export function refusalNamesSomething(error: FailedRequest): boolean {
  const params = error.graphQLErrors[0]?.extensions?.params;
  if (typeof params !== "object" || params === null) return false;
  const { routes, proxies, proxyId } = params as { routes?: unknown; proxies?: unknown; proxyId?: unknown };
  return Array.isArray(routes) || Array.isArray(proxies) || typeof proxyId === "number";
}

/**
 * The params with each route key turned into the name the routes page shows
 * for it, and each proxy id into its name: `proxies` as a list, `proxyId` as
 * `proxy`. A proxy not known by name reads as its number.
 */
function namedParams(t: Translate, params: unknown, names?: RefusalNames): unknown {
  if (typeof params !== "object" || params === null) return params;
  const { routes, proxies, proxyId } = params as { routes?: unknown; proxies?: unknown; proxyId?: unknown };
  const proxy = (id: number) => names?.proxy?.(id) ?? t("next.networking.proxyNumber", { id });
  const named: Record<string, unknown> = { ...params };
  if (names?.route && Array.isArray(routes)) {
    named.routes = routes.map((key) => (typeof key === "string" ? names.route!(key) : key));
  }
  if (Array.isArray(proxies) && proxies.every((id) => typeof id === "number")) named.proxies = proxies.map(proxy);
  if (typeof proxyId === "number") named.proxy = proxy(proxyId);
  return named;
}

/**
 * The sentence at `key`, naming what `params` holds, or the daemon's own
 * `message` when this build has no sentence there.
 */
export function codedSentence(t: Translate, key: string, params: unknown, message: string): string {
  const sentence = t(key, namedValues(params));
  return sentence === key ? message : sentence;
}

/** The numbers and words a refusal names, for a sentence that repeats them. */
function namedValues(params: unknown): Record<string, string | number> | undefined {
  if (typeof params !== "object" || params === null) {
    return undefined;
  }
  const values: Record<string, string | number> = {};
  for (const [name, value] of Object.entries(params)) {
    if (typeof value === "string" || typeof value === "number") {
      values[name] = value;
    } else if (Array.isArray(value) && value.every((item) => typeof item === "string")) {
      // A list of names, such as the profiles that still use a server list.
      values[name] = value.join(", ");
    }
  }
  return values;
}
