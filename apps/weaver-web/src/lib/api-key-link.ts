/**
 * The deep link that mints an API key.
 *
 * Scryer, and anything else that needs a key from a person looking at their
 * own Weaver, links to `settings/security?createApiKey=1&name=…&scope=…`
 * rather than asking them to find the panel and match a scope. The longer
 * spellings (`apiKeyGenerate`, `apiKeyName`, `apiKeyScope`) are the ones the
 * classic interface accepted first, so links written against it keep working.
 *
 * Reading a link consumes it: the caller puts the returned query string back
 * in the address bar, so a reload cannot quietly mint a second key.
 */

const GENERATE_PARAMS = ["apiKeyGenerate", "createApiKey"] as const;
const NAME_PARAMS = ["apiKeyName", "name"] as const;
const SCOPE_PARAMS = ["apiKeyScope", "scope"] as const;

export type ApiKeyLinkScope = "READ" | "CONTROL" | "ADMIN";

export interface ApiKeyLink {
  name: string;
  scope: ApiKeyLinkScope;
}

/** The scope a link asked for; `integration` is Scryer's word for control. */
function linkScope(value: string | null): ApiKeyLinkScope | null {
  switch (value?.trim().toLowerCase()) {
    case "control":
    case "integration":
      return "CONTROL";
    case "read":
      return "READ";
    case "admin":
      return "ADMIN";
    default:
      return null;
  }
}

/** The first of these parameters the query string carries, if any. */
function firstParam(params: URLSearchParams, names: readonly string[]): string | null {
  for (const name of names) {
    const value = params.get(name);
    if (value !== null) return value;
  }
  return null;
}

/**
 * Read a key-minting link out of `search`, returning what is left of the query
 * string. A query string that asks for nothing, or names no key, is returned
 * untouched.
 */
export function takeApiKeyLink(search: string): { link: ApiKeyLink | null; search: string } {
  const params = new URLSearchParams(search);
  const generate = firstParam(params, GENERATE_PARAMS);
  if (generate !== "1" && generate?.trim().toLowerCase() !== "true") {
    return { link: null, search };
  }
  const name = firstParam(params, NAME_PARAMS)?.trim() ?? "";
  if (!name) {
    return { link: null, search };
  }
  const scope = linkScope(firstParam(params, SCOPE_PARAMS)) ?? "CONTROL";
  for (const param of [...GENERATE_PARAMS, ...NAME_PARAMS, ...SCOPE_PARAMS]) {
    params.delete(param);
  }
  return { link: { name, scope }, search: params.toString() };
}
