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
export function refusalMessage(t: Translate, error: FailedRequest): string {
  const refusal = error.graphQLErrors[0];
  if (!refusal) {
    return error.message;
  }
  const code = refusal.extensions?.code;
  if (typeof code === "string") {
    return codedSentence(t, refusalKey(code), refusal.extensions?.params, refusal.message);
  }
  return refusal.message;
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
    }
  }
  return values;
}
