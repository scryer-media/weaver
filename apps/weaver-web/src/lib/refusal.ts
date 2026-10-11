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
    const key = refusalKey(code);
    const sentence = t(key);
    if (sentence !== key) {
      return sentence;
    }
  }
  return refusal.message;
}
