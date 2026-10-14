import type { Translate } from "@/lib/context/translate-context";
import { codedSentence } from "./refusal.ts";

/**
 * A proxy's failure, or a proxy test's outcome, as the daemon reports it: a
 * code, the values the sentence for it names, and the daemon's English.
 */
export type CodedOutcome = { code: string; params?: unknown; message: string };

/** Where the sentence for a proxy failure's code lives in the dictionary. */
export const proxyFaultKey = (code: string) => `next.proxyFault.${code}`;

/** Where the sentence for a passed proxy test's code lives in the dictionary. */
export const proxyTestKey = (code: string) => `next.proxyTest.${code}`;

/** What to show for a proxy's failure: our sentence for its code, or the daemon's words. */
export function proxyFaultSentence(t: Translate, fault: CodedOutcome): string {
  return codedSentence(t, proxyFaultKey(fault.code), fault.params, fault.message);
}

/** What to show for a proxy test, passed or failed. */
export function proxyTestSentence(t: Translate, test: CodedOutcome & { success: boolean }): string {
  return codedSentence(t, (test.success ? proxyTestKey : proxyFaultKey)(test.code), test.params, test.message);
}
