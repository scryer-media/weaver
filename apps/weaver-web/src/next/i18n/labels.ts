import type { Translate } from "@/lib/context/translate-context";

/**
 * Helpers for the phrases a plain key lookup cannot build.
 *
 * Counted phrases take two keys, `<key>.one` and `<key>.other`; languages that
 * do not inflect for number carry the same text in both.
 */

export function countLabel(
  t: Translate,
  key: string,
  count: number,
  values?: Record<string, string | number | boolean | null | undefined>,
): string {
  return t(`${key}.${count === 1 ? "one" : "other"}`, { count, ...values });
}

/**
 * A phrase with one element set inside it, such as a link: the text either side
 * of its `{{slot}}` placeholder, so each language places the element itself.
 */
export function aroundSlot(
  t: Translate,
  key: string,
  slot: string,
  values?: Record<string, string | number | boolean | null | undefined>,
): [string, string] {
  const text = t(key, values);
  const marker = `{{${slot}}}`;
  const at = text.indexOf(marker);
  return at < 0 ? [text, ""] : [text.slice(0, at), text.slice(at + marker.length)];
}

/** Server health reports its state as a lowercase word; unknown ones pass through. */
export function providerStateLabel(t: Translate, state: string): string {
  const key = `next.provider.state.${state.toLowerCase()}`;
  const label = t(key);
  return label === key ? state.replace(/_/g, " ") : label;
}
