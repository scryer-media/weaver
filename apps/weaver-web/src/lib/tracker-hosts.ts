// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * The host patterns a tracker rule can name, read from hosts and announce
 * URLs. A private tracker's URL carries the operator's passkey, so only hosts
 * ever leave this module.
 */

const LABEL = /^[a-z0-9_-]+$/;
const IPV4 = /^\d+\.\d+\.\d+\.\d+$/;

/**
 * The host an announce URL names, as the daemon stores and matches it: read
 * as an http URL reads it whatever the tracker's scheme, so lower case, an
 * international name in punycode and an IPv4 address in dotted decimal,
 * without a trailing dot. Null for a URL with no host a rule can name.
 */
export function trackerHost(url: string): string | null {
  let host: string;
  try {
    host = new URL(`http://${new URL(url.trim()).hostname}/`).hostname;
  } catch {
    return null;
  }
  host = host.replace(/\.+$/, "");
  return nameable(host) ? host : null;
}

/** Whether a rule can name `host`: labels of letters, digits, `-` and `_`. */
const nameable = (host: string): boolean => host !== "" && host.split(".").every((label) => LABEL.test(label));

/**
 * The patterns that name `host`: the host itself, then the wildcard over its
 * parent domain, or over the host when it is a domain of two labels.
 */
export function hostPatterns(host: string): string[] {
  const labels = host.split(".");
  if (labels.length < 2 || IPV4.test(host)) {
    return [host];
  }
  return [host, `*.${labels.length > 2 ? labels.slice(1).join(".") : host}`];
}

/** The patterns that name `hosts`, each once, in the order the hosts come. */
export function patternsOf(hosts: readonly string[]): string[] {
  return [...new Set(hosts.filter(nameable).flatMap(hostPatterns))];
}

/** The hosts announce URLs name, each once. */
export const announceHosts = (urls: readonly string[]): string[] => [
  ...new Set(urls.map(trackerHost).filter((host): host is string => host !== null)),
];

/* ------------------------------------------------------------------ bencode */

type Bencoded = Uint8Array | number | Bencoded[] | Map<string, Bencoded>;

const DIGIT_0 = 0x30;
const COLON = 0x3a;
const UTF8 = new TextDecoder();

/** One value at `at`, and where the next one starts. Throws on a malformed value. */
function decode(bytes: Uint8Array, at: number): [Bencoded, number] {
  const head = bytes[at];
  if (head === 0x69 /* i */) {
    const end = bytes.indexOf(0x65 /* e */, at);
    if (end < 0) throw new Error("integer");
    return [Number(UTF8.decode(bytes.subarray(at + 1, end))), end + 1];
  }
  if (head === 0x6c /* l */ || head === 0x64 /* d */) {
    const items: Bencoded[] = [];
    let next = at + 1;
    while (bytes[next] !== 0x65) {
      if (next >= bytes.length) throw new Error("container");
      const [item, after] = decode(bytes, next);
      items.push(item);
      next = after;
    }
    if (head === 0x6c) return [items, next + 1];
    const dict = new Map<string, Bencoded>();
    for (let index = 0; index + 1 < items.length; index += 2) {
      const key = items[index];
      if (!(key instanceof Uint8Array)) throw new Error("key");
      dict.set(UTF8.decode(key), items[index + 1]);
    }
    return [dict, next + 1];
  }
  let length = 0;
  let next = at;
  while (bytes[next] !== COLON) {
    const digit = bytes[next] - DIGIT_0;
    if (next >= bytes.length || digit < 0 || digit > 9) throw new Error("string");
    length = length * 10 + digit;
    next += 1;
  }
  const start = next + 1;
  if (start + length > bytes.length) throw new Error("string");
  return [bytes.subarray(start, start + length), start + length];
}

const text = (value: Bencoded | undefined): string[] =>
  value instanceof Uint8Array ? [UTF8.decode(value)] : [];

/**
 * The announce URLs a .torrent file names, its announce list's tiers in
 * order and its lone announce URL. Null when the bytes are not a torrent.
 */
export function torrentAnnounces(bytes: Uint8Array): string[] | null {
  let root: Bencoded;
  try {
    root = decode(bytes, 0)[0];
  } catch {
    return null;
  }
  if (!(root instanceof Map) || !(root.get("info") instanceof Map)) {
    return null;
  }
  const tiers = root.get("announce-list");
  const listed = Array.isArray(tiers) ? tiers.flatMap((tier) => (Array.isArray(tier) ? tier.flatMap(text) : [])) : [];
  return [...new Set([...listed, ...text(root.get("announce"))])];
}
