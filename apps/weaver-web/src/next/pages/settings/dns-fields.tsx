// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import {
  DNS_WIRES,
  MAX_DNS_SERVERS,
  blankServer,
  dnsWireKey,
  type DnsForm,
  type DnsServerRow,
  type ProxyDnsPath,
  type ProxyDnsTrust,
  type ProxyDnsWho,
  type ProxyDnsWire,
} from "@/lib/proxy-dns";
import { testIds } from "@/next/testids";
import { SecondaryButton, Segmented, Select, TextField } from "../../components/controls";
import { IconButton } from "../../features/networking/presentation";
import type { FieldSpec } from "./framework";

/** A row moved one place up (`-1`) or down (`1`); the order is the order the servers are asked in. */
function moved(rows: readonly DnsServerRow[], index: number, by: -1 | 1): DnsServerRow[] {
  const to = index + by;
  if (to < 0 || to >= rows.length) return [...rows];
  const next = [...rows];
  [next[index], next[to]] = [next[to]!, next[index]!];
  return next;
}

/**
 * The three questions of a DNS setting, as settings fields: who answers, how
 * the listed servers are reached and in what order they are asked, and what
 * their answers must carry. A proxy and an egress ask them alike; an egress
 * offers no proxy to answer and no path, since its servers are always asked
 * straight from it.
 */
export function dnsFields(
  t: Translate,
  dns: DnsForm,
  setDns: (change: (current: DnsForm) => DnsForm) => void,
  who: readonly ProxyDnsWho[],
  { path, whoHelp }: { path: boolean; whoHelp?: string },
): FieldSpec[] {
  const whoLabel: Record<ProxyDnsWho, string> = {
    HOST: t("next.proxies.dnsWhoHost"),
    PROXY: t("next.proxies.dnsWhoProxy"),
    SERVERS: t("next.proxies.dnsWhoServers"),
  };
  const patchServer = (index: number, next: Partial<DnsServerRow>) =>
    setDns((current) => ({
      ...current,
      servers: current.servers.map((row, at) => (at === index ? { ...row, ...next } : row)),
    }));
  const wireOptions = DNS_WIRES.map((wire) => ({ value: wire, label: t(dnsWireKey(wire)) }));
  const serverFields: FieldSpec[] = dns.servers.map((row, index) => ({
    id: `dnsServer-${index}`,
    label: t("next.proxies.dnsServer", { number: index + 1 }),
    control: {
      kind: "custom",
      control: (
        <div className="flex w-[268px] max-w-full flex-col gap-2">
          <div className="flex min-w-0 items-center gap-1">
            <Select<ProxyDnsWire>
              label={t("next.proxies.dnsWire")}
              value={row.wire}
              options={wireOptions}
              className="mr-1 min-w-0 flex-1"
              testId={testIds.field(`dnsServer-${index}-wire`)}
              onChange={(wire) => patchServer(index, { wire })}
            />
            <IconButton
              icon="moveUp"
              label={t("next.routing.moveUp")}
              disabled={index === 0}
              testId={testIds.field(`dnsServer-${index}-moveUp`)}
              onClick={() => setDns((current) => ({ ...current, servers: moved(current.servers, index, -1) }))}
            />
            <IconButton
              icon="moveDown"
              label={t("next.routing.moveDown")}
              disabled={index === dns.servers.length - 1}
              testId={testIds.field(`dnsServer-${index}-moveDown`)}
              onClick={() => setDns((current) => ({ ...current, servers: moved(current.servers, index, 1) }))}
            />
            <IconButton
              icon="remove"
              label={t("next.common.remove")}
              disabled={dns.servers.length <= 1}
              testId={testIds.field(`dnsServer-${index}-remove`)}
              onClick={() =>
                setDns((current) => ({ ...current, servers: current.servers.filter((_, at) => at !== index) }))}
            />
          </div>
          <TextField
            label={t("next.proxies.dnsAddress")}
            value={row.address}
            type={row.wire === "HTTPS" ? "url" : "text"}
            placeholder={row.wire === "HTTPS" ? "https://1.1.1.1/dns-query" : undefined}
            className="w-full"
            testId={testIds.field(`dnsServer-${index}-address`)}
            onChange={(address) => patchServer(index, { address })}
          />
          {row.wire === "TLS" ? (
            <TextField
              label={t("next.proxies.dnsTlsName")}
              value={row.tlsName}
              placeholder={t("next.proxies.dnsTlsNameOptional")}
              className="w-full"
              testId={testIds.field(`dnsServer-${index}-tlsName`)}
              onChange={(tlsName) => patchServer(index, { tlsName })}
            />
          ) : null}
        </div>
      ),
    },
  }));
  return [
    {
      id: "dnsWho",
      label: t("next.proxies.dnsWho"),
      help: whoHelp,
      control: {
        kind: "segmented",
        value: dns.who,
        options: who.map((value) => ({ value, label: whoLabel[value] })),
        onChange: (next) => setDns((current) => ({ ...current, who: next as ProxyDnsWho })),
      },
    },
    // The host or the proxy answering is the whole answer: there is nothing more to ask.
    ...(dns.who === "SERVERS"
      ? [
          ...(path
            ? [
                {
                  id: "dnsPath",
                  label: t("next.proxies.dnsPath"),
                  control: {
                    kind: "custom" as const,
                    control: (
                      <Segmented<ProxyDnsPath>
                        label={t("next.proxies.dnsPath")}
                        value={dns.path}
                        options={[
                          { value: "DIRECT", label: t("next.proxies.dnsPathDirect") },
                          { value: "THROUGH_PROXY", label: t("next.proxies.dnsPathProxy") },
                        ]}
                        testId={testIds.field("dnsPath")}
                        onChange={(next) => setDns((current) => ({ ...current, path: next }))}
                      />
                    ),
                  },
                },
              ]
            : []),
          ...serverFields,
          {
            id: "dnsServerAdd",
            label: "",
            help: t("next.proxies.dnsOrderHelp", { count: MAX_DNS_SERVERS }),
            control: {
              kind: "custom" as const,
              control: (
                <SecondaryButton
                  icon="add"
                  disabled={dns.servers.length >= MAX_DNS_SERVERS}
                  testId={testIds.field("dnsServerAdd")}
                  onClick={() =>
                    setDns((current) =>
                      current.servers.length >= MAX_DNS_SERVERS
                        ? current
                        : { ...current, servers: [...current.servers, blankServer()] })}
                >
                  {t("next.proxies.dnsAddServer")}
                </SecondaryButton>
              ),
            },
          },
          {
            id: "dnsTrust",
            label: t("next.proxies.dnsTrust"),
            help:
              dns.trust === "REQUIRE_AD"
                ? t("next.proxies.dnsRequireNote")
                : dns.trust === "VALIDATE"
                  ? t("next.proxies.dnsValidateNote")
                  : undefined,
            control: {
              kind: "segmented" as const,
              value: dns.trust,
              options: [
                { value: "AS_SENT", label: t("next.proxies.dnsTrustAsSent") },
                { value: "REQUIRE_AD", label: t("next.proxies.dnsTrustRequire") },
                { value: "VALIDATE", label: t("next.proxies.dnsTrustValidate") },
              ],
              onChange: (next: string) => setDns((current) => ({ ...current, trust: next as ProxyDnsTrust })),
            },
          },
        ]
      : []),
  ];
}

/** The warning a section of DNS fields carries when a plain server could forge the authenticated mark. */
export function DnsPlainWarning({ t }: { t: Translate }) {
  return (
    <div
      data-testid={testIds.fieldHelp("dnsPlainWarning")}
      className="flex-none border-b border-wv-hairline px-4 py-3 text-[12.5px] leading-[1.5] text-wv-warn sm:px-6"
    >
      {t("next.proxies.dnsPlainWarning")}
    </div>
  );
}
