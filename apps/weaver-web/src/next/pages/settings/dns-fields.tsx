// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Translate } from "@/lib/context/translate-context";
import {
  DNS_PROBE_KINDS,
  DNS_WIRES,
  MAX_DNS_SERVERS,
  blankServer,
  dnsWireKey,
  type DnsCapabilityCell,
  type DnsProbeKind,
  type DnsCapabilityTable,
  type DnsServerRow,
  type DnsWire,
} from "@/lib/dns";
import { codedSentence } from "@/lib/refusal";
import { lookupErrorKey } from "@/lib/test-via";
import { cn } from "@/lib/utils";
import { networkIds, testIds } from "@/next/testids";
import { Eyebrow } from "../../components/chrome";
import { SecondaryButton, Select, TextField } from "../../components/controls";
import { Icon } from "../../components/icons";
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

const millis = (value: number) => `${Math.round(value)} ms`;

const columnLabel = (t: Translate, probe: DnsProbeKind) =>
  probe === "dnssec" ? t("next.dns.test.dnssec") : t(dnsWireKey(probe.toUpperCase() as DnsWire));

const CAPABILITY_COLUMNS = "minmax(112px,1fr) repeat(4, minmax(72px,84px))";

/** Why a probe did not answer, in the lookup-error wording. */
const failure = (t: Translate, cell: DnsCapabilityCell) =>
  cell.error ? codedSentence(t, lookupErrorKey(cell.error.code), undefined, cell.error.detail) : null;

/**
 * A cell's detail, for hover, focus and the phone line: why it failed, and on
 * the DNSSEC cell that signatures validated.
 */
function cellDetail(t: Translate, probe: DnsProbeKind, cell: DnsCapabilityCell): string | null {
  const parts: string[] = [];
  if (probe === "dnssec" && cell.supported) parts.push(t("next.dns.test.signaturesValidated"));
  const why = failure(t, cell);
  if (why) parts.push(why);
  return parts.length > 0 ? parts.join(" · ") : null;
}

/**
 * One capability: a check when supported, an x when not, a dash while
 * pending. The shape and the accessible label carry it, not the colour alone.
 * A supported probe shows its time; its detail shows on hover and focus. The
 * cell for the row's own wire carries a ring.
 */
function CapabilityCell({ t, row, probe, cell }: { t: Translate; row: number; probe: DnsProbeKind; cell: DnsCapabilityCell }) {
  const detail = cellDetail(t, probe, cell);
  const time = cell.supported && cell.millis !== null ? millis(cell.millis) : null;
  const state =
    cell.supported === null
      ? t("next.dns.test.pending")
      : cell.supported
        ? t("next.dns.test.supported")
        : t("next.dns.test.unsupported");
  const label = [columnLabel(t, probe), state, ...(cell.own ? [t("next.dns.test.ownWire")] : []), ...(time ? [time] : []), ...(detail ? [detail] : [])].join(": ");
  return (
    <div
      role="cell"
      aria-label={label}
      title={[...(time ? [time] : []), ...(detail ? [detail] : [])].join(" · ") || undefined}
      tabIndex={detail ? 0 : undefined}
      data-testid={networkIds.dnsServerResult(row, probe)}
      data-supported={cell.supported === null ? undefined : String(cell.supported)}
      data-own={cell.own ? "true" : undefined}
      className={cn(
        "flex min-w-0 items-center gap-1 rounded-[3px] px-1 py-[2px] font-wv-mono text-[11px]",
        cell.own && "ring-1 ring-wv-line-strong",
        cell.own && cell.supported === false && "ring-wv-error",
      )}
    >
      {cell.supported === null ? (
        <span aria-hidden className="text-wv-faint">–</span>
      ) : (
        <span aria-hidden className={cell.supported ? "text-wv-green" : "text-wv-error"}>
          <Icon name={cell.supported ? "supported" : "unsupported"} size={14} />
        </span>
      )}
      {time ? <span className="truncate text-wv-muted">{time}</span> : null}
    </div>
  );
}

/**
 * A capability test's results: one row per tested server, one column per
 * capability, under a caption naming the route it went through. It scrolls
 * inside its own box on a phone. There, each cell's detail shows in a line
 * under its row, since there is no hover.
 */
export function DnsCapabilityResults({ t, table }: { t: Translate; table: DnsCapabilityTable }) {
  const caption = t("next.dns.test.caption", { route: table.route });
  return (
    <div className="flex-none border-b border-wv-hairline px-4 py-3 sm:px-6">
      <p className="mb-2 text-[12.5px] leading-[1.5] text-wv-muted">
        {table.pending ? `${caption} · ${t("next.networking.testVia.running")}` : caption}
      </p>
      <div
        role="table"
        aria-label={caption}
        aria-busy={table.pending}
        data-testid={networkIds.dnsCapabilities}
        className="min-w-0 overflow-x-auto"
      >
        {/* The narrowest the columns go: 112 + 4 × 72 + 4 gaps of 12. Below it the table scrolls. */}
        <div className="min-w-[448px]">
          <div role="row" className="grid items-end gap-3 border-b border-wv-hairline py-2" style={{ gridTemplateColumns: CAPABILITY_COLUMNS }}>
            {[t("next.dns.test.server"), ...DNS_PROBE_KINDS.map((probe) => columnLabel(t, probe))].map((header) => (
              <div key={header} role="columnheader" className="min-w-0">
                <Eyebrow tone="rail" className="leading-[1.35] break-words">
                  {header}
                </Eyebrow>
              </div>
            ))}
          </div>
          {table.rows.map((line) => {
            const details = DNS_PROBE_KINDS.flatMap((probe) => {
              const detail = cellDetail(t, probe, line.cells[probe]);
              return detail ? [`${columnLabel(t, probe)}: ${detail}`] : [];
            });
            return (
              <div key={line.row} role="rowgroup" className="border-b border-wv-hairline py-2">
                <div role="row" className="grid items-center gap-3" style={{ gridTemplateColumns: CAPABILITY_COLUMNS }}>
                  <div role="rowheader" className="min-w-0 truncate font-wv-mono text-[12px] text-wv-fg" title={line.address}>
                    {line.address}
                  </div>
                  {DNS_PROBE_KINDS.map((probe) => (
                    <CapabilityCell key={probe} t={t} row={line.row} probe={probe} cell={line.cells[probe]} />
                  ))}
                </div>
                {details.length > 0 ? (
                  // On a phone the details are a row of their own, one cell across the table.
                  <div role="row" className="sm:hidden">
                    <div role="cell" aria-colspan={DNS_PROBE_KINDS.length + 1}>
                      <ul className="mt-1 flex flex-col gap-[2px] text-[11.5px] leading-[1.5] text-wv-muted">
                        {details.map((text) => (
                          <li key={text}>{text}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      </div>
      <p className="mt-2 text-[12px] leading-[1.5] text-wv-muted">{t("next.dns.test.help")}</p>
    </div>
  );
}

/**
 * A server list's servers, as settings fields: one row per server, in the
 * order they are asked, and the button that adds one.
 */
export function dnsServerFields(
  t: Translate,
  servers: readonly DnsServerRow[],
  setServers: (change: (current: DnsServerRow[]) => DnsServerRow[]) => void,
  /** A refusal that names one row, drawn under that row. */
  refused?: { row: number; sentence: string },
): FieldSpec[] {
  const patchServer = (index: number, next: Partial<DnsServerRow>) =>
    setServers((current) => current.map((row, at) => (at === index ? { ...row, ...next } : row)));
  const wireOptions = DNS_WIRES.map((wire) => ({ value: wire, label: t(dnsWireKey(wire)) }));
  const rows: FieldSpec[] = servers.map((row, index) => ({
      id: `dnsServer-${index}`,
      label: t("next.dns.server", { number: index + 1 }),
      control: {
        kind: "custom",
        control: (
          <div className="flex w-[268px] max-w-full flex-col gap-2">
            <div className="flex min-w-0 items-center gap-1">
              <Select<DnsWire>
                label={t("next.dns.wire")}
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
                onClick={() => setServers((current) => moved(current, index, -1))}
              />
              <IconButton
                icon="moveDown"
                label={t("next.routing.moveDown")}
                disabled={index === servers.length - 1}
                testId={testIds.field(`dnsServer-${index}-moveDown`)}
                onClick={() => setServers((current) => moved(current, index, 1))}
              />
              <IconButton
                icon="remove"
                label={t("next.common.remove")}
                disabled={servers.length <= 1}
                testId={testIds.field(`dnsServer-${index}-remove`)}
                onClick={() => setServers((current) => current.filter((_, at) => at !== index))}
              />
            </div>
            <TextField
              label={t("next.dns.address")}
              value={row.address}
              type={row.wire === "HTTPS" ? "url" : "text"}
              placeholder={row.wire === "HTTPS" ? "https://1.1.1.1/dns-query" : undefined}
              className="w-full"
              testId={testIds.field(`dnsServer-${index}-address`)}
              onChange={(address) => patchServer(index, { address })}
            />
            {row.wire === "TLS" ? (
              <TextField
                label={t("next.dns.tlsName")}
                value={row.tlsName}
                placeholder={t("next.dns.tlsNameOptional")}
                className="w-full"
                testId={testIds.field(`dnsServer-${index}-tlsName`)}
                onChange={(tlsName) => patchServer(index, { tlsName })}
              />
            ) : null}
            {refused?.row === index ? (
              <p
                role="alert"
                data-testid={testIds.fieldHelp(`dnsServer-${index}`)}
                className="text-[12.5px] leading-[1.5] text-wv-error-text"
              >
                {refused.sentence}
              </p>
            ) : null}
          </div>
        ),
      },
    }));
  return [
    ...rows,
    {
      id: "dnsServerAdd",
      label: "",
      help: t("next.dns.orderHelp", { count: MAX_DNS_SERVERS }),
      control: {
        kind: "custom",
        control: (
          <SecondaryButton
            icon="add"
            disabled={servers.length >= MAX_DNS_SERVERS}
            testId={testIds.field("dnsServerAdd")}
            onClick={() =>
              setServers((current) => (current.length >= MAX_DNS_SERVERS ? current : [...current, blankServer()]))}
          >
            {t("next.dns.addServer")}
          </SecondaryButton>
        ),
      },
    },
  ];
}
