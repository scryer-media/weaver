// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Locator, Page, Response } from "@playwright/test";
import { expect } from "../helpers";
import { isOperation, operationResponse } from "./graphql-operations";
import type { DnsTransit, DnsTrust, DnsWho, DnsWire } from "./network-flow";
import { choose } from "./settings-locators";
import { networkIds, testIds } from "./testids";

const field = testIds.field;

export const DNS_PAGE = "/settings/networking/dns";
export const ROUTES_PAGE = "/settings/networking/routes";
export const PROXIES_PAGE = "/settings/networking/proxies";

/** The id a save answered with, after checking it answered without errors. */
export async function savedId(response: Response, mutation: string): Promise<number> {
  expect(response.ok(), `${mutation} answered`).toBeTruthy();
  const payload = await response.json() as { data?: Record<string, { id: number } | null>; errors?: unknown[] };
  expect(payload.errors ?? [], `${mutation} errors`).toEqual([]);
  const id = payload.data?.[mutation]?.id;
  expect(typeof id, `${mutation} returned an id`).toBe("number");
  return id!;
}

/** How many GraphQL `name` operations the page has sent since this call. */
export function countOperations(page: Page, name: string): () => number {
  let count = 0;
  page.on("request", (request) => { if (isOperation(request, name)) count += 1; });
  return () => count;
}

export async function openDns(page: Page): Promise<void> {
  await page.goto(DNS_PAGE);
  await expect(page.getByTestId(testIds.section("dnsLists"))).toBeVisible();
}

/** The server list editor of a new list. */
export async function newListEditor(page: Page): Promise<Locator> {
  await page.getByTestId(testIds.tableAdd("dnsLists")).click();
  const editor = page.getByTestId(testIds.dialog("dnsList"));
  await expect(editor).toBeVisible();
  return editor;
}

export type ServerRow = { wire?: DnsWire; address: string };

/** Fill a new list's rows in order: the first row is there already, each later one is added. */
export async function fillServers(page: Page, editor: Locator, rows: readonly ServerRow[]): Promise<void> {
  for (const [index, row] of rows.entries()) {
    if (index > 0) await editor.getByTestId(field("dnsServerAdd")).click();
    if (row.wire && row.wire !== "PLAIN") await choose(page, editor, field(`dnsServer-${index}-wire`), row.wire);
    await editor.getByTestId(field(`dnsServer-${index}-address`)).fill(row.address);
  }
}

/** Every row's address as the editor shows it, in order. */
export async function expectAddresses(editor: Locator, addresses: readonly string[], what: string): Promise<void> {
  for (const [index, address] of addresses.entries()) {
    await expect(editor.getByTestId(field(`dnsServer-${index}-address`)), `${what}: row ${index}`).toHaveValue(address);
  }
  await expect(editor.getByTestId(field(`dnsServer-${addresses.length}-address`)), `${what}: no row after ${addresses.length}`).toHaveCount(0);
}

export type ProfileForm = {
  name: string; who: DnsWho; listId?: number; transit?: DnsTransit; trust?: DnsTrust;
};

/** Fill an open profile editor; a field the form leaves out keeps what the editor shows. */
export async function fillProfile(page: Page, editor: Locator, form: ProfileForm): Promise<void> {
  await editor.getByTestId(field("dnsProfileName")).fill(form.name);
  await pick(editor, "dnsWho", form.who);
  if (form.listId !== undefined) await choose(page, editor, field("dnsList"), String(form.listId));
  if (form.transit) await pick(editor, "dnsTransit", form.transit);
  if (form.trust) await pick(editor, "dnsTrust", form.trust);
}

/** Pick `value` on the segmented field `id` and wait until it shows as chosen. */
export async function pick(scope: Locator, id: string, value: string): Promise<void> {
  const option = scope.getByTestId(testIds.option(field(id), value));
  await option.click();
  await expect(option, `${id} shows ${value}`).toHaveAttribute("aria-checked", "true");
}

/** Save an open DNS editor and return the saved record's id once the editor has closed. */
export async function saveEditor(page: Page, editor: Locator, operation: string, mutation: string): Promise<number> {
  const saved = operationResponse(page, operation);
  await editor.getByTestId(testIds.editorSave).click();
  const id = await savedId(await saved, mutation);
  await expect(editor, `${operation} closed its editor`).toBeHidden();
  return id;
}

/** Open the route editor of the consumer `key` from the routes page. */
export async function openRoute(page: Page, key: string): Promise<Locator> {
  await page.goto(ROUTES_PAGE);
  await page.getByTestId(testIds.row("routes", key)).click();
  const dialog = page.getByTestId(testIds.dialog("route"));
  await expect(dialog).toBeVisible();
  return dialog;
}

/** Choose the DNS profile `id` in an open route editor. */
export async function chooseRouteProfile(page: Page, dialog: Locator, id: number): Promise<void> {
  await choose(page, dialog, field("dnsProfile"), String(id));
  await expect(dialog.getByTestId(testIds.selected(field("dnsProfile"), String(id)))).toBeVisible();
}

/** The results cell of form row `row` for `probe`. */
export const resultCell = (editor: Locator, row: number, probe: "plain" | "tls" | "https" | "dnssec") =>
  editor.getByTestId(networkIds.dnsServerResult(row, probe));
