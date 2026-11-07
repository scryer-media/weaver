// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { Page, Request } from "@playwright/test";

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Whether `request` is the GraphQL POST of the operation `name`: its
 * `operationName` when the client sends one, else the `query`/`mutation`
 * `<name>` heading of its document. A name that merely appears inside another
 * operation never matches.
 */
export function isOperation(request: Request, name: string): boolean {
  if (request.method() !== "POST" || !new URL(request.url()).pathname.endsWith("/graphql")) return false;
  let body: unknown;
  try {
    body = request.postDataJSON();
  } catch {
    return false;
  }
  if (typeof body !== "object" || body === null) return false;
  const { operationName, query } = body as { operationName?: unknown; query?: unknown };
  if (typeof operationName === "string" && operationName !== "") return operationName === name;
  if (typeof query !== "string") return false;
  return new RegExp(`\\b(?:query|mutation|subscription)\\s+${escapeRegExp(name)}\\b`).test(query);
}

/** The response to the next GraphQL operation `name` the page sends. */
export function operationResponse(page: Page, name: string) {
  return page.waitForResponse((response) => isOperation(response.request(), name));
}
