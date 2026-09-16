import { getOperationAST, parse } from "graphql";

/** Only a known read may be replayed after refreshing browser credentials. */
export function canRetrySessionRequest(input: RequestInfo | URL, init?: RequestInit): boolean {
  const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
  if (method === "GET" || method === "HEAD") return true;
  const url = input instanceof Request ? input.url : String(input);
  if (method !== "POST" || !/(?:^|\/)graphql(?:\?|$)/.test(url) || typeof init?.body !== "string") {
    return false;
  }
  try {
    const body = JSON.parse(init.body);
    if (typeof body.query !== "string") return false;
    return getOperationAST(parse(body.query), body.operationName)?.operation === "query";
  } catch {
    return false;
  }
}
