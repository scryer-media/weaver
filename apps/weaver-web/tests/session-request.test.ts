import assert from "node:assert/strict";
import test from "node:test";
import { canRetrySessionRequest } from "../src/graphql/session-request.ts";

test("safe reads can retry once, ordinary mutations cannot", () => {
  assert.equal(canRetrySessionRequest("api/status"), true);
  for (const method of ["POST", "PUT", "PATCH", "DELETE"]) {
    assert.equal(canRetrySessionRequest("api/system/restart", { method }), false);
  }
});

test("GraphQL retry classifies the selected operation, never text or comments", () => {
  const request = (query: string, operationName?: string) => ({
    method: "POST", body: JSON.stringify({ query, operationName }),
  });
  assert.equal(canRetrySessionRequest("graphql", request("{ __typename }")), true);
  assert.equal(canRetrySessionRequest("/prefix/graphql", request("query Read { __typename }")), true);
  assert.equal(canRetrySessionRequest("graphql", request("# query harmless\nmutation { restartServer }")), false);
  assert.equal(canRetrySessionRequest("graphql", request("query Read { __typename } mutation Write { restartServer }", "Write")), false);
  assert.equal(canRetrySessionRequest("graphql", request("query Read { __typename } mutation Write { restartServer }", "Read")), true);
  assert.equal(canRetrySessionRequest("graphql", request("query Read { __typename } mutation Write { restartServer }")), false);
  assert.equal(canRetrySessionRequest("api/backup/restore", request("{ __typename }")), false);
  assert.equal(canRetrySessionRequest("graphql", request("invalid graphql")), false);
  assert.equal(canRetrySessionRequest("graphql", { method: "POST", body: "not JSON" }), false);
});
