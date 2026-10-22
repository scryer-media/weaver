// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import { test } from "node:test";
import type { Client } from "urql";
import { ALIASED_GROUP_SIZE, executeAliasedIdMutation } from "../src/graphql/aliased-mutations.ts";

// A client that answers each aliased field with whether its id is even, and
// records the ids each request carried. The request at index `failing` fails whole.
function fakeClient(failing: number | null = null) {
  const requests: number[][] = [];
  const client = {
    mutation: (_query: unknown, variables: Record<string, unknown>) => ({
      toPromise: async () => {
        const ids = Object.entries(variables)
          .filter(([name]) => /^id\d+$/.test(name))
          .map(([, id]) => id as number);
        requests.push(ids);
        if (requests.length - 1 === failing) {
          return { data: undefined, error: { message: "Query is too complex." } };
        }
        return { data: Object.fromEntries(ids.map((id, index) => [`cancelJob${index}`, id % 2 === 0])) };
      },
    }),
  } as unknown as Client;
  return { client, requests };
}

const definition = { operationName: "CancelSelectedJobs", aliasPrefix: "cancelJob", fieldName: "cancelJob" };

test("a large selection goes in bounded groups, keyed by each id's place", async () => {
  const ids = Array.from({ length: ALIASED_GROUP_SIZE * 2 + 5 }, (_, index) => 20_000 + index);
  const { client, requests } = fakeClient();

  const result = await executeAliasedIdMutation({ client, ids, ...definition });

  assert.deepEqual(
    requests.map((request) => request.length),
    [ALIASED_GROUP_SIZE, ALIASED_GROUP_SIZE, 5],
  );
  assert.deepEqual(requests.flat(), ids);
  assert.equal(result.error, undefined);
  ids.forEach((id, index) => assert.equal(result.data[`cancelJob${index}`], id % 2 === 0));
});

test("a failed group leaves only its own ids out, and the rest still run", async () => {
  const ids = Array.from({ length: ALIASED_GROUP_SIZE * 3 }, (_, index) => index * 2);
  const { client, requests } = fakeClient(1);

  const result = await executeAliasedIdMutation({ client, ids, ...definition });

  assert.equal(requests.length, 3);
  assert.equal(result.error?.message, "Query is too complex.");
  ids.forEach((_id, index) => {
    const inFailedGroup = index >= ALIASED_GROUP_SIZE && index < ALIASED_GROUP_SIZE * 2;
    assert.equal(result.data[`cancelJob${index}`], inFailedGroup ? undefined : true);
  });
});
