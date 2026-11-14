// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import {
  findSchemaCompatibilityChanges,
  hasSchemaCompatibilityFailure,
  main,
  parseSchemaCompatibilityOptions,
} from "./check-graphql-schema-compat.mjs";

const BASE_SCHEMA = `
  type Query {
    job(id: ID!): Job
    queue(category: String): [Job!]!
  }

  type Mutation {
    pauseJob(input: PauseJobInput!): JobPayload!
  }

  type Job {
    id: ID!
    name: String!
    state: JobState!
  }

  type JobPayload {
    job: Job!
  }

  input PauseJobInput {
    id: ID!
    reason: String
  }

  enum JobState {
    DOWNLOADING
    COMPLETED
  }
`;

function changesFor(newSchema) {
  return findSchemaCompatibilityChanges(BASE_SCHEMA, newSchema);
}

test("first use of a specified directive does not compare exporter spec versions", () => {
  const declaration='directive @deprecated(reason:String="No longer supported") on FIELD_DEFINITION | ARGUMENT_DEFINITION | INPUT_FIELD_DEFINITION | ENUM_VALUE';
  const changes=changesFor(`${BASE_SCHEMA.replace("name: String!",'name: String! @deprecated(reason:"Use displayName")')}\n${declaration}`);
  assert.equal(hasSchemaCompatibilityFailure(changes),false);
});

test("explicit directive restrictions and custom directive removals still fail", () => {
  const old=`${BASE_SCHEMA}\ndirective @deprecated(reason:String="No longer supported") on FIELD_DEFINITION | ENUM_VALUE`;
  assert.equal(hasSchemaCompatibilityFailure(findSchemaCompatibilityChanges(old,old.replace(" | ENUM_VALUE",""))),true);
  assert.equal(hasSchemaCompatibilityFailure(findSchemaCompatibilityChanges(`${BASE_SCHEMA}\ndirective @audit on FIELD`,BASE_SCHEMA)),true);
});

test("allows additive nullable fields", () => {
  const changes = changesFor(`
    type Query {
      job(id: ID!): Job
      queue(category: String): [Job!]!
    }

    type Mutation {
      pauseJob(input: PauseJobInput!): JobPayload!
    }

    type Job {
      id: ID!
      name: String!
      state: JobState!
      etaSeconds: Int
    }

    type JobPayload {
      job: Job!
    }

    input PauseJobInput {
      id: ID!
      reason: String
    }

    enum JobState {
      DOWNLOADING
      COMPLETED
    }
  `);

  assert.equal(hasSchemaCompatibilityFailure(changes), false);
});

test("allows additive optional input fields", () => {
  const changes = changesFor(BASE_SCHEMA.replace(
    "reason: String",
    "reason: String\n    propagationDelaySecs: Int",
  ));
  assert.equal(hasSchemaCompatibilityFailure(changes), false);
});

test("optional input additions do not hide breaking changes", () => {
  const changes = changesFor(BASE_SCHEMA
    .replace("reason: String", "reason: String\n    propagationDelaySecs: Int")
    .replace("name: String!", "name: Int!"));
  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.ok(changes.breaking.some((change) => change.description.includes("name")));
});

test("rejects removed fields", () => {
  const changes = changesFor(`
    type Query {
      job(id: ID!): Job
      queue(category: String): [Job!]!
    }

    type Mutation {
      pauseJob(input: PauseJobInput!): JobPayload!
    }

    type Job {
      id: ID!
      state: JobState!
    }

    type JobPayload {
      job: Job!
    }

    input PauseJobInput {
      id: ID!
      reason: String
    }

    enum JobState {
      DOWNLOADING
      COMPLETED
    }
  `);

  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.ok(
    changes.breaking.some((change) => change.description.includes("name")),
  );
});

test("rejects changed field types", () => {
  const changes = changesFor(`
    type Query {
      job(id: ID!): Job
      queue(category: String): [Job!]!
    }

    type Mutation {
      pauseJob(input: PauseJobInput!): JobPayload!
    }

    type Job {
      id: ID!
      name: Int!
      state: JobState!
    }

    type JobPayload {
      job: Job!
    }

    input PauseJobInput {
      id: ID!
      reason: String
    }

    enum JobState {
      DOWNLOADING
      COMPLETED
    }
  `);

  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.ok(
    changes.breaking.some((change) => change.description.includes("name")),
  );
});

test("rejects new required input fields and arguments", () => {
  const changes = changesFor(`
    type Query {
      job(id: ID!, serverId: ID!): Job
      queue(category: String): [Job!]!
    }

    type Mutation {
      pauseJob(input: PauseJobInput!): JobPayload!
    }

    type Job {
      id: ID!
      name: String!
      state: JobState!
    }

    type JobPayload {
      job: Job!
    }

    input PauseJobInput {
      id: ID!
      reason: String
      serverId: ID!
    }

    enum JobState {
      DOWNLOADING
      COMPLETED
    }
  `);

  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.ok(
    changes.breaking.some((change) => change.description.includes("serverId")),
  );
});

test("rejects enum value additions as dangerous", () => {
  const changes = changesFor(`
    type Query {
      job(id: ID!): Job
      queue(category: String): [Job!]!
    }

    type Mutation {
      pauseJob(input: PauseJobInput!): JobPayload!
    }

    type Job {
      id: ID!
      name: String!
      state: JobState!
    }

    type JobPayload {
      job: Job!
    }

    input PauseJobInput {
      id: ID!
      reason: String
    }

    enum JobState {
      DOWNLOADING
      COMPLETED
      REPAIRING
    }
  `);

  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.equal(changes.breaking.length, 0);
  assert.ok(
    changes.dangerous.some((change) => change.description.includes("REPAIRING")),
  );
});

test("permits dangerous changes only with an explicit override", () => {
  const changes = changesFor(BASE_SCHEMA.replace("COMPLETED", "COMPLETED\n    REPAIRING"));
  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowDangerous: true }), false);
});

test("the dangerous override still rejects breaking changes", () => {
  const changes = changesFor(BASE_SCHEMA
    .replace("COMPLETED", "COMPLETED\n    REPAIRING")
    .replace("name: String!", "name: Int!"));
  assert.ok(changes.breaking.length > 0);
  assert.ok(changes.dangerous.length > 0);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowDangerous: true }), true);
});

test("breaking changes fail without --allow-breaking and pass with it", () => {
  const changes = changesFor(BASE_SCHEMA.replace("    name: String!\n", ""));
  assert.ok(changes.breaking.length > 0);
  assert.equal(changes.dangerous.length, 0);
  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowDangerous: true }), true);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowBreaking: true }), false);
});

test("the breaking override permits dangerous changes too", () => {
  const changes = changesFor(BASE_SCHEMA.replace("COMPLETED", "COMPLETED\n    REPAIRING"));
  assert.equal(changes.breaking.length, 0);
  assert.ok(changes.dangerous.length > 0);
  assert.equal(hasSchemaCompatibilityFailure(changes), true);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowBreaking: true }), false);
});

test("the breaking override alone permits breaking and dangerous changes together", () => {
  const changes = changesFor(BASE_SCHEMA
    .replace("COMPLETED", "COMPLETED\n    REPAIRING")
    .replace("name: String!", "name: Int!"));
  assert.ok(changes.breaking.length > 0);
  assert.ok(changes.dangerous.length > 0);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowBreaking: true }), false);
  assert.equal(hasSchemaCompatibilityFailure(changes, { allowDangerous: true }), true);
  assert.equal(
    hasSchemaCompatibilityFailure(changes, { allowDangerous: true, allowBreaking: true }),
    false,
  );
});

test("the overrides are read in either order, and breaking covers dangerous", () => {
  assert.deepEqual(parseSchemaCompatibilityOptions([]), {
    allowDangerous: false,
    allowBreaking: false,
  });
  assert.deepEqual(parseSchemaCompatibilityOptions(["--allow-dangerous"]), {
    allowDangerous: true,
    allowBreaking: false,
  });
  assert.deepEqual(parseSchemaCompatibilityOptions(["--allow-breaking"]), {
    allowDangerous: true,
    allowBreaking: true,
  });
  for (const options of [
    ["--allow-dangerous", "--allow-breaking"],
    ["--allow-breaking", "--allow-dangerous"],
  ]) {
    assert.deepEqual(parseSchemaCompatibilityOptions(options), {
      allowDangerous: true,
      allowBreaking: true,
    });
  }
});

test("unknown or repeated options print usage and fail", async () => {
  for (const options of [
    ["--allow-everything"],
    ["--allow-breaking", "--allow-breaking"],
    ["--allow-dangerous", "--allow-dangerous"],
    ["--allow-dangerous", "--allow-breaking", "extra"],
  ]) {
    assert.equal(parseSchemaCompatibilityOptions(options), null);
    const errors = [];
    const original = console.error;
    console.error = (line) => errors.push(line);
    try {
      assert.equal(await main(["old.graphql", "new.graphql", ...options]), 2);
    } finally {
      console.error = original;
    }
    assert.ok(errors.some((line) => line.includes("[--allow-dangerous] [--allow-breaking]")));
  }
});
