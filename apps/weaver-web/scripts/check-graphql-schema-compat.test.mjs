import assert from "node:assert/strict";
import test from "node:test";
import {
  findSchemaCompatibilityChanges,
  hasSchemaCompatibilityFailure,
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
