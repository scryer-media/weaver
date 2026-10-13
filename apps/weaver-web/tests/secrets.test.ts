import assert from "node:assert/strict";
import test from "node:test";
import { secretChange, sortedSecrets, usedByText, type Secret } from "../src/next/data/secrets.ts";

function secret(id: string, name: string, usedBy: Secret["usedBy"] = []): Secret {
  return { id, name, createdAt: "2026-01-01T00:00:00Z", updatedAt: "2026-01-01T00:00:00Z", usedBy };
}

test("a new secret needs a name within the limit and a value", () => {
  assert.deepEqual(secretChange({ name: "  ", value: "x" }, null), { ok: false, problem: "next.secrets.nameInvalid" });
  assert.deepEqual(secretChange({ name: "a".repeat(129), value: "x" }, null), {
    ok: false,
    problem: "next.secrets.nameInvalid",
  });
  // The limit is in bytes, as the daemon counts it.
  assert.deepEqual(secretChange({ name: "é".repeat(65), value: "x" }, null), {
    ok: false,
    problem: "next.secrets.nameInvalid",
  });
  assert.deepEqual(secretChange({ name: "Mail", value: "" }, null), { ok: false, problem: "next.secrets.valueRequired" });
  assert.deepEqual(secretChange({ name: " Mail ", value: "typed" }, null), { ok: true, name: "Mail", value: "typed" });
});

test("a saved secret sends only what changed, and a blank value keeps the saved one", () => {
  const saved = secret("s1", "Mail");
  assert.deepEqual(secretChange({ name: "Mail", value: "" }, saved), { ok: true, name: null, value: null });
  assert.deepEqual(secretChange({ name: "Mail token", value: "" }, saved), { ok: true, name: "Mail token", value: null });
  assert.deepEqual(secretChange({ name: "Mail", value: "rotated" }, saved), { ok: true, name: null, value: "rotated" });
});

test("secrets list by name ignoring case, and name the instances that link them", () => {
  const listed = sortedSecrets([secret("b", "beta"), secret("a", "Alpha"), secret("c", "alpha")]);
  assert.deepEqual(
    listed.map((entry) => entry.id),
    ["a", "c", "b"],
  );
  assert.equal(usedByText(secret("s", "Mail")), "");
  assert.equal(
    usedByText(
      secret("s", "Mail", [
        { id: "i1", name: "Notify" },
        { id: "i2", name: "Mirror" },
      ]),
    ),
    "Notify, Mirror",
  );
});
