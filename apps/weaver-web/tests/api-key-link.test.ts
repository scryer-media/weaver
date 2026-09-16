import assert from "node:assert/strict";
import test from "node:test";
import { takeApiKeyLink } from "../src/lib/api-key-link.ts";

test("the link Scryer writes mints a control key", () => {
  const taken = takeApiKeyLink("?createApiKey=1&name=Scryer&scope=integration");
  assert.deepEqual(taken.link, { name: "Scryer", scope: "CONTROL" });
  assert.equal(taken.search, "");
});

test("the spellings the classic interface accepted still read", () => {
  assert.deepEqual(takeApiKeyLink("?apiKeyGenerate=true&apiKeyName=Sonarr&apiKeyScope=read").link, {
    name: "Sonarr",
    scope: "READ",
  });
  assert.deepEqual(takeApiKeyLink("?createApiKey=1&name=%20Radarr%20&scope=ADMIN").link, {
    name: "Radarr",
    scope: "ADMIN",
  });
});

test("a scope that means nothing here falls back to control", () => {
  assert.deepEqual(takeApiKeyLink("?createApiKey=1&name=Scryer&scope=everything").link, {
    name: "Scryer",
    scope: "CONTROL",
  });
  assert.deepEqual(takeApiKeyLink("?createApiKey=1&name=Scryer").link, {
    name: "Scryer",
    scope: "CONTROL",
  });
});

test("a link is consumed, and the rest of the query string is kept", () => {
  const taken = takeApiKeyLink("?panel=security&createApiKey=1&name=Scryer&scope=integration&lang=de");
  assert.equal(taken.search, "panel=security&lang=de");
});

test("a query string that asks for no key is left alone", () => {
  for (const search of ["", "?lang=de", "?createApiKey=0&name=Scryer", "?createApiKey=1", "?createApiKey=1&name=%20"]) {
    const taken = takeApiKeyLink(search);
    assert.equal(taken.link, null);
    assert.equal(taken.search, search);
  }
});
