import assert from "node:assert/strict";
import test from "node:test";

import { createLiveJobDownloadRatesStore } from "../src/lib/live-job-download-rates.ts";

test("rates are unknown until the first live snapshot arrives", () => {
  const store = createLiveJobDownloadRatesStore();

  assert.equal(store.getRate(7), undefined);

  store.setRates([{ jobId: 7, rateBps: 61_100_000 }]);

  assert.equal(store.getRate(7), 61_100_000);
  assert.equal(store.getRate(8), null);
  assert.equal(store.getRate(null), null);
});

test("dropping the live snapshot returns rows to the queue-item fallback", () => {
  const store = createLiveJobDownloadRatesStore();
  store.setRates([{ jobId: 7, rateBps: 1 }]);
  store.setRates(undefined);

  assert.equal(store.getRate(7), undefined);
});

test("listeners fire once per new snapshot and not for a repeated one", () => {
  const store = createLiveJobDownloadRatesStore();
  let notified = 0;
  const unsubscribe = store.subscribe(() => {
    notified += 1;
  });

  const first = [{ jobId: 1, rateBps: 10 }];
  store.setRates(first);
  store.setRates(first);
  store.setRates([{ jobId: 1, rateBps: 11 }]);
  assert.equal(notified, 2);

  unsubscribe();
  store.setRates([]);
  assert.equal(notified, 2);
});
