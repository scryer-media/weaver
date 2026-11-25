// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import assert from "node:assert/strict";
import test from "node:test";
import { waitReasonDetail } from "../src/next/data/wait-reason.ts";
import { englishTranslate as t } from "./english-translate.ts";

test("a download waiting on a server refusing the login names the server", () => {
  assert.equal(
    waitReasonDetail(t, "login_refused:news.example.com:563"),
    "Waiting: login refused by news.example.com:563",
  );
  assert.equal(
    waitReasonDetail(t, "login_refused:a.example.com:563, b.example.com:119"),
    "Waiting: login refused by a.example.com:563, b.example.com:119",
  );
  assert.equal(
    waitReasonDetail(t, "login_refused:[2001:db8::1]:563"),
    "Waiting: login refused by [2001:db8::1]:563",
  );
});

test("the other wait reasons read as before", () => {
  assert.equal(waitReasonDetail(t, "disk_space"), "Low disk space");
  assert.equal(waitReasonDetail(t, "nzb_slot"), "Waiting for an NZB slot");
  // A reason that is no code is the daemon's own sentence, shown as it came.
  assert.equal(
    waitReasonDetail(t, "NNTP authentication blocked"),
    "NNTP authentication blocked",
  );
});
