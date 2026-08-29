import assert from "node:assert/strict";
import test from "node:test";
import {
  NZB_UPLOAD_ACCEPT,
  isSupportedNzbUploadFilename,
} from "../src/features/upload/upload-file-types.ts";

test("accepts NZB and XZ-compressed NZB filenames", () => {
  assert.equal(NZB_UPLOAD_ACCEPT, ".nzb,.xz,.nzb.xz");
  assert.equal(isSupportedNzbUploadFilename("release.nzb"), true);
  assert.equal(isSupportedNzbUploadFilename("release.NZB.XZ"), true);
});

test("rejects non-NZB upload filenames", () => {
  assert.equal(isSupportedNzbUploadFilename("release.tar.xz"), false);
  assert.equal(isSupportedNzbUploadFilename("release.zip"), false);
});
