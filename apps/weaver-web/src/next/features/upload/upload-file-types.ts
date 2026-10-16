// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export const NZB_UPLOAD_ACCEPT = ".nzb,.xz,.nzb.xz";

export function isSupportedNzbUploadFilename(filename: string): boolean {
  const normalized = filename.toLowerCase();
  return normalized.endsWith(".nzb") || normalized.endsWith(".nzb.xz");
}
