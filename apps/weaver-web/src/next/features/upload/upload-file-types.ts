// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

export const NZB_UPLOAD_ACCEPT = ".nzb,.xz,.nzb.xz";
/** What the Add dialog's picker offers: NZBs and torrent files. */
export const ADD_DIALOG_ACCEPT = `${NZB_UPLOAD_ACCEPT},.torrent`;

export function isSupportedNzbUploadFilename(filename: string): boolean {
  const normalized = filename.toLowerCase();
  return normalized.endsWith(".nzb") || normalized.endsWith(".nzb.xz");
}
