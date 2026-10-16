// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { nntpBodyMetrics } from "../../helpers";

export async function nntpBodyTransferCount(): Promise<number> {
  return (await nntpBodyMetrics()).body_transfers;
}
