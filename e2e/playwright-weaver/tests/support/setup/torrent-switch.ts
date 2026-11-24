// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { APIRequestContext } from "@playwright/test";

import { expect, graphql } from "../../helpers";

type TorrentSwitch = { enabled: boolean; upnp: boolean };

async function readTorrentSwitch(request: APIRequestContext): Promise<TorrentSwitch> {
  const data = await graphql<{ settings: { torrent: TorrentSwitch } }>(
    request,
    "query WeaverE2ETorrentSwitchRead { settings { torrent { enabled upnp } } }",
  );
  return data.settings.torrent;
}

async function saveTorrentSwitch(request: APIRequestContext, input: TorrentSwitch): Promise<void> {
  const data = await graphql<{ updateSettings: { torrent: TorrentSwitch } }>(
    request,
    `mutation WeaverE2ETorrentSwitch($input: GeneralSettingsInput!) {
      updateSettings(input: $input) { torrent { enabled upnp } }
    }`,
    { input: { torrent: input } },
  );
  expect(data.updateSettings.torrent).toEqual(input);
}

/**
 * Run `body` with torrents on, as the torrent lane turns them on (UPnP off),
 * and put the switch back as it was however `body` ends, so later tests see
 * the starting state.
 */
export async function withTorrentsOn(request: APIRequestContext, body: () => Promise<void>): Promise<void> {
  const before = await readTorrentSwitch(request);
  await saveTorrentSwitch(request, { enabled: true, upnp: false });
  try {
    await body();
  } finally {
    await saveTorrentSwitch(request, before);
  }
}
