// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useSyncExternalStore } from "react";

function subscribe(changed: () => void) {
  document.addEventListener("visibilitychange", changed);
  return () => document.removeEventListener("visibilitychange", changed);
}

const visible = () => document.visibilityState !== "hidden";

/** Whether the tab is shown: a hidden tab follows nothing, and reads again once shown. */
export function usePageVisible(): boolean {
  return useSyncExternalStore(subscribe, visible, () => true);
}
