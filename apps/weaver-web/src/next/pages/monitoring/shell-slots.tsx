// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createContext, useContext, type ReactNode } from "react";
import { createPortal } from "react-dom";

/** The parts of the Monitoring shell a dashboard fills: title note, title controls, the row under the nav, the footer figure. */
export type ShellSlot = "note" | "controls" | "strip" | "status";

export type ShellHosts = Record<ShellSlot, HTMLElement | null>;

export const ShellHostsContext = createContext<ShellHosts | null>(null);

/**
 * Puts `children` into one part of the shell Monitoring keeps up while its
 * dashboards change, so a dashboard fills the shell rather than drawing its own.
 */
export function DashboardSlot({ slot, children }: { slot: ShellSlot; children: ReactNode }) {
  const host = useContext(ShellHostsContext)?.[slot] ?? null;
  return host ? createPortal(children, host) : null;
}
