// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { createContext } from "react";

/**
 * Each queued job's time left by id, as the queue screen worked it out on its
 * latest render. Only the rows' time-left text reads it, so a new estimate
 * redraws that text and not the rows around it.
 */
export const QueueEtaContext = createContext<ReadonlyMap<number, string>>(new Map());
