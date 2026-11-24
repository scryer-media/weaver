// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR2 completion under the graph engine: jobs whose sets verify, repair or
// come up short, each run in posting order and again restarted after every
// article, judged by what the job delivered. Every row lives under
// `schedules`, which runs only from the matrix profiles.

mod schedules;
