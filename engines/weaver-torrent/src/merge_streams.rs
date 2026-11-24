// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use futures::stream::Stream;

pub fn merge_streams<
    I,
    S1: Stream<Item = I> + 'static + Unpin,
    S2: Stream<Item = I> + 'static + Unpin,
>(
    s1: S1,
    s2: S2,
) -> impl Stream<Item = I> + Unpin + 'static {
    use tokio_stream::StreamExt;
    s1.merge(s2)
}
