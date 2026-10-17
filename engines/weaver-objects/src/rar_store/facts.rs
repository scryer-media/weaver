// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR store keeps no facts yet: the trait's own body says so, and its
// restore refuses, so a restart fetches the set's bytes again. Once the
// store's lift lands, its facts are each volume's layout as far as its
// headers were read (the header bytes kept, where its members' pieces lie,
// its end record), the entries with their extents per volume and the links
// between their blocks, the solid flag, the recovery record's range, how
// nameless volumes were chained, and per encrypted member its salt, IV,
// KDF count and key tuple, never the key, the password or the KDF output.

use weaver_graph::{Facts, Object, RestoreError};

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    Err(RestoreError::Unsupported { kind: facts.kind })
}
