# Third-Party Notices

This file records third-party notices that require prominent inclusion. It is
not an exhaustive inventory of dependency licenses.

## unrar-rs / RARLAB UnRAR

The Weaver source repository does not vendor unrar-rs source code. Official
builds incorporate unrar-rs in compiled form for reading and extracting RAR
archives. unrar-rs is a Rust port of RARLAB's reference UnRAR implementation.
It does not provide RAR archive creation or compression.

The following notice applies to the UnRAR-derived component:

UnRAR source code may be used in any software to handle
RAR archives without limitations free of charge, but cannot be
used to develop RAR (WinRAR) compatible archiver and to
re-create RAR compression algorithm, which is proprietary.
Distribution of modified UnRAR source code in separate form
or as a part of other software is permitted, provided that
full text of this paragraph, starting from "UnRAR source code"
words, is included in license, or in documentation if license
is not available, and in source code comments of resulting package.

The exact unrar-rs version incorporated by a build is recorded in `Cargo.lock`.
The upstream package is available at <https://crates.io/crates/unrar-rs>.
