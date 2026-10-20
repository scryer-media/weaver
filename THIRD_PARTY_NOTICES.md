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

## Microsoft Edge WebView2

Official Windows builds of the desktop wrapper (`weaver-tray.exe`) statically
link the WebView2 loader library that Microsoft distributes with the WebView2
SDK, obtained through the `webview2-com-sys` crate. The loader locates and
starts the Microsoft Edge WebView2 Runtime already installed on the user's
machine; no part of the runtime itself is redistributed with Weaver.

The WebView2 loader and the WebView2 Runtime are Microsoft software governed
by Microsoft's own license terms, not by Weaver's license. The exact
`webview2-com-sys` version incorporated by a build is recorded in
`Cargo.lock`. The upstream package is available at
<https://crates.io/crates/webview2-com-sys>, and the SDK it repackages at
<https://developer.microsoft.com/microsoft-edge/webview2/>.

## rqbit / librqbit

`engines/weaver-torrent` is derived from the Rust crates of rqbit
(<https://github.com/ikatson/rqbit>), Copyright 2021 Igor Katson, licensed
under the Apache License, Version 2.0. The full licence text is in
`LICENSE-APACHE`. The code was imported from ikatson/rqbit commit
57e95d16c830863a1fde32d8fa98153b53a3d0b4 (2026-10-08) by way of the
scryer-media/rqbit fork at 78baa6ebee97ca088a729da647aa42d166741460, and has
been modified by the Weaver authors from 2026-10-20 onward; every imported
file carries a header saying so. The HTTP API, web UI, UPnP server, desktop
app and the rqbit binary were not imported. The modified work as a whole is
distributed under Weaver's licence (GPL-3.0-or-later), which is compatible
with Apache-2.0.
