// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

fn main() {
    let sdl = weaver_server_api::export_schema_sdl();
    print!("{sdl}");
    if !sdl.ends_with('\n') {
        println!();
    }
}
