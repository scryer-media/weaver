// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Weaver's format objects. Every format-specific object lives here, one
// module per format, behind the graph's format-blind `Object` and `Recovery`
// faces; nothing outside a format's own module knows its fields. The graph
// asks for an object by kind through `build`.

pub mod crypt;
pub mod engine;
mod grid;
mod kept;
pub mod keys;
pub mod par2;
pub mod par3;
pub mod rar_engine;
pub mod sevenz_engine;
pub mod sevenz_store;
pub mod sfv;
pub mod stream;
pub mod tar;
pub mod zip;
pub mod zip_engine;

#[cfg(test)]
mod testing;

use weaver_graph::{Builder, Kind, Object, ObjectId};

// The object for a kind this crate has, or `None` for one it has not.
pub fn build(kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
    match kind {
        Kind::Par2 => Some(Box::new(par2::Par2::new(id))),
        Kind::Par3 => Some(Box::new(par3::Par3::new(id))),
        Kind::RarEngine => Some(Box::new(rar_engine::RarEngine::new())),
        Kind::SevenZipEngine => Some(Box::new(sevenz_engine::SevenZipEngine::new())),
        Kind::SevenZipStore => Some(Box::new(sevenz_store::SevenZipStore::new())),
        Kind::Sfv => Some(Box::new(sfv::Sfv::new(id))),
        Kind::Tar => Some(Box::new(tar::Tar::new())),
        Kind::Xz => Some(Box::new(stream::XzStream::new())),
        Kind::Gzip => Some(Box::new(stream::GzipStream::new())),
        Kind::Bzip2 => Some(Box::new(stream::Bzip2Stream::new())),
        Kind::Zstd => Some(Box::new(stream::ZstdStream::new())),
        Kind::Brotli => Some(Box::new(stream::BrotliStream::new())),
        Kind::Deflate => Some(Box::new(stream::DeflateStream::new())),
        Kind::Zip => Some(Box::new(zip::Zip::new())),
        Kind::ZipEngine => Some(Box::new(zip_engine::ZipEngine::new())),
        _ => None,
    }
}

// The graph's builder over `build`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Objects;

impl Builder for Objects {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        build(kind, id)
    }
}
