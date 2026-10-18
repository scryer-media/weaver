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
pub mod rar_store;
mod runs;
pub mod sevenz_engine;
pub mod sevenz_store;
pub mod sfv;
pub mod stream;
pub mod tar;
pub mod zip;
pub mod zip_engine;

#[cfg(test)]
mod testing;

use weaver_graph::{Builder, Durable, Facts, Kind, Object, ObjectId, Restore, RestoreError};

// The object for a kind this crate has, or `None` for one it has not.
pub fn build(kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
    match kind {
        Kind::Par2 => Some(Box::new(par2::Par2::new(id))),
        Kind::Par3 => Some(Box::new(par3::Par3::new(id))),
        Kind::RarEngine => Some(Box::new(rar_engine::RarEngine::new())),
        Kind::RarStore => Some(Box::new(rar_store::RarStore::new())),
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
    }
}

// The object of the kind a blob names, at the state the blob describes.
pub fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    fn back<T: Durable + Object + 'static>(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        Ok(Box::new(facts.read::<T>(facts.kind)?))
    }
    match facts.kind {
        Kind::Par2 => par2::facts::restore(facts),
        Kind::Par3 => par3::facts::restore(facts),
        Kind::RarStore => rar_store::facts::restore(facts),
        Kind::RarEngine => back::<rar_engine::RarEngine>(facts),
        Kind::SevenZipEngine => back::<sevenz_engine::SevenZipEngine>(facts),
        Kind::SevenZipStore => back::<sevenz_store::SevenZipStore>(facts),
        Kind::Sfv => back::<sfv::Sfv>(facts),
        Kind::Tar => back::<tar::Tar>(facts),
        Kind::Xz => back::<stream::XzStream>(facts),
        Kind::Gzip => back::<stream::GzipStream>(facts),
        Kind::Bzip2 => back::<stream::Bzip2Stream>(facts),
        Kind::Zstd => back::<stream::ZstdStream>(facts),
        Kind::Brotli => back::<stream::BrotliStream>(facts),
        Kind::Deflate => back::<stream::DeflateStream>(facts),
        Kind::Zip => back::<zip::Zip>(facts),
        Kind::ZipEngine => back::<zip_engine::ZipEngine>(facts),
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

impl Restore for Objects {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        restore(facts)
    }
}
