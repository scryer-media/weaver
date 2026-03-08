//! `weaver-rar` -- Pure Rust RAR5 archive parser and extractor.
//!
//! This crate provides reading and extraction of RAR5 archives. It supports:
//! - Parsing all 5 RAR5 header types (main, file, service, encryption, end)
//! - Variable-length integer (vint) decoding
//! - Header CRC32 validation
//! - Metadata extraction (header-only mode)
//! - Store (method 0) extraction with CRC32 verification
//! - Multi-volume topology tracking
//! - Detection of encrypted archives (returns clear error)
//! - RAR4 archive support (header parsing, store extraction)
//! - SFX (self-extracting) archive support
//! - AES-256-CBC decryption with PBKDF2 key derivation
//! - LZ decompression (methods 1-5) with Huffman decoding and sliding window
//! - PPMd decompression (variant H)
//! - Post-decompression filters (Delta, E8, E8E9, ARM)
//! - Path sanitization to prevent traversal attacks

pub mod archive;
pub mod crypto;
pub mod decompress;
pub mod early;
pub mod error;
pub mod extract;
pub mod header;
pub mod limits;
pub mod path;
pub mod probe;
pub mod progress;
pub mod rar4;
pub mod signature;
pub mod types;
pub mod vint;
pub mod volume;

// Re-export primary public API types
pub use archive::{DataSegment, RarArchive, ReadSeek};
pub use error::{RarError, RarResult};
pub use extract::ExtractOptions;
pub use progress::{NoProgress, ProgressHandler};
pub use types::{
    ArchiveFormat, ArchiveMetadata, CompressionInfo, CompressionMethod, FileHash, HostOs,
    MemberInfo, VolumeSpan,
};
pub use limits::Limits;
pub use probe::{VolumeProbe, ProbeFile, probe_volume};
pub use early::{EncryptionStatus, detect_encryption};
pub use volume::{
    VolumeSet, VolumeProvider, VolumeProviderError,
    StaticVolumeProvider, WaitingVolumeProvider,
};
