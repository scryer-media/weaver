//! `weaver-unrar` -- Pure Rust RAR archive reader and extractor.
//!
//! This crate provides reading, decompression, and extraction of existing RAR
//! archives only. It intentionally exposes no archive writer, builder, or
//! creation APIs. It supports:
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

#[cfg(all(
    feature = "native-crypto",
    not(any(
        all(target_arch = "x86_64", target_os = "macos"),
        all(target_arch = "aarch64", target_os = "macos"),
        all(target_arch = "x86_64", target_os = "linux", target_env = "musl"),
        all(target_arch = "aarch64", target_os = "linux", target_env = "musl"),
        all(target_arch = "x86_64", target_os = "windows", target_env = "msvc"),
        all(target_arch = "aarch64", target_os = "windows", target_env = "msvc")
    ))
))]
compile_error!(
    "weaver-unrar native-crypto only supports x86_64/aarch64 on macOS, Linux musl, and Windows MSVC"
);

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
pub mod recovery;
pub mod signature;
pub mod types;
pub mod vint;
pub mod volume;

// Re-export primary public API types
pub use archive::{
    CachedArchiveHeaders, DataSegment, RarArchive, RarVolumeFacts, RarVolumeMemberFacts,
    RarVolumeServiceFacts, ReadSeek,
};
pub use early::{EncryptionStatus, detect_encryption};
pub use error::{RarError, RarResult};
pub use extract::{ExtractOptions, ExtractedMember};
pub use limits::Limits;
pub use path::sanitize_path;
pub use probe::{ProbeFile, VolumeProbe, probe_volume};
pub use progress::{NoProgress, ProgressHandler};
pub use recovery::{RecoveryOptions, RecoveryReport, restore_volumes_from_paths};
pub use types::{
    ArchiveFormat, ArchiveMetadata, CompressionInfo, CompressionMethod, FileHash, HostOs,
    MemberInfo, TopologyMemberInfo, UnixOwnerInfo, VolumeSpan,
};
pub use volume::{StaticVolumeProvider, VolumeProvider, VolumeProviderError, VolumeSet};
