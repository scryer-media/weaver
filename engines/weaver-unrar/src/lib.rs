//! `weaver-unrar` -- RAR archive reader and extractor.
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
//! - Detection and extraction of supported encrypted archives
//! - RAR4 archive support, including legacy RAR 1.5/2.0/2.9 decompression
//! - SFX (self-extracting) archive support
//! - AES decryption with UnRAR-compatible key derivation
//! - LZ decompression (methods 1-5) with Huffman decoding and sliding window
//! - PPMd decompression (variant H)
//! - Post-decompression filters (Delta, E8, E8E9, ARM)
//! - Path sanitization to prevent traversal attacks

// Scryer's supported extraction targets are Darwin/macOS, Linux/Unix, and
// Windows. Other RAR host OS values may be listed in metadata, but they do not
// need AWS-LC target support here.
#[cfg(not(all(
    any(target_arch = "x86_64", target_arch = "aarch64"),
    any(
        target_os = "macos",
        target_os = "linux",
        all(target_os = "windows", target_env = "msvc")
    )
)))]
compile_error!(
    "weaver-unrar AWS-LC crypto only supports x86_64/aarch64 on macOS, Linux, and Windows MSVC"
);

pub mod archive;
pub mod crypto;
pub mod decompress;
pub mod early;
pub mod error;
pub mod extract;
pub(crate) mod hash_pipeline;
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
    CachedArchiveHeaders, DataSegment, RarArchive, RarVolumeFacts, RarVolumeHostOs,
    RarVolumeMemberFacts, RarVolumeServiceFacts, RarVolumeUnixOwnerFacts, ReadSeek,
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
