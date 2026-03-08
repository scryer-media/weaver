//! PAR2 verification and repair engine for the Weaver Usenet downloader.
//!
//! This crate implements parsing and verification of PAR2 (Parity Archive Volume Set
//! v2.0) files. It supports:
//!
//! - Parsing all PAR2 packet types (Main, File Description, IFSC, Recovery Slice, Creator)
//! - Validating packet headers (magic, MD5 hash, length alignment)
//! - Aggregating packets from multiple .par2 files into a unified set
//! - Slice-level verification using CRC32 + MD5 from IFSC packets
//! - 16KB quick-check hash for fast file identification
//! - Full-file MD5 verification
//! - Streaming checksum computation
//! - Graceful handling of malformed/truncated packets (scan for next valid packet)

pub mod error;
pub mod types;
pub mod packet;
pub mod par2_set;
pub mod checksum;
pub mod verify;
pub mod disk;
pub mod session;
pub mod gf;
pub mod matrix;
pub mod repair;
pub mod rename;

// Re-export key types for convenience.
pub use error::{Par2Error, Result};
pub use types::{FileId, RecoverySetId, SliceChecksum, SliceIndex, RecoveryExponent};
pub use packet::{
    Packet, PacketHeader, PacketType,
    MainPacket, FileDescriptionPacket, IfscPacket, RecoverySlicePacket, CreatorPacket,
    scan_packets, parse_packet,
};
pub use par2_set::{Par2FileSet, FileDescription, RecoverySlice, MergeResult, Par2Diagnostic, Par2ParseResult};
pub use checksum::{SliceChecksumState, FileHashState};
pub use types::{CancellationToken, ProgressUpdate, ProgressStage, ProgressCallback};
pub use verify::{
    FileAccess, MemoryFileAccess, FileStatus, FileVerification,
    Repairability, VerificationResult, VerifyOptions,
    quick_check_16k, verify_full_hash, verify_slices, verify_slices_from_crcs,
    verify_all, verify_all_with_options,
};
pub use gf::{add as gf_add, mul as gf_mul, pow as gf_pow, inv as gf_inv, input_slice_constants};
pub use matrix::{Matrix, build_decode_matrix};
pub use disk::{DiskFileAccess, MultiDirectoryFileAccess};
pub use session::VerificationSession;
pub use repair::{RepairPlan, RepairOptions, plan_repair, execute_repair, execute_repair_with_options, prepare_recovery_buffers, xor_out_slice, reconstruct_and_write};
pub use rename::{
    RenameSuggestion, MatchType, scan_for_renames, identify_par2_files,
    SplitFileGroup, detect_split_files,
};
