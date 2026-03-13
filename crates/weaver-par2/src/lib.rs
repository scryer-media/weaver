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

pub mod checksum;
pub mod disk;
pub mod error;
pub mod gf;
pub mod gf_simd;
pub mod matrix;
pub mod packet;
pub mod par2_set;
pub mod placement;
pub mod rename;
pub mod repair;
pub mod session;
pub mod types;
pub mod verify;

// Re-export key types for convenience.
pub use checksum::{FileHashState, SliceChecksumState};
pub use disk::{DiskFileAccess, MultiDirectoryFileAccess, PlacementFileAccess};
pub use error::{Par2Error, Result};
pub use gf::{add as gf_add, input_slice_constants, inv as gf_inv, mul as gf_mul, pow as gf_pow};
pub use gf_simd::mul_acc_region;
pub use matrix::{Matrix, build_decode_matrix};
pub use packet::{
    CreatorPacket, FileDescriptionPacket, IfscPacket, MainPacket, Packet, PacketHeader, PacketType,
    RecoverySlicePacket, parse_packet, scan_packets,
};
pub use par2_set::{
    FileDescription, MergeResult, Par2Diagnostic, Par2FileSet, Par2ParseResult, RecoverySlice,
};
pub use placement::{PlacementEntry, PlacementPlan, apply_placement_plan, scan_placement};
pub use rename::{
    MatchType, RenameSuggestion, SplitFileGroup, detect_split_files, identify_par2_files,
    scan_for_renames,
};
pub use repair::{
    RepairOptions, RepairPlan, execute_repair, execute_repair_with_options, plan_repair,
    prepare_recovery_buffers, reconstruct_and_write, xor_out_slice,
};
pub use session::VerificationSession;
pub use types::{CancellationToken, ProgressCallback, ProgressStage, ProgressUpdate};
pub use types::{FileId, RecoveryExponent, RecoverySetId, SliceChecksum, SliceIndex};
pub use verify::{
    FileAccess, FileStatus, FileVerification, MemoryFileAccess, Repairability, VerificationResult,
    VerifyOptions, quick_check_16k, verify_all, verify_all_with_options, verify_full_hash,
    verify_selected_file_ids, verify_selected_file_ids_with_options, verify_slices,
    verify_slices_from_crcs,
};
