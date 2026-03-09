use thiserror::Error;

/// Errors that can occur during PAR2 parsing, verification, and repair.
#[derive(Debug, Error)]
pub enum Par2Error {
    // --- Parsing errors ---
    #[error("invalid magic sequence at offset {offset}")]
    InvalidMagic { offset: u64 },

    #[error("packet too short: expected at least {expected} bytes, got {actual}")]
    PacketTooShort { expected: u64, actual: u64 },

    #[error("packet length {length} not a multiple of 4")]
    InvalidPacketLength { length: u64 },

    #[error("packet hash mismatch at offset {offset}")]
    PacketHashMismatch { offset: u64 },

    #[error("invalid main packet: {reason}")]
    InvalidMainPacket { reason: String },

    #[error("invalid file description packet: {reason}")]
    InvalidFileDescPacket { reason: String },

    #[error("invalid IFSC packet: {reason}")]
    InvalidIfscPacket { reason: String },

    #[error("invalid recovery slice packet: {reason}")]
    InvalidRecoveryPacket { reason: String },

    #[error("duplicate main packet with different recovery set ID")]
    ConflictingRecoverySet,

    #[error("no main packet found in PAR2 file set")]
    NoMainPacket,

    // --- Verification errors ---
    #[error("file not found: {filename}")]
    FileNotFound { filename: String },

    #[error("file {filename}: {damaged_slices} of {total_slices} slices damaged")]
    FileDamaged {
        filename: String,
        damaged_slices: u32,
        total_slices: u32,
    },

    // --- Repair errors ---
    #[error(
        "insufficient recovery data: need {needed} blocks, have {available} (deficit: {deficit})"
    )]
    InsufficientRecoveryData {
        needed: u32,
        available: u32,
        deficit: u32,
    },

    #[error("reed-solomon decode failed: {reason}")]
    ReedSolomonError { reason: String },

    #[error("repair target write failed for {filename} at offset {offset}: {source}")]
    RepairWriteFailed {
        filename: String,
        offset: u64,
        source: std::io::Error,
    },

    // --- Cancellation ---
    #[error("operation cancelled")]
    Cancelled,

    // --- I/O ---
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Par2Error>;
