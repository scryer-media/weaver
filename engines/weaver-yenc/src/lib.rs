//! yEnc encoding and decoding for Usenet binary articles.
//!
//! This crate provides synchronous yEnc decode and encode functions.
//! All operations work on caller-provided buffers with no internal allocation
//! on the decode path. CRC32 is computed in a streaming fashion during decode.
//!
//! # Usage
//!
//! ```rust
//! use weaver_yenc::{CrcVerification, decode, encode, max_decoded_len};
//!
//! // Encode some data.
//! let data = b"Hello, World!";
//! let mut encoded = Vec::new();
//! encode(data, &mut encoded, 128, "hello.bin").unwrap();
//!
//! // Decode it back. Size the destination from the encoded length, never from
//! // `result.metadata.size` -- a poster may have omitted `size=` entirely.
//! let mut decoded = vec![0u8; max_decoded_len(encoded.len())];
//! let result = decode(&encoded, &mut decoded).unwrap();
//! assert_eq!(&decoded[..result.bytes_written], data.as_slice());
//! assert_eq!(result.crc_status, CrcVerification::Verified);
//! ```

pub mod crc;
pub mod decode;
pub mod encode;
pub mod error;
pub mod header;
pub mod segment;
pub mod simd;
pub mod types;

// Convenience re-exports.
pub use crc::{Crc32Combine, crc32_combine};
pub use decode::{
    DecodeOptions, DecodeState, DecodedArticle, RapidyencDecodeEnd, RapidyencDecodeProgress,
    RapidyencDecodeState, StreamingArticleDecoder, decode, decode_body,
    decode_body_chunk_until_control, decode_chunk, decode_nntp, decode_nntp_append,
    decode_rapidyenc, decode_rapidyenc_ex, decode_rapidyenc_incremental, decode_with_options,
    finish_streaming_article, finish_streaming_result, max_decoded_len,
};
pub use encode::{encode, encode_part};
pub use error::YencError;
pub use header::extract_filename_from_subject;
pub use segment::{
    CheckpointCollapseReason, CheckpointPlan, CheckpointPlanBuild, CheckpointPlanDegradation,
    MAX_CHECKPOINT_GRIDS, Segment, SegmentedCrc32, combine_contiguous,
};
pub use types::{CrcVerification, DecodeResult, YencHeaderDefects, YencMetadata};
