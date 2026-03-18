//! yEnc encoding and decoding for Usenet binary articles.
//!
//! This crate provides synchronous yEnc decode and encode functions.
//! All operations work on caller-provided buffers with no internal allocation
//! on the decode path. CRC32 is computed in a streaming fashion during decode.
//!
//! # Usage
//!
//! ```rust
//! use weaver_yenc::{decode, encode};
//!
//! // Encode some data.
//! let data = b"Hello, World!";
//! let mut encoded = Vec::new();
//! encode(data, &mut encoded, 128, "hello.bin").unwrap();
//!
//! // Decode it back.
//! let mut decoded = vec![0u8; 1024];
//! let result = decode(&encoded, &mut decoded).unwrap();
//! assert_eq!(&decoded[..result.bytes_written], data.as_slice());
//! assert!(result.crc_valid);
//! ```

pub mod crc;
pub mod decode;
pub mod encode;
pub mod error;
pub mod header;
pub mod simd;
pub mod types;

// Convenience re-exports.
pub use decode::{
    DecodeOptions, DecodeState, decode, decode_body, decode_chunk, decode_nntp, decode_with_options,
};
pub use encode::{encode, encode_part};
pub use error::YencError;
pub use header::extract_filename_from_subject;
pub use types::{DecodeResult, YencMetadata};
