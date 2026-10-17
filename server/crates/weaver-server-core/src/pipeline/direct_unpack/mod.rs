// Direct unpack: extracting archive members while their source is downloading.
//
// RAR uses incremental physical header prefixes and separate gated volume
// readers, preserving the dictionary across solid members and volume changes.
// Mixed direct-store sets use published virtual source ranges and extract only
// compressed members into private staging. Repair invalidates that speculative
// output; final consumption either installs its verified manifest or falls
// back to conventional extraction. Both paths share the existing chase worker
// and extraction memory limits.
//
// The 7z reader design is described below.
//
// Conventional extraction waits for every part file to land and then runs the
// decoder over them. Direct unpack starts the same decoder early and lets it
// chase the download, so unpacking overlaps the transfer instead of following
// it. For 7z, that needs a change to
// what it reads through: [`GatedSplitReader`] presents the set as one
// archive stream whose frontier is the download's verified watermark, parking
// rather than returning short or wrong bytes when the decoder gets ahead.
//
// Three pieces, and they are all this module is:
//
// - [`SetCoverage`] — how much of each part is committed, written by the
//   download path and waited on by the reader.
// - [`GatedSplitReader`] — the `Read + Seek` view built over that coverage.
// - [`StartHeader`] — the 32-byte signature header, which is where the
//   archive's exact total length comes from before any of it has arrived.
//
// # What the design rests on
//
// Correctness needs only the watermark invariant described on [`SetCoverage`]:
// bytes below a part's watermark are committed and verified, and they stay on
// disk. Because they stay, the reader serves arbitrary access patterns —
// backward seeks and re-reads included — and blocks only at the frontier.
//
// *How much* overlap direct unpack actually buys is a separate, empirical
// question, and it is the one the read-pattern tests answer. Measured across
// every codec chain the 7z writer can produce — store, LZMA, LZMA2, BZip2,
// Deflate, PPMd, Zstd, Brotli, LZ4, Delta and BCJ-x86 filter chains, AES256
// with and without header encryption, solid and non-solid — the pattern is
// uniform: at most one probe into the archive's tail for the end header, then
// a single ascending sweep of the payload. Nothing revisits packed bytes it
// has already read, so the whole payload can be chased at download speed.
//
// The one chain that cannot be measured here is BCJ2, which splits data across
// four pack streams read by concurrent cursors. sevenz-rust2 decodes BCJ2 but
// cannot encode it, so no fixture can be built in-process; whether its cursors
// force backward reads is still open, and it needs an externally-produced
// fixture to settle. It decodes correctly through this reader either way — the
// only question is how much overlap it gives up.

// # Known limitation: a pause discards the chase
//
// Pausing a job aborts its chases rather than suspending them, because a pause
// has no schedule and a parked chase holds a blocking thread for as long as it
// lasts. The set re-arms when a later part completes after the resume, so the
// cost is the decode done so far, not correctness. If pauses turn out to be
// common on jobs large enough to chase, the fix is a quiesce-and-resume
// barrier of the kind direct-store keeps, not a longer park.

pub mod coverage;
pub(crate) mod decode_memory;
pub(crate) mod rar;
pub(crate) mod rar_reader;
pub(crate) mod rar_virtual;
pub mod reader;
pub mod settings;
pub(crate) mod sevenz_runs;
pub mod start_header;
mod stream;
pub(crate) mod wiring;

pub use coverage::SetCoverage;
pub use reader::GatedSplitReader;
pub use settings::{DirectUnpackGate, DirectUnpackSettings};
pub use start_header::{StartHeader, StartHeaderError};
pub(crate) use stream::ChaseExpectedMembers;

#[cfg(test)]
mod gating_tests;
#[cfg(test)]
mod rar_install_tests;
#[cfg(test)]
mod read_pattern_tests;
