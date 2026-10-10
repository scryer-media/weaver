// Campaigns beyond the bounded four-article matrix: wider format coverage,
// further repair and demotion axes, and deeper schedules over the core
// direct-store RAR layouts.
//
// Smoke selections here run in the default suite. The campaigns are opt-in
// and separate from the archive matrix proper; run them with:
// `cargo nextest run --profile archive-matrix-extended --run-ignored all --no-fail-fast`
use super::*;

mod depth;
mod formats;
mod repair;
mod tier2;
