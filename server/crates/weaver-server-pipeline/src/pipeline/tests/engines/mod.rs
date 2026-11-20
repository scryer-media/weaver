// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Acceptance through the graph engine: one generated job admitted, served
// its articles, settled through the completion gate, and judged by what it
// delivered.
//
// The harness API (everything a scenario needs; the body need not be read):
//
//   ARTICLE                       one article's decoded size, and the PAR2
//                                 slice size, so an article is one slice
//   payload(len, seed)            bytes no format sniffs as its own
//   Post::plain(name, bytes)      a posted file; its role comes from its name
//   par2_set(base, files, blocks) an index and one volume of `blocks`
//                                 recovery blocks over `files`, sliced at
//                                 ARTICLE, named `<base>.par2` and
//                                 `<base>.volNN+MM.par2`
//   par3_set(base, files, blocks) a PAR3 index and its volumes of `blocks`
//                                 recovery blocks over `files`, blocks of
//                                 ARTICLE, named as the creator names them
//   recovery_blocks(post)         per recovery block of a volume, the
//                                 articles holding its bytes
//   one_block_article(post)       an article of a volume whose loss costs
//                                 exactly one recovery block
//   Scenario::new(posts)          a job of `posts`, every article intact
//     .damaged(file, ordinal)     that article arrives with a checksum
//                                 mismatch and corrupted bytes
//     .lost(file, ordinal)        every server lacks that article
//     .corrupt_on_primary(file,   the primary serves that article damaged
//          ordinal)               and a backup serves it whole
//     .headerless_on(file,        that server serves the article with no
//          ordinal, server)       `=ybegin` line
//     .servers(n)                 the pipeline has at least `n` servers
//     .framed(size)               articles of `size` bytes, not ARTICLE
//     .backwards()                every batch the download queue hands
//                                 out arrives last article first
//     .uuencoded()                every article is posted uuencoded: no
//                                 position of its own, placed in order
//     .password(secret)           the password the job is submitted with
//     .global_password(secret)    a password among the candidates every
//                                 job is offered, the job has none of its own
//   run(&scenario)                -> Outcome
//   try_run(&scenario)            -> Result<Outcome, Stall>: Err when the job
//                                 stops moving before it finishes
//   Stall { status, state }       the status it stalled at, and the
//                                 pipeline's state then, for the message
//   admit(&scenario)              a fresh pipeline with the job admitted,
//                                 for a row that drives the job itself
//
//   Outcome {
//     status,                     the job's last status
//     failure,                    the failure message, when it failed
//     failure_kind,               the history row's failure kind
//     files,                      delivered path (under the complete
//                                 directory) to bytes
//     claims,                     post name to Claim, payload posts only,
//                                 read off what was delivered
//     requests,                   (file, ordinal) to times it was asked for
//     order,                      every (file, ordinal) asked, in order
//     reads,                      member reads its repairs made
//     graph: GraphView,           the graph job as last seen before it
//                                 finished: namespace claims by output
//                                 name, blocks reachable, repair
//                                 outcomes, and the delivery: the files
//                                 handed over whole, damaged, unrepaired
//   }
//   Outcome::refetched()          articles asked for more than once
//   Claim::{Complete, Damaged, Missing}
//
// Files are indexed as the posts are listed; ordinals count from zero.

pub mod accounting_fixture;
mod actor_drive;
pub mod archive_scenario_fixtures;
pub mod caps;
pub mod depth;
pub mod disk_archive;
pub mod disk_move_fixture;
pub mod disk_reserve;
pub mod feed_fixtures;
pub mod harness;
pub mod rar_fixtures;
pub mod recovery_actor;
pub mod recovery_fixtures;
pub mod restart_fixtures;
pub mod restart_move_fixture;
pub mod restart_regressions;
mod state;

pub mod large_window_fixtures;
pub mod lent_windows;
pub mod lifecycle_fixture;
pub mod loans;
pub mod post_health_seats;
pub mod rar_actor;
pub mod recovery_duplicate_wire;
pub mod recovery_geometry_wire;
pub mod recovery_restart_wire;
pub mod refused_then_repaired;
pub mod remaining_par2;
pub mod remaining_stalls;
pub mod repaired_source;
pub mod seek;
pub mod send_back_fixtures;
pub mod sevenz_fixtures;
pub mod sfv;
pub mod spent_fixtures;
pub mod stream_fixtures;
pub mod wire_recovery;
pub mod zip;
