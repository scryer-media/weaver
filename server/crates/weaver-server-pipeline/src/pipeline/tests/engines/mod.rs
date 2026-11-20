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

#[cfg(test)]
mod abandon_cost;
#[cfg(test)]
mod accounting;
#[cfg(test)]
mod bound_recovery_health;
#[cfg(test)]
mod caps;
#[cfg(test)]
mod checkpoint_cost;
#[cfg(test)]
mod dead_posts;
#[cfg(test)]
mod deep;
pub mod depth;
#[cfg(test)]
mod disk_wait;
#[cfg(test)]
mod download_summary;
#[cfg(test)]
mod embedded;
#[cfg(test)]
mod encrypted_hex;
#[cfg(test)]
mod extractor_claims;
#[cfg(test)]
mod first_article_sample;
pub mod harness;
pub mod rar_fixtures;
pub mod restart_fixtures;
mod state;

#[cfg(test)]
mod hold_breach;
#[cfg(test)]
mod lent_handed;
#[cfg(test)]
mod lent_windows;
#[cfg(test)]
mod lifecycle;
#[cfg(test)]
mod lines;
#[cfg(test)]
pub(in crate::pipeline) mod loans;
#[cfg(test)]
mod nameless_reversed;
#[cfg(test)]
mod pairings;
#[cfg(test)]
mod par3;
#[cfg(test)]
mod password;
#[cfg(test)]
mod plain_par2;
#[cfg(test)]
mod post_health_seats;
#[cfg(test)]
mod rar;
#[cfg(test)]
mod rar_rev;
#[cfg(test)]
mod recovery_audit;
#[cfg(test)]
mod recovery_damage_wire;
#[cfg(test)]
mod recovery_duplicate_wire;
#[cfg(test)]
mod recovery_encrypted_wire;
#[cfg(test)]
mod recovery_geometry_wire;
#[cfg(test)]
mod recovery_restart_wire;
#[cfg(test)]
mod refused_then_repaired;
#[cfg(test)]
mod remaining_par2;
#[cfg(test)]
mod remaining_stalls;
#[cfg(test)]
mod repaired_source;
#[cfg(test)]
mod reposted;
#[cfg(test)]
pub(in crate::pipeline::tests) mod restart;
#[cfg(test)]
mod seek;
#[cfg(test)]
mod sets;
#[cfg(test)]
mod settled_rulings;
#[cfg(test)]
mod sevenz;
pub mod sevenz_fixtures;
#[cfg(test)]
mod sevenz_hex;
pub mod sfv;
#[cfg(test)]
mod solid_hex;
#[cfg(test)]
mod spent;
pub mod stream_fixtures;
#[cfg(test)]
use stream_fixtures as streams;
#[cfg(test)]
mod too_large;
pub mod zip;
