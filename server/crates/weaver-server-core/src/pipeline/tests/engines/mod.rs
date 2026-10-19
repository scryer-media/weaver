// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Acceptance under both engines: one generated job admitted twice, once
// under `Engine::Classic` and once under `Engine::Graph`, served the same
// articles, settled through the same completion gate, and compared.
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
//   recovery_blocks(post)       per recovery block of a volume, the
//                                 articles holding its bytes
//   one_block_article(post)       an article of a volume whose loss costs
//                                 exactly one recovery block
//   Scenario::new(posts)          a job of `posts`, every article intact
//     .damaged(file, ordinal)     that article arrives with a checksum
//                                 mismatch and corrupted bytes
//     .lost(file, ordinal)        every server lacks that article
//     .framed(size)               articles of `size` bytes, not ARTICLE
//     .backwards()                every batch the download queue hands
//                                 out arrives last article first
//     .password(secret)           the password the job is submitted with
//     .global_password(secret)    a password among the candidates every
//                                 job is offered, the job has none of its own
//   run_both(&scenario)           -> (classic Outcome, graph Outcome)
//   run(&scenario, engine)        -> Outcome, for divergence tests
//   try_run(&scenario, engine)    -> Result<Outcome, Stall>: Err when the job
//                                 stops moving before it finishes
//   Stall { engine, status,       the status it stalled at, and the
//           state }               pipeline's state then, for the message
//   assert_same(&classic, &graph) the same status, failure kind, delivered
//                                 files and bytes, claims, and article
//                                 requests; neither engine asked for an
//                                 article twice
//
//   Outcome {
//     engine, status,             the job's last status
//     failure,                    the failure message, when it failed
//     failure_kind,               the history row's failure kind
//     files,                      delivered path (under the complete
//                                 directory) to bytes
//     claims,                     post name to Claim, payload posts only,
//                                 read off what was delivered
//     requests,                   (file, ordinal) to times it was asked for
//     order,                      every (file, ordinal) asked, in order
//     reads,                      graph: member reads its repairs made;
//                                 classic: PAR2 verification passes that
//                                 read files back
//     graph: Option<GraphView>,   the graph job as last seen before it
//                                 finished: namespace claims by output
//                                 name, blocks reachable, repair
//                                 outcomes, and the delivery: the files
//                                 handed over whole, damaged, unrepaired
//   }
//   Outcome::refetched()          articles asked for more than once
//   Claim::{Complete, Damaged, Missing}
//
// Files are indexed as the posts are listed; ordinals count from zero.

mod caps;
mod depth;
mod divergence;
mod embedded;
mod harness;
mod lines;
mod nesting;
mod par3;
mod partial;
mod plain_par2;
mod rar;
mod rar_engine;
mod restart;
mod seek;
mod sets;
mod sevenz;
mod sfv;
mod streams;
mod zip;
