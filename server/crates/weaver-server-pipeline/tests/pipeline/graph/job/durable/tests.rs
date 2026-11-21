// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::graph::job::passwords::Passwords;
use crate::pipeline::graph::root::tests::{file, job_with};

#[test]
fn kept_and_offered_sources_visit_their_own_flags_at_their_own_generations() {
    let (mut job, _dir, _) = job_with(&[file("member.bin", &[64])], Vec::new());
    let before = job.generations();
    job.writer
        .restore_kept(&KeptRecord {
            node: NodeId::ROOT,
            envelope: 7,
            runs: Vec::new(),
            closed: None,
            settled: false,
            file: None,
        })
        .unwrap();
    job.passwords = Passwords::new(vec!["candidate".to_string()]);
    assert_eq!(job.passwords.resume_at(NodeId::ROOT, 0), Some("candidate"));
    let now = job.generations();
    let collect = |at| {
        let mut visited = Vec::new();
        job.visit_touched_since(at, |node, flags| visited.push((node, flags)));
        visited
    };
    assert!(collect(now).is_empty());
    assert_eq!(
        collect(Generations {
            kept: before.kept,
            ..now
        }),
        vec![(
            NodeId::ROOT,
            Touched {
                kept: true,
                ..Touched::default()
            },
        )]
    );
    assert_eq!(
        collect(Generations {
            offered: before.offered,
            ..now
        }),
        vec![(
            NodeId::ROOT,
            Touched {
                own: true,
                ..Touched::default()
            },
        )]
    );
    assert_eq!(
        collect(Generations {
            kept: before.kept,
            offered: before.offered,
            ..now
        }),
        vec![
            (
                NodeId::ROOT,
                Touched {
                    kept: true,
                    ..Touched::default()
                }
            ),
            (
                NodeId::ROOT,
                Touched {
                    own: true,
                    ..Touched::default()
                }
            ),
        ]
    );
}
