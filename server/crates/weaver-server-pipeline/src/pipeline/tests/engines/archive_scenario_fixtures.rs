// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod download_summary {
    use super::super::harness::Outcome;
    use crate::jobs::download_summary::DownloadSummary;

    pub fn summary(outcome: &Outcome) -> DownloadSummary {
        assert!(
            outcome.completed(),
            "{:?} {:?}",
            outcome.status,
            outcome.failure
        );
        let text = outcome
            .download_summary
            .as_deref()
            .expect("the history row keeps a summary");
        DownloadSummary::from_storage_json(text).expect("a summary this build reads")
    }
}

pub mod embedded {
    use super::super::harness::{ARTICLE, Outcome, Post, payload};
    use sevenz_turbo::encoder_options::AesEncoderOptions;
    use sevenz_turbo::{
        ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password,
    };
    use std::io::Cursor;

    pub fn members() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("ember.mkv", payload(5 * ARTICLE + 11, 41)),
            ("quill.mkv", payload(3 * ARTICLE - 3, 42)),
        ]
    }

    // One stored container of `members()`, enciphered under `password` when
    // there is one.
    pub fn stored(password: Option<&str>) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        let mut methods = vec![EncoderConfiguration::new(EncoderMethod::COPY)];
        if let Some(password) = password {
            methods.insert(0, AesEncoderOptions::new(Password::new(password)).into());
        }
        writer.set_content_methods(methods);
        for (name, bytes) in members() {
            let mut entry = ArchiveEntry::new_file(name);
            entry.size = bytes.len() as u64;
            writer
                .push_archive_entry(entry, Some(Cursor::new(bytes)))
                .unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    pub fn delivered(name: &str) -> String {
        format!("lantern beacon/{name}")
    }

    // The volume with one byte of its member's data changed before it was
    // posted: every article arrives as posted, so no article is damaged and
    // only the record's own checks can find the byte.
    pub fn flipped(set: Vec<(String, Vec<u8>)>, body: &[u8]) -> Vec<Post> {
        set.into_iter()
            .map(|(name, mut bytes)| {
                let at = bytes
                    .windows(16)
                    .position(|window| window == &body[..16])
                    .expect("the member is stored in the volume");
                bytes[at + 2 * ARTICLE + 7] ^= 0x5a;
                Post::plain(&name, bytes)
            })
            .collect()
    }

    // The member is delivered whole, rebuilt by the record with nothing asked
    // for again.
    pub fn assert_record_rebuilt(graph: &Outcome, body: &[u8]) {
        assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
        assert_eq!(
            graph.files.get(&delivered("record.bin")),
            Some(&body.to_vec())
        );
        assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    }
}

pub mod pairings {
    pub async fn capped_summary(
        scenario: &super::super::harness::Scenario,
        cap: usize,
    ) -> crate::jobs::download_summary::DownloadSummary {
        use crate::pipeline::graph::summary::NODES_KEPT_HERE;
        let previous = NODES_KEPT_HERE.with(|kept| kept.replace(cap));
        let outcome = super::super::harness::run(scenario).await;
        NODES_KEPT_HERE.with(|kept| kept.set(previous));
        super::download_summary::summary(&outcome)
    }

    use super::super::harness::{ARTICLE, Post, Scenario, payload};
    use super::super::sevenz_fixtures::store::SECRET;
    use crate::pipeline::tests::direct_store::scenarios::{
        HeaderCheck, header_encrypted_store_set,
    };
    use sevenz_turbo::encoder_options::AesEncoderOptions;
    use sevenz_turbo::{
        ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password,
    };
    use std::io::Cursor;

    pub enum Entry<'a> {
        File(&'a str, &'a [u8]),
        Dir(&'a str),
    }

    pub fn copy() -> EncoderConfiguration {
        EncoderConfiguration::new(EncoderMethod::COPY)
    }

    // A 7z of `entries` coded by `method`, enciphered under `password` when
    // there is one, and its end header with it when `sealed`.
    pub fn sevenz(
        entries: &[Entry<'_>],
        method: EncoderConfiguration,
        password: Option<&str>,
        sealed: bool,
    ) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        let mut methods = vec![method];
        if let Some(password) = password {
            // Encryption applies to coded bytes, so it sits at the output end.
            methods.insert(0, AesEncoderOptions::new(Password::new(password)).into());
        }
        writer.set_content_methods(methods);
        writer.set_encrypt_header(sealed);
        for entry in entries {
            match entry {
                Entry::File(name, bytes) => writer
                    .push_archive_entry(
                        ArchiveEntry::new_file(name),
                        Some(Cursor::new(bytes.to_vec())),
                    )
                    .unwrap(),
                Entry::Dir(name) => writer
                    .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory(name), None)
                    .unwrap(),
            };
        }
        writer.finish().unwrap().into_inner()
    }

    pub fn posts(volumes: &[(String, Vec<u8>)]) -> Vec<Post> {
        volumes
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect()
    }

    pub fn nested_encrypted() -> Scenario {
        let iron = payload(4 * ARTICLE + 11, 77);
        let inner = sevenz(
            &[Entry::File("iron.bin", &iron)],
            copy(),
            Some(SECRET),
            false,
        );
        let volumes =
            header_encrypted_store_set("inner.7z", &inner, 1, SECRET, HeaderCheck::For(SECRET));
        Scenario::new(posts(&volumes)).password(SECRET)
    }
}

pub mod password {
    use super::super::harness::{ARTICLE, Outcome, Post, Scenario, payload};
    use super::super::rar_fixtures::delivered;
    use sha2::Digest;

    pub const MEMBER: &str = "beacon.bin";

    pub const SECRET: &str = "quill-ember-secret";

    pub const WRONG: &str = "quill-ember-wrong";

    pub fn body() -> Vec<u8> {
        payload(7 * ARTICLE + 13, 53)
    }

    pub fn wrong_then_right(posts: Vec<Post>, right: &str) -> Scenario {
        Scenario::new(posts)
            .global_password(WRONG)
            .global_password(right)
    }

    pub fn assert_whole(outcome: &Outcome, name: &str, sha256: &str, len: usize) {
        assert!(
            outcome.completed(),
            "{:?} {:?} {:?}",
            outcome.status,
            outcome.failure,
            outcome.graph
        );
        assert_eq!(
            outcome.files.keys().cloned().collect::<Vec<_>>(),
            vec![delivered(name)],
            "delivered names",
        );
        let bytes = &outcome.files[&delivered(name)];
        assert_eq!(bytes.len(), len, "delivered length");
        assert_eq!(
            hex::encode(sha2::Sha256::digest(bytes)),
            sha256,
            "delivered bytes {:?}",
            outcome.graph
        );
    }

    pub fn assert_body(outcome: &Outcome, body: &[u8]) {
        assert_whole(
            outcome,
            MEMBER,
            &hex::encode(sha2::Sha256::digest(body)),
            body.len(),
        );
    }
}

pub mod rar_rev {
    use super::super::harness::{Outcome, Post, Scenario, run};
    use super::super::rar_fixtures::{body, rar5_revs};
    use super::super::rar_fixtures::{delivered, posts};
    use crate::pipeline::tests::direct_store::single_member_store_set;
    use sha2::Digest;
    use weaver_graph::recovery::RepairOutcome;

    pub const MEMBER: &str = "beacon.bin";

    pub fn crc(bytes: &[u8]) -> u32 {
        par2_rs::checksum::crc32(bytes)
    }

    // The set's volumes but the ones at `absent`, then the recovery volumes.
    pub fn posted(volumes: Vec<(String, Vec<u8>)>, absent: &[usize], revs: Vec<Post>) -> Vec<Post> {
        let kept = volumes
            .into_iter()
            .enumerate()
            .filter(|(index, _)| !absent.contains(index))
            .map(|(_, volume)| volume)
            .collect();
        let mut out = posts(kept);
        out.extend(revs);
        out
    }

    pub fn rar5_set(absent: &[usize], revs: usize) -> (Vec<u8>, Scenario) {
        let body = body();
        let volumes = single_member_store_set(MEMBER, &body, 4);
        let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
        let revs = rar5_revs("silver.horizon", &bytes, revs);
        (body, Scenario::new(posted(volumes, absent, revs)))
    }

    pub async fn records_and_recovery_volumes_rebuild(scenario: Scenario, body: &[u8]) {
        let outcome = run(&scenario.damaged(0, 2)).await;
        assert!(
            outcome.completed(),
            "{:?} {:?}; rulings: {:?}",
            outcome.status,
            outcome.failure,
            outcome.graph().rulings,
        );
        assert_eq!(outcome.files.len(), 1);
        assert_eq!(outcome.files.values().next().unwrap(), body);
        for kind in ["rar_record", "rar_recovery"] {
            assert!(
                outcome
                    .graph()
                    .rulings
                    .iter()
                    .any(|(engine, result)| engine == kind
                        && matches!(result, Some(RepairOutcome::Repaired { .. }))),
                "{kind} did not repair: {:?}",
                outcome.graph().rulings,
            );
        }
        assert!(outcome.refetched().is_empty(), "{:?}", outcome.refetched());
    }

    // A RAR3 recovery volume as RAR writes it beside volumes whose end headers
    // reserve seven zero bytes for it (REVSPACE): the set's parity, as wide as
    // the widest volume, the footer in place of the parity of those zeros.
    pub fn rar3_rev(volumes: &[&[u8]]) -> Vec<u8> {
        const FOOTER: usize = 7;
        let width = volumes.iter().map(|volume| volume.len()).max().unwrap_or(0);
        let coder = reedsolomon_rs::rar3::Rar3RsCoder::new(1).expect("one recovery volume");
        let mut column = vec![0u8; volumes.len()];
        let mut parity = [0u8; 1];
        let mut rev = Vec::with_capacity(width);
        for position in 0..width - FOOTER {
            for (slot, volume) in column.iter_mut().zip(volumes) {
                *slot = volume.get(position).copied().unwrap_or(0);
            }
            coder.encode(&column, &mut parity);
            rev.push(parity[0]);
        }
        rev.extend_from_slice(&[(volumes.len() - 1) as u8, 0, 0]);
        let crc = crc(&rev);
        rev.extend_from_slice(&crc.to_le_bytes());
        rev
    }

    // One volume of the set rebuilt in one ruling, and the set's one member
    // delivered as RAR wrote it.
    pub fn assert_lz_four_rebuilt(graph: &Outcome) {
        assert_eq!(
            graph.graph().outcomes,
            vec![Some(RepairOutcome::Repaired { slices: 1 })]
        );
        assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
        let member = delivered("compressible.txt");
        assert_eq!(
            graph.files.keys().cloned().collect::<Vec<_>>(),
            vec![member.clone()]
        );
        let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weaver-server-core/tests/fixtures/extraction_profiles/expected.json"
        )))
        .unwrap();
        let expected = &oracle["archives"]["rar4_lz_four"]["compressible.txt"];
        assert_eq!(
            graph.files[&member].len() as u64,
            expected["size"].as_u64().unwrap()
        );
        assert_eq!(
            hex::encode(sha2::Sha256::digest(&graph.files[&member])),
            expected["sha256"].as_str().unwrap()
        );
    }
}

pub mod sets {
    pub fn namespace_move_counts() -> (u64, u64) {
        let tally = crate::pipeline::graph::namespace::TALLY.with(std::cell::Cell::get);
        (tally.moves, tally.waiting_opens)
    }

    // How many times, on this thread, the ledger named again a member that
    // has a file.
    pub fn namespace_rename_count() -> u64 {
        crate::pipeline::graph::namespace::TALLY
            .with(std::cell::Cell::get)
            .renamed
    }

    use super::super::harness::Outcome;
    use std::collections::BTreeMap;

    // Every job's files land in a folder named for the job.
    pub const FOLDER: &str = "lantern beacon";

    pub fn delivered<'a>(outcome: &'a Outcome, name: &str) -> Option<&'a [u8]> {
        outcome
            .files
            .get(&format!("{FOLDER}/{name}"))
            .map(Vec::as_slice)
    }

    // Under the real names with the posted bytes, and nothing else: the posted
    // names and the set's own files are gone from the output.
    pub fn delivered_as_described(outcome: &Outcome, files: &[(&str, Vec<u8>)]) {
        let expected: BTreeMap<String, &[u8]> = files
            .iter()
            .map(|(name, bytes)| (format!("{FOLDER}/{name}"), bytes.as_slice()))
            .collect();
        let got: BTreeMap<String, &[u8]> = outcome
            .files
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.as_slice()))
            .collect();
        assert!(got == expected, "{:?}", got.keys());
    }
}
