//! Tail-metadata discovery under every bounded arrival/duplicate schedule.
use super::super::archive_schedules::{
    Interruption, arrival_orders, combined_campaign, combined_schedules, run_archive, run_schedule,
    schedules,
};
use super::*;

#[derive(Clone, Copy, Debug)]
enum Shape {
    Copy,
    Multiple,
    EmptyEntry,
    Nested,
    Lzma2Fallback,
    EncryptedCopy,
    EncryptedHeaders,
    EncryptedLzma2,
}

async fn campaign(shape: Shape, shard: Option<usize>) {
    let first = payload(13, 6001);
    let second = payload(29, 307);
    let name = if matches!(shape, Shape::Nested) {
        "nested/feature.mkv"
    } else {
        "feature.mkv"
    };
    let mut entries = vec![Entry::file(name, first.clone())];
    let mut expected = BTreeMap::from([(name, first)]);
    match shape {
        Shape::Multiple => {
            entries.push(Entry::file("second.nfo", second.clone()));
            expected.insert("second.nfo", second);
        }
        Shape::EmptyEntry => {
            entries.push(Entry::empty_file("empty.nfo"));
            expected.insert("empty.nfo", Vec::new());
        }
        _ => {}
    }
    let method = if matches!(shape, Shape::Lzma2Fallback | Shape::EncryptedLzma2) {
        EncoderMethod::LZMA2
    } else {
        EncoderMethod::COPY
    };
    let password = matches!(
        shape,
        Shape::EncryptedCopy | Shape::EncryptedHeaders | Shape::EncryptedLzma2
    )
    .then_some("moonlit-harbour");
    let archive = build_7z_shaped(
        &entries,
        method,
        password,
        matches!(shape, Shape::EncryptedHeaders),
    );
    let volumes = split_volumes(&archive, 2);
    let mut spec = sevenz_job_spec(&volumes, 2);
    spec.password = password.map(str::to_owned);
    let wanted = expected.keys().copied().collect::<Vec<_>>();
    if password.is_some() && shard.is_none() {
        for order in arrival_orders() {
            let mut wrong = spec.clone();
            wrong.password = Some("incorrect-key".to_string());
            let rejected =
                run_archive(DirectStoreGate::Enabled, wrong, &volumes, &order, &wanted).await;
            assert!(
                matches!(rejected.status, Some(JobStatus::Failed { .. })),
                "wrong password {shape:?} {order:?}: {:?}",
                rejected.status
            );
            assert_eq!(rejected.finalized, 0);
            assert!(
                rejected.files.values().all(Option::is_none),
                "wrong password published output: {shape:?} {order:?}"
            );
        }
    }
    let cases = shard.map_or_else(
        || schedules().into_iter().enumerate().collect(),
        combined_schedules,
    );
    for (case, (order, interruption)) in cases {
        eprintln!(
            "{shape:?} shard={shard:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_schedule(
            DirectStoreGate::Enabled,
            spec.clone(),
            &volumes,
            &order,
            &wanted,
            interruption,
        )
        .await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{shape:?} case={case} order={order:?} interruption={interruption:?}: {:?}",
            outcome.trace
        );
        if matches!(interruption, Interruption::None) {
            let expected = usize::from(matches!(
                shape,
                Shape::Copy | Shape::Multiple | Shape::EmptyEntry | Shape::Nested
            ));
            assert_eq!(
                outcome.finalized, expected,
                "{shape:?} case={case} order={order:?}: {:?}",
                outcome.trace
            );
        }
        for (name, bytes) in &expected {
            assert_eq!(
                outcome.files.get(*name).and_then(Option::as_deref),
                Some(bytes.as_slice()),
                "{shape:?} member={name} case={case} order={order:?}: {:?}",
                outcome.trace
            );
        }
    }
}

#[tokio::test]
async fn copy_arrival_schedules() {
    campaign(Shape::Copy, None).await;
}
#[tokio::test]
async fn multiple_member_arrival_schedules() {
    campaign(Shape::Multiple, None).await;
}
#[tokio::test]
async fn empty_entry_arrival_schedules() {
    campaign(Shape::EmptyEntry, None).await;
}
#[tokio::test]
async fn nested_member_arrival_schedules() {
    campaign(Shape::Nested, None).await;
}
#[tokio::test]
async fn compressed_fallback_arrival_schedules() {
    campaign(Shape::Lzma2Fallback, None).await;
}

#[tokio::test]
async fn encrypted_copy_schedules() {
    campaign(Shape::EncryptedCopy, None).await;
}
#[tokio::test]
async fn encrypted_header_schedules() {
    campaign(Shape::EncryptedHeaders, None).await;
}
#[tokio::test]
async fn encrypted_compressed_schedules() {
    campaign(Shape::EncryptedLzma2, None).await;
}

combined_campaign!(combined_copy, Shape::Copy, campaign);
combined_campaign!(combined_multiple, Shape::Multiple, campaign);
combined_campaign!(combined_empty_entry, Shape::EmptyEntry, campaign);
combined_campaign!(combined_nested, Shape::Nested, campaign);
combined_campaign!(combined_lzma2, Shape::Lzma2Fallback, campaign);
combined_campaign!(combined_encrypted_copy, Shape::EncryptedCopy, campaign);
combined_campaign!(combined_encrypted_header, Shape::EncryptedHeaders, campaign);
combined_campaign!(combined_encrypted_lzma2, Shape::EncryptedLzma2, campaign);
