use super::*;

const SET: &str = "payload.bin";

fn parts() -> (Vec<(String, Vec<u8>)>, Vec<u8>) {
    let (_, _, members) = fixture(SimpleArchiveKind::Gz);
    let payload = members.into_iter().next().unwrap().1;
    let parts = payload
        .chunks(1024 * 1024)
        .enumerate()
        .map(|(index, bytes)| (format!("{SET}.{:03}", index + 1), bytes.to_vec()))
        .collect();
    (parts, payload)
}

#[tokio::test]
async fn joins_parts_before_later_parts_download() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(43110);
    let (parts, payload) = parts();
    insert_stream(&mut pipeline, job, &parts).await;
    for number in 0..parts[0].1.len().div_ceil(ARTICLE) {
        land(
            &mut pipeline,
            NzbFileId {
                job_id: job,
                file_index: 0,
            },
            &parts[0].0,
            &parts[0].1,
            number,
        )
        .await;
    }
    let early = wait_for_output(
        &pipeline.direct_unpack_staging_dir(job, SET).join(SET),
        ARTICLE as u64,
    )
    .await;
    for (index, (name, bytes)) in parts.iter().enumerate().skip(1) {
        for number in (0..bytes.len().div_ceil(ARTICLE)).rev() {
            land(
                &mut pipeline,
                NzbFileId {
                    job_id: job,
                    file_index: index as u32,
                },
                name,
                bytes,
                number,
            )
            .await;
        }
    }
    finish(&mut pipeline, job, SET).await;
    assert!(
        early,
        "the first part should reach the joined output before later parts arrive"
    );
    assert_eq!(pipeline.direct_unpack.counters().consumed, 1);
    extracted(&mut pipeline, job, &[(SET.to_string(), payload)]).await;
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

#[tokio::test]
async fn par2_repairs_split_part_damage_and_missing_articles() {
    let (parts, payload) = parts();
    for missing in [false, true] {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job = JobId(43111);
        insert_stream(&mut pipeline, job, &parts).await;
        let described: Vec<_> = parts
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let par2 = build_repairable_par2_set_for_files(&described, ARTICLE as u64, 2);
        install_test_par2_runtime(&mut pipeline, job, par2.clone(), &[]);
        for (index, (name, original)) in parts.iter().enumerate() {
            let mut bytes = original.clone();
            if index == 1 && !missing {
                bytes[ARTICLE + 100] ^= 0x7f;
            }
            for number in 0..bytes.len().div_ceil(ARTICLE) {
                if missing && index == 1 && number == 1 {
                    continue;
                }
                land(
                    &mut pipeline,
                    NzbFileId {
                        job_id: job,
                        file_index: index as u32,
                    },
                    name,
                    &bytes,
                    number,
                )
                .await;
            }
        }
        let working = pipeline.jobs[&job].working_dir.clone();
        let mut options = par2_rs::Par2RepairerOptions::new(working.clone(), Vec::new());
        options.file_set = Some(par2);
        options.repair = false;
        let analysis = par2_rs::Par2Repairer::new(options.clone())
            .verify_or_repair()
            .unwrap();
        pipeline.decide_direct_unpack_before_repair(job, Some(&analysis.verification));
        options.repair = true;
        let repair = par2_rs::Par2Repairer::new(options)
            .verify_or_repair()
            .map_err(|e| e.to_string());
        pipeline.settle_direct_unpack_after_repair(job, true, &repair);
        finish(&mut pipeline, job, SET).await;
        assert!(repair.is_ok(), "{:?}", repair.as_ref().err());
        assert!(matches!(
            repair.unwrap().status,
            par2_rs::Par2RepairStatus::Repaired
        ));
        for (name, original) in &parts {
            assert_eq!(std::fs::read(working.join(name)).unwrap(), *original);
        }
        assert!(pipeline.direct_unpack.counters().armed > 0);
        pipeline
            .extract_simple_archive(job, SET, SimpleArchiveKind::Split)
            .await
            .unwrap();
        extracted(&mut pipeline, job, &[(SET.to_string(), payload.clone())]).await;
        assert_eq!(pipeline.write_buffered_bytes, 0);
    }
}
