use super::*;
use par3_rs::inside::{ContainerKind, ContainerLayout, ContainerLimits, InsertionPlan};
use std::io::{Cursor, Write};

fn archive(kind: ContainerKind) -> Vec<u8> {
    let payload = bytes(8192, 117);
    if kind == ContainerKind::SevenZip {
        use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        writer.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
        writer
            .push_archive_entry(
                ArchiveEntry::new_file("member.bin"),
                Some(Cursor::new(payload)),
            )
            .unwrap();
        return writer.finish().unwrap().into_inner();
    }
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(
            "member.bin",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
    writer.write_all(&payload).unwrap();
    let mut result = writer.finish().unwrap().into_inner();
    if kind == ContainerKind::Zip64 {
        // Force the ZIP64 end records without a multi-gigabyte test payload.
        // The central directory and member bytes remain those of ZipWriter.
        let at = result.len() - 22;
        let size = u32::from_le_bytes(result[at + 12..at + 16].try_into().unwrap()) as u64;
        let offset = u32::from_le_bytes(result[at + 16..at + 20].try_into().unwrap()) as u64;
        let mut footer = result.split_off(at);
        result.extend_from_slice(&0x06064b50u32.to_le_bytes());
        result.extend_from_slice(&44u64.to_le_bytes());
        result.extend_from_slice(&45u16.to_le_bytes());
        result.extend_from_slice(&45u16.to_le_bytes());
        result.extend_from_slice(&[0; 8]);
        result.extend_from_slice(&1u64.to_le_bytes());
        result.extend_from_slice(&1u64.to_le_bytes());
        result.extend_from_slice(&size.to_le_bytes());
        result.extend_from_slice(&offset.to_le_bytes());
        result.extend_from_slice(&0x07064b50u32.to_le_bytes());
        result.extend_from_slice(&0u32.to_le_bytes());
        result.extend_from_slice(&(at as u64).to_le_bytes());
        result.extend_from_slice(&1u32.to_le_bytes());
        footer[8..12].fill(0xff);
        footer[12..20].fill(0xff);
        result.extend_from_slice(&footer);
    }
    result
}

fn inside_campaign(kind: ContainerKind) {
    let root = tempfile::tempdir().unwrap();
    let original = archive(kind);
    let source = SourceId(0);
    let access = memory(source, &original, 1);
    let layout = ContainerLayout::inspect(
        access.as_ref(),
        source,
        &ExecutionOptions::default(),
        &ContainerLimits::default(),
    )
    .unwrap();
    assert_eq!(
        layout.kind(),
        kind,
        "the fixture must exercise the requested container"
    );
    let name = if kind == ContainerKind::SevenZip {
        "archive.7z"
    } else {
        "archive.zip"
    };
    let path = root.path().join(name);
    InsertionPlan::build(
        access,
        source,
        name,
        CreationOptions {
            block_size: 256,
            recovery_count: 8,
            execution: test_options(),
            ..CreationOptions::default()
        },
        &ContainerLimits::default(),
    )
    .unwrap()
    .execute(&path, root.path())
    .unwrap();
    let inserted = std::fs::read(&path).unwrap();
    assert_eq!(&inserted[..original.len()], original);
    // Framing, body, missing protected bytes, and protection-only holes are
    // independent faults. No parity is fabricated to make a repair pass.
    for fault in 0..6 {
        let mut damaged = inserted.clone();
        if fault == 1 {
            damaged[0] ^= 0x80;
        }
        if fault == 2 {
            damaged[100] ^= 0x80;
        }
        let len = damaged.len() as u64;
        let final_ranges = match fault {
            5 => vec![0..256, 4096..len],
            3 => vec![0..256, 512..len],
            4 => vec![
                0..original.len() as u64 + 17,
                original.len() as u64 + 33..len,
            ],
            _ => std::iter::once(0..len).collect(),
        };
        for tail_first in [false, true] {
            let output = tempfile::tempdir().unwrap();
            let case_path = output.path().join(name);
            std::fs::write(&case_path, &damaged).unwrap();
            let mut job = new_job();
            let early = if tail_first {
                std::iter::once(original.len() as u64..len).collect()
            } else {
                std::iter::once(0..256).collect()
            };
            for (step, ranges) in [early, final_ranges.clone(), final_ranges.clone()]
                .into_iter()
                .enumerate()
            {
                job.scan_embedded(
                    source,
                    case_path.clone(),
                    name.into(),
                    Some(ranges.clone()),
                    0,
                )
                .unwrap();
                job.assess().unwrap();
                let mut fresh = new_job();
                fresh
                    .scan_embedded(source, case_path.clone(), name.into(), Some(ranges), 0)
                    .unwrap();
                fresh.assess().unwrap();
                assert_eq!(
                    signature(&job),
                    signature(&fresh),
                    "kind={kind:?} fault={fault} tail_first={tail_first} step={step}"
                );
            }
            assert_eq!(job.sets.len(), 1, "kind={kind:?} fault={fault}");
            let (&id, set) = job.sets.first_key_value().unwrap();
            let status = set.view.as_ref().unwrap().status;
            if fault == 5 {
                assert_eq!(
                    status,
                    RepairStatus::NeedRecovery,
                    "insufficient embedded parity must refuse completion"
                );
                assert_eq!(
                    std::fs::read(&case_path).unwrap(),
                    damaged,
                    "refused repair cannot change its input"
                );
                continue;
            }
            if status == RepairStatus::Ready {
                job.repair(id, output.path()).unwrap();
                let restored = std::fs::read(output.path().join(name)).unwrap();
                assert_eq!(
                    &restored[..original.len()],
                    original,
                    "kind={kind:?} fault={fault}"
                );
            } else {
                assert_eq!(
                    status,
                    RepairStatus::Complete,
                    "kind={kind:?} fault={fault}"
                );
                assert!(
                    fault == 0 || fault == 4,
                    "protected damage must require repair"
                );
            }
        }
    }
}

#[test]
fn zip_inside_schedules() {
    inside_campaign(ContainerKind::Zip);
}
#[test]
fn zip64_inside_schedules() {
    inside_campaign(ContainerKind::Zip64);
}
#[test]
fn seven_zip_inside_schedules() {
    inside_campaign(ContainerKind::SevenZip);
}
