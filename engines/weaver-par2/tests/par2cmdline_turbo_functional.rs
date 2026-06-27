#![cfg(feature = "slow-tests")]

#[path = "support/par2cmdline_turbo_support.rs"]
mod support;

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use support::{
    Rng, SyntheticPar2, assert_file_matches, assert_repaired_or_verified, assert_verified,
    build_synthetic_par2, extract_fixture, run_repair, run_verify, temp_case_dir,
};
use weaver_par2::{Par2Error, Par2RepairStatus, Par2Repairer, Par2RepairerOptions};

fn run_synthetic_repair_with_skip_leeway(
    base_dir: &Path,
    synthetic: &SyntheticPar2,
    skip_leeway: u64,
) -> weaver_par2::Par2RepairOutcome {
    let mut options = Par2RepairerOptions::new(base_dir.to_path_buf(), Vec::new());
    options.file_set = Some(synthetic.par2_set.clone());
    options.repair = true;
    options.scan_skip_data = true;
    options.scan_skip_leeway = skip_leeway;
    Par2Repairer::new(options)
        .verify_or_repair()
        .expect("run synthetic par2 repairer")
}

fn split_synthetic_packet_stream(stream: &[u8]) -> Vec<&[u8]> {
    let mut packets = Vec::new();
    let mut offset = 0usize;
    while offset < stream.len() {
        assert!(
            stream.len() - offset >= 64,
            "synthetic packet stream ended mid-header"
        );
        assert_eq!(&stream[offset..offset + 8], b"PAR2\0PKT");
        let length =
            u64::from_le_bytes(stream[offset + 8..offset + 16].try_into().unwrap()) as usize;
        assert!(length >= 64, "synthetic packet length was too short");
        assert!(
            offset
                .checked_add(length)
                .is_some_and(|end| end <= stream.len()),
            "synthetic packet length exceeded stream"
        );
        packets.push(&stream[offset..offset + length]);
        offset += length;
    }
    packets
}

#[test]
fn upstream_test2_verify_par2_flatdata() {
    let temp = temp_case_dir("test2");
    extract_fixture("flatdata.tar.gz", temp.path());
    extract_fixture("flatdata-par2files.tar.gz", temp.path());

    let outcome = run_verify(temp.path(), temp.path().join("testdata.par2"));
    assert_verified(&outcome, "test2");
}

#[test]
fn upstream_test4_repair_two_missing_files() {
    let temp = temp_case_dir("test4");
    extract_fixture("flatdata.tar.gz", temp.path());
    extract_fixture("flatdata-par2files.tar.gz", temp.path());

    for name in ["test-1.data", "test-3.data"] {
        let path = temp.path().join(name);
        fs::copy(&path, temp.path().join(format!("{name}.orig"))).expect("copy original");
        fs::remove_file(&path).expect("remove source file");
    }

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test4");
    assert_file_matches(
        temp.path().join("test-1.data"),
        temp.path().join("test-1.data.orig"),
        "test4 test-1.data",
    );
    assert_file_matches(
        temp.path().join("test-3.data"),
        temp.path().join("test-3.data.orig"),
        "test4 test-3.data",
    );
}

#[test]
fn upstream_test5_repair_full_multifile_loss() {
    let temp = temp_case_dir("test5");
    let mut rng = Rng::new(5_005_005);
    let synthetic = build_synthetic_par2(&[192, 381, 572, 763, 954], 192, 16, &mut rng);

    for file in &synthetic.files {
        fs::write(temp.path().join(&file.filename), &file.data).expect("write source file");
        fs::write(
            temp.path().join(format!("{}.orig", file.filename)),
            &file.data,
        )
        .expect("write reference file");
        fs::remove_file(temp.path().join(&file.filename)).expect("remove source file");
    }

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    options.file_set = Some(synthetic.par2_set);
    let outcome = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("test5 full loss repair should succeed");
    assert_eq!(outcome.status, Par2RepairStatus::Repaired);

    for file in &synthetic.files {
        assert_file_matches(
            temp.path().join(&file.filename),
            temp.path().join(format!("{}.orig", file.filename)),
            &format!("test5 repaired {}", file.filename),
        );
    }
}

#[test]
fn upstream_test11_repairs_repeated_damage_from_same_recovery_set() {
    let temp = temp_case_dir("test11");
    let mut rng = Rng::new(11_011_011);
    let sizes = vec![64usize; 100];
    let synthetic = build_synthetic_par2(&sizes, 64, 5, &mut rng);

    for file in &synthetic.files {
        fs::write(temp.path().join(&file.filename), &file.data).expect("write source file");
    }

    for index in [0usize, 1, 3] {
        fs::remove_file(temp.path().join(&synthetic.files[index].filename))
            .expect("remove first-round source file");
    }

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    options.file_set = Some(synthetic.par2_set.clone());
    let first = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("test11 first repair should succeed");
    assert_eq!(first.status, Par2RepairStatus::Repaired);
    for file in &synthetic.files {
        assert_eq!(
            fs::read(temp.path().join(&file.filename)).expect("read first repaired file"),
            file.data,
            "test11 first repair {}",
            file.filename
        );
    }

    fs::remove_file(temp.path().join(&synthetic.files[5].filename))
        .expect("remove second-round source file");

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    options.file_set = Some(synthetic.par2_set);
    let second = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("test11 second repair should succeed");
    assert_eq!(second.status, Par2RepairStatus::Repaired);
    for file in &synthetic.files {
        assert_eq!(
            fs::read(temp.path().join(&file.filename)).expect("read second repaired file"),
            file.data,
            "test11 second repair {}",
            file.filename
        );
    }
}

#[test]
fn upstream_test6_repair_subdir_unix_paths() {
    let temp = temp_case_dir("test6");
    extract_fixture("subdirdata.tar.gz", temp.path());
    extract_fixture("subdirdata-par2files-unix.tar.gz", temp.path());

    for rel in ["subdir1/test-2.data", "subdir2/test-7.data"] {
        let path = temp.path().join(rel);
        fs::copy(&path, temp.path().join(format!("{rel}.orig"))).expect("copy original");
        fs::remove_file(&path).expect("remove source file");
    }

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test6");
    assert_file_matches(
        temp.path().join("subdir1/test-2.data"),
        temp.path().join("subdir1/test-2.data.orig"),
        "test6 subdir1/test-2.data",
    );
    assert_file_matches(
        temp.path().join("subdir2/test-7.data"),
        temp.path().join("subdir2/test-7.data.orig"),
        "test6 subdir2/test-7.data",
    );
}

#[test]
fn upstream_test7_repair_subdir_windows_paths() {
    let temp = temp_case_dir("test7");
    extract_fixture("subdirdata.tar.gz", temp.path());
    extract_fixture("subdirdata-par2files-win.tar.gz", temp.path());

    for rel in ["subdir1/test-2.data", "subdir2/test-7.data"] {
        let path = temp.path().join(rel);
        fs::copy(&path, temp.path().join(format!("{rel}.orig"))).expect("copy original");
        fs::remove_file(&path).expect("remove source file");
    }

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test7");
    assert_file_matches(
        temp.path().join("subdir1/test-2.data"),
        temp.path().join("subdir1/test-2.data.orig"),
        "test7 subdir1/test-2.data",
    );
    assert_file_matches(
        temp.path().join("subdir2/test-7.data"),
        temp.path().join("subdir2/test-7.data.orig"),
        "test7 subdir2/test-7.data",
    );
}

#[test]
fn upstream_test9_repair_after_renames() {
    let temp = temp_case_dir("test9");
    extract_fixture("flatdata.tar.gz", temp.path());
    extract_fixture("flatdata-par2files.tar.gz", temp.path());

    let renames = [
        ("test-1.data", "rename4"),
        ("test-2.data", "rename5"),
        ("test-3.data", "rename6"),
        ("test-4.data", "rename7"),
        ("test-5.data", "rename9"),
        ("test-6.data", "rename8"),
        ("test-7.data", "rename3"),
        ("test-8.data", "rename1"),
        ("test-9.data", "rename2"),
    ];
    for (from, to) in renames {
        fs::rename(temp.path().join(from), temp.path().join(to)).expect("rename file");
    }

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test9");

    for name in [
        "test-1.data",
        "test-2.data",
        "test-3.data",
        "test-4.data",
        "test-5.data",
        "test-6.data",
        "test-7.data",
        "test-8.data",
        "test-9.data",
    ] {
        assert!(
            temp.path().join(name).is_file(),
            "test9: expected renamed file {name} to be restored"
        );
    }
}

#[test]
fn upstream_test10_repair_deleted_subdir() {
    let temp = temp_case_dir("test10");
    extract_fixture("smallsubdirdata.tar.gz", temp.path());
    extract_fixture("smallsubdirdata-par2files.tar.gz", temp.path());

    fs::copy(
        temp.path().join("subdir1/test-0.data"),
        temp.path().join("test-0.data.orig"),
    )
    .expect("copy original");
    fs::remove_dir_all(temp.path().join("subdir1")).expect("remove subdir1");

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test10");
    assert_file_matches(
        temp.path().join("subdir1/test-0.data"),
        temp.path().join("test-0.data.orig"),
        "test10 subdir1/test-0.data",
    );
}

#[test]
fn upstream_test12_repair_truncated_file() {
    let temp = temp_case_dir("test12");
    extract_fixture("readbeyondeof.tar.gz", temp.path());

    fs::rename(
        temp.path().join("test.data"),
        temp.path().join("test.data-correct"),
    )
    .expect("move correct file aside");
    let source = fs::read(temp.path().join("test.data-correct")).expect("read correct file");
    fs::write(temp.path().join("test.data"), &source[..113_579]).expect("write truncated copy");

    let outcome = run_repair(temp.path(), temp.path().join("test.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test12");
    assert_file_matches(
        temp.path().join("test.data"),
        temp.path().join("test.data-correct"),
        "test12 repaired file",
    );
}

#[test]
fn upstream_test13_repair_file_truncated_at_end() {
    let temp = temp_case_dir("test13");
    extract_fixture("test13-generated.tar.gz", temp.path());

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test13");
    assert_file_matches(
        temp.path().join("test-1.data"),
        temp.path().join("test-1.data-correct"),
        "test13 repaired file",
    );
}

#[test]
fn upstream_test14_repair_file_truncated_at_beginning() {
    let temp = temp_case_dir("test14");
    extract_fixture("test14-generated.tar.gz", temp.path());

    let outcome = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test14");
    assert_file_matches(
        temp.path().join("test-1.data"),
        temp.path().join("test-1.data-correct"),
        "test14 repaired file",
    );
}

#[test]
fn upstream_test15_issue_35_fixture_repairs() {
    let temp = temp_case_dir("test15");
    extract_fixture("par2-0.6.8-crash.tar.gz", temp.path());

    let base = temp.path().join("par2-0.6.8-crash");
    let outcome = run_repair(
        &base,
        base.join("pack-ea5f7f848340980493ed39f5b7173d956c680e43.par2"),
        &[],
    );
    assert_repaired_or_verified(&outcome, "test15");
}

#[test]
fn upstream_test17_repair_removed_subdir_structure() {
    let temp = temp_case_dir("test17");
    extract_fixture("bug44.tar.gz", temp.path());
    fs::remove_dir_all(temp.path().join("subdir1")).expect("remove subdir1");

    let outcome = run_repair(temp.path(), temp.path().join("recovery.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test17");
}

#[test]
fn upstream_test18_repair_single_file_archive() {
    let temp = temp_case_dir("test18");
    extract_fixture("test18-generated.tar.gz", temp.path());

    let outcome = run_repair(temp.path(), temp.path().join("recovery.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test18");
    assert_file_matches(
        temp.path().join("flatdata.tar.gz"),
        temp.path().join("flatdata.tar.gz-correct"),
        "test18 repaired archive",
    );
}

#[test]
fn upstream_test19_skip_leeway_boundary() {
    let temp = temp_case_dir("test19");
    let mut rng = Rng::new(873_945_932);
    let synthetic = build_synthetic_par2(&[1024], 200, 1, &mut rng);
    let file = synthetic.files.first().expect("synthetic file");
    let path = temp.path().join(&file.filename);

    fs::write(&path, &file.data[100..]).expect("write shifted data file");

    let too_narrow = run_synthetic_repair_with_skip_leeway(temp.path(), &synthetic, 99);
    assert_eq!(
        too_narrow.status,
        Par2RepairStatus::Insufficient,
        "test19: leeway 99 should skip over every shifted block boundary"
    );
    assert!(
        too_narrow.verification.total_missing_blocks > 1,
        "test19: leeway 99 should leave more damage than one recovery block can repair"
    );

    let repairable = run_synthetic_repair_with_skip_leeway(temp.path(), &synthetic, 100);
    assert_eq!(
        repairable.status,
        Par2RepairStatus::Repaired,
        "test19: leeway 100 should scan the shifted block boundaries"
    );
    assert_eq!(
        fs::read(&path).expect("read repaired data file"),
        file.data,
        "test19: repaired bytes differed from original"
    );
}

#[test]
fn upstream_test20_repair_from_split_fragments() {
    let temp = temp_case_dir("test20");
    extract_fixture("test20-generated.tar.gz", temp.path());

    let extras = [
        temp.path().join("myfile.dat.001"),
        temp.path().join("myfile.dat.002"),
    ];
    let outcome = run_repair(temp.path(), temp.path().join("recovery.par2"), &extras);
    assert_repaired_or_verified(&outcome, "test20");
    assert_file_matches(
        temp.path().join("myfile.dat"),
        temp.path().join("myfile.dat-correct"),
        "test20 repaired file",
    );
}

#[test]
fn upstream_test23_verify_faraway_base_dir() {
    let temp = temp_case_dir("test23");
    let faraway = temp.path().join("in/a/folder/far/far/away");
    extract_fixture("flatdata.tar.gz", &faraway);
    extract_fixture("flatdata-par2files.tar.gz", &faraway);

    let outcome = run_verify(&faraway, faraway.join("testdata.par2"));
    assert_verified(&outcome, "test23");
}

#[test]
fn upstream_test24_repair_faraway_base_dir() {
    let temp = temp_case_dir("test24");
    let faraway = temp.path().join("in/a/folder/far/far/away");
    extract_fixture("flatdata.tar.gz", &faraway);
    extract_fixture("flatdata-par2files.tar.gz", &faraway);

    for name in ["test-1.data", "test-3.data"] {
        let path = faraway.join(name);
        fs::copy(&path, faraway.join(format!("{name}.orig"))).expect("copy original");
        fs::remove_file(&path).expect("remove source file");
    }

    let outcome = run_repair(&faraway, faraway.join("testdata.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test24");
    assert_file_matches(
        faraway.join("test-1.data"),
        faraway.join("test-1.data.orig"),
        "test24 test-1.data",
    );
    assert_file_matches(
        faraway.join("test-3.data"),
        faraway.join("test-3.data.orig"),
        "test24 test-3.data",
    );
}

#[test]
fn upstream_test25_repairs_when_primary_input_is_source_file() {
    let temp = temp_case_dir("test25");
    let mut rng = Rng::new(25_025_025);
    let synthetic = build_synthetic_par2(&[128], 64, 2, &mut rng);
    let file = &synthetic.files[0];
    let source = temp.path().join(&file.filename);
    let par2 = temp.path().join(format!("{}.par2", file.filename));

    fs::write(&source, &file.data).expect("write source file");
    fs::write(&par2, &synthetic.packet_bytes).expect("write sibling par2 file");
    let mut damaged = file.data.clone();
    damaged[0] ^= 0x5a;
    fs::write(&source, &damaged).expect("damage source file");

    let outcome = run_repair(temp.path(), &source, &[]);
    assert_eq!(outcome.status, Par2RepairStatus::Repaired);
    assert_eq!(
        fs::read(&source).expect("read repaired source file"),
        file.data,
        "test25 repaired source file differed"
    );
}

#[test]
fn upstream_repair_purge_removes_backups_and_loaded_par2_files() {
    let temp = temp_case_dir("purge");
    let mut rng = Rng::new(25_125_125);
    let synthetic = build_synthetic_par2(&[128], 64, 2, &mut rng);
    let file = &synthetic.files[0];
    let source = temp.path().join(&file.filename);
    let par2 = temp.path().join(format!("{}.par2", file.filename));

    fs::write(&source, &file.data).expect("write source file");
    fs::write(&par2, &synthetic.packet_bytes).expect("write sibling par2 file");
    let mut damaged = file.data.clone();
    damaged[0] ^= 0xc3;
    fs::write(&source, &damaged).expect("damage source file");

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), vec![source.clone()]);
    options.purge = true;
    let outcome = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("purge repair should succeed");

    assert_eq!(outcome.status, Par2RepairStatus::Repaired);
    assert_eq!(
        fs::read(&source).expect("read repaired source file"),
        file.data,
        "purge repaired source file differed"
    );
    assert!(
        !par2.exists(),
        "purge should remove the loaded sibling par2 file"
    );
    assert!(
        !temp.path().join(format!("{}.1", file.filename)).exists(),
        "purge should remove the numbered backup"
    );
}

#[test]
fn upstream_repair_purge_keeps_extra_par2_marker_packet_inputs() {
    let temp = temp_case_dir("purge-extra-par2-marker");
    let mut rng = Rng::new(25_175_175);
    let synthetic = build_synthetic_par2(&[128], 64, 1, &mut rng);
    let file = &synthetic.files[0];
    let source = temp.path().join(&file.filename);
    let par2 = temp.path().join(format!("{}.par2", file.filename));
    let extra_recovery = temp.path().join(format!("{}.par2.bak", file.filename));
    let packets = split_synthetic_packet_stream(&synthetic.packet_bytes);

    fs::write(&source, &file.data).expect("write source file");
    fs::write(&par2, packets[..3].concat()).expect("write primary par2 file");
    fs::write(&extra_recovery, packets[3..].concat()).expect("write extra recovery packet file");
    let mut damaged = file.data.clone();
    damaged[0] ^= 0x6d;
    fs::write(&source, &damaged).expect("damage source file");

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), vec![source.clone()]);
    options.extra_paths.push(extra_recovery.clone());
    options.purge = true;
    let outcome = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("purge repair with extra PAR marker packets should succeed");

    assert_eq!(outcome.status, Par2RepairStatus::Repaired);
    assert_eq!(
        fs::read(&source).expect("read repaired source file"),
        file.data,
        "purge extra PAR marker repaired source file differed"
    );
    assert!(
        !par2.exists(),
        "purge should remove the primary sibling par2 file"
    );
    assert!(
        extra_recovery.exists(),
        "purge should keep command-line extra PAR marker packet inputs"
    );
    assert!(
        !temp.path().join(format!("{}.1", file.filename)).exists(),
        "purge should still remove the numbered backup"
    );
}

#[test]
fn upstream_verify_purge_removes_all_loaded_par2_files() {
    let temp = temp_case_dir("verify-purge");
    let mut rng = Rng::new(25_225_225);
    let synthetic = build_synthetic_par2(&[128], 64, 2, &mut rng);
    let file = &synthetic.files[0];
    let source = temp.path().join(&file.filename);
    let par2 = temp.path().join(format!("{}.par2", file.filename));
    let volume = temp.path().join(format!("{}.vol0+2.par2", file.filename));
    let packets = split_synthetic_packet_stream(&synthetic.packet_bytes);

    fs::write(&source, &file.data).expect("write source file");
    fs::write(&par2, packets[..3].concat()).expect("write primary par2 file");
    fs::write(&volume, packets[3..].concat()).expect("write recovery volume file");

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), vec![source.clone()]);
    options.repair = false;
    options.purge = true;
    let outcome = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("verify purge should succeed");

    assert_eq!(outcome.status, Par2RepairStatus::Verified);
    assert_eq!(
        fs::read(&source).expect("read verified source file"),
        file.data,
        "verify purge should leave the verified source file intact"
    );
    assert!(
        !par2.exists(),
        "verify purge should remove the primary par2"
    );
    assert!(
        !volume.exists(),
        "verify purge should remove the loaded recovery volume"
    );
}

#[test]
fn upstream_test26_repairs_source_file_inside_subfolder() {
    let temp = temp_case_dir("test26");
    let work = temp.path().join("subfolder");
    fs::create_dir_all(&work).expect("create subfolder");
    let mut rng = Rng::new(26_026_026);
    let synthetic = build_synthetic_par2(&[128], 64, 2, &mut rng);
    let file = &synthetic.files[0];
    let source = work.join(&file.filename);
    let par2 = work.join(format!("{}.par2", file.filename));

    fs::write(&source, &file.data).expect("write subfolder source file");
    fs::write(&par2, &synthetic.packet_bytes).expect("write subfolder sibling par2 file");
    let mut damaged = file.data.clone();
    damaged[0] ^= 0xa5;
    fs::write(&source, &damaged).expect("damage subfolder source file");

    let outcome = run_repair(&work, &source, &[]);
    assert_eq!(outcome.status, Par2RepairStatus::Repaired);
    assert_eq!(
        fs::read(&source).expect("read repaired subfolder source file"),
        file.data,
        "test26 repaired subfolder source file differed"
    );
}

#[cfg(unix)]
#[test]
fn upstream_test27_repairs_source_file_inside_symlinked_directory() {
    let temp = temp_case_dir("test27");
    let real_work = temp.path().join("folder/subfolder");
    let link_work = temp.path().join("tmp");
    fs::create_dir_all(&real_work).expect("create real subfolder");
    std::os::unix::fs::symlink(&real_work, &link_work).expect("create directory symlink");

    let mut rng = Rng::new(27_027_027);
    let synthetic = build_synthetic_par2(&[128], 64, 2, &mut rng);
    let file = &synthetic.files[0];
    let source = link_work.join(&file.filename);
    let par2 = link_work.join(format!("{}.par2", file.filename));

    fs::write(&source, &file.data).expect("write symlinked source file");
    fs::write(&par2, &synthetic.packet_bytes).expect("write symlinked sibling par2 file");
    let mut damaged = file.data.clone();
    damaged[0] ^= 0x33;
    fs::write(&source, &damaged).expect("damage symlinked source file");

    let outcome = run_repair(&link_work, &source, &[]);
    assert_eq!(outcome.status, Par2RepairStatus::Repaired);
    assert_eq!(
        fs::read(&source).expect("read repaired symlinked source file"),
        file.data,
        "test27 repaired symlinked source file differed"
    );
}

#[test]
fn upstream_test29_repair_issue_190_bitflip() {
    let temp = temp_case_dir("test29");
    extract_fixture("test29-generated.tar.gz", temp.path());

    let expected = fs::read(temp.path().join("9MBones_crc_ok_orig")).expect("read reference file");
    fs::remove_file(temp.path().join("9MBones_crc_ok_orig")).expect("remove reference file");
    fs::remove_file(temp.path().join("9MBones_crc_ok_bad")).expect("remove extra damaged copy");

    let outcome = run_repair(temp.path(), temp.path().join("9MBones_crc_ok.par2"), &[]);
    assert_repaired_or_verified(&outcome, "test29");
    assert_eq!(
        fs::read(temp.path().join("9MBones_crc_ok")).expect("read repaired file"),
        expected,
        "test29 repaired file bytes differed from the reference copy",
    );
}

#[test]
fn upstream_test30_zero_byte_extra_file_does_not_break_repair() {
    let temp = temp_case_dir("test30");
    extract_fixture("flatdata.tar.gz", temp.path());
    extract_fixture("bug128-parfiles.tar.gz", temp.path());

    fs::write(temp.path().join("test-a.data"), []).expect("create zero-byte file");

    let verify = run_verify(temp.path(), temp.path().join("recovery.par2"));
    assert_verified(&verify, "test30 verify");

    fs::remove_file(temp.path().join("test-a.data")).expect("remove zero-byte file");
    let repair = run_repair(temp.path(), temp.path().join("recovery.par2"), &[]);
    assert_repaired_or_verified(&repair, "test30 repair");
}

#[test]
fn upstream_test33_renamed_files_repair_cleanly() {
    let temp = temp_case_dir("test33");
    extract_fixture("flatdata.tar.gz", temp.path());
    extract_fixture("flatdata-par2files.tar.gz", temp.path());

    let originals = ["test-0.data", "test-1.data", "test-2.data"].map(|name| {
        (
            name,
            fs::read(temp.path().join(name)).expect("read original"),
        )
    });

    fs::rename(
        temp.path().join("test-0.data"),
        temp.path().join("renamed-file-a.data"),
    )
    .expect("rename file a");
    fs::rename(
        temp.path().join("test-1.data"),
        temp.path().join("renamed-file-b.data"),
    )
    .expect("rename file b");
    fs::rename(
        temp.path().join("test-2.data"),
        temp.path().join("renamed-file-c.data"),
    )
    .expect("rename file c");

    let pre = run_verify(temp.path(), temp.path().join("testdata.par2"));
    let renamed = pre
        .verification
        .files
        .iter()
        .filter(|file| matches!(file.status, weaver_par2::FileStatus::Renamed(_)))
        .count();
    assert!(
        pre.verification.total_missing_blocks > 0 || renamed > 0,
        "test33: pre-repair verify should detect either missing or renamed files"
    );

    let renamed_paths = [
        "renamed-file-a.data",
        "renamed-file-b.data",
        "renamed-file-c.data",
    ]
    .map(|name| temp.path().join(name));
    let repair = run_repair(
        temp.path(),
        temp.path().join("testdata.par2"),
        &renamed_paths,
    );
    assert_repaired_or_verified(&repair, "test33");
    for (name, original) in originals {
        let repaired = fs::read(temp.path().join(name)).expect("read repaired file");
        assert_eq!(repaired, original, "test33 repaired file {name}");
    }
    for name in renamed_paths {
        assert!(
            !name.exists(),
            "test33 should consume complete renamed source {}",
            name.display()
        );
    }
}

#[test]
fn upstream_test34_damaged_renamed_candidate_does_not_confuse_repair() {
    let temp = temp_case_dir("test34");
    extract_fixture("flatdata.tar.gz", temp.path());
    extract_fixture("flatdata-par2files.tar.gz", temp.path());

    fs::copy(
        temp.path().join("test-0.data"),
        temp.path().join("test-0.data.orig"),
    )
    .expect("copy original");
    fs::remove_file(temp.path().join("test-0.data")).expect("remove original");
    fs::copy(
        temp.path().join("test-0.data.orig"),
        temp.path().join("renamed-damaged.data"),
    )
    .expect("copy damaged candidate");

    let mut file = OpenOptions::new()
        .write(true)
        .open(temp.path().join("renamed-damaged.data"))
        .expect("open damaged candidate");
    file.seek(SeekFrom::Start(0))
        .expect("seek damaged candidate");
    file.write_all(&[0u8; 100])
        .expect("overwrite damaged prefix");

    let repair = run_repair(temp.path(), temp.path().join("testdata.par2"), &[]);
    assert_repaired_or_verified(&repair, "test34");
    assert_file_matches(
        temp.path().join("test-0.data"),
        temp.path().join("test-0.data.orig"),
        "test34 repaired file",
    );
}

#[test]
fn upstream_test34_rename_only_ignores_damaged_candidate_blocks() {
    let temp = temp_case_dir("test34-rename-only");
    let mut rng = Rng::new(34_034_034);
    let synthetic = build_synthetic_par2(&[128], 64, 1, &mut rng);
    let file = &synthetic.files[0];
    let damaged = temp.path().join("renamed-damaged.data");

    let mut damaged_data = file.data.clone();
    damaged_data[80] ^= 0x7f;
    fs::write(&damaged, &damaged_data).expect("write damaged renamed candidate");

    let mut normal_options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    normal_options.file_set = Some(synthetic.par2_set.clone());
    normal_options.extra_paths = vec![damaged.clone()];
    let normal = Par2Repairer::new(normal_options)
        .verify_or_repair()
        .expect("test34 normal repair should use intact blocks from damaged candidate");
    assert_eq!(normal.status, Par2RepairStatus::Repaired);
    assert_eq!(
        fs::read(temp.path().join(&file.filename)).expect("read normal repaired file"),
        file.data,
        "test34 normal repaired file"
    );

    let temp = temp_case_dir("test34-rename-only");
    let damaged = temp.path().join("renamed-damaged.data");
    fs::write(&damaged, &damaged_data).expect("write damaged renamed candidate");
    let mut rename_only_options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    rename_only_options.file_set = Some(synthetic.par2_set);
    rename_only_options.extra_paths = vec![damaged];
    rename_only_options.rename_only = true;
    let rename_only = Par2Repairer::new(rename_only_options)
        .verify_or_repair()
        .expect("test34 rename-only repair should return an outcome");
    assert_eq!(rename_only.status, Par2RepairStatus::Insufficient);
    assert!(
        !temp.path().join(&file.filename).exists(),
        "test34 rename-only should not reconstruct from partial renamed data"
    );
}

#[cfg(unix)]
#[test]
fn upstream_test35_repairs_existing_primary_symlink_without_following_target() {
    let temp = temp_case_dir("test35");
    let original = b"01234567890abcdef".to_vec();
    let mut rng = Rng::new(35_035_035);
    let synthetic = build_synthetic_par2(&[original.len()], 64, 1, &mut rng);
    let original = synthetic.files[0].data.clone();
    let file_id = synthetic.files[0].file_id;

    let target = temp.path().join("test_file");
    let link = temp.path().join("test_link");
    fs::write(&target, &original).expect("write symlink target");
    std::os::unix::fs::symlink(&target, &link).expect("create primary symlink");

    let mut damaged = original.clone();
    damaged.push(b'!');
    fs::write(&target, &damaged).expect("damage symlink target");

    let mut set = synthetic.par2_set;
    let desc = set
        .files
        .get_mut(&file_id)
        .expect("synthetic file description");
    desc.filename = "test_link".to_owned();
    desc.par2_name = "test_link".to_owned();

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    options.file_set = Some(set);
    let outcome = Par2Repairer::new(options)
        .verify_or_repair()
        .expect("test35 existing symlink repair should succeed");

    assert_eq!(outcome.status, Par2RepairStatus::Repaired);
    assert_eq!(fs::read(&link).expect("read repaired link path"), original);
    assert!(
        !link
            .symlink_metadata()
            .expect("link path metadata")
            .file_type()
            .is_symlink(),
        "test35 should replace the symlink itself with a repaired regular file"
    );
    assert_eq!(
        fs::read(&target).expect("read original symlink target"),
        damaged,
        "test35 should not write through the original symlink target"
    );
    assert!(
        temp.path()
            .join("test_link.1")
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_symlink()),
        "test35 should keep the original symlink as the numbered backup"
    );
}

#[test]
fn upstream_test42_rejects_oversized_source_block_counts() {
    let temp = temp_case_dir("test42");
    let mut rng = Rng::new(42_424_242);
    let mut synthetic = build_synthetic_par2(&[4, 4], 4, 1, &mut rng);
    for (file, length) in synthetic
        .files
        .iter()
        .zip([0x8000_0000u64 * 4, 0x8000_0001u64 * 4])
    {
        synthetic
            .par2_set
            .files
            .get_mut(&file.file_id)
            .expect("synthetic file description")
            .length = length;
    }

    let mut options = Par2RepairerOptions::new(temp.path().to_path_buf(), Vec::new());
    options.file_set = Some(synthetic.par2_set);
    let error = Par2Repairer::new(options)
        .verify_or_repair()
        .expect_err("test42: oversized source block counts should be rejected");

    match error {
        Par2Error::ResourceLimitExceeded { reason } => {
            assert!(
                reason.contains("max is") || reason.contains("addressable PAR2 slices"),
                "test42: unexpected resource-limit reason: {reason}"
            );
        }
        other => panic!("test42: expected ResourceLimitExceeded, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn upstream_test43_rejects_repair_through_dangling_symlink() {
    let temp = temp_case_dir("test43");
    let outside = temp.path().join("outside");
    let work = temp.path().join("work");
    let target_rel = ".config/autostart/poc.desktop";
    let target = work.join(target_rel);
    let escaped = outside.join("escaped-by-repair");
    fs::create_dir_all(target.parent().expect("target parent")).expect("create target parent");
    fs::create_dir_all(&outside).expect("create outside dir");

    let mut rng = Rng::new(43_043_043);
    let mut synthetic =
        build_synthetic_par2(&[b"symlink escape file body\n".len()], 64, 1, &mut rng);
    let file_id = synthetic.files[0].file_id;
    let original_filename = synthetic.files[0].filename.clone();
    let desc = synthetic
        .par2_set
        .files
        .get_mut(&file_id)
        .expect("synthetic file description");
    desc.filename = target_rel.to_owned();
    desc.par2_name = target_rel.to_owned();

    std::os::unix::fs::symlink(&escaped, &target).expect("create dangling symlink target");

    let mut options = Par2RepairerOptions::new(work, Vec::new());
    options.file_set = Some(synthetic.par2_set);
    let error = Par2Repairer::new(options)
        .verify_or_repair()
        .expect_err("test43: dangling symlink repair target should be rejected");

    match error {
        Par2Error::Io(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(
                error.to_string().contains("symbolic link"),
                "test43: unexpected symlink rejection error: {error}"
            );
        }
        other => panic!("test43: expected symlink I/O rejection, got {other:?}"),
    }
    assert!(
        !escaped.exists(),
        "test43: repair wrote through dangling symlink outside repair tree"
    );
    assert!(
        target
            .symlink_metadata()
            .is_ok_and(|meta| meta.file_type().is_symlink()),
        "test43: dangling symlink target should remain in place"
    );
    assert_ne!(
        original_filename, target_rel,
        "test43 setup should exercise a rewritten synthetic PAR2 target"
    );
}
