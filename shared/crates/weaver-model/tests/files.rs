// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_model::files::{
    DOWNLOAD_FILENAME_MAX_BYTES, FileRole, archive_base_name, path_component_with_suffix,
    sanitize_download_filename, unique_download_filenames,
};

#[test]
fn sanitize_download_filename_replaces_reserved_chars() {
    assert_eq!(
        sanitize_download_filename("Fixture:<Payload>\"*.mkv"),
        "Fixture__Payload___.mkv"
    );
    assert_eq!(
        sanitize_download_filename("Fixture/Segment\\Payload?.par2"),
        "Fixture_Segment_Payload_.par2"
    );
    assert_eq!(
        sanitize_download_filename("Fixture\u{1f}Payload|Name.rar"),
        "Fixture_Payload_Name.rar"
    );
}

#[test]
fn sanitize_download_filename_trims_trailing_dot_and_space() {
    assert_eq!(
        sanitize_download_filename("Fixture.Payload.mkv. "),
        "Fixture.Payload.mkv"
    );
    assert_eq!(sanitize_download_filename("..."), "unknown");
}

#[test]
fn sanitize_download_filename_disarms_windows_device_names() {
    assert_eq!(sanitize_download_filename("CON"), "_CON");
    assert_eq!(sanitize_download_filename("com1.rar"), "_com1.rar");
    assert_eq!(sanitize_download_filename("LPT9.par2"), "_LPT9.par2");
    assert_eq!(sanitize_download_filename("LPT10.par2"), "LPT10.par2");
}

#[test]
fn sanitize_download_filename_truncates_long_components_with_extension() {
    let filename = format!("{}.mkv", "a".repeat(400));
    let sanitized = sanitize_download_filename(&filename);

    assert!(sanitized.len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(sanitized.ends_with(".mkv"));
    assert!(sanitized.is_char_boundary(sanitized.len()));
}

#[test]
fn sanitize_download_filename_truncates_utf8_safely() {
    let filename = format!("{}.rar", "é".repeat(200));
    let sanitized = sanitize_download_filename(&filename);

    assert!(sanitized.len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(sanitized.ends_with(".rar"));
    assert!(sanitized.is_char_boundary(sanitized.len()));
}

#[test]
fn sanitize_download_filename_disarms_reserved_name_after_truncation() {
    let filename = format!("CON.{}.rar", "x".repeat(400));
    let sanitized = sanitize_download_filename(&filename);

    assert!(sanitized.len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(sanitized.starts_with("_CON."));
    assert!(sanitized.ends_with(".rar"));
}

#[test]
fn path_component_with_suffix_keeps_suffix_under_limit() {
    let component = sanitize_download_filename(&"a".repeat(400));
    let suffixed = path_component_with_suffix(&component, ".#123456");

    assert!(suffixed.len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(suffixed.ends_with(".#123456"));
    assert!(suffixed.is_char_boundary(suffixed.len()));
}

#[test]
fn unique_download_filenames_disambiguates_sanitized_collisions() {
    assert_eq!(
        unique_download_filenames(["A/B.rar", "A_B.rar"]),
        vec!["A_B.rar", "A_B.duplicate1.rar"]
    );
}

#[test]
fn unique_download_filenames_disambiguates_case_collisions() {
    assert_eq!(
        unique_download_filenames(["Show.RAR", "show.rar"]),
        vec!["Show.RAR", "show.duplicate1.rar"]
    );
}

#[test]
fn unique_download_filenames_disambiguates_windows_device_collisions() {
    assert_eq!(
        unique_download_filenames(["CON.rar", "_CON.rar"]),
        vec!["_CON.rar", "_CON.duplicate1.rar"]
    );
}

#[test]
fn unique_download_filenames_preserves_compound_extensions() {
    assert_eq!(
        unique_download_filenames([
            "Show.part01.rar",
            "Show.part01.rar",
            "Archive.7z.001",
            "Archive.7z.001",
            "Set.vol00+01.par2",
            "Set.vol00+01.par2",
        ]),
        vec![
            "Show.part01.rar",
            "Show.part01.duplicate1.rar",
            "Archive.7z.001",
            "Archive.7z.duplicate1.001",
            "Set.vol00+01.par2",
            "Set.duplicate1.vol00+01.par2",
        ]
    );
}

#[test]
fn unique_download_filenames_keeps_long_duplicates_under_limit() {
    let filename = format!("{}.rar", "a".repeat(400));
    let filenames = unique_download_filenames([filename.as_str(), filename.as_str()]);

    assert_eq!(filenames.len(), 2);
    assert!(filenames[0].len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(filenames[1].len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(filenames[1].contains(".duplicate1."));
    assert!(filenames[1].ends_with(".rar"));
}

#[test]
fn unique_download_filenames_preserves_long_par2_recovery_suffix() {
    let filename = format!("{}.vol00+01.par2", "a".repeat(400));
    let filenames = unique_download_filenames([filename.as_str(), filename.as_str()]);

    assert_eq!(filenames.len(), 2);
    assert!(filenames[0].len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(filenames[1].len() <= DOWNLOAD_FILENAME_MAX_BYTES);
    assert!(filenames[0].ends_with(".vol00+01.par2"));
    assert!(filenames[1].contains(".duplicate1.vol00+01.par2"));
}

#[test]
fn file_role_ignores_duplicate_marker() {
    assert_eq!(
        FileRole::from_filename("Show.part02.duplicate1.rar"),
        FileRole::RarVolume { volume_number: 1 }
    );
    assert_eq!(
        FileRole::from_filename("Archive.7z.duplicate1.002"),
        FileRole::SevenZipSplit { number: 1 }
    );
    assert_eq!(
        FileRole::from_filename("Set.duplicate1.vol00+02.par2"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 2
        }
    );
}

#[test]
fn par3_carrier_names_are_only_scheduling_hints() {
    for name in [
        "movie.par3",
        "MOVIE.PAR3",
        "movie.volbad+3.par3",
        "movie.vol1+.par3",
    ] {
        let role = FileRole::from_filename(name);
        assert_eq!(role, FileRole::Par3 { is_index: true });
        assert_eq!(role.download_priority(), 0);
        assert!(!role.is_recovery());
        assert!(!role.counts_toward_health());
    }
    for name in [
        "movie.vol00+01.par3",
        "MOVIE.VOL3+5.PAR3",
        "movie.duplicate1.vol00+02.par3",
        "movie.vol999999999999999999999999+1.par3",
    ] {
        let role = FileRole::from_filename(name);
        assert_eq!(role, FileRole::Par3 { is_index: false });
        assert_eq!(role.download_priority(), 1000);
        assert!(role.is_recovery());
        assert!(!role.counts_toward_health());
    }
    assert_eq!(
        FileRole::from_filename("movie.par3.zip"),
        FileRole::ZipArchive
    );
    assert_eq!(
        FileRole::from_filename("movie.par3.7z"),
        FileRole::SevenZipArchive
    );
    assert_eq!(FileRole::from_filename("movie.par3.exe"), FileRole::Unknown);
}

#[test]
fn par2_index() {
    assert_eq!(
        FileRole::from_filename("movie.par2"),
        FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0
        }
    );
}

#[test]
fn par2_recovery() {
    assert_eq!(
        FileRole::from_filename("movie.vol00+01.par2"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 1
        }
    );
    assert_eq!(
        FileRole::from_filename("movie.vol03+05.par2"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 5
        }
    );
}

#[test]
fn par2_recovery_dash_format() {
    assert_eq!(
        FileRole::from_filename("movie.vol-01.par2"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 1
        }
    );
    assert_eq!(
        FileRole::from_filename("movie.vol-05.par2"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 5
        }
    );
}

// A volume named by its first block and the one past its last declares the
// blocks between; a range that does not move forward declares none and is
// still a volume.
#[test]
fn par2_recovery_block_range_format() {
    for (name, blocks) in [
        ("fixture.release.vol000-001.par2", 1),
        ("fixture.release.vol001-003.par2", 2),
        ("fixture.release.vol063-105.par2", 42),
        ("fixture.release-sample.vol00-01.par2", 1),
        ("fixture.release-sample.vol31-58.par2", 27),
        ("FIXTURE.RELEASE.VOL7-15.PAR2", 8),
        ("fixture.release.vol15-07.par2", 0),
    ] {
        assert_eq!(
            FileRole::from_filename(name),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: blocks
            },
            "{name}"
        );
    }
    assert_eq!(
        FileRole::from_filename("fixture.release-sample.par2"),
        FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0
        }
    );
}

#[test]
fn par2_case_insensitive() {
    assert_eq!(
        FileRole::from_filename("Movie.PAR2"),
        FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0
        }
    );
    assert_eq!(
        FileRole::from_filename("Movie.Vol00+02.PAR2"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 2
        }
    );
}

#[test]
fn rar_new_style() {
    assert_eq!(
        FileRole::from_filename("movie.part01.rar"),
        FileRole::RarVolume { volume_number: 0 }
    );
    assert_eq!(
        FileRole::from_filename("movie.part02.rar"),
        FileRole::RarVolume { volume_number: 1 }
    );
    assert_eq!(
        FileRole::from_filename("movie.part10.rar"),
        FileRole::RarVolume { volume_number: 9 }
    );
}

#[test]
fn rar_plain() {
    assert_eq!(
        FileRole::from_filename("movie.rar"),
        FileRole::RarVolume { volume_number: 0 }
    );
}

#[test]
fn rar_old_style() {
    assert_eq!(
        FileRole::from_filename("movie.r00"),
        FileRole::RarVolume { volume_number: 1 }
    );
    assert_eq!(
        FileRole::from_filename("movie.r01"),
        FileRole::RarVolume { volume_number: 2 }
    );
    // `.s00` continues where `.r99` stops, a hundred to a letter.
    for (name, volume_number) in [("movie.r99", 100), ("movie.s00", 101), ("movie.S01", 102)] {
        assert_eq!(
            FileRole::from_filename(name),
            FileRole::RarVolume { volume_number },
            "{name}"
        );
    }
    // Later letters are ordinary files by name alone.
    for name in [
        "movie.r+1",
        "movie.r1",
        "movie.q00",
        "movie.t00",
        "movie.y99",
        "movie.z00",
        "map.t01",
        "track.v10",
    ] {
        assert_eq!(FileRole::from_filename(name), FileRole::Unknown, "{name}");
    }
}

// A `.tNN` … `.yNN` name is a RAR volume only beside its set's RAR
// volumes; by name alone, or beside another stem's set, it is an ordinary
// file.
#[test]
fn later_lettered_names_are_rar_volumes_only_beside_their_set() {
    let roles = |names: &[&str]| FileRole::of_post(names.iter().map(|name| Some(*name)));
    assert_eq!(
        roles(&["name.rar", "Name.r00", "name.t00", "NAME.Y99", "other.t00"]),
        vec![
            FileRole::RarVolume { volume_number: 0 },
            FileRole::RarVolume { volume_number: 1 },
            FileRole::RarVolume { volume_number: 201 },
            FileRole::RarVolume { volume_number: 800 },
            FileRole::Unknown,
        ]
    );
    assert_eq!(
        roles(&["setup.x64", "lib.x86", "map.t01"]),
        vec![FileRole::Unknown; 3]
    );
    for name in ["setup.x64", "lib.x86", "map.t01", "tool.w32"] {
        assert_eq!(FileRole::from_filename(name), FileRole::Unknown, "{name}");
        assert!(!weaver_model::files::names_a_z_part(name), "{name}");
    }
}

// A `.zNN` name is a split zip's part or a RAR volume after `.y99`: the
// post tells which.
#[test]
fn z_numbered_names_take_their_role_from_the_post() {
    let roles = |names: &[&str]| FileRole::of_post(names.iter().map(|name| Some(*name)));
    assert_eq!(
        roles(&[
            "Movie.rar",
            "movie.y99",
            "movie.z00",
            "Movie.Z99",
            "other.z00"
        ]),
        vec![
            FileRole::RarVolume { volume_number: 0 },
            FileRole::RarVolume { volume_number: 800 },
            FileRole::RarVolume { volume_number: 801 },
            FileRole::RarVolume { volume_number: 900 },
            FileRole::Unknown,
        ]
    );
    // A split zip keeps its parts, with or without a RAR set of another stem.
    assert_eq!(
        roles(&["disc.z01", "disc.zip", "movie.r00", "movie.z01"]),
        vec![
            FileRole::Unknown,
            FileRole::ZipArchive,
            FileRole::RarVolume { volume_number: 1 },
            FileRole::RarVolume { volume_number: 802 },
        ]
    );
    // One stem with both a `.zip` and RAR volumes: its `.zNN` stay the zip's.
    assert_eq!(
        roles(&["both.rar", "both.zip", "both.z01"])[2],
        FileRole::Unknown
    );
    assert_eq!(
        FileRole::of_post([None, Some("a.z01")]),
        vec![FileRole::Unknown; 2]
    );
    for name in ["a.z00", "a.Z01", "a.z99"] {
        assert!(weaver_model::files::names_a_z_part(name), "{name}");
    }
    for name in ["a.zip", "a.z1", "a.z100", "a.r00"] {
        assert!(!weaver_model::files::names_a_z_part(name), "{name}");
    }
}

#[test]
fn standalone() {
    assert_eq!(FileRole::from_filename("info.nfo"), FileRole::Standalone);
    assert_eq!(FileRole::from_filename("readme.txt"), FileRole::Standalone);
    assert_eq!(FileRole::from_filename("cover.jpg"), FileRole::Standalone);
}

#[test]
fn sevenz_single() {
    assert_eq!(
        FileRole::from_filename("archive.7z"),
        FileRole::SevenZipArchive
    );
    assert_eq!(
        FileRole::from_filename("Movie.7Z"),
        FileRole::SevenZipArchive
    );
}

#[test]
fn sevenz_split() {
    assert_eq!(
        FileRole::from_filename("archive.7z.001"),
        FileRole::SevenZipSplit { number: 0 }
    );
    assert_eq!(
        FileRole::from_filename("archive.7z.002"),
        FileRole::SevenZipSplit { number: 1 }
    );
    assert_eq!(
        FileRole::from_filename("archive.7z.010"),
        FileRole::SevenZipSplit { number: 9 }
    );
    assert_eq!(
        FileRole::from_filename("Movie.7z.003"),
        FileRole::SevenZipSplit { number: 2 }
    );
}

#[test]
fn unknown() {
    assert_eq!(FileRole::from_filename("data.bin"), FileRole::Unknown);
}

#[test]
fn file_role_classifies_trailing_sanitized_artifacts() {
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.rar_"),
        FileRole::RarVolume { volume_number: 0 }
    );
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.vol00+01.par2_"),
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 1
        }
    );
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.7z_"),
        FileRole::SevenZipArchive
    );
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.mkv_"),
        FileRole::Unknown
    );
    assert_eq!(
        archive_base_name(
            "Fixture.Payload.part01.rar_",
            &FileRole::from_filename("Fixture.Payload.part01.rar_")
        ),
        Some("fixture.payload".to_string())
    );
}

#[test]
fn file_role_does_not_classify_double_extension_malware() {
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.mkv.exe"),
        FileRole::Unknown
    );
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.rar.exe"),
        FileRole::Unknown
    );
    assert_eq!(
        FileRole::from_filename("Fixture.Payload.par2.exe"),
        FileRole::Unknown
    );
}

#[test]
fn archive_base_name_7z() {
    assert_eq!(
        archive_base_name("archive.7z", &FileRole::SevenZipArchive),
        Some("archive.7z".into())
    );
    assert_eq!(
        archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 }),
        Some("show.s01e01.7z".into())
    );
    assert_eq!(
        archive_base_name("Show.S01E01.7z.003", &FileRole::SevenZipSplit { number: 2 }),
        Some("show.s01e01.7z".into())
    );
    assert_ne!(
        archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 }),
        archive_base_name("Show.S01E02.7z.001", &FileRole::SevenZipSplit { number: 0 }),
    );
    assert_eq!(
        archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 }),
        archive_base_name("show.s01e01.7Z.002", &FileRole::SevenZipSplit { number: 1 }),
    );
}

#[test]
fn archive_base_name_rar() {
    assert_eq!(
        archive_base_name(
            "movie.part01.rar",
            &FileRole::RarVolume { volume_number: 0 }
        ),
        Some("movie".into())
    );
    assert_eq!(
        archive_base_name("movie.rar", &FileRole::RarVolume { volume_number: 0 }),
        Some("movie".into())
    );
    assert_eq!(
        archive_base_name("movie.r00", &FileRole::RarVolume { volume_number: 1 }),
        Some("movie".into())
    );
    assert_eq!(
        archive_base_name(
            "Movie.part01.rar",
            &FileRole::RarVolume { volume_number: 0 }
        ),
        archive_base_name(
            "movie.part02.RAR",
            &FileRole::RarVolume { volume_number: 1 }
        ),
    );
}

#[test]
fn zip_archive() {
    assert_eq!(FileRole::from_filename("archive.zip"), FileRole::ZipArchive);
    assert_eq!(FileRole::from_filename("Movie.ZIP"), FileRole::ZipArchive);
}

#[test]
fn tar_archive() {
    assert_eq!(FileRole::from_filename("backup.tar"), FileRole::TarArchive);
    assert_eq!(FileRole::from_filename("data.TAR"), FileRole::TarArchive);
}

#[test]
fn tar_gz_archive() {
    assert_eq!(
        FileRole::from_filename("backup.tar.gz"),
        FileRole::TarGzArchive
    );
    assert_eq!(FileRole::from_filename("data.tgz"), FileRole::TarGzArchive);
    assert_eq!(
        FileRole::from_filename("backup.tar.gzip"),
        FileRole::TarGzArchive
    );
    assert_eq!(
        FileRole::from_filename("Archive.TAR.GZ"),
        FileRole::TarGzArchive
    );
}

#[test]
fn tar_bz2_archive() {
    assert_eq!(
        FileRole::from_filename("backup.tar.bz2"),
        FileRole::TarBz2Archive
    );
    assert_eq!(FileRole::from_filename("data.tbz"), FileRole::TarBz2Archive);
    assert_eq!(
        FileRole::from_filename("movie.tbz2"),
        FileRole::TarBz2Archive
    );
    assert_eq!(
        FileRole::from_filename("archive.tar.bzip2"),
        FileRole::TarBz2Archive
    );
    assert_eq!(
        FileRole::from_filename("Archive.TAR.BZ2"),
        FileRole::TarBz2Archive
    );
}

#[test]
fn tar_xz_archive() {
    assert_eq!(
        FileRole::from_filename("backup.tar.xz"),
        FileRole::TarXzArchive
    );
    assert_eq!(FileRole::from_filename("data.txz"), FileRole::TarXzArchive);
    assert_eq!(
        FileRole::from_filename("Archive.TAR.XZ"),
        FileRole::TarXzArchive
    );
}

#[test]
fn gz_archive() {
    assert_eq!(FileRole::from_filename("file.gz"), FileRole::GzArchive);
    assert_eq!(FileRole::from_filename("data.GZ"), FileRole::GzArchive);
    assert_ne!(
        FileRole::from_filename("backup.tar.gz"),
        FileRole::GzArchive
    );
}

#[test]
fn deflate_archive() {
    assert_eq!(
        FileRole::from_filename("file.deflate"),
        FileRole::DeflateArchive
    );
    assert_eq!(
        FileRole::from_filename("data.DEFLATE"),
        FileRole::DeflateArchive
    );
}

#[test]
fn brotli_archive() {
    assert_eq!(FileRole::from_filename("file.br"), FileRole::BrotliArchive);
    assert_eq!(FileRole::from_filename("data.BR"), FileRole::BrotliArchive);
}

#[test]
fn zstd_archive() {
    assert_eq!(FileRole::from_filename("file.zst"), FileRole::ZstdArchive);
    assert_eq!(FileRole::from_filename("file.zstd"), FileRole::ZstdArchive);
    assert_eq!(FileRole::from_filename("data.ZST"), FileRole::ZstdArchive);
}

#[test]
fn bzip2_archive() {
    assert_eq!(FileRole::from_filename("file.bz2"), FileRole::Bzip2Archive);
    assert_eq!(FileRole::from_filename("data.BZ2"), FileRole::Bzip2Archive);
    assert_ne!(
        FileRole::from_filename("backup.tar.bz2"),
        FileRole::Bzip2Archive
    );
}

#[test]
fn xz_archive() {
    assert_eq!(FileRole::from_filename("file.xz"), FileRole::XzArchive);
    assert_eq!(FileRole::from_filename("data.XZ"), FileRole::XzArchive);
    assert_ne!(
        FileRole::from_filename("backup.tar.xz"),
        FileRole::XzArchive
    );
}

#[test]
fn split_files() {
    assert_eq!(
        FileRole::from_filename("movie.mkv.001"),
        FileRole::SplitFile { number: 0 }
    );
    assert_eq!(
        FileRole::from_filename("release.xz.001"),
        FileRole::SplitFile { number: 0 }
    );
    assert_eq!(
        FileRole::from_filename("movie.mkv.002"),
        FileRole::SplitFile { number: 1 }
    );
    assert_eq!(
        FileRole::from_filename("movie.mkv.010"),
        FileRole::SplitFile { number: 9 }
    );
    assert_eq!(
        FileRole::from_filename("archive.7z.001"),
        FileRole::SevenZipSplit { number: 0 }
    );
}

// The names a split piece is told by. The same table pins `split_piece` in
// `engines/weaver-graph/src/sniff.rs`; the two rules must agree, and a
// change to one changes both tables.
const SPLIT_PIECE_NAMES: &[(&str, bool)] = &[
    ("a.001", true),
    ("a.mkv.001", true),
    ("a.mkv.002", true),
    ("a.mkv.0001", true),
    ("a.mkv.00001", true),
    ("a.mkv.1000", true),
    ("[71ECB6A7].mkv.001_", true),
    ("a.mkv.002\"", true),
    ("a.mkv.003.", true),
    ("a.mkv.004 ", true),
    ("a.mkv.01", false),
    ("a.mkv.1", false),
    ("c0ffee.99", false),
    ("Release%211.4.0", false),
    (".001", false),
    ("a.mkv.00a", false),
    ("a.001.mkv", false),
    ("a.mkv", false),
];

// The posts whose two-digit names are pieces of a set, name by name. The
// same table pins `split_piece_among` in `engines/weaver-graph/src/sniff.rs`.
const SHORT_SET_POSTS: &[&[(&str, bool)]] = &[
    &[("a.mkv.01", true), ("a.mkv.02", true), ("a.mkv.03", true)],
    &[("a.mkv.00", true), ("a.mkv.01", true)],
    &[("A.MKV.01", true), ("a.mkv.02_", true), ("a.mkv.07", true)],
    &[("a.mkv.01", true), ("a.mkv.02", true), ("b.mkv.03", false)],
    &[("a.mkv.01", false)],
    &[("notes.10", false)],
    &[("file.99", false)],
    &[("Release.1.4.10", false)],
    &[("notes.10", false), ("notes.11", false)],
    &[("a.mkv.02", false), ("a.mkv.03", false)],
    &[("a.mkv.01", false), ("b.mkv.02", false)],
    &[
        ("a.mkv.01", false),
        ("a.mkv.002", true),
        ("a.mkv.003", true),
    ],
    &[("notes.4.0", false), ("notes.4.1", false)],
];

#[test]
fn short_split_sets_follow_the_graph_rule() {
    for post in SHORT_SET_POSTS {
        let roles = FileRole::of_post(post.iter().map(|(name, _)| Some(*name)));
        for (&(name, piece), role) in post.iter().zip(&roles) {
            assert_eq!(
                matches!(role, FileRole::SplitFile { .. }),
                piece,
                "{name} in {post:?}: {role:?}"
            );
        }
    }
    assert_eq!(
        FileRole::of_post([Some("a.mkv.01"), Some("a.mkv.02")]),
        vec![
            FileRole::SplitFile { number: 0 },
            FileRole::SplitFile { number: 1 }
        ]
    );
    assert_eq!(
        FileRole::of_post([Some("a.mkv.00"), Some("a.mkv.01")]),
        vec![
            FileRole::SplitFile { number: 0 },
            FileRole::SplitFile { number: 0 }
        ]
    );
}

#[test]
fn split_pieces_follow_the_graph_rule() {
    for &(name, piece) in SPLIT_PIECE_NAMES {
        assert_eq!(
            matches!(FileRole::from_filename(name), FileRole::SplitFile { .. }),
            piece,
            "{name}"
        );
    }
    assert_eq!(
        FileRole::from_filename("a.mkv.1000"),
        FileRole::SplitFile { number: 999 }
    );
    assert_eq!(
        FileRole::from_filename("[71ECB6A7].mkv.001_"),
        FileRole::SplitFile { number: 0 }
    );
    // A number past `u32` is still a piece, sorted last.
    let huge = FileRole::from_filename("a.mkv.99999999999");
    assert_eq!(huge, FileRole::SplitFile { number: u32::MAX });
    assert_eq!(huge.download_priority(), u32::MAX);
}

#[test]
fn archive_base_name_zip() {
    assert_eq!(
        archive_base_name("archive.zip", &FileRole::ZipArchive),
        Some("archive.zip".into())
    );
}

#[test]
fn archive_base_name_xz() {
    assert_eq!(
        archive_base_name("archive.xz", &FileRole::XzArchive),
        Some("archive.xz".into())
    );
    assert_eq!(
        archive_base_name("archive.tar.xz", &FileRole::TarXzArchive),
        Some("archive.tar.xz".into())
    );
}

#[test]
fn archive_base_name_split() {
    assert_eq!(
        archive_base_name("movie.mkv.001", &FileRole::SplitFile { number: 0 }),
        Some("movie.mkv".into())
    );
    assert_eq!(
        archive_base_name("movie.mkv.003", &FileRole::SplitFile { number: 2 }),
        Some("movie.mkv".into())
    );
    assert_eq!(
        archive_base_name("movie.mkv.001", &FileRole::SplitFile { number: 0 }),
        archive_base_name("movie.mkv.002", &FileRole::SplitFile { number: 1 }),
    );
}

#[test]
fn archive_base_name_non_archive() {
    assert_eq!(archive_base_name("info.nfo", &FileRole::Standalone), None);
    assert_eq!(archive_base_name("data.bin", &FileRole::Unknown), None);
}

#[test]
fn priority_ordering() {
    let par2_index = FileRole::Par2 {
        is_index: true,
        recovery_block_count: 0,
    };
    let rar_first = FileRole::RarVolume { volume_number: 0 };
    let rar_second = FileRole::RarVolume { volume_number: 1 };
    let standalone = FileRole::Standalone;
    let par2_recovery = FileRole::Par2 {
        is_index: false,
        recovery_block_count: 5,
    };

    assert!(par2_index.download_priority() < rar_first.download_priority());
    assert!(rar_first.download_priority() < standalone.download_priority());
    assert!(standalone.download_priority() < rar_second.download_priority());
    assert!(rar_second.download_priority() < par2_recovery.download_priority());
}

// A name whose last four bytes begin inside a longer character has no
// old-style RAR extension, and one that ends in such an extension past a
// longer character still does.
#[test]
fn a_name_ending_inside_a_longer_character_is_classified() {
    for name in ["éabc", "日本", "aé", "é", "x.r0é"] {
        assert_eq!(FileRole::from_filename(name), FileRole::Unknown, "{name}");
    }
    let role = FileRole::from_filename("é.r00");
    assert_eq!(role, FileRole::RarVolume { volume_number: 1 });
    assert_eq!(archive_base_name("é.r00", &role), Some("é".to_string()));
    let other = FileRole::RarVolume { volume_number: 1 };
    assert_eq!(archive_base_name("éabc", &other), Some("éabc".to_string()));
}
