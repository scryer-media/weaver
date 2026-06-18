use weaver_model::files::{
    FileRole, archive_base_name, sanitize_download_filename, unique_download_filenames,
};

#[test]
fn sanitize_download_filename_replaces_nzbget_reserved_chars() {
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
fn unique_download_filenames_uses_nzbget_style_extension_splits() {
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
fn file_role_ignores_nzbget_duplicate_marker() {
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
    assert_eq!(
        FileRole::from_filename("movie.s00"),
        FileRole::RarVolume { volume_number: 1 }
    );
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
        Some("Fixture.Payload".to_string())
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
        Some("Show.S01E01.7z".into())
    );
    assert_eq!(
        archive_base_name("Show.S01E01.7z.003", &FileRole::SevenZipSplit { number: 2 }),
        Some("Show.S01E01.7z".into())
    );
    assert_ne!(
        archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 }),
        archive_base_name("Show.S01E02.7z.001", &FileRole::SevenZipSplit { number: 0 }),
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
fn split_files() {
    assert_eq!(
        FileRole::from_filename("movie.mkv.001"),
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

#[test]
fn archive_base_name_zip() {
    assert_eq!(
        archive_base_name("archive.zip", &FileRole::ZipArchive),
        Some("archive.zip".into())
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
