//! Integration tests for deobfuscation heuristics and subject line parsing.
//!
//! Test cases sourced from NZBGet (tests/queue/Deobfuscation.cpp) and
//! SABnzbd (tests/test_deobfuscate_filenames.py) to ensure parity with
//! both established Usenet downloaders.

use std::path::Path;
use weaver_nzb::{extract_filename, is_obfuscated, is_protected_media_structure};

// ═══════════════════════════════════════════════════════════════════════════
// is_obfuscated — NZBGet parity (IsExcessivelyObfuscated)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nzbget_obfuscated_hex_hash_with_numeric_ext() {
    assert!(is_obfuscated("2c0837e5fa42c8cfb5d5e583168a2af4.mkv"));
}

#[test]
fn nzbget_obfuscated_mixed_case_alphanum() {
    assert!(is_obfuscated("5KzdcWdGVGUG83Q9jv8KXht4O2k57w.mkv"));
}

#[test]
fn nzbget_obfuscated_hex_dotted_chain() {
    assert!(is_obfuscated(
        "a4c7d1f239b71a.a1c0a8b1790e65c9430d5a601037a4.7893"
    ));
}

#[test]
fn nzbget_obfuscated_long_hex_dotted() {
    assert!(is_obfuscated(
        "a1b2c3d4e5f678.901234567890abcdef01234567890123.4567"
    ));
}

#[test]
fn nzbget_obfuscated_abc_xyz_prefix() {
    assert!(is_obfuscated("abc.xyz.a1b2c3d4e5f678.mkv"));
}

#[test]
fn nzbget_obfuscated_b00bs_prefix() {
    assert!(is_obfuscated("b00bs.a1b2c3d4e5f678.mkv"));
}

#[test]
fn nzbget_not_obfuscated_normal_name() {
    assert!(!is_obfuscated("Not.obfuscated.rar"));
}

#[test]
fn nzbget_not_obfuscated_rar_archive() {
    assert!(!is_obfuscated(
        "a1b2c3d4e5f678.901234567890abcdef01234567890123.rar"
    ));
}

#[test]
fn nzbget_not_obfuscated_r00_archive() {
    assert!(!is_obfuscated(
        "a1b2c3d4e5f678.901234567890abcdef01234567890123.r00"
    ));
}

#[test]
fn nzbget_not_obfuscated_7z_split() {
    assert!(!is_obfuscated("2fpJZyw12WSJz8JunjkxpZcw0XIZKKMP.7z.15"));
    assert!(!is_obfuscated(
        "2fpJZyw12WSJz8JunjkxpZcw0XIZKKMP.7z.015"
    ));
}

#[test]
fn nzbget_not_obfuscated_zip_archive() {
    assert!(!is_obfuscated(
        "a1b2c3d4e5f678.901234567890abcdef01234567890123.zip"
    ));
}

#[test]
fn nzbget_not_obfuscated_par2_archive() {
    assert!(!is_obfuscated(
        "a1b2c3d4e5f678.901234567890abcdef01234567890123.par2"
    ));
}

#[test]
fn nzbget_not_obfuscated_short_rar() {
    assert!(!is_obfuscated("ac4rcq47pkqt4flatz2xf.rar"));
}

#[test]
fn nzbget_obfuscated_lowercase_16_chars() {
    assert!(is_obfuscated("ac4rcq47pkqt4fla"));
}

#[test]
fn nzbget_obfuscated_lowercase_20_chars() {
    assert!(is_obfuscated("ac4rcq47pkqt4flatz2xf"));
}

#[test]
fn nzbget_obfuscated_very_long_alphanum() {
    assert!(is_obfuscated(
        "ac4rcq47pkqt4flatz2xfac4rcq47pkqt4flatz2xf4567"
    ));
}

// ═══════════════════════════════════════════════════════════════════════════
// is_obfuscated — SABnzbd parity (is_probably_obfuscated)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sabnzbd_obfuscated_32_hex() {
    assert!(is_obfuscated("599c1c9e2bdfb5114044bf25152b7eaa.mkv"));
}

#[test]
fn sabnzbd_obfuscated_short_mixed() {
    assert!(is_obfuscated("MUGNjK3zi65TtN.mkv"));
}

#[test]
fn sabnzbd_obfuscated_caps_digits() {
    assert!(is_obfuscated("T306077.avi"));
}

#[test]
fn sabnzbd_obfuscated_lowercase_mixed() {
    assert!(is_obfuscated("bar10nmbkkjjdfr.mkv"));
}

#[test]
fn sabnzbd_obfuscated_very_long_mixed() {
    assert!(is_obfuscated(
        "e0nFmxBNTprpbQiVQ44WeEwSrBkLlJ7IgaSj3uzFu455FVYG3q.bin"
    ));
}

#[test]
fn sabnzbd_obfuscated_no_extension() {
    assert!(is_obfuscated(
        "e0nFmxBNTprpbQiVQ44WeEwSrBkLlJ7IgaSj3uzFu455FVYG3q"
    ));
}

#[test]
fn sabnzbd_obfuscated_hex_dotted_long() {
    assert!(is_obfuscated(
        "0675e29e9abfd2.f7d069dab0b853283cc1b069a25f82.6547"
    ));
}

#[test]
fn sabnzbd_obfuscated_bracketed_hex() {
    assert!(is_obfuscated(
        "[BlaBla] something [More] something b2.bef89a622e4a23f07b0d3757ad5e8a.a0 [Brrr]"
    ));
}

#[test]
fn sabnzbd_not_obfuscated_title_with_season() {
    assert!(!is_obfuscated("My Favorite Distro S03E04.iso"));
}

#[test]
fn sabnzbd_not_obfuscated_title_with_year_parens() {
    assert!(!is_obfuscated("Great Distro (2020).iso"));
}

#[test]
fn sabnzbd_not_obfuscated_dotted_release() {
    assert!(!is_obfuscated("Der.Mechaniker.HDRip.XviD-SG.avi"));
}

#[test]
fn sabnzbd_not_obfuscated_french_release() {
    assert!(!is_obfuscated(
        "Bonjour.1969.FRENCH.BRRiP.XviD.AC3-HuSh.avi"
    ));
}

#[test]
fn sabnzbd_not_obfuscated_simple_year() {
    assert!(!is_obfuscated("Bonjour.1969.avi"));
}

#[test]
fn sabnzbd_not_obfuscated_spaces_season() {
    assert!(!is_obfuscated("This That S01E11"));
}

#[test]
fn sabnzbd_not_obfuscated_underscores_season() {
    assert!(!is_obfuscated("This_That_S01E11"));
}

#[test]
fn sabnzbd_not_obfuscated_mixed_case_underscores() {
    assert!(!is_obfuscated("this_that_S01E11"));
}

#[test]
fn sabnzbd_not_obfuscated_dotted_year() {
    assert!(!is_obfuscated("My.Download.2020"));
}

#[test]
fn sabnzbd_not_obfuscated_underscored_words() {
    assert!(!is_obfuscated("this_that_there_here.avi"));
}

#[test]
fn sabnzbd_not_obfuscated_simple_title() {
    assert!(!is_obfuscated("Lorem Ipsum.avi"));
    assert!(!is_obfuscated("Lorem Ipsum"));
}

#[test]
fn sabnzbd_not_obfuscated_catullus() {
    assert!(!is_obfuscated("Catullus.avi"));
}

// ═══════════════════════════════════════════════════════════════════════════
// extract_filename — NZBGet parity (Deobfuscate)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nzbget_extract_empty() {
    assert!(extract_filename("").is_none());
}

#[test]
fn nzbget_extract_quoted_single_char() {
    let (name, conf) = extract_filename("\"A\"").unwrap();
    assert_eq!(name, "A");
    assert_eq!(conf, 1.0);
}

#[test]
fn nzbget_extract_par2_with_part_info() {
    let (name, _) = extract_filename(
        "Any.Show.2024.S01E01.Die.verborgene.Hand.GERMAN.5.1.DL.EAC3.2160p.WEB-DL.DV.HDR.x265-TvR.vol127+128.par2 (1/0)",
    ).unwrap();
    assert_eq!(
        name,
        "Any.Show.2024.S01E01.Die.verborgene.Hand.GERMAN.5.1.DL.EAC3.2160p.WEB-DL.DV.HDR.x265-TvR.vol127+128.par2"
    );
}

#[test]
fn nzbget_extract_private_complex_filename() {
    let (name, conf) = extract_filename(
        "[PRiVATE]-[WtFnZb]-[setup_app_-_reforced_161554.339115__54385_-1.bin]-[1/10] - \"\" yEnc  4288754174 (1/8377)",
    ).unwrap();
    assert_eq!(name, "setup_app_-_reforced_161554.339115__54385_-1.bin");
    assert_eq!(conf, 0.8);
}

#[test]
fn nzbget_extract_private_with_path() {
    let (name, _) = extract_filename(
        "[PRiVATE]-[WtFnZb]-[1/series/Any.Show.S01E01.Pilot.1080p.DSNP.WEBRip.DDP.5.1.H.265.-EDGE2020.mkv]-[1/7] - \"\" yEnc  225628476 (1/315)",
    ).unwrap();
    assert_eq!(
        name,
        "Any.Show.S01E01.Pilot.1080p.DSNP.WEBRip.DDP.5.1.H.265.-EDGE2020.mkv"
    );
}

#[test]
fn nzbget_extract_private_with_brackets_in_name() {
    let (name, _) = extract_filename(
        "[PRiVATE]-[WtFnZb]-[Movie_(1999)_DTS-HD_MA_5.1_-RELEASE_[TBoP].mkv]-[3/15] - \"\" yEnc 9876543210 (2/12345)",
    ).unwrap();
    assert_eq!(name, "Movie_(1999)_DTS-HD_MA_5.1_-RELEASE_[TBoP].mkv");
}

#[test]
fn nzbget_extract_private_mpls() {
    let (name, _) = extract_filename(
        "[PRiVATE]-[WtFnZb]-[00101.mpls]-[163/591] - \"\" yEnc (2/12345)",
    )
    .unwrap();
    assert_eq!(name, "00101.mpls");
}

#[test]
fn nzbget_extract_private_numeric_segment_first() {
    let (name, _) = extract_filename(
        "[PRiVATE]-[WtFnZb]-[24]-[12/filename.ext] - \"\" yEnc (2/12345)",
    )
    .unwrap();
    assert_eq!(name, "filename.ext");
}

#[test]
fn nzbget_extract_private_dotless_filename() {
    let (name, _) = extract_filename(
        "[PRiVATE]-[WtFnZb]-[24]-[filename] - \"\" yEnc (2/12345)",
    )
    .unwrap();
    assert_eq!(name, "filename");
}

#[test]
fn nzbget_extract_private_with_n3wz_prefix() {
    let (name, _) = extract_filename(
        "[N3wZ] \\6aZWVk237607\\::[PRiVATE]-[WtFnZb]-[The.Show.S01E02.1080p.NF.WEB-DL.DDP5.1.Atmos.H.264-playWEB.mkv]-[2/8] - \"\" yEnc 2241590477 (1/3128)",
    ).unwrap();
    assert_eq!(
        name,
        "The.Show.S01E02.1080p.NF.WEB-DL.DDP5.1.Atmos.H.264-playWEB.mkv"
    );
}

#[test]
fn nzbget_extract_private_encryptnzb_subsplease() {
    let (name, _) = extract_filename(
        "[N3wZ] _mC3M3U14246___[PRiVATE]-[EnCrYpTnZb]-[[SubsPlease].Some.file.name.-.20.(1080p).[64032D13].mkv]-[1_1] - \"\" yEnc  1454715270 (1/2030)",
    ).unwrap();
    assert_eq!(
        name,
        "[SubsPlease].Some.file.name.-.20.(1080p).[64032D13].mkv"
    );
}

#[test]
fn nzbget_extract_quoted_hash() {
    let (name, _) = extract_filename(
        "\"2c0837e5fa42c8cfb5d5e583168a2af4.10\" yEnc (1/111)",
    )
    .unwrap();
    assert_eq!(name, "2c0837e5fa42c8cfb5d5e583168a2af4.10");
}

#[test]
fn nzbget_extract_quoted_release_rar() {
    let (name, _) = extract_filename(
        "[02/11] - \"Some.Show.S01E18.Terminal.EAC3.2.0.1080p.WEBRip.x265-iVy.part1.rar\" yEnc(1/144)",
    ).unwrap();
    assert_eq!(
        name,
        "Some.Show.S01E18.Terminal.EAC3.2.0.1080p.WEBRip.x265-iVy.part1.rar"
    );
}

#[test]
fn nzbget_extract_re_prefix_mp3() {
    let (name, _) = extract_filename(
        "Re: Artist Band's The Album-Thanks much - Band, John - Artist - The Album.mp3 (2/3)",
    )
    .unwrap();
    assert!(name.ends_with(".mp3"));
}

#[test]
fn nzbget_extract_re_single_char() {
    let (name, _) = extract_filename("Re: A (2/3)").unwrap();
    assert_eq!(name, "A");
}

#[test]
fn nzbget_extract_re_single_char_no_parens() {
    let (name, _) = extract_filename("Re: A").unwrap();
    assert_eq!(name, "A");
}

#[test]
fn nzbget_extract_fallback_bdmv() {
    let (name, _) = extract_filename("[34/44] - id.bdmv yEnc (1/1) 104").unwrap();
    assert_eq!(name, "id.bdmv");
}

// ═══════════════════════════════════════════════════════════════════════════
// is_protected_media_structure — SABnzbd parity (VIDEO_TS/BDMV skip)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn dvd_video_ts_protected() {
    assert!(is_protected_media_structure(Path::new(
        "/downloads/movie/VIDEO_TS/VTS_01_1.VOB"
    )));
}

#[test]
fn dvd_audio_ts_protected() {
    assert!(is_protected_media_structure(Path::new(
        "/downloads/dvd_audio/AUDIO_TS/ATS_01_1.AOB"
    )));
}

#[test]
fn bluray_bdmv_protected() {
    assert!(is_protected_media_structure(Path::new(
        "/downloads/bluray/BDMV/STREAM/00000.m2ts"
    )));
}

#[test]
fn regular_path_not_protected() {
    assert!(!is_protected_media_structure(Path::new(
        "/downloads/movie/movie.mkv"
    )));
}

#[test]
fn windows_paths_protected() {
    assert!(is_protected_media_structure(Path::new(
        "C:\\Downloads\\movie\\VIDEO_TS\\VTS_01_1.VOB"
    )));
}
