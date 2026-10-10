use super::classify::{
    NameShape, StemClass, classify_name, file_ext, parse_subject_numbers, stem_class,
};
use super::render::MAX_COLUMNS;
use super::*;
use crate::parser::parse_nzb_with_diagnostics;

// 2026-01-01T00:00:00Z, the instant every test measures ages from.
const NOW: u64 = 1_767_225_600;
const DAY: u64 = 86_400;

struct FileSpec<'a> {
    subject: String,
    poster: &'a str,
    date: u64,
    groups: &'a [&'a str],
    segments: Vec<(u32, u32, String)>,
}

fn file(subject: impl Into<String>, segments: Vec<(u32, u32, String)>) -> FileSpec<'static> {
    FileSpec {
        subject: subject.into(),
        poster: "poster@example.invalid",
        date: NOW - 10 * DAY,
        groups: &["alt.binaries.example"],
        segments,
    }
}

// `count` segments of `size` bytes with ids `<prefix>-<n>@<domain>`.
fn segments(prefix: &str, domain: &str, count: u32, size: u32) -> Vec<(u32, u32, String)> {
    (1..=count)
        .map(|number| (number, size, format!("{prefix}-{number}@{domain}")))
        .collect()
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn nzb_xml(meta: &[(&str, &str)], files: &[FileSpec<'_>]) -> Vec<u8> {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n<head>\n",
    );
    for (kind, value) in meta {
        xml.push_str(&format!(
            "<meta type=\"{}\">{}</meta>\n",
            escape(kind),
            escape(value)
        ));
    }
    xml.push_str("</head>\n");
    for spec in files {
        xml.push_str(&format!(
            "<file poster=\"{}\" date=\"{}\" subject=\"{}\">\n<groups>\n",
            escape(spec.poster),
            spec.date,
            escape(&spec.subject)
        ));
        for group in spec.groups {
            xml.push_str(&format!("<group>{}</group>\n", escape(group)));
        }
        xml.push_str("</groups>\n<segments>\n");
        for (number, bytes, id) in &spec.segments {
            xml.push_str(&format!(
                "<segment bytes=\"{bytes}\" number=\"{number}\">{}</segment>\n",
                escape(id)
            ));
        }
        xml.push_str("</segments>\n</file>\n");
    }
    xml.push_str("</nzb>\n");
    xml.into_bytes()
}

fn report_of(xml: &[u8]) -> NzbReport {
    let (nzb, diagnostics) = parse_nzb_with_diagnostics(xml).expect("fixture parses");
    analyze(&nzb, &diagnostics, NOW)
}

// A clean five-volume RAR post with one PAR2 index and two PAR2 volumes.
fn clean_release() -> Vec<FileSpec<'static>> {
    let mut files = Vec::new();
    let total = 8;
    for part in 1..=5u32 {
        files.push(file(
            format!(
                "Invented.Title.2031 [{part:02}/{total:02}] - \"Invented.Title.2031.part{part:02}.rar\" yEnc (1/4) 3000000"
            ),
            segments(&format!("rar{part}"), "example.invalid", 4, 750_000),
        ));
    }
    files.push(file(
        format!("Invented.Title.2031 [06/{total:02}] - \"Invented.Title.2031.par2\" yEnc (1/1)"),
        segments("par-index", "example.invalid", 1, 40_000),
    ));
    files.push(file(
        format!(
            "Invented.Title.2031 [07/{total:02}] - \"Invented.Title.2031.vol00+10.par2\" yEnc (1/1)"
        ),
        segments("par-vol0", "example.invalid", 1, 400_000),
    ));
    files.push(file(
        format!(
            "Invented.Title.2031 [08/{total:02}] - \"Invented.Title.2031.vol10+20.par2\" yEnc (1/2)"
        ),
        segments("par-vol1", "example.invalid", 2, 400_000),
    ));
    files
}

#[test]
fn no_identifying_text_survives_in_json_or_text() {
    // Every field an NZB has carries the sentinel. None of it may reach a report.
    let sentinel = "zqxsentinel";
    let groups: &'static [&'static str] = &["alt.binaries.zqxsentinel", "zqxsentinel.group"];
    let mut files = Vec::new();
    for (index, name) in [
        "zqxsentinel.Name.part01.rar",
        "zqxsentinel.Name.part03.rar",
        "zqxsentinel.Name.zqxsentinelext",
        "zqxsentinel.Name.vol00+05.par2",
        "zqxsentinel.Name.nfo",
        "zqxsentinel-sample.mkv",
    ]
    .iter()
    .enumerate()
    {
        files.push(FileSpec {
            subject: format!(
                "zqxsentinel subject {{{{zqxsentinelpw}}}} [{:02}/09] - \"{name}\" yEnc (1/3) 999999999",
                index + 1
            ),
            poster: "zqxsentinel <zqxsentinel@zqxsentinel.invalid>",
            date: NOW - 400 * DAY,
            groups,
            segments: vec![
                (1, 700_000, format!("zqxsentinel{index}a@zqxsentinel.invalid")),
                // A gap, a repeat, and a message-ID shared with another file.
                (3, 700_000, format!("zqxsentinel{index}b@zqxsentinel.invalid")),
                (3, 700_000, format!("zqxsentinel{index}c@zqxsentinel.invalid")),
                (4, 1, "zqxsentinelshared@zqxsentinel.invalid".to_string()),
            ],
        });
    }
    // A subject with no name at all, and one whose only name is unquoted.
    files.push(FileSpec {
        subject: "zqxsentinel yEnc (1/1)".to_string(),
        poster: "zqxsentinel",
        date: 0,
        groups,
        segments: vec![(1, 10, "zqxsentinel-x@zqxsentinel".to_string())],
    });
    files.push(FileSpec {
        subject: "[09/09] - zqxsentinel.unquoted.bin yEnc (1/1) 5".to_string(),
        poster: "zqxsentinel",
        date: NOW - DAY,
        groups,
        segments: vec![(1, 10, "zqxsentinel-y@zqxsentinel".to_string())],
    });
    let xml = nzb_xml(
        &[
            ("title", "zqxsentinel title"),
            ("password", "zqxsentinel"),
            ("tag", "zqxsentinel"),
            ("zqxsentinel", "zqxsentinel"),
            ("category", "zqxsentinel"),
        ],
        &files,
    );
    let report = report_of(&xml);
    let json = report.to_json().to_ascii_lowercase();
    let text = report.to_text().to_ascii_lowercase();
    assert!(!json.contains(sentinel), "json leaks: {json}");
    assert!(!text.contains(sentinel), "text leaks: {text}");
    // Nor any of the pieces a careless report would copy out.
    for piece in ["zqx", "example", "alt.binaries", ".invalid"] {
        assert!(!json.contains(piece), "json carries {piece}");
        assert!(!text.contains(piece), "text carries {piece}");
    }
    // The fixture really exercised the redacted fields.
    assert!(report.layout.password.in_meta);
    assert!(report.layout.password.in_name);
    assert!(report.shape.shared_message_ids > 0);
    assert!(report.shape.duplicate_segments > 0);
    assert_eq!(report.layout.extras.sample, 1);
    assert!(report.files.iter().any(|file| file.ext == FileExt::OTHER));
}

#[test]
fn clean_release_reports_layout_and_no_flags() {
    let report = report_of(&nzb_xml(&[], &clean_release()));
    assert_eq!(report.red_flags, Vec::new());
    assert_eq!(report.shape.file_count, 8);
    assert_eq!(report.shape.declared_file_count, Some(8));
    assert_eq!(report.shape.segment_count, 24);
    assert_eq!(report.layout.payload, PayloadKind::Archive);

    let archives = &report.layout.archive_sets;
    assert_eq!(archives.len(), 1);
    assert_eq!(archives[0].kind, ArchiveKind::Rar);
    assert_eq!(archives[0].scheme, NameShape::PartNn);
    assert_eq!(archives[0].volumes, 5);
    assert_eq!(archives[0].missing_volumes, 0);
    assert_eq!(archives[0].bytes, 15_000_000);
    assert_eq!(archives[0].stem, StemClass::Readable);

    let par2 = &report.layout.par2_sets;
    assert_eq!(par2.len(), 1);
    assert!(par2[0].has_index);
    assert_eq!(par2[0].volumes, 2);
    assert_eq!(par2[0].declared_blocks, 30);
    assert_eq!(par2[0].recovery_bytes, 1_200_000);
    assert_eq!(par2[0].protected_bytes, 15_000_000);
    assert_eq!(par2[0].estimated_recovery_permille, Some(80));
    assert_eq!(report.layout.obfuscation.level, ObfuscationLevel::None);

    let sizes = report.shape.segment_sizes.as_ref().expect("segments exist");
    assert_eq!(
        (sizes.min, sizes.median, sizes.max, sizes.common),
        (40_000, 750_000, 750_000, 750_000)
    );
    let declared = report.shape.declared_bytes.as_ref().expect("declared");
    assert_eq!((declared.files, declared.short_files), (5, 0));
}

#[test]
fn missing_volume_and_segments_lead_the_flags() {
    let mut files = clean_release();
    files.remove(2); // part03
    files[0].segments.remove(1); // part01 loses segment 2
    let report = report_of(&nzb_xml(&[], &files));
    let set = report.layout.archive_sets[0].token;
    assert_eq!(
        report.red_flags[..3],
        [
            RedFlag::MissingVolumes { set, missing: 1 },
            RedFlag::MissingFiles {
                declared: 8,
                listed: 7
            },
            RedFlag::MissingSegments {
                files: 1,
                segments: 1
            },
        ]
    );
    assert!(
        report
            .red_flags
            .contains(&RedFlag::ShortDeclaredSize { files: 1 })
    );
    assert!(
        report
            .red_flags
            .contains(&RedFlag::SegmentCountMismatch { files: 1 })
    );
    let part01 = &report.files[0];
    assert_eq!(part01.missing_segments, 1);
    assert_eq!(part01.declared_segments, Some(4));
    assert!(report.to_text().contains("f01"));
}

#[test]
fn rnn_volumes_continue_into_snn() {
    let mut files = vec![file(
        "\"pack.rar\" yEnc (1/1)",
        segments("v0", "example.invalid", 1, 100),
    )];
    for number in (0..100).chain(100..102) {
        let name = if number < 100 {
            format!("pack.r{number:02}")
        } else {
            format!("pack.s{:02}", number - 100)
        };
        files.push(file(
            format!("\"{name}\" yEnc (1/1)"),
            segments(&format!("v{}", number + 1), "example.invalid", 1, 100),
        ));
    }
    let report = report_of(&nzb_xml(&[], &files));
    let set = &report.layout.archive_sets[0];
    assert_eq!(set.scheme, NameShape::RNn);
    assert_eq!(
        (set.volumes, set.first_volume, set.last_volume),
        (103, 0, 102)
    );
    assert_eq!(set.missing_volumes, 0);
}

#[test]
fn hashed_names_without_recovery_are_flagged() {
    let files: Vec<_> = (0..4u32)
        .map(|index| {
            file(
                format!(
                    "[{:02}/04] - \"{:032x}.7z.{:03}\" yEnc (1/2)",
                    index + 1,
                    0xabcdef_u64,
                    index + 1
                ),
                segments(&format!("h{index}"), "example.invalid", 2, 500),
            )
        })
        .collect();
    let report = report_of(&nzb_xml(&[], &files));
    assert_eq!(
        report.layout.obfuscation.level,
        ObfuscationLevel::HashedNames
    );
    assert_eq!(report.layout.archive_sets[0].scheme, NameShape::SevenZNnn);
    assert_eq!(report.layout.archive_sets[0].stem, StemClass::Hex32);
    assert_eq!(report.layout.archive_sets[0].kind, ArchiveKind::SevenZip);
    assert!(report.red_flags.contains(&RedFlag::NoRecoveryData));
    assert!(report.red_flags.contains(&RedFlag::HashedWithoutRecovery));
}

#[test]
fn subjects_without_names_mean_names_live_in_yenc_only() {
    let files: Vec<_> = (0..3u32)
        .map(|index| {
            file(
                format!("Q7rT2mX9pL4vN8kZ yEnc ({}/3)", index + 1),
                segments(&format!("y{index}"), "example.invalid", 3, 500),
            )
        })
        .collect();
    let report = report_of(&nzb_xml(&[], &files));
    assert_eq!(
        report.layout.obfuscation.level,
        ObfuscationLevel::NamesInYencOnly
    );
    assert_eq!(report.layout.obfuscation.unnamed_files, 3);
    assert_eq!(report.layout.unclassified_files, 3);
    assert_eq!(report.layout.payload, PayloadKind::Unknown);
}

#[test]
fn thin_recovery_on_an_old_post_is_flagged() {
    let mut files = clean_release();
    for spec in &mut files {
        spec.date = NOW - 500 * DAY;
    }
    // 30 KB of recovery for 15 MB of archive.
    files.truncate(7);
    files[6].segments = segments("thin", "example.invalid", 1, 300_000);
    let report = report_of(&nzb_xml(&[], &files));
    let set = report.layout.par2_sets[0].token;
    assert!(report.red_flags.contains(&RedFlag::ThinRecovery {
        set,
        permille: 20,
        oldest_age_days: Some(500),
    }));
}

#[test]
fn parser_diagnostics_reach_the_report() {
    let mut spec = file(
        "\"single.mkv\" yEnc (1/4)",
        vec![
            (3, 100, "c@example.invalid".to_string()),
            (1, 100, "a@example.invalid".to_string()),
            (1, 100, "a2@example.invalid".to_string()),
            (2, 100, "a@example.invalid".to_string()),
        ],
    );
    spec.date = NOW - 2 * DAY;
    let repeat = file(
        "\"single.copy.mkv\" yEnc (1/3)",
        vec![
            (1, 100, "a@example.invalid".to_string()),
            (3, 100, "c@example.invalid".to_string()),
        ],
    );
    let mut xml = String::from_utf8(nzb_xml(&[], &[spec, repeat])).unwrap();
    // One segment the parser must skip, and one file it must drop.
    xml = xml.replacen(
        "</segments>",
        "<segment bytes=\"x\" number=\"9\">bad@example.invalid</segment>\n</segments>",
        1,
    );
    xml = xml.replace(
        "</nzb>",
        "<file poster=\"p\" date=\"1\" subject=\"empty\"><groups/><segments/></file>\n</nzb>",
    );
    let report = report_of(xml.as_bytes());
    let first = &report.files[0];
    assert!(first.out_of_order);
    assert_eq!(first.duplicate_segments, 2);
    assert_eq!(first.malformed_segments, 1);
    assert_eq!(first.missing_segments, 2);
    assert_eq!(report.shape.files_without_segments, 1);
    assert_eq!(report.shape.shared_message_ids, 2);
    assert_eq!(report.layout.payload, PayloadKind::Direct);
    assert!(
        report
            .red_flags
            .contains(&RedFlag::DroppedFiles { files: 1 })
    );
    assert!(
        report
            .red_flags
            .contains(&RedFlag::MalformedSegments { count: 1 })
    );
}

#[test]
fn identical_files_are_reported_as_repeats() {
    let mut files = clean_release();
    let copy = FileSpec {
        subject: "\"other.name.bin\" yEnc (1/4)".to_string(),
        poster: "poster@example.invalid",
        date: NOW - DAY,
        groups: &["alt.binaries.example"],
        segments: files[1].segments.clone(),
    };
    files.push(copy);
    let report = report_of(&nzb_xml(&[], &files));
    assert_eq!(report.shape.duplicate_files, 1);
    assert_eq!(report.files[8].repeats, Some(report.files[1].token));
    assert!(
        report
            .red_flags
            .contains(&RedFlag::DuplicateFiles { files: 1 })
    );
}

#[test]
fn fingerprint_ignores_file_order_and_matches_hex_shape() {
    let files = clean_release();
    let forward = report_of(&nzb_xml(&[], &files));
    let reversed: Vec<_> = clean_release().into_iter().rev().collect();
    let backward = report_of(&nzb_xml(&[], &reversed));
    assert_eq!(forward.identity.fingerprint, backward.identity.fingerprint);
    let hex = forward.identity.fingerprint.to_hex();
    assert_eq!(hex.len(), 32);
    assert!(hex.bytes().all(|byte| byte.is_ascii_hexdigit()));

    let mut other = clean_release();
    other[0].segments[0].2 = "different@example.invalid".to_string();
    assert_ne!(
        forward.identity.fingerprint,
        report_of(&nzb_xml(&[], &other)).identity.fingerprint
    );
}

#[test]
fn poster_tool_needs_nearly_every_message_id() {
    let tool_files = |domain: &'static str| -> Vec<FileSpec<'static>> {
        (0..10u32)
            .map(|index| {
                file(
                    format!("\"x{index}.bin\" yEnc (1/1)"),
                    segments(&format!("t{index}"), domain, 1, 10),
                )
            })
            .collect()
    };
    assert_eq!(
        report_of(&nzb_xml(&[], &tool_files("nyuu"))).poster.tool,
        PosterTool::NyuuLike
    );
    let mut mixed = tool_files("ngPost");
    mixed[0].segments = segments("odd", "example.invalid", 1, 10);
    mixed[1].segments = segments("odd2", "example.invalid", 1, 10);
    let report = report_of(&nzb_xml(&[], &mixed));
    assert_eq!(report.poster.tool, PosterTool::Unknown);
    assert_eq!(report.poster.message_id_domains, DomainSpread::Few);
}

#[test]
fn text_fits_a_fenced_block() {
    // The largest report shape: many sets, flags, and anomalous files.
    let mut files = clean_release();
    for spec in &mut files {
        spec.segments.remove(0);
        spec.date = NOW - 5_000 * DAY;
    }
    for index in 0..20u32 {
        files.push(file(
            format!(
                "\"{:032x}.7z.{:03}\" yEnc (1/9) 12345678901",
                u64::from(index),
                index * 2 + 1
            ),
            segments(&format!("w{index}"), "example.invalid", 2, 400_000),
        ));
    }
    let report = report_of(&nzb_xml(&[("password", "pw")], &files));
    let text = report.with_weaver_version("9.99.999").to_text();
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines.len() <= 60, "{} lines:\n{text}", lines.len());
    for line in &lines {
        assert!(line.len() <= MAX_COLUMNS, "too wide: {line:?}");
        assert!(line.is_ascii(), "not ascii: {line:?}");
    }
    assert!(text.contains("weaver 9.99.999"));
    assert!(text.contains("more in the json"), "{text}");
}

#[test]
fn json_round_trips_through_a_value() {
    let report = report_of(&nzb_xml(&[], &clean_release()));
    let value: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
    assert_eq!(value["identity"]["schema_version"], REPORT_SCHEMA_VERSION);
    assert_eq!(value["layout"]["archive_sets"][0]["token"], "a1");
    assert_eq!(value["layout"]["archive_sets"][0]["scheme"], "partNN.rar");
    assert_eq!(value["layout"]["par2_sets"][0]["token"], "p1");
    assert_eq!(value["files"][0]["token"], "f01");
    assert_eq!(value["files"][0]["ext"], "rar");
    assert_eq!(value["poster"]["tool"], "unknown");
}

#[test]
fn subject_numbers_parse() {
    let numbers = parse_subject_numbers("Title [03/17] - \"a.rar\" yEnc (1/42) 31415926");
    assert_eq!(numbers.file_total, Some(17));
    assert_eq!(numbers.segment_total, Some(42));
    assert_eq!(numbers.declared_bytes, Some(31_415_926));

    let bare = parse_subject_numbers("[PRiVATE]-[grp]-[a.bin]-[1/10] - \"\" yEnc");
    assert_eq!(bare.file_total, Some(10));
    assert_eq!(bare.segment_total, None);

    let none = parse_subject_numbers("no numbers here");
    assert_eq!(none, Default::default());
}

#[test]
fn name_classes() {
    assert_eq!(classify_name("a.part007.rar"), NameShape::PartNn);
    assert_eq!(classify_name("a.rar"), NameShape::Rar);
    assert_eq!(classify_name("a.r17"), NameShape::RNn);
    assert_eq!(classify_name("a.s02"), NameShape::SNn);
    assert_eq!(classify_name("a.7z"), NameShape::SevenZ);
    assert_eq!(classify_name("a.7z.004"), NameShape::SevenZNnn);
    assert_eq!(classify_name("a.004"), NameShape::Nnn);
    assert_eq!(classify_name("a.vol03+04.par2"), NameShape::Par2Volume);
    assert_eq!(classify_name("a.PAR2"), NameShape::Par2Index);
    assert_eq!(classify_name("a.vol0+1.par3"), NameShape::Par3Volume);
    assert_eq!(classify_name("a.mkv"), NameShape::Plain);
    assert_eq!(classify_name("abc"), NameShape::Bare);

    assert_eq!(file_ext("A.MKV").as_str(), "mkv");
    assert_eq!(file_ext("a.r05").as_str(), "rNN");
    assert_eq!(file_ext("a.7z.001").as_str(), "NNN");
    assert_eq!(file_ext("a.weird").as_str(), "other");
    assert_eq!(file_ext("abc").as_str(), "none");

    assert_eq!(stem_class(&"0123456789abcdef".repeat(2)), StemClass::Hex32);
    assert_eq!(stem_class("0123456789abcdef01"), StemClass::Hex);
    assert_eq!(stem_class("Q7rT2mX9pL4vN8kZ"), StemClass::Random);
    assert_eq!(stem_class("20310101"), StemClass::Numeric);
    assert_eq!(stem_class("Invented.Title.2031"), StemClass::Readable);
    assert_eq!(stem_class("plainlowercasewordsonly"), StemClass::Readable);
}
