// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{Cursor, Write};

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

use super::*;

const PASSWORD: &str = "a-test-passphrase";

const MINIMAL: &str = r#"
version = 1
name = "Example Tracker"
revision = "2026-10-25"
host_pattern = "*.Tracker.Example."
"#;

fn with(extra: &str) -> String {
    format!("{MINIMAL}\n{extra}")
}

fn refused(bytes: &[u8], password: Option<&str>) -> String {
    read(bytes, password).expect_err("the file is refused")
}

// A certificate for `subject` and its key.
fn identity(subject: &str) -> TrackerCertificate {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, subject);
    TrackerCertificate {
        certificate_pem: params.self_signed(&key).unwrap().pem(),
        private_key_pem: key.serialize_pem(),
    }
}

fn inline_ssl(ssl: &TrackerCertificate) -> String {
    format!(
        "[ssl]\ncertificate_pem = '''\n{}'''\nprivate_key_pem = '''\n{}'''\n",
        ssl.certificate_pem, ssl.private_key_pem
    )
}

const FILE_SSL: &str = "[ssl]\ncertificate_file = \"cert.pem\"\nprivate_key_file = \"key.pem\"\n";

// A 7z archive of `entries`, its content and, when asked, its header
// enciphered under `password`, with a cheap key derivation.
fn sevenz(entries: &[(&str, &[u8])], password: Option<&str>, encrypt_header: bool) -> Vec<u8> {
    let methods = match password {
        Some(password) => vec![
            EncoderConfiguration::from(
                AesEncoderOptions::new(Password::new(password)).with_num_cycles_power(4),
            ),
            EncoderConfiguration::new(EncoderMethod::COPY),
        ],
        None => {
            let mut options = Lzma2Options::from_level(1);
            options.set_dictionary_size(64 << 10);
            vec![EncoderConfiguration::from(options)]
        }
    };
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(methods);
    writer.set_encrypt_header(encrypt_header);
    for (name, bytes) in entries {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(Cursor::new(bytes.to_vec())),
            )
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

enum ZipEntry<'a> {
    File(&'a str, &'a [u8]),
    Link(&'a str, &'a str),
}

fn zip_with(entries: &[ZipEntry<'_>], password: Option<&str>, deflate: bool) -> Vec<u8> {
    let mut writer = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
    let method = if deflate {
        ::zip::CompressionMethod::Deflated
    } else {
        ::zip::CompressionMethod::Stored
    };
    let options = ::zip::write::SimpleFileOptions::default().compression_method(method);
    let options = match password {
        Some(password) => options.with_aes_encryption(::zip::AesMode::Aes256, password),
        None => options,
    };
    for entry in entries {
        match entry {
            ZipEntry::File(name, bytes) => {
                writer.start_file(*name, options).unwrap();
                writer.write_all(bytes).unwrap();
            }
            ZipEntry::Link(name, target) => writer.add_symlink(*name, *target, options).unwrap(),
        }
    }
    writer.finish().unwrap().into_inner()
}

fn zip(entries: &[(&str, &[u8])], password: Option<&str>) -> Vec<u8> {
    let entries: Vec<_> = entries
        .iter()
        .map(|(name, bytes)| ZipEntry::File(name, bytes))
        .collect();
    zip_with(&entries, password, false)
}

// A RAR 2.9 archive of stored members, each enciphered under `password`
// when one is given, written here because the workspace has no RAR writer.
// `volume` marks it the first volume of several.
fn rar(entries: &[(&str, &[u8])], password: Option<&str>, volume: bool) -> Vec<u8> {
    let crc =
        |bytes: &[u8]| crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32;
    let block = |kind: u8, flags: u16, body: &[u8]| {
        let mut rest = vec![kind];
        rest.extend(flags.to_le_bytes());
        rest.extend(((7 + body.len()) as u16).to_le_bytes());
        rest.extend(body);
        let mut out = (crc(&rest) as u16).to_le_bytes().to_vec();
        out.extend(rest);
        out
    };
    let mut out = b"Rar!\x1a\x07\x00".to_vec();
    out.extend(block(0x73, u16::from(volume), &[0; 6]));
    let salt = [7u8; 8];
    for (name, bytes) in entries {
        let packed = match password {
            Some(password) => {
                let (key, iv) = unrar_rs::rar4_derive_key(password, Some(&salt));
                let mut cipher = bytes.to_vec();
                cipher.resize(bytes.len().next_multiple_of(16), 0);
                unrar_rs::encrypt_cipher_range_rar4(&key, &iv, &mut cipher).unwrap();
                cipher
            }
            None => bytes.to_vec(),
        };
        let mut flags = 0x8000u16;
        if password.is_some() {
            flags |= 0x0004 | 0x0400;
        }
        if volume {
            flags |= 0x0002;
        }
        let mut body = Vec::new();
        body.extend((packed.len() as u32).to_le_bytes());
        body.extend((bytes.len() as u32).to_le_bytes());
        body.push(2);
        body.extend(crc(bytes).to_le_bytes());
        body.extend(0x5a21_0000u32.to_le_bytes());
        body.push(29);
        body.push(0x30);
        body.extend((name.len() as u16).to_le_bytes());
        body.extend(0x20u32.to_le_bytes());
        body.extend(name.as_bytes());
        if password.is_some() {
            body.extend(salt);
        }
        out.extend(block(0x74, flags, &body));
        out.extend(packed);
    }
    out.extend(block(0x7b, 0x4000 | u16::from(volume), &[]));
    out
}

// A plain file sets what it may, as it says it; repeats and blanks in its
// password list are dropped and each password is kept as written.
#[test]
fn a_plain_tracker_file_reads() {
    let ssl = identity("user-1");
    let text = with(&format!(
        r#"
[required]
dht = false
pex = false
lsd = false
encryption = "require"

[recommended]
seeding = {{ ratio_goal = 1, seed_time_goal_minutes = 4320, inactive_goal_minutes = 60, mode = "all", action = "stop" }}

[archive_passwords]
values = ["first", " inner space ", "first", "   ", ""]

{}"#,
        inline_ssl(&ssl)
    ));
    let file = read(text.as_bytes(), None).unwrap();
    assert_eq!(file.name, "Example Tracker");
    assert_eq!(file.revision, "2026-10-25");
    assert_eq!(file.host_pattern, "*.tracker.example");
    assert_eq!(
        file.required,
        Terms {
            dht: Some(false),
            pex: Some(false),
            lsd: Some(false),
            encryption: Some(TorrentEncryption::Require),
            seeding: None,
        }
    );
    assert_eq!(
        file.recommended.seeding,
        Some(SeedingTerms {
            ratio_goal: Leaf::Set(1.0),
            seed_time_goal_minutes: Leaf::Set(4320),
            inactive_goal_minutes: Leaf::Set(60),
            mode: Leaf::Set(SeedingGoalMode::All),
            action: Leaf::Set(SeedingAction::Stop),
        })
    );
    assert_eq!(
        file.archive_passwords,
        Some(vec!["first".to_owned(), " inner space ".to_owned()])
    );
    assert_eq!(file.ssl, Some(ssl));

    let debug = format!("{file:?}");
    assert!(!debug.contains("first"), "{debug}");
    assert!(!debug.contains("BEGIN"), "{debug}");

    let bare = read(MINIMAL.as_bytes(), None).unwrap();
    assert_eq!(bare.required, Terms::default());
    assert_eq!(bare.archive_passwords, None);
    assert_eq!(bare.ssl, None);
    let cleared = read(with("[archive_passwords]\nvalues = []").as_bytes(), None).unwrap();
    assert_eq!(cleared.archive_passwords, Some(Vec::new()));
}

// Every key the file may not set is refused by name, as is a loosening, a
// required seeding goal, and a key in both sections.
#[test]
fn a_tracker_file_refuses_what_it_may_not_set() {
    for (extra, named) in [
        ("client = \"qbittorrent\"", "client"),
        ("[required]\nclient = \"qbittorrent\"", "required.client"),
        ("[required]\ntorrent_route = true", "required.torrent_route"),
        ("[recommended]\nforced = true", "recommended.forced"),
        (
            "[recommended]\nnever_pause = true",
            "recommended.never_pause",
        ),
        ("[required]\nunpack = false", "required.unpack"),
        (
            "[recommended]\nupload_limit = 100",
            "recommended.upload_limit",
        ),
        (
            "[recommended]\nseeding = { max_upload_bps = 100 }",
            "recommended.seeding.max_upload_bps",
        ),
        (
            "[required]\nseeding = { ratio_goal = 1.0 }",
            "required.seeding",
        ),
        ("[required]\nencryption = \"prefer\"", "\"prefer\""),
        ("[recommended]\nencryption = \"prefer\"", "\"prefer\""),
        ("[required]\ndht = \"no\"", "required.dht"),
        (
            "[archive_passwords]\nfile = \"x\"",
            "archive_passwords.file",
        ),
        (
            "[ssl]\ncertificate_path = \"/etc/x\"",
            "ssl.certificate_path",
        ),
        ("[recommended]\nseeding = { mode = \"most\" }", "\"most\""),
    ] {
        let error = refused(with(extra).as_bytes(), None);
        assert!(error.contains(named), "{extra}: {error}");
    }

    let error = refused(
        with("[required]\npex = false\n[recommended]\npex = true").as_bytes(),
        None,
    );
    assert_eq!(error, "pex is in both [required] and [recommended].");

    for (text, named) in [
        (MINIMAL.replace("version = 1", "version = 2"), "version"),
        (MINIMAL.replace("version = 1", ""), "version"),
        (
            MINIMAL.replace("Example Tracker", &"n".repeat(129)),
            "128 bytes",
        ),
        (MINIMAL.replace("Example Tracker", " "), "name is empty"),
        (MINIMAL.replace("2026-10-25", &"r".repeat(65)), "64 bytes"),
        (
            MINIMAL.replace("*.Tracker.Example.", "tracker example"),
            "host_pattern",
        ),
        ("not = [toml".to_owned(), "not TOML"),
    ] {
        let error = refused(text.as_bytes(), None);
        assert!(error.contains(named), "{text}: {error}");
    }

    let many: Vec<String> = (0..=MAX_ARCHIVE_PASSWORDS)
        .map(|n| format!("\"password-{n}\""))
        .collect();
    let error = refused(
        with(&format!(
            "[archive_passwords]\nvalues = [{}]",
            many.join(",")
        ))
        .as_bytes(),
        None,
    );
    assert!(error.contains("more than 10 passwords"), "{error}");
    let long = format!("\"{}\"", "p".repeat(MAX_ARCHIVE_PASSWORD_BYTES + 1));
    let error = refused(
        with(&format!("[archive_passwords]\nvalues = [{long}]")).as_bytes(),
        None,
    );
    assert!(error.contains("larger than 64 KiB"), "{error}");
}

// An identity inline and by archive file at once, half of one, a file named
// from a plain file, and a key of another certificate are each refused.
#[test]
fn a_tracker_files_ssl_is_one_whole_pair() {
    let ssl = identity("user-1");
    let other = identity("user-2");
    let mixed = format!(
        "[ssl]\ncertificate_pem = '''\n{}'''\nprivate_key_file = \"key.pem\"\n",
        ssl.certificate_pem
    );
    let archive = sevenz(
        &[
            ("tracker.toml", with(&mixed).as_bytes()),
            ("key.pem", ssl.private_key_pem.as_bytes()),
        ],
        None,
        false,
    );
    let error = refused(&archive, None);
    assert!(error.contains("only one"), "{error}");

    let error = refused(with(FILE_SSL).as_bytes(), None);
    assert!(error.contains("not in an archive"), "{error}");

    let half = format!("[ssl]\ncertificate_pem = '''\n{}'''\n", ssl.certificate_pem);
    let error = refused(with(&half).as_bytes(), None);
    assert!(error.contains("both"), "{error}");

    let mismatched = TrackerCertificate {
        certificate_pem: ssl.certificate_pem.clone(),
        private_key_pem: other.private_key_pem.clone(),
    };
    let error = refused(with(&inline_ssl(&mismatched)).as_bytes(), None);
    assert!(
        error.starts_with("The [ssl] certificate is refused"),
        "{error}"
    );
    assert!(!error.contains("BEGIN"), "{error}");

    let archive = sevenz(
        &[
            ("tracker.toml", with(FILE_SSL).as_bytes()),
            ("cert.pem", ssl.certificate_pem.as_bytes()),
            ("key.pem", other.private_key_pem.as_bytes()),
        ],
        None,
        false,
    );
    let error = refused(&archive, None);
    assert!(
        error.starts_with("The [ssl] certificate is refused"),
        "{error}"
    );

    let outside = "[ssl]\ncertificate_file = \"../cert.pem\"\nprivate_key_file = \"key.pem\"\n";
    let archive = zip(
        &[
            ("tracker.toml", with(outside).as_bytes()),
            ("key.pem", ssl.private_key_pem.as_bytes()),
        ],
        None,
    );
    let error = refused(&archive, None);
    assert!(error.contains("not a path outside it"), "{error}");
}

// Each archive kind reads plain and enciphered, the identity coming from
// two of its members; the approved path is a 7z with its header enciphered.
#[test]
fn a_tracker_file_reads_from_each_kind_of_archive() {
    let ssl = identity("user-1");
    let text = with(&format!("[required]\npex = false\n{FILE_SSL}"));
    let entries: [(&str, &[u8]); 3] = [
        ("tracker.toml", text.as_bytes()),
        ("cert.pem", ssl.certificate_pem.as_bytes()),
        ("key.pem", ssl.private_key_pem.as_bytes()),
    ];
    for (kind, bytes, password) in [
        ("7z", sevenz(&entries, None, false), None),
        (
            "7z enciphered",
            sevenz(&entries, Some(PASSWORD), false),
            Some(PASSWORD),
        ),
        (
            "7z, header enciphered",
            sevenz(&entries, Some(PASSWORD), true),
            Some(PASSWORD),
        ),
        ("zip", zip(&entries, None), None),
        (
            "zip enciphered",
            zip(&entries, Some(PASSWORD)),
            Some(PASSWORD),
        ),
        ("rar", rar(&entries, None, false), None),
        (
            "rar enciphered",
            rar(&entries, Some(PASSWORD), false),
            Some(PASSWORD),
        ),
    ] {
        let file = read(&bytes, password).unwrap_or_else(|error| panic!("{kind}: {error}"));
        assert_eq!(file.required.pex, Some(false), "{kind}");
        assert_eq!(file.ssl.as_ref(), Some(&ssl), "{kind}");
    }
}

// An enciphered archive with no password asks for one; with the wrong one it
// says so, and never repeats what was tried.
#[test]
fn a_wrong_password_is_named_and_never_repeated() {
    let entries: [(&str, &[u8]); 1] = [("tracker.toml", MINIMAL.as_bytes())];
    let tried = "tried-and-wrong";
    for (kind, bytes) in [
        ("7z", sevenz(&entries, Some(PASSWORD), false)),
        (
            "7z, header enciphered",
            sevenz(&entries, Some(PASSWORD), true),
        ),
        ("zip", zip(&entries, Some(PASSWORD))),
        ("rar", rar(&entries, Some(PASSWORD), false)),
    ] {
        let error = refused(&bytes, Some(tried));
        assert_eq!(error, WRONG_PASSWORD, "{kind}");
        let error = refused(&bytes, None);
        assert_eq!(error, NEEDS_PASSWORD, "{kind}");
    }
}

// Too large an upload, an archive that decodes past the bound or declares
// it, and too many entries are refused before the bound is passed.
#[test]
fn a_tracker_file_is_refused_past_its_bounds() {
    let error = refused(&vec![b'#'; MAX_UPLOAD_BYTES + 1], None);
    assert!(error.contains("larger than 1024 KiB"), "{error}");

    let zeros = vec![0u8; MAX_DECODED_BYTES as usize + 1];
    let entries = [
        ZipEntry::File("tracker.toml", MINIMAL.as_bytes()),
        ZipEntry::File("padding.bin", &zeros),
    ];
    let bomb = zip_with(&entries, None, true);
    assert!(bomb.len() < MAX_UPLOAD_BYTES);
    let error = refused(&bomb, None);
    assert!(error.contains("decodes to more than 1024 KiB"), "{error}");

    let bomb = sevenz(
        &[
            ("tracker.toml", MINIMAL.as_bytes()),
            ("padding.bin", &zeros),
        ],
        None,
        false,
    );
    assert!(bomb.len() < MAX_UPLOAD_BYTES);
    let error = refused(&bomb, None);
    assert!(error.contains("more than a tracker file may"), "{error}");

    let names: Vec<String> = (0..=MAX_ENTRIES).map(|n| format!("{n}.txt")).collect();
    let many: Vec<(&str, &[u8])> = names
        .iter()
        .map(|name| (name.as_str(), &b"x"[..]))
        .collect();
    for (kind, bytes) in [
        ("7z", sevenz(&many, None, false)),
        ("zip", zip(&many, None)),
        ("rar", rar(&many, None, false)),
    ] {
        let error = refused(&bytes, None);
        assert!(error.contains("16"), "{kind}: {error}");
    }
}

// Two TOMLs, none, a member the TOML does not name, a path out of the
// archive, a link and an archive inside the archive are each refused.
#[test]
fn a_tracker_archive_holds_one_toml_and_what_it_names() {
    let toml = MINIMAL.as_bytes();
    let error = refused(&zip(&[("a.toml", toml), ("b.TOML", toml)], None), None);
    assert_eq!(error, "The archive holds more than one TOML file.");
    let error = refused(&zip(&[("readme.txt", b"hi")], None), None);
    assert_eq!(error, "The archive holds no TOML file.");
    let error = refused(
        &sevenz(&[("tracker.toml", toml), ("extra.pem", b"x")], None, false),
        None,
    );
    assert_eq!(
        error,
        "The archive holds extra.pem, which the tracker file does not name."
    );

    for (kind, bytes) in [
        ("7z", sevenz(&[("../tracker.toml", toml)], None, false)),
        ("zip", zip(&[("../tracker.toml", toml)], None)),
        ("zip absolute", zip(&[("/etc/tracker.toml", toml)], None)),
        ("rar", rar(&[("../tracker.toml", toml)], None, false)),
    ] {
        let error = refused(&bytes, None);
        assert!(error.contains("a path outside it"), "{kind}: {error}");
    }

    let linked = zip_with(
        &[
            ZipEntry::File("tracker.toml", toml),
            ZipEntry::Link("key.pem", "/etc/ssl/private/key.pem"),
        ],
        None,
        false,
    );
    let error = refused(&linked, None);
    assert_eq!(error, "The archive's key.pem is a link.");

    let inner = zip(&[("tracker.toml", toml)], None);
    for name in ["inner.zip", "inner.bin"] {
        let error = refused(&zip(&[("tracker.toml", toml), (name, &inner)], None), None);
        assert_eq!(
            error,
            format!("The archive's {name} is an archive inside it.")
        );
    }
}

// One volume of several is refused, whatever the kind.
#[test]
fn a_tracker_archive_is_one_volume() {
    let entries: [(&str, &[u8]); 1] = [("tracker.toml", MINIMAL.as_bytes())];
    let whole = sevenz(&entries, None, false);
    let first = &whole[..whole.len() - 8];
    assert_eq!(refused(first, None), MULTI_VOLUME);
    let mut spanned = b"PK\x07\x08".to_vec();
    spanned.extend(zip(&entries, None));
    assert_eq!(refused(&spanned, None), MULTI_VOLUME);
    assert_eq!(refused(&rar(&entries, None, true), None), MULTI_VOLUME);
}

// No more than two archives decode at once; a plain file needs no place.
#[test]
fn no_more_than_two_tracker_archives_decode_at_once() {
    let archive = zip(&[("tracker.toml", MINIMAL.as_bytes())], None);
    let held: Vec<_> = (0..MAX_DECODING)
        .map(|_| DecodeSlot::take().unwrap())
        .collect();
    let error = refused(&archive, None);
    assert!(error.contains("being read already"), "{error}");
    read(MINIMAL.as_bytes(), None).unwrap();
    drop(held);
    read(&archive, None).unwrap();
}
