use super::*;
use crate::jobs::{ArchivePasswordCandidate, ArchivePasswordSource};

fn archive(members: &[(&str, Option<&str>)]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, password) in members {
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        let options = match password {
            Some(password) => options
                .with_aes_encryption_and_salt(password.as_bytes(), zip::AesSalt::Aes128([7; 8])),
            None => options,
        };
        writer.start_file(*name, options).unwrap();
        writer
            .write_all(b"synthetic payload for password integrity testing")
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn candidates(values: impl IntoIterator<Item = String>) -> Vec<ArchivePasswordCandidate> {
    values
        .into_iter()
        .map(|value| ArchivePasswordCandidate::new(ArchivePasswordSource::Explicit, value))
        .collect()
}

fn sevenzip(password: Option<&str>, encrypt_header: bool) -> Vec<u8> {
    use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration};
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    let mut methods = vec![EncoderConfiguration::from(Lzma2Options::from_level(1))];
    if let Some(password) = password {
        methods.push(
            AesEncoderOptions {
                password: sevenz_turbo::Password::new(password),
                iv: [11; 16],
                salt: [19; 16],
                num_cycles_power: 8,
            }
            .into(),
        );
    }
    writer.set_content_methods(methods);
    writer.set_encrypt_header(encrypt_header);
    writer
        .push_archive_entry(
            ArchiveEntry::new_file("synthetic.mkv"),
            Some(Cursor::new(b"synthetic archive member".to_vec())),
        )
        .unwrap();
    writer.finish().unwrap().into_inner()
}

fn extract_zip_bytes(
    bytes: Vec<u8>,
    values: &[&str],
) -> Result<(Vec<String>, Option<String>), String> {
    let output = TempDir::new().unwrap();
    let (root, budget) = test_extraction_security(output.path());
    let (events, _) = tokio::sync::broadcast::channel(32);
    extract_zip_stream_candidates(
        Cursor::new(bytes),
        &root,
        &budget,
        &candidates(values.iter().map(|value| value.to_string())),
        &events,
        JobId(1),
        "synthetic.zip",
        None,
    )
}

#[test]
fn plaintext_zip_has_no_validated_password() {
    let (members, selected) =
        extract_zip_bytes(archive(&[("plain.mkv", None)]), &["unused"]).unwrap();
    assert_eq!(members, ["plain.mkv"]);
    assert_eq!(selected, None);
}

#[test]
fn mixed_zip_reaches_late_password_and_reuses_winner() {
    let values = (0..18)
        .map(|index| format!("synthetic-wrong-{index}"))
        .chain(["synthetic-right".into(), "unused".into()])
        .collect::<Vec<_>>();
    let (members, selected) = extract_zip_bytes(
        archive(&[
            ("plain.mkv", None),
            ("secret.srt", Some("synthetic-right")),
            ("second.srt", Some("synthetic-right")),
        ]),
        &values.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(members.len(), 3);
    assert_eq!(selected.as_deref(), Some("synthetic-right"));
}

#[test]
fn zip_integrity_failure_does_not_select_a_later_password() {
    let mut bytes = archive(&[("secret.mkv", Some("synthetic-right"))]);
    let offset = {
        let mut reader = zip::ZipArchive::new(Cursor::new(&bytes)).unwrap();
        reader.by_index_raw(0).unwrap().data_start().unwrap() as usize + 10
    };
    bytes[offset] ^= 1;
    let error =
        extract_zip_bytes(bytes, &["synthetic-right", "unused-after-corruption"]).unwrap_err();
    assert!(!error.starts_with("WEAVER_PASSWORD_REQUIRED:"));
}

#[test]
fn zip_exhaustion_has_password_classification() {
    let error =
        extract_zip_bytes(archive(&[("secret.mkv", Some("right"))]), &["wrong"]).unwrap_err();
    assert!(error.starts_with("WEAVER_PASSWORD_REQUIRED:"));
}

#[test]
fn zip_without_candidates_requires_a_password() {
    let error = extract_zip_bytes(archive(&[("secret.mkv", Some("right"))]), &[]).unwrap_err();
    assert!(error.starts_with("WEAVER_PASSWORD_REQUIRED:"));
}

#[test]
fn zip_password_whitespace_is_literal() {
    let (_, selected) = extract_zip_bytes(
        archive(&[("secret.mkv", Some(" right "))]),
        &["right", " right "],
    )
    .unwrap();
    assert_eq!(selected.as_deref(), Some(" right "));
}

#[test]
fn zip_members_can_have_distinct_passwords() {
    let (members, selected) = extract_zip_bytes(
        archive(&[("first", Some("first-key")), ("second", Some("second-key"))]),
        &["first-key", "second-key"],
    )
    .unwrap();
    assert_eq!(members.len(), 2);
    assert_eq!(selected.as_deref(), Some("second-key"));
}

#[test]
fn malformed_zip_is_not_a_password_failure() {
    let error = extract_zip_bytes(vec![0; 50], &["unused"]).unwrap_err();
    assert!(!error.starts_with("WEAVER_PASSWORD_REQUIRED:"));
}

#[test]
fn zip_output_io_failure_is_not_a_password_failure() {
    let output = TempDir::new().unwrap();
    fs::write(output.path().join("blocked"), b"existing").unwrap();
    let (root, budget) = test_extraction_security(output.path());
    let (events, _) = tokio::sync::broadcast::channel(32);
    let error = extract_zip_stream_candidates(
        Cursor::new(archive(&[("blocked/secret", Some("right"))])),
        &root,
        &budget,
        &candidates(["right".into(), "unused".into()]),
        &events,
        JobId(1),
        "synthetic.zip",
        None,
    )
    .unwrap_err();
    assert!(!error.starts_with("WEAVER_PASSWORD_REQUIRED:"));
}

fn check_sevenzip(password: Option<&str>, header: bool) {
    let bytes = sevenzip(password, header);
    let output = TempDir::new().unwrap();
    let (root, budget) = test_extraction_security(output.path());
    let context = conventional_7z_context(output.path(), root, budget, &bytes, 1);
    let result = extract_7z_with_password_candidates(
        context,
        &candidates(["synthetic-right".into()]),
        || Ok(Cursor::new(bytes.clone())),
    )
    .unwrap();
    assert_eq!(result.selected_password.as_deref(), password);
    assert_eq!(
        fs::read(output.path().join("synthetic.mkv")).unwrap(),
        b"synthetic archive member"
    );
}

#[test]
fn sevenzip_plaintext_has_no_validated_password() {
    check_sevenzip(None, false);
}
#[test]
fn sevenzip_encrypted_data_records_validated_password() {
    check_sevenzip(Some("synthetic-right"), false);
}
#[test]
fn sevenzip_encrypted_headers_record_validated_password() {
    check_sevenzip(Some("synthetic-right"), true);
}

#[test]
fn sevenzip_stream_records_only_a_validated_password() {
    for (password, header) in [
        (None, false),
        (Some(" synthetic-Ω "), false),
        (Some(" synthetic-Ω "), true),
    ] {
        let bytes = sevenzip(password, header);
        let output = TempDir::new().unwrap();
        let (root, budget) = test_extraction_security(output.path());
        let mut context = conventional_7z_context(output.path(), root, budget, &bytes, 1);
        context.password = sevenz_turbo::Password::new(" synthetic-Ω ");
        let result = extract_7z_stream(&context, || Ok(Cursor::new(bytes.clone()))).unwrap();
        assert_eq!(result.selected_password.as_deref(), password);
        assert_eq!(
            fs::read(output.path().join("synthetic.mkv")).unwrap(),
            b"synthetic archive member"
        );
    }
}

#[test]
fn sevenzip_ambiguous_integrity_stops_before_output() {
    for header in [false, true] {
        let bytes = sevenzip(Some("synthetic-right"), header);
        let output = TempDir::new().unwrap();
        let (root, budget) = test_extraction_security(output.path());
        let context = conventional_7z_context(output.path(), root, budget, &bytes, 1);
        let result = extract_7z_with_password_candidates(
            context,
            &candidates(["synthetic-wrong".into(), "synthetic-right".into()]),
            || Ok(Cursor::new(bytes.clone())),
        );
        assert!(result.is_err());
        assert!(!output.path().join("synthetic.mkv").exists());
    }
}

#[test]
fn sevenzip_missing_password_is_classified() {
    let bytes = sevenzip(Some("synthetic-right"), true);
    let output = TempDir::new().unwrap();
    let (root, budget) = test_extraction_security(output.path());
    let context = conventional_7z_context(output.path(), root, budget, &bytes, 1);
    let error =
        extract_7z_with_password_candidates(context, &[], || Ok(Cursor::new(bytes.clone())))
            .err()
            .unwrap();
    assert!(error.starts_with("WEAVER_PASSWORD_REQUIRED:"), "{error}");
}

#[test]
fn sevenzip_io_failure_stops_at_first_open() {
    let output = TempDir::new().unwrap();
    let (root, budget) = test_extraction_security(output.path());
    let context = conventional_7z_context(output.path(), root, budget, &sevenzip(None, false), 1);
    let mut opens = 0;
    let error = extract_7z_with_password_candidates(
        context,
        &candidates(["first".into(), "second".into()]),
        || {
            opens += 1;
            Err::<Cursor<Vec<u8>>, _>("synthetic I/O failure".into())
        },
    )
    .err()
    .unwrap();
    assert_eq!(error, "synthetic I/O failure");
    assert_eq!(opens, 1);
}
