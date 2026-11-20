// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_settings::settings::archive_passwords::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::model::ArchivePasswordSource;
    use crate::persistence::encryption::is_encrypted;
    use crate::settings::queries::SettingsQueries;
    use crate::{Database, StateError};

    #[test]
    fn archive_passwords_are_encrypted_and_files_refresh() {
        let db = Database::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("passwords.txt");
        std::fs::write(&path, " synthetic \r\nfile\nfile\n").unwrap();
        db.save_archive_password_settings(
            Some(vec!["enc:v1:literal".into(), " synthetic ".into()]),
            Some(Some(path.to_str().unwrap().into())),
        )
        .unwrap();
        let stored = db.get_setting(PASSWORDS).unwrap().unwrap();
        assert!(is_encrypted(&stored));
        assert!(!stored.contains("synthetic"));
        let candidates = db.global_archive_password_candidates().unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.value())
                .collect::<Vec<_>>(),
            ["enc:v1:literal", " synthetic ", "file"]
        );
        assert!(!format!("{candidates:?}").contains("synthetic"));
        std::fs::write(&path, "replacement\n").unwrap();
        assert_eq!(
            db.global_archive_password_candidates()
                .unwrap()
                .last()
                .unwrap()
                .value(),
            "replacement"
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(db.global_archive_password_candidates().unwrap().len(), 2);
        db.save_archive_password_settings(Some(vec![]), Some(None))
            .unwrap();
        assert_eq!(db.archive_password_settings().unwrap(), (false, None));
    }

    #[test]
    fn global_archive_passwords_are_bounded_at_fifty_across_both_sources() {
        let db = Database::open_in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("passwords.txt");
        let numbered = |prefix: &str, count: usize| -> Vec<String> {
            (0..count)
                .map(|index| format!("{prefix}-{index}"))
                .collect()
        };

        let too_many = numbered("settings", MAX_ARCHIVE_PASSWORDS + 1);
        let error = db
            .save_archive_password_settings(Some(too_many), None)
            .unwrap_err();
        assert!(matches!(error, StateError::Conflict(_)));

        // Duplicates and blanks are dropped before the count is taken.
        let mut padded = numbered("settings", MAX_ARCHIVE_PASSWORDS);
        padded.push("settings-0".into());
        padded.push("   ".into());
        db.save_archive_password_settings(Some(padded), None)
            .unwrap();
        assert_eq!(
            db.global_archive_password_candidates().unwrap().len(),
            MAX_ARCHIVE_PASSWORDS
        );

        // The file fills what the Settings list leaves, and no more.
        std::fs::write(&path, numbered("file", 40).join("\n")).unwrap();
        db.save_archive_password_settings(
            Some(numbered("settings", 30)),
            Some(Some(path.to_str().unwrap().into())),
        )
        .unwrap();
        let loaded = db.global_archive_password_candidates().unwrap();
        assert_eq!(loaded.len(), 70);
        let candidates = crate::pipeline::assemble_archive_password_candidates(None, &[], &loaded);
        assert_eq!(candidates.len(), MAX_ARCHIVE_PASSWORDS);
        assert_eq!(
            candidates
                .iter()
                .filter(|candidate| candidate.source() == ArchivePasswordSource::PasswordFile)
                .count(),
            MAX_ARCHIVE_PASSWORDS - 30
        );
        assert_eq!(candidates.last().unwrap().value(), "file-19");
    }
}
