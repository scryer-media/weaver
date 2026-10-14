use super::*;
use crate::pipeline::extraction::ExtractionLimits;
use std::sync::Arc;

fn test_budget() -> (tempfile::TempDir, Arc<JobExtractionBudget>) {
    let dir = tempfile::TempDir::new().unwrap();
    let limits = Arc::new(ExtractionLimits {
        max_job_bytes: 1 << 40,
        max_member_bytes: 1 << 40,
        max_entries: 1_000,
        max_ratio: 100,
        max_seconds: 3_600,
        min_free_bytes: 1,
        max_memory_bytes: 1 << 30,
    });
    let budget = JobExtractionBudget::new(
        limits,
        dir.path().to_path_buf(),
        1 << 30,
        0,
        0,
        crate::operations::metrics::PipelineMetrics::new(),
    )
    .unwrap();
    (dir, budget)
}

#[test]
fn an_io_error_is_classed_by_the_typed_error_it_carries() {
    let rar = io::Error::other(unrar_rs::RarError::DataCrcMismatch {
        member: "member.bin".into(),
        expected: 1,
        actual: 2,
    });
    assert_eq!(io_error_class(&rar), ExtractionErrorClass::Recoverable);

    let unavailable = io::Error::other(unrar_rs::VolumeProviderError::Unavailable {
        volume: 2,
        reason: "pending".into(),
    });
    assert_eq!(
        io_error_class(&unavailable),
        ExtractionErrorClass::StaleTopology
    );

    let sevenz = io::Error::other(sevenz_turbo::Error::PasswordRequired);
    assert_eq!(
        io_error_class(&sevenz),
        ExtractionErrorClass::PasswordRequired
    );

    let zip = io::Error::from(zip::result::ZipError::InvalidPassword);
    assert_eq!(io_error_class(&zip), ExtractionErrorClass::PasswordRequired);

    let nested = io::Error::other(ExtractionError::structure_refused("names collide"));
    assert_eq!(
        io_error_class(&nested),
        ExtractionErrorClass::StructureRefused
    );
}

#[test]
fn descriptor_exhaustion_is_capacity_pressure_whatever_its_wording() {
    for code in [libc::EMFILE, libc::ENFILE] {
        assert_eq!(
            io_error_class(&io::Error::from_raw_os_error(code)),
            ExtractionErrorClass::CapacityPressure
        );
    }
    assert_eq!(
        io_error_class(&io::Error::from_raw_os_error(libc::EIO)),
        ExtractionErrorClass::Terminal
    );
}

#[test]
fn rar_errors_keep_their_routes() {
    use unrar_rs::RarError;
    let recoverable = [
        RarError::InvalidSignature,
        RarError::TruncatedHeader { offset: 1 },
        RarError::HeaderCrcMismatch {
            expected: 1,
            actual: 2,
        },
        RarError::PackedDataCrcMismatch {
            member: "member.bin".into(),
            volume: 0,
            expected: 1,
            actual: 2,
        },
    ];
    for error in &recoverable {
        assert_eq!(
            rar_error_class(error),
            ExtractionErrorClass::Recoverable,
            "{error:?}"
        );
    }
    let terminal = [
        RarError::Blake2Mismatch {
            member: "member.bin".into(),
        },
        RarError::TruncatedData { offset: 1 },
        RarError::UnsupportedFilter { filter_type: 9 },
    ];
    for error in &terminal {
        assert_eq!(
            rar_error_class(error),
            ExtractionErrorClass::Terminal,
            "{error:?}"
        );
    }
    assert_eq!(
        rar_error_class(&RarError::WrongPassword {
            member: "member.bin".into()
        }),
        ExtractionErrorClass::PasswordRequired
    );
}

#[test]
fn sevenz_errors_keep_their_routes() {
    use sevenz_turbo::{BlockErrorKind, Error};
    let block = |kind| Error::BlockDecode {
        block_index: 0,
        packed_offset: 32,
        kind,
        message: "detail".into(),
    };
    assert_eq!(
        sevenz_error_class(&block(BlockErrorKind::ChecksumMismatch)),
        ExtractionErrorClass::Recoverable
    );
    assert_eq!(
        sevenz_error_class(&block(BlockErrorKind::Corrupted)),
        ExtractionErrorClass::Recoverable
    );
    assert_eq!(
        sevenz_error_class(&block(BlockErrorKind::UnsupportedMethod)),
        ExtractionErrorClass::Terminal
    );
    assert_eq!(
        sevenz_error_class(&Error::ChecksumVerificationFailed),
        ExtractionErrorClass::Recoverable
    );
    assert_eq!(
        sevenz_error_class(&Error::PasswordRequired),
        ExtractionErrorClass::PasswordRequired
    );
    assert_eq!(
        sevenz_error_class(&Error::Io(
            io::Error::from_raw_os_error(libc::EMFILE),
            "".into()
        )),
        ExtractionErrorClass::CapacityPressure
    );
}

#[test]
fn zip_errors_keep_their_routes() {
    use zip::result::ZipError;
    assert_eq!(
        zip_error_class(&ZipError::InvalidPassword),
        ExtractionErrorClass::PasswordRequired
    );
    assert_eq!(
        zip_error_class(&ZipError::UnsupportedArchive("multi-disk")),
        ExtractionErrorClass::Terminal
    );
}

#[test]
fn a_worker_error_under_a_stopped_budget_is_the_budgets_rejection() {
    let (_dir, budget) = test_budget();
    let error = ExtractionError::recoverable("a read failed");
    assert!(
        error
            .clone()
            .settle(&budget)
            .is(ExtractionErrorClass::Recoverable)
    );

    let failure = budget.reject_unsafe_path("escapes the root");
    let settled = error.settle(&budget);
    assert!(settled.is(ExtractionErrorClass::Rejected));
    assert_eq!(
        settled.rejection().map(|r| r.reason()),
        Some(failure.reason())
    );
}

#[test]
fn a_content_policy_rejection_keeps_its_own_sentence() {
    let (_dir, budget) = test_budget();
    let failure = budget.reject_content_policy("an unwanted extension");
    let error = ExtractionError::from(failure.clone());
    assert!(error.is(ExtractionErrorClass::Rejected));
    assert_eq!(error.job_failure_reason(), failure.job_failure_reason());
}
