use super::*;
use crate::jobs::ids::NzbFileId;

#[test]
fn event_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PipelineEvent>();
}

#[test]
fn verify_status_equality() {
    assert_eq!(FileVerifyStatus::Intact, FileVerifyStatus::Intact);
    assert_ne!(
        FileVerifyStatus::Intact,
        FileVerifyStatus::Damaged {
            bad_slices: 1,
            total_slices: 10
        }
    );
}

#[test]
fn event_debug_format() {
    let event = PipelineEvent::JobCreated {
        job_id: JobId(1),
        name: "test".into(),
        total_files: 5,
        total_bytes: 1024,
    };
    let debug = format!("{event:?}");
    assert!(debug.contains("JobCreated"));
    assert!(debug.contains("test"));
}

#[test]
fn event_clone() {
    let event = PipelineEvent::FileComplete {
        file_id: NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        },
        filename: "test.rar".into(),
        total_bytes: 1_000_000,
    };
    let cloned = event.clone();
    match cloned {
        PipelineEvent::FileComplete {
            filename,
            total_bytes,
            ..
        } => {
            assert_eq!(filename, "test.rar");
            assert_eq!(total_bytes, 1_000_000);
        }
        _ => panic!("wrong variant"),
    }
}
