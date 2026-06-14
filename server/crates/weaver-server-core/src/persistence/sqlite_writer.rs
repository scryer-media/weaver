use std::sync::Arc;

pub(crate) type SqliteWriterGate = Arc<tokio::sync::Mutex<()>>;

pub(crate) fn new_writer_gate() -> SqliteWriterGate {
    Arc::new(tokio::sync::Mutex::new(()))
}
