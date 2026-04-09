use crate::runtime::log_buffer::LogRingBuffer;

pub fn snapshot_service_logs(buffer: &LogRingBuffer, limit: i32) -> Vec<String> {
    let clamped = limit.clamp(1, 2000) as usize;
    buffer.snapshot(clamped)
}
