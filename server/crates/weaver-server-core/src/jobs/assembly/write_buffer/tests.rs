use super::*;

#[test]
fn sequential_inserts_return_immediately() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    // Insert segments in perfect order.
    buf.insert(0, vec![0u8; 1000]);
    let ready = buf.drain_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].0, 0);
    assert_eq!(ready[0].1.len(), 1000);

    buf.insert(1000, vec![1u8; 500]);
    let ready = buf.drain_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].0, 1000);

    buf.insert(1500, vec![2u8; 200]);
    let ready = buf.drain_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].0, 1500);

    // Nothing should remain buffered.
    let flushed = buf.flush_all();
    assert!(flushed.is_empty());
}

#[test]
fn out_of_order_buffers_until_gap_filled() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    // Insert segment 2 first (offset 2000), then segment 1 (offset 1000).
    // Neither can be written yet because segment 0 (offset 0) is missing.
    buf.insert(2000, vec![0u8; 1000]);
    let ready = buf.drain_ready();
    assert!(ready.is_empty());

    buf.insert(1000, vec![0u8; 1000]);
    let ready = buf.drain_ready();
    assert!(ready.is_empty());

    // Now insert segment 0 — all three should drain in order.
    buf.insert(0, vec![0u8; 1000]);
    let ready = buf.drain_ready();
    assert_eq!(ready.len(), 3);
    assert_eq!(ready[0].0, 0);
    assert_eq!(ready[1].0, 1000);
    assert_eq!(ready[2].0, 2000);
}

#[test]
fn overflow_forces_oldest_eviction() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(2);

    // Insert 2 segments that don't start at write_cursor (0).
    buf.insert(1000, vec![0u8; 500]);
    assert!(buf.drain_ready().is_empty());

    buf.insert(2000, vec![0u8; 500]);
    assert!(buf.drain_ready().is_empty());

    // Third insert exceeds max_pending (2), should force-evict oldest.
    buf.insert(3000, vec![0u8; 500]);
    assert!(buf.exceeds_max_pending());
    let evicted = buf.take_oldest_buffered().unwrap();
    assert_eq!(evicted.0, 1000); // Lowest offset evicted.
    buf.mark_persisted(evicted.0, evicted.1.len());
}

#[test]
fn flush_all_drains_everything() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    buf.insert(5000, vec![0u8; 100]);
    buf.insert(3000, vec![0u8; 200]);
    buf.insert(1000, vec![0u8; 300]);

    let flushed = buf.flush_all();
    assert_eq!(flushed.len(), 3);
    // Should be sorted by offset.
    assert_eq!(flushed[0].0, 1000);
    assert_eq!(flushed[1].0, 3000);
    assert_eq!(flushed[2].0, 5000);
}

#[test]
fn partial_contiguous_run() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    // Insert offset 0 and 1000, but gap at 2000.
    buf.insert(0, vec![0u8; 1000]);
    let ready = buf.drain_ready();
    assert_eq!(ready.len(), 1);

    // Insert offset 2000 (gap at 1000).
    buf.insert(2000, vec![0u8; 500]);
    let ready = buf.drain_ready();
    assert!(ready.is_empty());

    // Fill the gap — should release 1000 and 2000.
    buf.insert(1000, vec![0u8; 1000]);
    let ready = buf.drain_ready();
    assert_eq!(ready.len(), 2);
    assert_eq!(ready[0].0, 1000);
    assert_eq!(ready[1].0, 2000);
}

#[test]
fn persisted_entries_advance_cursor_when_gap_fills() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    buf.insert(1000, vec![1u8; 1000]);
    let (offset, chunk) = buf.take_oldest_buffered().unwrap();
    assert_eq!(offset, 1000);
    buf.mark_persisted(offset, chunk.len());

    buf.insert(0, vec![0u8; 1000]);
    let ready = buf.drain_ready();

    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].0, 0);
    assert!(buf.flush_all().is_empty());
}
