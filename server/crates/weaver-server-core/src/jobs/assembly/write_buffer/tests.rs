use super::*;

/// Mirrors the pipeline's `note_write_buffered` / `release_write_buffered`
/// pairing: the caller charges the global write backlog for every insert, and
/// the only ways bytes come back off it are the buffer handing the chunk back
/// or the buffer's own `buffered_bytes`/`buffered_len` at job teardown
/// (`clear_job_write_backlog`). Anything the buffer swallows silently is a
/// permanent leak in that counter.
#[derive(Default)]
struct BacklogLedger {
    outstanding_bytes: usize,
    outstanding_segments: usize,
}

impl BacklogLedger {
    fn note(&mut self, len: usize) {
        self.outstanding_bytes += len;
        self.outstanding_segments += 1;
    }

    fn release(&mut self, handed_back: &[(u64, Vec<u8>)]) {
        for (_, chunk) in handed_back {
            self.outstanding_bytes -= chunk.len();
            self.outstanding_segments -= 1;
        }
    }

    fn assert_balanced(&self, buf: &WriteReorderBuffer<Vec<u8>>) {
        assert_eq!(
            self.outstanding_bytes,
            buf.buffered_bytes(),
            "backlog bytes the caller still owns must match what the buffer still holds"
        );
        assert_eq!(
            self.outstanding_segments,
            buf.buffered_len(),
            "backlog segments the caller still owns must match what the buffer still holds"
        );
    }
}

/// Insert through the ledger the way the decode worker does: charge the
/// backlog, then release whatever the drain hands back.
fn insert_and_drain(
    buf: &mut WriteReorderBuffer<Vec<u8>>,
    ledger: &mut BacklogLedger,
    offset: u64,
    data: Vec<u8>,
) -> Vec<(u64, Vec<u8>)> {
    ledger.note(data.len());
    buf.insert(offset, data);
    let ready = buf.drain_ready();
    ledger.release(&ready);
    ledger.assert_balanced(buf);
    ready
}

fn offsets(chunks: &[(u64, Vec<u8>)]) -> Vec<u64> {
    chunks.iter().map(|(offset, _)| *offset).collect()
}

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
fn oldest_buffered_batch_drains_lowest_offsets() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(8);

    buf.insert(3000, vec![3u8; 1000]);
    buf.insert(1000, vec![1u8; 1000]);
    buf.insert(5000, vec![5u8; 1000]);
    buf.mark_persisted(2000, 1000);
    buf.insert(4000, vec![4u8; 1000]);

    let drained = buf.take_oldest_buffered_batch(3);
    let offsets: Vec<u64> = drained.iter().map(|(offset, _)| *offset).collect();

    assert_eq!(offsets, vec![1000, 3000, 4000]);
    assert_eq!(buf.buffered_len(), 1);
    assert_eq!(buf.buffered_bytes(), 1000);
}

#[test]
fn oldest_buffered_batch_respects_zero_limit() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(8);

    buf.insert(1000, vec![1u8; 1000]);

    assert!(buf.take_oldest_buffered_batch(0).is_empty());
    assert_eq!(buf.buffered_len(), 1);
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

#[test]
fn below_cursor_duplicate_does_not_stall_later_segments() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    buf.insert(0, vec![0u8; 1000]);
    assert_eq!(offsets(&buf.drain_ready()), vec![0]);

    // The same article is decoded twice; the replay lands behind the cursor.
    buf.insert(0, vec![0u8; 1000]);
    let _ = buf.drain_ready();

    buf.insert(1000, vec![1u8; 500]);
    let (ready, contiguous_end) = buf.drain_ready_with_contiguous_end();
    assert_eq!(
        offsets(&ready),
        vec![1000],
        "a duplicate behind the cursor must not block the contiguous drain"
    );
    assert_eq!(
        contiguous_end, 1500,
        "the duplicate must not disturb the cursor either"
    );
    assert!(
        buf.is_empty(),
        "nothing may stay behind once every range has been released for writing"
    );
}

#[test]
fn below_cursor_duplicate_is_handed_back_instead_of_dropped() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);
    let mut ledger = BacklogLedger::default();

    assert_eq!(
        offsets(&insert_and_drain(&mut buf, &mut ledger, 0, vec![0u8; 1000])),
        vec![0]
    );

    // Its bytes are already on disk, but the caller charged the write backlog
    // for them, so the buffer has to give the chunk back to release them.
    assert_eq!(
        offsets(&insert_and_drain(&mut buf, &mut ledger, 0, vec![0u8; 1000])),
        vec![0]
    );

    assert_eq!(buf.buffered_bytes(), 0);
    assert_eq!(buf.buffered_len(), 0);
    assert!(buf.is_empty());
}

#[test]
fn queued_duplicate_keeps_the_buffer_non_empty() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    buf.insert(0, vec![0u8; 1000]);
    assert_eq!(offsets(&buf.drain_ready()), vec![0]);

    buf.insert(0, vec![0u8; 1000]);
    assert!(
        !buf.is_empty(),
        "the buffer still owns the duplicate's bytes, so it must not look drainable-and-done"
    );
    assert_eq!(buf.buffered_len(), 1);
    assert_eq!(buf.buffered_bytes(), 1000);
}

#[test]
fn evicted_below_cursor_duplicate_leaves_no_blocking_marker() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    buf.insert(0, vec![0u8; 1000]);
    assert_eq!(offsets(&buf.drain_ready()), vec![0]);

    // Backlog relief writes the duplicate directly and reports it persisted,
    // exactly as `persist_out_of_order_segments` does for every eviction.
    buf.insert(0, vec![0u8; 1000]);
    let (offset, chunk) = buf.take_oldest_buffered().expect("duplicate is evictable");
    assert_eq!(offset, 0);
    buf.mark_persisted(offset, chunk.len());

    buf.insert(1000, vec![1u8; 500]);
    let (ready, contiguous_end) = buf.drain_ready_with_contiguous_end();
    assert_eq!(
        offsets(&ready),
        vec![1000],
        "a persisted marker behind the cursor bridges nothing and must not be recorded"
    );
    assert_eq!(contiguous_end, 1500);
    assert!(buf.is_empty());
}

#[test]
fn duplicate_at_the_write_cursor_advances_the_cursor_once() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);
    let mut ledger = BacklogLedger::default();

    // Both copies land before the drain, so one is redundant on arrival.
    ledger.note(1000);
    buf.insert(0, vec![0u8; 1000]);
    ledger.note(1000);
    buf.insert(0, vec![0u8; 1000]);

    let (ready, contiguous_end) = buf.drain_ready_with_contiguous_end();
    ledger.release(&ready);
    ledger.assert_balanced(&buf);

    assert_eq!(
        offsets(&ready),
        vec![0, 0],
        "both copies must come back so the caller can release both charges"
    );
    assert_eq!(
        contiguous_end, 1000,
        "the cursor moves by one segment, not two"
    );
    assert!(buf.is_empty());
}

#[test]
fn duplicate_of_a_persisted_offset_keeps_the_persisted_bridge() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);
    let mut ledger = BacklogLedger::default();

    ledger.note(1000);
    buf.insert(1000, vec![1u8; 1000]);
    let (offset, chunk) = buf.take_oldest_buffered().unwrap();
    ledger.release(&[(offset, chunk.clone())]);
    buf.mark_persisted(offset, chunk.len());

    // A replay of the segment that was already written out of order.
    assert_eq!(
        offsets(&insert_and_drain(
            &mut buf,
            &mut ledger,
            1000,
            vec![1u8; 1000]
        )),
        vec![1000],
        "the replay must be handed back, not stashed on top of the bridge"
    );

    // The bridge still spans 1000..2000, so filling the gap reaches 2000.
    assert_eq!(
        offsets(&insert_and_drain(&mut buf, &mut ledger, 0, vec![0u8; 1000])),
        vec![0],
        "the marker still covers 1000..2000, so only the gap chunk needs writing"
    );
    assert_eq!(
        offsets(&insert_and_drain(
            &mut buf,
            &mut ledger,
            2000,
            vec![2u8; 100]
        )),
        vec![2000],
        "the persisted marker must still bridge its range after the replay"
    );
    assert!(buf.is_empty());
}

#[test]
fn same_offset_duplicate_with_a_different_length_keeps_accounting_whole() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);
    let mut ledger = BacklogLedger::default();

    // Impossible from yEnc, but the buffer must not corrupt its own state.
    ledger.note(1000);
    buf.insert(0, vec![0u8; 1000]);
    ledger.note(700);
    buf.insert(0, vec![0u8; 700]);
    ledger.assert_balanced(&buf);

    let (ready, contiguous_end) = buf.drain_ready_with_contiguous_end();
    ledger.release(&ready);
    ledger.assert_balanced(&buf);
    assert_eq!(ready.len(), 2, "neither copy may be swallowed");
    assert_eq!(buf.buffered_bytes(), 0);

    // The copy that holds the offset is written last and owns the cursor, so
    // the cursor always describes what actually ended up on disk.
    assert_eq!(ready.last().map(|(_, chunk)| chunk.len()), Some(1000));
    assert_eq!(
        contiguous_end, 1000,
        "the cursor advances by one copy, never by both"
    );
    assert!(buf.is_empty());
}

#[test]
fn duplicates_count_toward_max_pending_and_stay_evictable() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(1);

    buf.insert(0, vec![0u8; 500]);
    assert_eq!(offsets(&buf.drain_ready()), vec![0]);

    buf.insert(0, vec![0u8; 500]);
    buf.insert(2000, vec![2u8; 500]);
    assert!(buf.exceeds_max_pending());

    // `relieve_global_write_backlog` picks any file with `buffered_len() > 0`
    // and then requires a non-empty batch; an unevictable duplicate would spin
    // that loop forever.
    let batch = buf.take_oldest_buffered_batch(8);
    assert_eq!(offsets(&batch), vec![0, 2000]);
    assert_eq!(buf.buffered_len(), 0);
    assert_eq!(buf.buffered_bytes(), 0);
    assert!(!buf.exceeds_max_pending());
}

#[test]
fn flush_all_returns_duplicates_too() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

    buf.insert(0, vec![0u8; 1000]);
    assert_eq!(offsets(&buf.drain_ready()), vec![0]);

    buf.insert(0, vec![0u8; 1000]);
    buf.insert(3000, vec![3u8; 200]);

    let flushed = buf.flush_all();
    assert_eq!(offsets(&flushed), vec![0, 3000]);
    assert_eq!(buf.buffered_bytes(), 0);
    assert_eq!(buf.buffered_len(), 0);
    assert!(buf.is_empty());
}

#[test]
fn replayed_stream_releases_every_charged_byte() {
    let mut buf = WriteReorderBuffer::<Vec<u8>>::new(4);
    let mut ledger = BacklogLedger::default();

    // Out-of-order arrivals with the whole duplicate space mixed in: behind the
    // cursor, exactly at it, and on top of a still-buffered offset.
    let arrivals: &[(u64, u8, usize)] = &[
        (2000, 2, 500),
        (0, 0, 500),
        (0, 0, 500),
        (500, 1, 500),
        (2000, 2, 500),
        (1000, 9, 500),
        (1500, 8, 500),
        (1000, 9, 500),
        (2500, 7, 500),
    ];

    for (offset, fill, len) in arrivals.iter().copied() {
        insert_and_drain(&mut buf, &mut ledger, offset, vec![fill; len]);
    }

    let flushed = buf.flush_all();
    ledger.release(&flushed);
    assert_eq!(
        ledger.outstanding_bytes, 0,
        "every byte charged to the write backlog must come back out"
    );
    assert_eq!(ledger.outstanding_segments, 0);
}
