// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;
use std::sync::{Arc, mpsc};

use bytes::Bytes;
use tokio::sync::Notify;

use super::{Done, Paid, Place, TorrentDisk, VolumeRoom, Writes};

// A torrent's writes to files under `dir`, through writers of their own,
// on a volume with room for two pieces of eight, less the share its
// writes keep: one piece fits, a second does not.
fn writes_in(dir: &Path) -> Writes {
    let writes = TorrentDisk::alone().writes(1, dir);
    writes.volume().set_buffer_limits(16, 16);
    writes
}

fn held(room: VolumeRoom) -> Paid {
    match room {
        VolumeRoom::Held(paid) => paid,
        VolumeRoom::Unmetered => panic!("unmetered"),
        VolumeRoom::Refused => panic!("refused"),
    }
}

// What a piece's writes came to, heard here; disconnected when it was
// dropped untold.
fn told() -> (Done, mpsc::Receiver<anyhow::Result<()>>) {
    let (tell, heard) = mpsc::channel();
    (
        Box::new(move |written| {
            let _ = tell.send(written);
        }),
        heard,
    )
}

// Queue one piece of eight bytes at the start of `path`, paid out of
// `paid`; what follows its writes records the volume's unwritten bytes
// as they stood on the writer then, and tells `done` it was written.
fn queue_piece(writes: &Writes, paid: Paid, path: &Path, done: Done) -> mpsc::Receiver<u64> {
    let volume = writes.volume();
    let (seen, at_landing) = mpsc::channel();
    let path = path.to_path_buf();
    let Ok(paying) = writes.pay(Some(paid)) else {
        panic!("paid on another volume");
    };
    writes.queue_paying(paying, move |batch| {
        batch.write(0, 0, Bytes::from_static(b"abcdefgh"), || Place {
            within: path.parent().unwrap_or(Path::new("")).to_path_buf(),
            path,
        });
        batch.then(done, move |done| {
            let _ = seen.send(volume.unwritten_bytes());
            done(Ok(()));
        });
    });
    at_landing
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

// A piece the volume has no room for is refused, and stays refused while
// the bytes before it are queued but not written; once the writer writes
// them, the room asked about is rung, and the piece is granted.
#[test]
fn a_refused_piece_is_rung_once_its_volume_has_written_enough() {
    let dir = tempfile::tempdir().unwrap();
    let writes = writes_in(dir.path());
    let volume = writes.volume();
    let room = Arc::new(Notify::new());
    let first = held(writes.reserve(8, &room));
    assert_eq!(volume.unwritten_bytes(), 8);
    assert!(matches!(writes.reserve(8, &room), VolumeRoom::Refused));
    assert_eq!(volume.unwritten_bytes(), 8, "a refusal counts nothing");

    let hold = writes.disk().writers().hold(&volume);
    let (done, heard) = told();
    let landed = queue_piece(&writes, first, &dir.path().join("a"), done);
    // Queued, not written: still no room.
    assert!(matches!(writes.reserve(8, &room), VolumeRoom::Refused));

    let mut rung = std::pin::pin!(room.notified());
    rung.as_mut().enable();
    hold.open();
    drop(hold);
    runtime().block_on(rung);
    assert_eq!(landed.recv().unwrap(), 0);
    heard.recv().unwrap().unwrap();
    let second = held(writes.reserve(8, &room));
    assert_eq!(volume.unwritten_bytes(), 8);
    drop(second);
    assert_eq!(volume.unwritten_bytes(), 0);
    assert_eq!(std::fs::read(dir.path().join("a")).unwrap(), b"abcdefgh");
}

// A piece's reservation counts until the writer has written it: held
// while the writer stands still, given back by the time what follows the
// write runs.
#[test]
fn a_reservation_is_given_back_when_its_piece_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let writes = writes_in(dir.path());
    let volume = writes.volume();
    let paid = held(writes.reserve(8, &Arc::default()));
    assert_eq!(paid.bytes(), 8);
    let hold = writes.disk().writers().hold(&volume);
    let (done, heard) = told();
    let landed = queue_piece(&writes, paid, &dir.path().join("a"), done);
    assert_eq!(volume.unwritten_bytes(), 8, "queued, not written");
    hold.open();
    drop(hold);
    assert_eq!(landed.recv().unwrap(), 0, "written, given back");
    heard.recv().unwrap().unwrap();
    assert_eq!(volume.unwritten_bytes(), 0);
}

// A write that fails tells the piece its error and gives back its
// reservation.
#[test]
fn a_failed_write_tells_its_piece_and_gives_back_its_reservation() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("plain"), b"").unwrap();
    let writes = writes_in(dir.path());
    let volume = writes.volume();
    let paid = held(writes.reserve(8, &Arc::default()));
    let (done, heard) = told();
    // Under a regular file: the file cannot be made.
    let landed = queue_piece(&writes, paid, &dir.path().join("plain").join("a"), done);
    assert!(heard.recv().unwrap().is_err());
    assert!(landed.recv().is_err(), "nothing after a failed write runs");
    assert_eq!(volume.unwritten_bytes(), 0);
    // The next piece writes on a generation of its own.
    let paid = held(writes.reserve(8, &Arc::default()));
    let (done, heard) = told();
    queue_piece(&writes, paid, &dir.path().join("a"), done);
    heard.recv().unwrap().unwrap();
    assert_eq!(volume.unwritten_bytes(), 0);
}

// Writes given up while they wait are not written: the piece is dropped
// untold, and its reservation is given back.
#[test]
fn abandoned_writes_give_back_their_reservations_unwritten() {
    let dir = tempfile::tempdir().unwrap();
    let writes = writes_in(dir.path());
    let volume = writes.volume();
    let paid = held(writes.reserve(8, &Arc::default()));
    let hold = writes.disk().writers().hold(&volume);
    let (done, heard) = told();
    let landed = queue_piece(&writes, paid, &dir.path().join("a"), done);
    let barrier = writes.abandon().unwrap();
    hold.open();
    drop(hold);
    barrier.wait_aside();
    assert!(heard.recv().is_err(), "dropped untold");
    assert!(landed.recv().is_err());
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(!dir.path().join("a").exists());
}

// A reservation dropped before its piece is queued is given back.
#[test]
fn a_dropped_reservation_is_given_back() {
    let dir = tempfile::tempdir().unwrap();
    let writes = writes_in(dir.path());
    let volume = writes.volume();
    let paid = held(writes.reserve(8, &Arc::default()));
    assert_eq!(volume.unwritten_bytes(), 8);
    drop(paid);
    assert_eq!(volume.unwritten_bytes(), 0);
}

// Every torrent on a volume reads in the one set of turns that volume
// has, the turns its writes' volume holds: a read turn taken for one is
// gone for the other. A volume whose kind is not known reads as a
// solid-state one, eight at once, block by block.
#[test]
fn torrents_on_one_volume_share_its_read_turns() {
    let dir = tempfile::tempdir().unwrap();
    let disk = TorrentDisk::alone();
    let one = disk.writes(1, dir.path());
    let two = disk.writes(2, dir.path());
    let (a, b) = (one.read_turns(), two.read_turns());
    assert!(!a.whole_pieces());
    let held = Arc::clone(a.semaphore()).try_acquire_many_owned(8).unwrap();
    assert_eq!(b.semaphore().available_permits(), 0);
    drop(held);
    assert_eq!(b.semaphore().available_permits(), 8);
    let volume = one.volume();
    let reads = volume.reads(|| unreachable!("the kind is decided once"));
    assert!(Arc::ptr_eq(reads.turns(), a.semaphore()));
}

// A piece's bytes written to `name` under the save folder `save`, and what
// the write came to.
#[cfg(unix)]
fn write_under(writes: &Writes, save: &Path, name: &str) -> anyhow::Result<()> {
    let (done, heard) = told();
    let place = Place {
        path: save.join(name),
        within: save.to_path_buf(),
    };
    writes.queue(move |batch| {
        batch.write(0, 0, Bytes::from_static(b"ours"), || place);
        batch.then(done, |done| done(Ok(())));
    });
    heard.recv().unwrap()
}

// A torrent's file is never written through a link that leads out of its
// save folder: a link to a file outside, a link to a folder outside, and a
// link in the middle of the name. The write fails the piece, says why, and
// nothing outside is made or changed; a file made whole is refused the
// same. Links that stay inside are written through.
#[cfg(unix)]
#[test]
fn a_write_through_a_link_that_leads_outside_is_refused() {
    use std::os::unix::fs::symlink;
    let top = tempfile::tempdir().unwrap();
    let (save, away) = (top.path().join("save"), top.path().join("away"));
    std::fs::create_dir_all(save.join("mid")).unwrap();
    std::fs::create_dir_all(away.join("deep")).unwrap();
    std::fs::write(away.join("secret"), b"theirs").unwrap();
    symlink(away.join("secret"), save.join("file.bin")).unwrap();
    symlink(&away, save.join("dir")).unwrap();
    symlink(&away, save.join("mid/link")).unwrap();
    let writes = TorrentDisk::alone().writes(1, &save);
    for name in ["file.bin", "dir/payload.bin", "mid/link/deep/payload.bin"] {
        let refused = write_under(&writes, &save, name).unwrap_err();
        assert!(
            format!("{refused:#}").contains("leads outside"),
            "{name}: {refused:#}"
        );
        let grown = writes
            .grow(
                Place {
                    path: save.join(name),
                    within: save.clone(),
                },
                4,
            )
            .unwrap_err();
        assert!(
            grown.to_string().contains("leads outside"),
            "{name}: {grown}"
        );
    }
    assert_eq!(std::fs::read(away.join("secret")).unwrap(), b"theirs");
    assert!(!away.join("payload.bin").exists());
    assert!(!away.join("deep/payload.bin").exists());

    std::fs::create_dir_all(save.join("real")).unwrap();
    symlink("real", save.join("in")).unwrap();
    write_under(&writes, &save, "in/inside.bin").unwrap();
    assert_eq!(
        std::fs::read(save.join("real/inside.bin")).unwrap(),
        b"ours"
    );
}
