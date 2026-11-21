// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Throughput of the RC4 cipher the encrypted peer stream runs every byte
// through, on the buffer sizes the stream hands it: one-off short messages
// (a 5-byte have, a 17-byte request), a 16 KiB block with its 13-byte Piece
// header, the 32 KiB read buffer, a 512 KiB grown read buffer and a 1 MiB
// run. Also the cost of setting a cipher up for a connection (key
// schedule and the 1024-byte keystream discard).
//
//   cargo run --release --example rc4_throughput
//   cargo run --release --example rc4_throughput -- <seconds per lane>

use std::hint::black_box;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "../src/mse/rc4.rs"]
mod rc4;

use rc4::Rc4;

fn lane(len: usize, budget: Duration) -> f64 {
    let mut buf: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
    let mut c = Rc4::new_discard_1024(b"twenty byte rc4 key!");
    // Warm up, then take the best of several timed rounds.
    for _ in 0..4 {
        c.apply(black_box(&mut buf));
    }
    // Enough calls between clock reads that reading the clock costs nothing
    // next to them, for the short buffers too.
    let reps = (64 << 10) / len + 1;
    let mut best = 0f64;
    let started = Instant::now();
    while started.elapsed() < budget {
        let mut bytes = 0usize;
        let t = Instant::now();
        while t.elapsed() < Duration::from_millis(50) {
            for _ in 0..reps {
                c.apply(black_box(&mut buf));
            }
            bytes += len * reps;
        }
        let secs = t.elapsed().as_secs_f64();
        best = best.max(bytes as f64 / secs / 1e6);
    }
    black_box(&buf);
    best
}

fn setup(budget: Duration) -> f64 {
    let mut n = 0u64;
    let t = Instant::now();
    while t.elapsed() < budget {
        for _ in 0..256 {
            black_box(Rc4::new_discard_1024(black_box(b"twenty byte rc4 key!")));
        }
        n += 256;
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn main() {
    let secs: f64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2.0);
    let budget = Duration::from_secs_f64(secs);
    for (name, len) in [
        ("5 B", 5),
        ("17 B", 17),
        ("1500 B", 1500),
        ("16 KiB", 16 << 10),
        ("16 KiB + 13 B", (16 << 10) + 13),
        ("32 KiB", 32 << 10),
        ("512 KiB", 512 << 10),
        ("1 MiB", 1 << 20),
    ] {
        let mbs = lane(len, budget);
        println!("apply {name:>14}: {mbs:8.1} MB/s {:6.3} ns/byte", 1e3 / mbs);
    }
    println!(
        "setup        : {:8.0} ns per connection cipher",
        setup(budget)
    );
}
