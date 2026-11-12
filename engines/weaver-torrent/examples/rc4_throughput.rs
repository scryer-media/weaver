// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Throughput of the RC4 cipher the encrypted peer stream runs every byte
// through, on the buffer sizes the stream hands it: a 16 KiB block and a
// 1 MiB run. Also the cost of setting a cipher up for a connection (key
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
    let mut best = 0f64;
    let started = Instant::now();
    while started.elapsed() < budget {
        let mut bytes = 0usize;
        let t = Instant::now();
        while t.elapsed() < Duration::from_millis(50) {
            c.apply(black_box(&mut buf));
            bytes += len;
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
    println!("apply  16 KiB: {:8.1} MB/s", lane(16 << 10, budget));
    println!("apply   1 MiB: {:8.1} MB/s", lane(1 << 20, budget));
    println!("apply  1500 B: {:8.1} MB/s", lane(1500, budget));
    println!(
        "setup        : {:8.0} ns per connection cipher",
        setup(budget)
    );
}
