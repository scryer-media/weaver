#![no_main]

use std::io::{BufReader, Cursor};

use libfuzzer_sys::fuzz_target;
use weaver_nzb::parse_nzb_reader;

fuzz_target!(|data: &[u8]| {
    let capacity = (data.len() % 256).max(1);
    let reader = BufReader::with_capacity(capacity, Cursor::new(data));
    let _ = parse_nzb_reader(reader);
});
