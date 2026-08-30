#![no_main]

use libfuzzer_sys::fuzz_target;
use weaver_yenc::decode_nntp_append;

fuzz_target!(|data: &[u8]| {
    let mut output = Vec::new();
    if let Ok(result) = decode_nntp_append(data, &mut output) {
        assert_eq!(result.bytes_written, output.len());
        assert!(output.len() <= data.len());
    }
});
