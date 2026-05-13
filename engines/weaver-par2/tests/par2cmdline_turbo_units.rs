#[path = "support/par2cmdline_turbo_support.rs"]
mod support;

use std::fs;
use std::path::Path;

use support::{Rng, build_synthetic_par2, run_repair_scenario, setup_par2_set};
use weaver_par2::checksum::{crc32, md5};
use weaver_par2::{
    DiskFileAccess, FileAccess, FileHashState, SliceChecksumState, gf_add, gf_inv, gf_mul, gf_pow,
    translate_par2_name_to_local_path, translate_par2_name_to_relative,
};

fn chunked_crc(data: &[u8], rng: &mut Rng) -> u32 {
    let mut state = SliceChecksumState::new();
    let mut offset = 0usize;
    while offset < data.len() {
        let len = 1 + (rng.next_u64() as usize % 256);
        let end = (offset + len).min(data.len());
        state.update(&data[offset..end]);
        offset = end;
    }
    let (crc, _) = state.finalize(None);
    crc
}

fn chunked_md5(data: &[u8], rng: &mut Rng) -> [u8; 16] {
    let mut state = FileHashState::new();
    let mut offset = 0usize;
    while offset < data.len() {
        let len = 1 + (rng.next_u64() as usize % 256);
        let end = (offset + len).min(data.len());
        state.update(&data[offset..end]);
        offset = end;
    }
    state.finalize()
}

fn assert_field_laws(a: u16, b: u16, c: u16) {
    assert_eq!(gf_add(gf_add(a, b), c), gf_add(a, gf_add(b, c)));
    assert_eq!(gf_mul(gf_mul(a, b), c), gf_mul(a, gf_mul(b, c)));
    assert_eq!(gf_add(a, b), gf_add(b, a));
    assert_eq!(gf_mul(a, b), gf_mul(b, a));
    assert_eq!(gf_mul(a, gf_add(b, c)), gf_add(gf_mul(a, b), gf_mul(a, c)));
    assert_eq!(gf_add(a, 0), a);
    assert_eq!(gf_add(0, a), a);
    assert_eq!(gf_mul(a, 1), a);
    assert_eq!(gf_mul(1, a), a);
    assert_eq!(gf_add(a, a), 0);
    if a != 0 {
        assert_eq!(gf_mul(a, gf_inv(a)), 1);
    }
}

fn assert_operator_invariants(a: u16, b: u16) {
    assert_eq!(gf_add(a, b), gf_add(b, a));
    assert_eq!(gf_add(a, a), 0);
    assert_eq!(gf_mul(a, b), gf_mul(b, a));

    if a != 0 {
        assert_eq!(gf_mul(a, gf_inv(a)), 1);
        assert_eq!(gf_mul(gf_mul(a, b), gf_inv(a)), b);
    }

    let power = (b & 0x00FF) as u32;
    let mut expected = 1u16;
    for _ in 0..power {
        expected = gf_mul(expected, a);
    }
    assert_eq!(gf_pow(a, power), expected);
}

mod crc {
    use super::*;

    #[test]
    fn test1() {
        let buffer = [0u8; 8];
        let checksum1 = crc32(&buffer);

        let mut state = SliceChecksumState::new();
        state.update(&buffer);
        let (checksum2, _) = state.finalize(None);

        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test2() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn test3() {
        let mut rng = Rng::new(345_087_209);
        let mut data = vec![0u8; 32 * 1024];
        rng.fill_bytes(&mut data);

        let checksum1 = chunked_crc(&data, &mut rng);
        let checksum2 = chunked_crc(&data, &mut rng);

        assert_eq!(checksum1, checksum2);
    }
}

mod descriptionpacket {
    use super::*;

    #[test]
    fn test3() {
        let base = Path::new("base");

        assert_eq!(
            translate_par2_name_to_relative("input1.txt").unwrap(),
            "input1.txt"
        );
        assert_eq!(
            translate_par2_name_to_local_path("dir/input1.txt", base).unwrap(),
            base.join("dir").join("input1.txt")
        );
        assert_eq!(
            translate_par2_name_to_local_path("\t", base).unwrap(),
            base.join("%09")
        );

        let punctuation = translate_par2_name_to_relative("\"*:<>?|%abcd").unwrap();
        if cfg!(windows) {
            assert_eq!(punctuation, "%22%2A%3A%3C%3E%3F%7C%abcd");
        } else {
            assert_eq!(punctuation, "\"*:<>?|%abcd");
        }

        assert_eq!(
            translate_par2_name_to_local_path("/system_file", base).unwrap(),
            base.join("%2F").join("system_file")
        );
        assert_eq!(
            translate_par2_name_to_local_path("../system_file", base).unwrap(),
            base.join("%2E%2E").join("system_file")
        );
        assert_eq!(
            translate_par2_name_to_local_path("tricky/../../system_file", base).unwrap(),
            base.join("tricky")
                .join("%2E%2E")
                .join("%2E%2E")
                .join("system_file")
        );
    }
}

mod diskfile {
    use super::*;

    #[test]
    fn test2() {
        let dir = support::temp_case_dir("diskfile-test2");
        let filename = "input1.txt";
        let file_data = b"diskfile_test test2 input1.txt";
        fs::write(dir.path().join(filename), file_data).expect("write input file");

        let (par2_set, file_id) = setup_par2_set(file_data, 1024, filename);
        let mut access = DiskFileAccess::new(dir.path().to_path_buf(), &par2_set);

        assert!(access.file_exists(&file_id));
        assert_eq!(access.file_length(&file_id), Some(file_data.len() as u64));
        assert_eq!(access.read_file(&file_id).unwrap(), file_data);

        let mut buffer = access.read_file(&file_id).unwrap();
        let mut rng = Rng::new(345_087_209);
        for _ in 0..100 {
            let offset = rng.next_u64() as usize % (file_data.len() - 1);
            let length = 1 + (rng.next_u64() as usize % (file_data.len() - offset));
            let chunk = access
                .read_file_range(&file_id, offset as u64, length as u64)
                .unwrap();
            buffer[offset..offset + chunk.len()].copy_from_slice(&chunk);
        }
        assert_eq!(buffer, file_data);

        access.write_file_range(&file_id, 0, b"DISK").unwrap();
        assert_eq!(access.read_file_range(&file_id, 0, 4).unwrap(), b"DISK");
    }
}

mod galois {
    use super::*;

    #[test]
    fn test1() {
        let mut rng = Rng::new(345_087_209);
        for value in 0..(256 * 256) {
            let a = (value % 256) as u16;
            let b = (rng.next_u64() as u16) & 0x00FF;
            let c = (rng.next_u64() as u16) & 0x00FF;
            assert_field_laws(a, b, c);
        }
    }

    #[test]
    fn test3() {
        let mut rng = Rng::new(14_531_119);
        for value in 0..(256 * 256) {
            let a = (value % 256) as u16;
            let b = (rng.next_u64() as u16) & 0x00FF;
            assert_operator_invariants(a, b);
        }
    }

    #[test]
    fn test5() {
        let mut used = vec![0u32; 65_536];
        used[0] = u32::MAX;

        let mut value = 2u16;
        for power in 1..=65_535u32 {
            let index = value as usize;
            assert_eq!(used[index], 0, "duplicate value at power {power}");
            used[index] = power;
            value = gf_mul(value, 2);
        }

        assert_eq!(value, 2);
    }
}

mod md5 {
    use super::*;

    #[test]
    fn test1() {
        let buffer = [0u8; 8];

        let hash1 = md5(&buffer);

        let mut state = FileHashState::new();
        state.update(&buffer);
        let hash2 = state.finalize();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test2() {
        assert_eq!(
            md5(b""),
            [
                0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
                0x42, 0x7e,
            ]
        );
    }

    #[test]
    fn test3() {
        let mut hash1 = md5(b"");
        let mut hash2 = hash1;

        assert_eq!(hash1, hash2);
        assert!(hash1 <= hash2);
        assert!(hash1 >= hash2);
        assert!(hash1 >= hash2);
        assert!(hash1 <= hash2);

        hash1[0] = 0x00;
        hash2[0] = 0x01;
        assert_ne!(hash1, hash2);
        assert!(hash1 < hash2);
        assert!(hash1 <= hash2);
        assert!(hash1 <= hash2);
        assert!(hash1 < hash2);

        hash1[0] = 0x00;
        hash2[0] = 0x00;
        hash1[15] = 0x00;
        hash2[15] = 0x01;
        assert_ne!(hash1, hash2);
        assert!(hash1 < hash2);
        assert!(hash1 <= hash2);
        assert!(hash1 <= hash2);
        assert!(hash1 < hash2);
    }

    #[test]
    fn test4() {
        let mut rng = Rng::new(345_087_209);
        let mut data = vec![0u8; 32 * 1024];
        rng.fill_bytes(&mut data);

        let hash1 = chunked_md5(&data, &mut rng);
        let hash2 = chunked_md5(&data, &mut rng);

        assert_eq!(hash1, hash2);
    }
}

mod reedsolomon {
    use super::*;

    #[test]
    fn test1() {
        let mut rng = Rng::new(873_945_932);
        let synthetic = build_synthetic_par2(&[1024, 1024, 1024, 1024], 1024, 2, &mut rng);

        for missing1 in 0..synthetic.files.len() {
            for missing2 in (missing1 + 1)..synthetic.files.len() {
                let label = format!("reedsolomon::test1::{missing1}-{missing2}");
                run_repair_scenario(
                    &synthetic,
                    |files, access| {
                        access.remove_file(files[missing1].file_id);
                        access.remove_file(files[missing2].file_id);
                    },
                    &label,
                );
            }
        }
    }

    #[test]
    fn test2() {
        let mut rng = Rng::new(873_945_932);
        let synthetic = build_synthetic_par2(&[1024, 1024, 1024, 1024, 1024], 1024, 5, &mut rng);

        run_repair_scenario(
            &synthetic,
            |files, access| {
                for file in files {
                    access.remove_file(file.file_id);
                }
            },
            "reedsolomon::test2",
        );
    }
}
