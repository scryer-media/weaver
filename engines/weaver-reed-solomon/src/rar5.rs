//! RAR5 recovery-volume Reed-Solomon coder.
//!
//! This ports UnRAR's `RSCoder16` matrix semantics: a Cauchy matrix over
//! GF(2^16) with primitive polynomial `0x1100B`. The arithmetic and SIMD
//! multiply-accumulate kernels are shared with PAR2 through `gf`/`gf_simd`,
//! but the row ordering here is RAR5-specific and intentionally not PAR2's
//! Vandermonde repair matrix.

use crate::{gf, gf_simd};

const GF_SIZE: usize = 65_535;

#[derive(Debug, Clone)]
pub struct Rar5RsCoder {
    data_count: usize,
    matrix: Vec<u16>,
    missing_data_count: usize,
}

impl Rar5RsCoder {
    /// Build a decoder for a RAR5 recovery set.
    ///
    /// `valid_flags` is ordered as all data volumes followed by all recovery
    /// volumes. Missing data rows are reconstructed with the first available
    /// recovery rows, matching UnRAR's `MakeDecoderMatrix`.
    pub fn new_decoder(data_count: usize, rec_count: usize, valid_flags: &[bool]) -> Option<Self> {
        if data_count == 0
            || rec_count == 0
            || data_count + rec_count > GF_SIZE
            || valid_flags.len() != data_count + rec_count
        {
            return None;
        }

        let missing_data_count = valid_flags[..data_count]
            .iter()
            .filter(|&&valid| !valid)
            .count();
        let valid_recovery_count = valid_flags[data_count..]
            .iter()
            .filter(|&&valid| valid)
            .count();
        if missing_data_count == 0
            || valid_recovery_count == 0
            || missing_data_count > valid_recovery_count
        {
            return None;
        }

        let mut matrix = make_decoder_matrix(data_count, valid_flags, missing_data_count)?;
        invert_decoder_matrix(data_count, valid_flags, missing_data_count, &mut matrix)?;

        Some(Self {
            data_count,
            matrix,
            missing_data_count,
        })
    }

    pub fn missing_data_count(&self) -> usize {
        self.missing_data_count
    }

    /// Apply one logical RAR5 data unit to all reconstructed output rows.
    ///
    /// `outputs` must contain one output buffer per missing data volume. Buffers
    /// are byte slices interpreted as little-endian GF(2^16) words.
    pub fn update_outputs(&self, data_num: usize, data: &[u8], outputs: &mut [&mut [u8]]) {
        assert!(data_num < self.data_count, "data_num out of range");
        assert_eq!(
            outputs.len(),
            self.missing_data_count,
            "one output buffer is required per missing data row"
        );
        assert!(
            data.len().is_multiple_of(2),
            "RAR5 RS chunk length must be even"
        );

        if data_num == 0 {
            for output in outputs.iter_mut() {
                output.fill(0);
            }
        }

        for (row, output) in outputs.iter_mut().enumerate() {
            assert_eq!(
                output.len(),
                data.len(),
                "output chunk length must match input"
            );
            let factor = self.matrix[row * self.data_count + data_num];
            if factor != 0 {
                gf_simd::mul_acc_region(factor, data, output);
            }
        }
    }
}

fn make_decoder_matrix(
    data_count: usize,
    valid_flags: &[bool],
    missing_data_count: usize,
) -> Option<Vec<u16>> {
    let mut matrix = vec![0u16; missing_data_count * data_count];
    let mut recovery_row = data_count;
    let mut dest = 0usize;

    for data_row in 0..data_count {
        if valid_flags[data_row] {
            continue;
        }

        while recovery_row < valid_flags.len() && !valid_flags[recovery_row] {
            recovery_row += 1;
        }
        if recovery_row >= valid_flags.len() {
            return None;
        }

        for col in 0..data_count {
            let cauchy = (recovery_row as u16) ^ (col as u16);
            matrix[dest * data_count + col] = gf::inv(cauchy);
        }
        dest += 1;
        recovery_row += 1;
    }

    Some(matrix)
}

fn invert_decoder_matrix(
    data_count: usize,
    valid_flags: &[bool],
    missing_data_count: usize,
    matrix: &mut [u16],
) -> Option<()> {
    let mut inverse = vec![0u16; missing_data_count * data_count];
    let mut key_full_row = 0usize;
    for key_reduced_row in 0..missing_data_count {
        while key_full_row < data_count && valid_flags[key_full_row] {
            key_full_row += 1;
        }
        if key_full_row >= data_count {
            return None;
        }
        inverse[key_reduced_row * data_count + key_full_row] = 1;
        key_full_row += 1;
    }

    let mut reduced_row = 0usize;
    let mut full_row = 0usize;
    while full_row < data_count {
        while full_row < data_count && valid_flags[full_row] {
            for row in 0..missing_data_count {
                let idx = row * data_count + full_row;
                inverse[idx] ^= matrix[idx];
            }
            full_row += 1;
        }

        if full_row == data_count {
            break;
        }
        if reduced_row >= missing_data_count {
            return None;
        }

        let pivot_idx = reduced_row * data_count + full_row;
        let pivot = matrix[pivot_idx];
        if pivot == 0 {
            return None;
        }
        let pivot_inv = gf::inv(pivot);

        let matrix_pivot_row = reduced_row * data_count;
        for col in 0..data_count {
            matrix[matrix_pivot_row + col] = gf::mul(matrix[matrix_pivot_row + col], pivot_inv);
            inverse[matrix_pivot_row + col] = gf::mul(inverse[matrix_pivot_row + col], pivot_inv);
        }

        for row in 0..missing_data_count {
            if row == reduced_row {
                continue;
            }
            let row_start = row * data_count;
            let factor = matrix[row_start + full_row];
            if factor == 0 {
                continue;
            }
            for col in 0..data_count {
                matrix[row_start + col] ^= gf::mul(matrix[matrix_pivot_row + col], factor);
                inverse[row_start + col] ^= gf::mul(inverse[matrix_pivot_row + col], factor);
            }
        }

        reduced_row += 1;
        full_row += 1;
    }

    matrix.copy_from_slice(&inverse);
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_recovery(data: &[Vec<u8>], rec_count: usize) -> Vec<Vec<u8>> {
        let data_count = data.len();
        let chunk_len = data[0].len();
        let mut recovery = vec![vec![0u8; chunk_len]; rec_count];
        for (data_num, data_unit) in data.iter().enumerate().take(data_count) {
            for (rec_num, rec) in recovery.iter_mut().enumerate() {
                if data_num == 0 {
                    rec.fill(0);
                }
                let factor = gf::inv(((rec_num + data_count) as u16) ^ (data_num as u16));
                gf_simd::mul_acc_region(factor, data_unit, rec);
            }
        }
        recovery
    }

    #[test]
    fn decoder_restores_one_missing_data_unit() {
        let data = vec![
            (0..64).map(|i| i as u8).collect::<Vec<_>>(),
            (0..64).map(|i| (i * 3) as u8).collect::<Vec<_>>(),
            (0..64).map(|i| (255 - i) as u8).collect::<Vec<_>>(),
        ];
        let recovery = encode_recovery(&data, 2);
        let valid = [true, false, true, true, true];
        let coder = Rar5RsCoder::new_decoder(3, 2, &valid).unwrap();

        let mut restored = vec![0u8; 64];
        let mut outputs: Vec<&mut [u8]> = vec![restored.as_mut_slice()];
        for (data_num, data_unit) in data.iter().enumerate().take(3) {
            if data_num == 1 {
                coder.update_outputs(data_num, &recovery[0], &mut outputs);
            } else {
                coder.update_outputs(data_num, data_unit, &mut outputs);
            }
        }

        assert_eq!(restored, data[1]);
    }

    #[test]
    fn decoder_restores_two_missing_data_units() {
        let data = vec![
            (0..128).map(|i| (i * 5) as u8).collect::<Vec<_>>(),
            (0..128).map(|i| (i * 7 + 11) as u8).collect::<Vec<_>>(),
            (0..128).map(|i| (i * 13 + 3) as u8).collect::<Vec<_>>(),
            (0..128).map(|i| (255 - i) as u8).collect::<Vec<_>>(),
        ];
        let recovery = encode_recovery(&data, 3);
        let valid = [false, true, false, true, true, true, true];
        let coder = Rar5RsCoder::new_decoder(4, 3, &valid).unwrap();

        let mut restored0 = vec![0u8; 128];
        let mut restored2 = vec![0u8; 128];
        let mut outputs: Vec<&mut [u8]> = vec![restored0.as_mut_slice(), restored2.as_mut_slice()];
        let mut next_recovery = 0usize;
        for data_num in 0..4 {
            if valid[data_num] {
                coder.update_outputs(data_num, &data[data_num], &mut outputs);
            } else {
                coder.update_outputs(data_num, &recovery[next_recovery], &mut outputs);
                next_recovery += 1;
            }
        }

        assert_eq!(restored0, data[0]);
        assert_eq!(restored2, data[2]);
    }

    #[test]
    fn decoder_rejects_insufficient_recovery() {
        let valid = [false, false, true, true, false];
        assert!(Rar5RsCoder::new_decoder(3, 2, &valid).is_none());
    }
}
