//! Matrix operations over GF(2^16) for PAR2 Reed-Solomon repair.
//!
//! Provides:
//! - A row-major matrix type over GF(2^16)
//! - Vandermonde matrix row construction
//! - Gaussian elimination with partial pivoting
//! - Decode matrix construction for repair

use crate::error::{Par2Error, Result};
use crate::gf;
use crate::gf_simd;
use rayon::prelude::*;

const SIMD_ELIMINATION_ROWS: usize = 16;
const PARALLEL_ELIMINATION_ROWS: usize = 128;
const PARALLEL_ELIMINATION_THRESHOLD: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodeMatrixError {
    pub bad_row: Option<usize>,
    pub reason: String,
}

impl DecodeMatrixError {
    fn new(reason: String) -> Self {
        Self {
            bad_row: None,
            reason,
        }
    }

    fn singular(bad_row: usize) -> Self {
        Self {
            bad_row: Some(bad_row),
            reason: "matrix is singular (no pivot found)".to_string(),
        }
    }

    fn into_par2_error(self) -> Par2Error {
        Par2Error::ReedSolomonError {
            reason: self.reason,
        }
    }
}

/// A row-major matrix over GF(2^16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matrix {
    pub rows: usize,
    pub cols: usize,
    pub data: Vec<u16>,
}

impl Matrix {
    /// Create a new matrix filled with zeros.
    pub fn zeros(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            data: vec![0u16; rows.saturating_mul(cols)],
        }
    }

    /// Create an identity matrix.
    pub fn identity(n: usize) -> Self {
        let mut m = Self::zeros(n, n);
        for i in 0..n {
            m.set(i, i, 1);
        }
        m
    }

    /// Compute a single row of the Vandermonde encoding matrix.
    ///
    /// For constants `[c0, c1, ..., cn-1]` and exponent `e`, produces
    /// the row `[c0^e, c1^e, ..., cn-1^e]`.
    pub fn vandermonde_row(constants: &[u16], exponent: u32) -> Vec<u16> {
        constants.iter().map(|&c| gf::pow(c, exponent)).collect()
    }

    #[inline]
    fn offset(&self, row: usize, col: usize) -> usize {
        row * self.cols + col
    }

    #[inline]
    pub(crate) fn get(&self, row: usize, col: usize) -> u16 {
        self.data[self.offset(row, col)]
    }

    #[inline]
    fn set(&mut self, row: usize, col: usize, value: u16) {
        let offset = self.offset(row, col);
        self.data[offset] = value;
    }

    #[inline]
    pub(crate) fn row(&self, row: usize) -> &[u16] {
        let start = row * self.cols;
        &self.data[start..start + self.cols]
    }

    #[inline]
    fn row_mut(&mut self, row: usize) -> &mut [u16] {
        let start = row * self.cols;
        &mut self.data[start..start + self.cols]
    }

    fn extract_columns(&self, start: usize, len: usize) -> Matrix {
        let mut extracted = Matrix::zeros(self.rows, len);
        for row_idx in 0..self.rows {
            let src = &self.row(row_idx)[start..start + len];
            extracted.row_mut(row_idx).copy_from_slice(src);
        }
        extracted
    }

    #[cfg(test)]
    fn copy_row_from_slice(&mut self, row: usize, values: &[u16]) {
        assert_eq!(
            values.len(),
            self.cols,
            "row width must match matrix columns"
        );
        self.row_mut(row).copy_from_slice(values);
    }

    fn swap_rows(data: &mut [u16], cols: usize, a: usize, b: usize) {
        if a == b {
            return;
        }

        let a_start = a * cols;
        let b_start = b * cols;
        if a_start < b_start {
            let (head, tail) = data.split_at_mut(b_start);
            head[a_start..a_start + cols].swap_with_slice(&mut tail[..cols]);
        } else {
            let (head, tail) = data.split_at_mut(a_start);
            tail[..cols].swap_with_slice(&mut head[b_start..b_start + cols]);
        }
    }

    fn split_two_rows(
        data: &mut [u16],
        cols: usize,
        a: usize,
        b: usize,
    ) -> (&mut [u16], &mut [u16]) {
        assert_ne!(a, b, "row split requires distinct rows");

        let a_start = a * cols;
        let b_start = b * cols;
        if a_start < b_start {
            let (head, tail) = data.split_at_mut(b_start);
            (&mut head[a_start..a_start + cols], &mut tail[..cols])
        } else {
            let (head, tail) = data.split_at_mut(a_start);
            (&mut tail[..cols], &mut head[b_start..b_start + cols])
        }
    }

    /// Perform in-place Gaussian elimination over GF(2^16).
    ///
    /// Transforms `self` into reduced row echelon form while applying the same
    /// row operations to `rhs`. The matrix must be square.
    ///
    /// After elimination, `self` will be the identity matrix (if invertible)
    /// and `rhs` will contain the solution/inverse.
    pub fn gaussian_eliminate(&mut self, rhs: &mut Matrix) -> Result<()> {
        let mut row_origins = (0..self.rows).collect::<Vec<_>>();
        self.gaussian_eliminate_tracked(rhs, &mut row_origins)
            .map_err(DecodeMatrixError::into_par2_error)
    }

    fn gaussian_eliminate_tracked(
        &mut self,
        rhs: &mut Matrix,
        row_origins: &mut [usize],
    ) -> std::result::Result<(), DecodeMatrixError> {
        let n = self.rows;
        if self.cols != n {
            return Err(DecodeMatrixError::new(format!(
                "matrix is not square: {}x{}",
                self.rows, self.cols
            )));
        }
        if rhs.rows != n {
            return Err(DecodeMatrixError::new(format!(
                "RHS row count {} does not match matrix rows {}",
                rhs.rows, n
            )));
        }
        if row_origins.len() != n {
            return Err(DecodeMatrixError::new(format!(
                "row origin count {} does not match matrix rows {}",
                row_origins.len(),
                n
            )));
        }

        for col in 0..n {
            // Partial pivoting: find a row with nonzero entry in this column.
            let pivot_row = (col..n).find(|&r| self.get(r, col) != 0);
            let pivot_row = match pivot_row {
                Some(r) => r,
                None => {
                    return Err(DecodeMatrixError::singular(row_origins[col]));
                }
            };

            // Swap pivot row into position.
            if pivot_row != col {
                Self::swap_rows(&mut self.data, self.cols, col, pivot_row);
                Self::swap_rows(&mut rhs.data, rhs.cols, col, pivot_row);
                row_origins.swap(col, pivot_row);
            }

            // Scale pivot row so that self[col][col] = 1.
            let pivot_val = self.get(col, col);
            if pivot_val != 1 {
                let pivot_inv = gf::inv(pivot_val);
                for value in &mut self.row_mut(col)[col..] {
                    *value = gf::mul(*value, pivot_inv);
                }
                for value in rhs.row_mut(col).iter_mut() {
                    *value = gf::mul(*value, pivot_inv);
                }
            }

            // Eliminate this column in all other rows.
            for row in 0..n {
                if row == col {
                    continue;
                }
                let factor = self.get(row, col);
                if factor == 0 {
                    continue;
                }

                let (target_row, pivot_row) =
                    Self::split_two_rows(&mut self.data, self.cols, row, col);
                if factor == 1 {
                    for (target, pivot) in target_row[col..].iter_mut().zip(&pivot_row[col..]) {
                        *target ^= *pivot;
                    }
                } else {
                    for (target, pivot) in target_row[col..].iter_mut().zip(&pivot_row[col..]) {
                        *target ^= gf::mul(factor, *pivot);
                    }
                }

                let (target_rhs, pivot_rhs) =
                    Self::split_two_rows(&mut rhs.data, rhs.cols, row, col);
                if factor == 1 {
                    for (target, pivot) in target_rhs.iter_mut().zip(pivot_rhs.iter()) {
                        *target ^= *pivot;
                    }
                } else {
                    for (target, pivot) in target_rhs.iter_mut().zip(pivot_rhs.iter()) {
                        *target ^= gf::mul(factor, *pivot);
                    }
                }
            }
        }

        Ok(())
    }

    /// Invert this square matrix in-place, returning the inverse.
    pub fn invert(&self) -> Result<Matrix> {
        let n = self.rows;
        let mut m = self.clone();
        let mut inv = Matrix::identity(n);
        m.gaussian_eliminate(&mut inv)?;
        Ok(inv)
    }

    fn gaussian_eliminate_vandermonde(
        &mut self,
        rhs: &mut Matrix,
    ) -> std::result::Result<(), DecodeMatrixError> {
        let n = self.rows;
        if self.cols != n {
            return Err(DecodeMatrixError::new(format!(
                "matrix is not square: {}x{}",
                self.rows, self.cols
            )));
        }
        if rhs.rows != n {
            return Err(DecodeMatrixError::new(format!(
                "RHS row count {} does not match matrix rows {}",
                rhs.rows, n
            )));
        }

        for col in 0..n {
            let pivot_val = self.get(col, col);
            if pivot_val == 0 {
                return Err(DecodeMatrixError::singular(col));
            }

            if pivot_val != 1 {
                let pivot_inv = gf::inv(pivot_val);
                for value in &mut self.row_mut(col)[col..] {
                    *value = gf::mul(*value, pivot_inv);
                }
                for value in rhs.row_mut(col).iter_mut() {
                    *value = gf::mul(*value, pivot_inv);
                }
            }

            let pivot_matrix_tail = self.row(col)[col..].to_vec();
            let pivot_rhs_row = rhs.row(col).to_vec();
            let pivot_matrix_bytes = words_as_bytes(&pivot_matrix_tail);
            let pivot_rhs_bytes = words_as_bytes(&pivot_rhs_row);
            let matrix_ptr = self.data.as_mut_ptr() as usize;
            let rhs_ptr = rhs.data.as_mut_ptr() as usize;
            let matrix_cols = self.cols;
            let rhs_cols = rhs.cols;
            let row_group =
                if n >= PARALLEL_ELIMINATION_THRESHOLD && rayon::current_num_threads() > 1 {
                    PARALLEL_ELIMINATION_ROWS
                } else {
                    SIMD_ELIMINATION_ROWS
                };
            let eliminate_batch = |batch_start: usize, batch_end: usize| unsafe {
                let mut matrix_pairs = Vec::with_capacity(batch_end - batch_start);
                let mut rhs_pairs = Vec::with_capacity(batch_end - batch_start);

                for row in batch_start..batch_end {
                    if row == col {
                        continue;
                    }

                    let factor = *((matrix_ptr as *const u16).add(row * matrix_cols + col));
                    if factor == 0 {
                        continue;
                    }

                    let row_start = row * matrix_cols + col;
                    let row_len = matrix_cols - col;
                    let rhs_start = row * rhs_cols;

                    let matrix_row = std::slice::from_raw_parts_mut(
                        (matrix_ptr as *mut u16).add(row_start),
                        row_len,
                    );
                    matrix_pairs.push(gf_simd::FactorDst {
                        factor,
                        dst: words_as_bytes_mut(matrix_row),
                    });

                    let rhs_row = std::slice::from_raw_parts_mut(
                        (rhs_ptr as *mut u16).add(rhs_start),
                        rhs_cols,
                    );
                    rhs_pairs.push(gf_simd::FactorDst {
                        factor,
                        dst: words_as_bytes_mut(rhs_row),
                    });
                }

                if !matrix_pairs.is_empty() {
                    gf_simd::mul_acc_multi_region(&mut matrix_pairs, pivot_matrix_bytes);
                    gf_simd::mul_acc_multi_region(&mut rhs_pairs, pivot_rhs_bytes);
                }
            };

            if row_group == SIMD_ELIMINATION_ROWS {
                for batch_start in (0..n).step_by(row_group) {
                    eliminate_batch(batch_start, (batch_start + row_group).min(n));
                }
            } else {
                let batch_starts: Vec<_> = (0..n).step_by(row_group).collect();
                batch_starts.into_par_iter().for_each(|batch_start| {
                    eliminate_batch(batch_start, (batch_start + row_group).min(n));
                });
            }
        }

        Ok(())
    }
}

#[inline]
fn words_as_bytes(words: &[u16]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), words.len() * 2) }
}

#[inline]
fn words_as_bytes_mut(words: &mut [u16]) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), words.len() * 2) }
}

/// Build the decode matrix needed for repair.
///
/// Given:
/// - `missing_indices`: global indices of missing input slices
/// - `recovery_exponents`: exponents of available recovery blocks to use
/// - `constants`: the PAR2 constant assignment for all input slices
///
/// Constructs the submatrix of the Vandermonde encoding matrix corresponding
/// to the selected recovery exponents and missing slice positions, then inverts it.
///
/// The number of recovery exponents must equal the number of missing indices.
pub fn build_decode_matrix(
    missing_indices: &[usize],
    recovery_exponents: &[u32],
    constants: &[u16],
) -> Result<Matrix> {
    build_decode_matrix_with_bad_row(missing_indices, recovery_exponents, constants)
        .map_err(DecodeMatrixError::into_par2_error)
}

pub(crate) fn build_decode_matrix_with_bad_row(
    missing_indices: &[usize],
    recovery_exponents: &[u32],
    constants: &[u16],
) -> std::result::Result<Matrix, DecodeMatrixError> {
    build_repair_matrix_with_bad_row(&[], missing_indices, recovery_exponents, constants)
        .map(|(_, decode)| decode)
}

pub(crate) fn build_repair_matrix_with_bad_row(
    available_indices: &[usize],
    missing_indices: &[usize],
    recovery_exponents: &[u32],
    constants: &[u16],
) -> std::result::Result<(Matrix, Matrix), DecodeMatrixError> {
    let n = missing_indices.len();
    if recovery_exponents.len() != n {
        return Err(DecodeMatrixError::new(format!(
            "recovery exponent count ({}) does not match missing slice count ({n})",
            recovery_exponents.len()
        )));
    }
    if n == 0 {
        return Ok((
            Matrix::zeros(0, available_indices.len()),
            Matrix::zeros(0, 0),
        ));
    }

    let mut submatrix = Matrix::zeros(n, n);
    let mut repair_matrix = Matrix::zeros(n, available_indices.len() + n);
    for (i, &exp) in recovery_exponents.iter().enumerate() {
        {
            let row = submatrix.row_mut(i);
            for (slot, &idx) in row.iter_mut().zip(missing_indices.iter()) {
                *slot = gf::pow(constants[idx], exp);
            }
        }

        {
            let row = repair_matrix.row_mut(i);
            for (slot, &idx) in row
                .iter_mut()
                .take(available_indices.len())
                .zip(available_indices.iter())
            {
                *slot = gf::pow(constants[idx], exp);
            }
            row[available_indices.len() + i] = 1;
        }
    }
    submatrix.gaussian_eliminate_vandermonde(&mut repair_matrix)?;
    let decode = repair_matrix.extract_columns(available_indices.len(), n);
    Ok((repair_matrix, decode))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_inversion() {
        let id = Matrix::identity(4);
        let inv = id.invert().unwrap();
        assert_eq!(inv, Matrix::identity(4));
    }

    #[test]
    fn vandermonde_row_basic() {
        let constants = vec![2u16, 4, 16];
        let row = Matrix::vandermonde_row(&constants, 0);
        // c^0 = 1 for all nonzero c
        assert_eq!(row, vec![1, 1, 1]);

        let row1 = Matrix::vandermonde_row(&constants, 1);
        // c^1 = c
        assert_eq!(row1, vec![2, 4, 16]);
    }

    #[test]
    fn small_matrix_inversion() {
        // Build a 2x2 Vandermonde-like matrix and verify M * M^-1 = I
        let constants = crate::gf::input_slice_constants(2);
        let mut m = Matrix::zeros(2, 2);
        for (i, exp) in [0u32, 1].iter().enumerate() {
            let row = Matrix::vandermonde_row(&constants, *exp);
            m.copy_row_from_slice(i, &row);
        }

        let inv = m.invert().unwrap();

        // Verify M * inv = I
        let n = 2;
        for i in 0..n {
            for j in 0..n {
                let mut sum = 0u16;
                for k in 0..n {
                    sum = gf::add(sum, gf::mul(m.get(i, k), inv.get(k, j)));
                }
                let expected = if i == j { 1 } else { 0 };
                assert_eq!(sum, expected, "M*M^-1 [{i}][{j}] should be {expected}");
            }
        }
    }

    #[test]
    fn larger_matrix_inversion() {
        // 5x5 Vandermonde matrix
        let constants = crate::gf::input_slice_constants(5);
        let exponents = [0u32, 1, 2, 4, 7]; // valid PAR2 exponents
        let mut m = Matrix::zeros(5, 5);
        for (i, &exp) in exponents.iter().enumerate() {
            let row = Matrix::vandermonde_row(&constants, exp);
            m.copy_row_from_slice(i, &row);
        }

        let orig = m.clone();
        let inv = m.invert().unwrap();

        // Verify orig * inv = I
        let n = 5;
        for i in 0..n {
            for j in 0..n {
                let mut sum = 0u16;
                for k in 0..n {
                    sum = gf::add(sum, gf::mul(orig.get(i, k), inv.get(k, j)));
                }
                let expected = if i == j { 1 } else { 0 };
                assert_eq!(
                    sum, expected,
                    "M*M^-1 [{i}][{j}] should be {expected}, got {sum}"
                );
            }
        }
    }

    #[test]
    fn singular_matrix_fails() {
        // Two identical rows
        let mut m = Matrix::zeros(2, 2);
        m.copy_row_from_slice(0, &[1, 2]);
        m.copy_row_from_slice(1, &[1, 2]);
        let err = m.invert().unwrap_err();
        assert!(matches!(err, Par2Error::ReedSolomonError { .. }));
    }

    #[test]
    fn singular_decode_matrix_reports_bad_recovery_row() {
        let constants = crate::gf::input_slice_constants(2);
        let missing = vec![0usize, 1];
        let err = build_decode_matrix_with_bad_row(&missing, &[0, 0], &constants).unwrap_err();
        assert_eq!(err.bad_row, Some(1));
    }

    #[test]
    fn build_decode_matrix_basic() {
        let constants = crate::gf::input_slice_constants(4);
        let missing = vec![1usize, 3];
        let exponents = vec![0u32, 1];

        let decode = build_decode_matrix(&missing, &exponents, &constants).unwrap();
        assert_eq!(decode.rows, 2);
        assert_eq!(decode.cols, 2);

        // Verify: submatrix * decode = I
        let mut sub = Matrix::zeros(2, 2);
        for (i, &exp) in exponents.iter().enumerate() {
            for (j, &idx) in missing.iter().enumerate() {
                sub.set(i, j, gf::pow(constants[idx], exp));
            }
        }

        for i in 0..2 {
            for j in 0..2 {
                let mut sum = 0u16;
                for k in 0..2 {
                    sum = gf::add(sum, gf::mul(sub.get(i, k), decode.get(k, j)));
                }
                let expected = if i == j { 1 } else { 0 };
                assert_eq!(sum, expected);
            }
        }
    }

    #[test]
    fn build_decode_matrix_mismatched_counts() {
        let constants = crate::gf::input_slice_constants(4);
        let err = build_decode_matrix(&[0, 1], &[0u32], &constants).unwrap_err();
        assert!(matches!(err, Par2Error::ReedSolomonError { .. }));
    }

    #[test]
    fn build_decode_matrix_empty() {
        let constants = crate::gf::input_slice_constants(4);
        let decode = build_decode_matrix(&[], &[], &constants).unwrap();
        assert_eq!(decode.rows, 0);
        assert_eq!(decode.cols, 0);
    }
}
