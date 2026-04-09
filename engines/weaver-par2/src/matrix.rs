//! Matrix operations over GF(2^16) for PAR2 Reed-Solomon repair.
//!
//! Provides:
//! - A row-major matrix type over GF(2^16)
//! - Vandermonde matrix row construction
//! - Gaussian elimination with partial pivoting
//! - Decode matrix construction for repair

use crate::error::{Par2Error, Result};
use crate::gf;

/// A row-major matrix over GF(2^16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matrix {
    pub rows: usize,
    pub cols: usize,
    pub data: Vec<Vec<u16>>,
}

impl Matrix {
    /// Create a new matrix filled with zeros.
    pub fn zeros(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            data: vec![vec![0u16; cols]; rows],
        }
    }

    /// Create an identity matrix.
    pub fn identity(n: usize) -> Self {
        let mut m = Self::zeros(n, n);
        for i in 0..n {
            m.data[i][i] = 1;
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

    /// Perform in-place Gaussian elimination over GF(2^16).
    ///
    /// Transforms `self` into reduced row echelon form while applying the same
    /// row operations to `rhs`. The matrix must be square.
    ///
    /// After elimination, `self` will be the identity matrix (if invertible)
    /// and `rhs` will contain the solution/inverse.
    pub fn gaussian_eliminate(&mut self, rhs: &mut Matrix) -> Result<()> {
        let n = self.rows;
        if self.cols != n {
            return Err(Par2Error::ReedSolomonError {
                reason: format!("matrix is not square: {}x{}", self.rows, self.cols),
            });
        }
        if rhs.rows != n {
            return Err(Par2Error::ReedSolomonError {
                reason: format!(
                    "RHS row count {} does not match matrix rows {}",
                    rhs.rows, n
                ),
            });
        }

        for col in 0..n {
            // Partial pivoting: find a row with nonzero entry in this column.
            let pivot_row = (col..n).find(|&r| self.data[r][col] != 0);
            let pivot_row = match pivot_row {
                Some(r) => r,
                None => {
                    return Err(Par2Error::ReedSolomonError {
                        reason: "matrix is singular (no pivot found)".to_string(),
                    });
                }
            };

            // Swap pivot row into position.
            if pivot_row != col {
                self.data.swap(col, pivot_row);
                rhs.data.swap(col, pivot_row);
            }

            // Scale pivot row so that self[col][col] = 1.
            let pivot_val = self.data[col][col];
            let pivot_inv = gf::inv(pivot_val);
            for j in 0..self.cols {
                self.data[col][j] = gf::mul(self.data[col][j], pivot_inv);
            }
            for j in 0..rhs.cols {
                rhs.data[col][j] = gf::mul(rhs.data[col][j], pivot_inv);
            }

            // Eliminate this column in all other rows.
            for row in 0..n {
                if row == col {
                    continue;
                }
                let factor = self.data[row][col];
                if factor == 0 {
                    continue;
                }
                for j in 0..self.cols {
                    let sub = gf::mul(factor, self.data[col][j]);
                    self.data[row][j] = gf::add(self.data[row][j], sub);
                }
                for j in 0..rhs.cols {
                    let sub = gf::mul(factor, rhs.data[col][j]);
                    rhs.data[row][j] = gf::add(rhs.data[row][j], sub);
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
    let n = missing_indices.len();
    if recovery_exponents.len() != n {
        return Err(Par2Error::ReedSolomonError {
            reason: format!(
                "recovery exponent count ({}) does not match missing slice count ({n})",
                recovery_exponents.len()
            ),
        });
    }
    if n == 0 {
        return Ok(Matrix::zeros(0, 0));
    }

    // Build the n x n submatrix: row i = Vandermonde row for recovery_exponents[i],
    // but only the columns corresponding to missing_indices.
    let mut submatrix = Matrix::zeros(n, n);
    for (i, &exp) in recovery_exponents.iter().enumerate() {
        for (j, &idx) in missing_indices.iter().enumerate() {
            submatrix.data[i][j] = gf::pow(constants[idx], exp);
        }
    }

    // Invert the submatrix.
    submatrix.invert()
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
            m.data[i] = row;
        }

        let inv = m.invert().unwrap();

        // Verify M * inv = I
        let n = 2;
        for i in 0..n {
            for j in 0..n {
                let mut sum = 0u16;
                for k in 0..n {
                    sum = gf::add(sum, gf::mul(m.data[i][k], inv.data[k][j]));
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
            m.data[i] = Matrix::vandermonde_row(&constants, exp);
        }

        let orig = m.clone();
        let inv = m.invert().unwrap();

        // Verify orig * inv = I
        let n = 5;
        for i in 0..n {
            for j in 0..n {
                let mut sum = 0u16;
                for k in 0..n {
                    sum = gf::add(sum, gf::mul(orig.data[i][k], inv.data[k][j]));
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
        m.data[0] = vec![1, 2];
        m.data[1] = vec![1, 2];
        let err = m.invert().unwrap_err();
        assert!(matches!(err, Par2Error::ReedSolomonError { .. }));
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
                sub.data[i][j] = gf::pow(constants[idx], exp);
            }
        }

        for i in 0..2 {
            for j in 0..2 {
                let mut sum = 0u16;
                for k in 0..2 {
                    sum = gf::add(sum, gf::mul(sub.data[i][k], decode.data[k][j]));
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
