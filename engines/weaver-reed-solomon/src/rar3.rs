//! Legacy RAR3 GF(2^8) Reed-Solomon erasure coder.
//!
//! This is a Rust port of UnRAR's `RSCoder` (`rs.cpp`) over polynomial
//! `0x11D`. RAR3 recovery applies this scalar decoder independently for each
//! byte position across all data and recovery volumes.

const MAX_PAR: usize = 255;
const MAX_POL: usize = 512;

#[derive(Clone)]
pub struct Rar3RsCoder {
    par_size: usize,
    first_block_done: bool,
    gf_exp: [usize; MAX_POL],
    gf_log: [usize; MAX_PAR + 1],
    gx_pol: [usize; MAX_POL * 2],
    error_locs: [usize; MAX_PAR + 1],
    err_count: usize,
    dnm: [usize; MAX_PAR + 1],
    el_pol: [usize; MAX_POL],
}

impl Rar3RsCoder {
    pub fn new(par_size: usize) -> Option<Self> {
        if par_size == 0 || par_size > MAX_PAR {
            return None;
        }

        let mut coder = Self {
            par_size,
            first_block_done: false,
            gf_exp: [0; MAX_POL],
            gf_log: [0; MAX_PAR + 1],
            gx_pol: [0; MAX_POL * 2],
            error_locs: [0; MAX_PAR + 1],
            err_count: 0,
            dnm: [0; MAX_PAR + 1],
            el_pol: [0; MAX_POL],
        };
        coder.gf_init();
        coder.pn_init();
        Some(coder)
    }

    pub fn encode(&self, data: &[u8], dest: &mut [u8]) {
        assert_eq!(dest.len(), self.par_size);

        let mut shift_reg = [0usize; MAX_PAR + 1];
        for &byte in data {
            let d = (byte as usize) ^ shift_reg[self.par_size - 1];
            for j in (1..self.par_size).rev() {
                shift_reg[j] = shift_reg[j - 1] ^ self.gf_mult(self.gx_pol[j], d);
            }
            shift_reg[0] = self.gf_mult(self.gx_pol[0], d);
        }
        for i in 0..self.par_size {
            dest[i] = shift_reg[self.par_size - i - 1] as u8;
        }
    }

    pub fn decode(&mut self, data: &mut [u8], erasures: &[usize]) -> bool {
        let data_size = data.len();
        if data_size == 0
            || data_size > MAX_PAR
            || erasures.len() > self.par_size
            || erasures.iter().any(|&loc| loc >= data_size)
        {
            return false;
        }

        let mut syn_data = [0usize; MAX_POL];
        let mut all_zeroes = true;
        for (i, slot) in syn_data.iter_mut().take(self.par_size).enumerate() {
            let mut sum = 0usize;
            for &byte in data.iter() {
                sum = (byte as usize) ^ self.gf_mult(self.gf_exp[i + 1], sum);
            }
            *slot = sum;
            if sum != 0 {
                all_zeroes = false;
            }
        }

        if all_zeroes {
            return true;
        }

        if !self.first_block_done {
            self.first_block_done = true;
            self.el_pol.fill(0);
            self.el_pol[0] = 1;

            for &era_pos in erasures {
                let m = self.gf_exp[data_size - era_pos - 1];
                for i in (1..=self.par_size).rev() {
                    self.el_pol[i] ^= self.gf_mult(m, self.el_pol[i - 1]);
                }
            }

            self.err_count = 0;
            for root in (MAX_PAR - data_size)..=MAX_PAR {
                let mut sum = 0usize;
                for b in 0..=self.par_size {
                    sum ^= self.gf_mult(self.gf_exp[(b * root) % MAX_PAR], self.el_pol[b]);
                }
                if sum == 0 {
                    self.error_locs[self.err_count] = MAX_PAR - root;
                    self.dnm[self.err_count] = 0;
                    for i in (1..=self.par_size).step_by(2) {
                        self.dnm[self.err_count] ^=
                            self.gf_mult(self.el_pol[i], self.gf_exp[root * (i - 1) % MAX_PAR]);
                    }
                    self.err_count += 1;
                }
            }
        }

        let mut ee_pol = [0usize; MAX_POL * 2];
        self.pn_mult(&self.el_pol, &syn_data, &mut ee_pol);

        if self.err_count <= self.par_size && self.err_count > 0 {
            for i in 0..self.err_count {
                let loc = self.error_locs[i];
                let dloc = MAX_PAR - loc;
                let mut n = 0usize;
                for (j, &ee) in ee_pol.iter().take(self.par_size).enumerate() {
                    n ^= self.gf_mult(ee, self.gf_exp[dloc * j % MAX_PAR]);
                }

                let data_pos = data_size as isize - loc as isize - 1;
                if data_pos >= 0 && (data_pos as usize) < data_size {
                    data[data_pos as usize] ^=
                        self.gf_mult(n, self.gf_exp[MAX_PAR - self.gf_log[self.dnm[i]]]) as u8;
                }
            }
        }

        self.err_count <= self.par_size
    }

    fn gf_init(&mut self) {
        let mut j = 1usize;
        for i in 0..MAX_PAR {
            self.gf_log[j] = i;
            self.gf_exp[i] = j;
            j <<= 1;
            if j > MAX_PAR {
                j ^= 0x11D;
            }
        }
        for i in MAX_PAR..MAX_POL {
            self.gf_exp[i] = self.gf_exp[i - MAX_PAR];
        }
    }

    #[inline]
    fn gf_mult(&self, a: usize, b: usize) -> usize {
        if a == 0 || b == 0 {
            0
        } else {
            self.gf_exp[self.gf_log[a] + self.gf_log[b]]
        }
    }

    fn pn_init(&mut self) {
        let mut p2 = [0usize; MAX_POL * 2];
        p2[0] = 1;

        for i in 1..=self.par_size {
            let mut p1 = [0usize; MAX_POL * 2];
            p1[0] = self.gf_exp[i];
            p1[1] = 1;

            let mut result = [0usize; MAX_POL * 2];
            self.pn_mult(&p1, &p2, &mut result);
            self.gx_pol[..self.par_size].copy_from_slice(&result[..self.par_size]);
            p2[..self.par_size].copy_from_slice(&self.gx_pol[..self.par_size]);
        }
    }

    fn pn_mult(&self, p1: &[usize], p2: &[usize], result: &mut [usize]) {
        result.fill(0);
        for i in 0..self.par_size {
            if p1[i] == 0 {
                continue;
            }
            for j in 0..self.par_size - i {
                result[i + j] ^= self.gf_mult(p1[i], p2[j]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_one_erasure() {
        let source = [1u8, 2, 3, 4, 5];
        let coder = Rar3RsCoder::new(2).unwrap();
        let mut parity = [0u8; 2];
        coder.encode(&source, &mut parity);

        let mut data = Vec::from(source);
        data.extend_from_slice(&parity);
        data[2] = 0;

        let mut decoder = Rar3RsCoder::new(2).unwrap();
        assert!(decoder.decode(&mut data, &[2]));
        assert_eq!(&data[..source.len()], source);
    }

    #[test]
    fn encode_decode_two_erasures() {
        let source = [9u8, 17, 34, 68, 136, 201];
        let coder = Rar3RsCoder::new(3).unwrap();
        let mut parity = [0u8; 3];
        coder.encode(&source, &mut parity);

        let mut data = Vec::from(source);
        data.extend_from_slice(&parity);
        data[0] = 0;
        data[5] = 0;

        let mut decoder = Rar3RsCoder::new(3).unwrap();
        assert!(decoder.decode(&mut data, &[0, 5]));
        assert_eq!(&data[..source.len()], source);
    }

    #[test]
    fn rejects_too_many_erasures() {
        let mut decoder = Rar3RsCoder::new(1).unwrap();
        let mut data = [0u8, 1, 2];
        assert!(!decoder.decode(&mut data, &[0, 1]));
    }
}
