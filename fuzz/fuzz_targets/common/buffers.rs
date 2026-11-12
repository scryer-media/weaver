// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Heap buffers whose allocation ends exactly where the bytes a kernel may
// touch end, so AddressSanitizer reports any read or write past them.
//
// `Exact::new(head, len)` allocates `head + len` bytes aligned to 64 and
// exposes the last `len` of them: the slice starts `head` bytes past a
// 64-byte boundary (any head misalignment the kernels must cope with) and
// ends at the end of the allocation.

#![allow(dead_code)]

use std::alloc::{Layout, alloc, dealloc};

pub struct Exact {
    ptr: *mut u8,
    layout: Layout,
    head: usize,
    len: usize,
}

impl Exact {
    pub fn new(head: usize, len: usize, fill: u8) -> Self {
        let size = head + len;
        if size == 0 {
            return Exact {
                ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
                layout: Layout::from_size_align(0, 64).unwrap(),
                head,
                len,
            };
        }
        let layout = Layout::from_size_align(size, 64).unwrap();
        // SAFETY: `size > 0`.
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "allocation of {size} bytes failed");
        // SAFETY: `ptr` is valid for `size` bytes.
        unsafe { std::ptr::write_bytes(ptr, fill, size) };
        Exact {
            ptr,
            layout,
            head,
            len,
        }
    }

    pub fn from_bytes(head: usize, bytes: &[u8]) -> Self {
        let mut b = Self::new(head, bytes.len(), 0);
        b.as_mut().copy_from_slice(bytes);
        b
    }

    pub fn as_ref(&self) -> &[u8] {
        // SAFETY: the allocation holds `head + len` initialised bytes (or
        // `len == 0` with a dangling, aligned pointer).
        unsafe { std::slice::from_raw_parts(self.ptr.add(self.head), self.len) }
    }

    pub fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: as `as_ref`, and `self` is borrowed mutably.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(self.head), self.len) }
    }
}

impl Drop for Exact {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            // SAFETY: allocated in `new` with this layout.
            unsafe { dealloc(self.ptr, self.layout) };
        }
    }
}

// A little-endian cursor over the fuzz input's configuration bytes; reads
// past the end give zeros, so every input is a valid configuration.
pub struct Config<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Config<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Config { bytes, at: 0 }
    }
    pub fn u8(&mut self) -> u8 {
        let b = self.bytes.get(self.at).copied().unwrap_or(0);
        self.at += 1;
        b
    }
    pub fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    pub fn u32(&mut self) -> u32 {
        u32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }
}

// Split `len` bytes into chunks: `seed` byte values give chunk lengths in
// turn (0 means "the rest"), so a fuzzer can place every boundary.
pub fn chunk_lengths(len: usize, seeds: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut left = len;
    let mut i = 0;
    while left > 0 {
        let s = seeds.get(i).copied().unwrap_or(0) as usize;
        i += 1;
        let n = if s == 0 || i > 64 { left } else { s.min(left) };
        out.push(n);
        left -= n;
    }
    out
}
