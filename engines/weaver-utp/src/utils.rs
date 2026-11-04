// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{cmp::Ordering, task::Waker};

use tokio::sync::mpsc::{UnboundedSender, WeakUnboundedSender};

use crate::Error;

pub fn update_optional_waker(waker: &mut Option<Waker>, cx: &std::task::Context<'_>) {
    match waker.as_mut() {
        Some(w) => {
            w.clone_from(cx.waker());
        }
        None => {
            waker.replace(cx.waker().clone());
        }
    }
}

pub fn seq_nr_offset(new: u16, old: u16, wrap_tolerance: u16) -> isize {
    match new.cmp(&old) {
        Ordering::Less => {
            // new is less. check if "new" wrapped within tolerance
            if new.wrapping_sub(old) <= wrap_tolerance {
                return new.wrapping_sub(old) as isize;
            }
            -((old - new) as isize)
        }
        Ordering::Equal => 0,
        Ordering::Greater => {
            // old is less. check if "old" wrapped within tolerance
            if old.wrapping_sub(new) <= wrap_tolerance {
                return -(old.wrapping_sub(new) as isize);
            }
            (new - old) as isize
        }
    }
}

pub fn prepare_2_ioslices<'a>(
    first: &'a [u8],
    second: &'a [u8],
    offset: usize,
    len: usize,
) -> crate::Result<[&'a [u8]; 2]> {
    // offset
    let first_offset = first.len().min(offset);
    let first = &first[first_offset..];
    let second_offset = offset - first_offset;
    let second = second
        .get(second_offset..)
        .ok_or(Error::BugOffsetBeyondBufferBounds)?;
    // len limit
    let first_len = first.len().min(len);
    let first = &first[..first_len];
    let second_len = len - first_len;
    let second = &second
        .get(..second_len)
        .ok_or(Error::BugRequestedLengthExceedsBufferBounds)?;
    Ok([first, second])
}

pub(crate) struct DropGuardSendBeforeDeath<Msg> {
    msg: Option<Msg>,
    tx: WeakUnboundedSender<Msg>,
}

impl<Msg> DropGuardSendBeforeDeath<Msg> {
    pub fn new(msg: Msg, tx: &UnboundedSender<Msg>) -> Self {
        Self {
            msg: Some(msg),
            tx: tx.downgrade(),
        }
    }

    pub fn disarm(&mut self) {
        self.msg = None;
    }
}

impl<Msg> Drop for DropGuardSendBeforeDeath<Msg> {
    fn drop(&mut self) {
        if let Some(msg) = self.msg.take()
            && let Some(tx) = self.tx.upgrade()
        {
            let _ = tx.send(msg);
        }
    }
}

#[inline(always)]
pub fn run_before_and_after_if_changed<
    'a,
    Object: 'a,
    Value: PartialEq + Copy + std::fmt::Debug + 'static,
    ChangeResult,
>(
    obj: &mut Object,
    calc: impl Fn(&Object) -> Value,
    maybe_change: impl FnOnce(&mut Object) -> ChangeResult,
    callback: impl FnOnce(&Object, &Value, &Value),
) -> ChangeResult {
    let before = calc(obj);
    let result = maybe_change(obj);
    let after = calc(obj);
    if before != after {
        callback(obj, &before, &after);
    }
    result
}

pub struct FnDropGuard<F: FnOnce()> {
    f: Option<F>,
}

impl<F: FnOnce()> FnDropGuard<F> {
    pub fn new(f: F) -> Self {
        Self { f: Some(f) }
    }

    pub fn disarm(&mut self) {
        self.f = None;
    }
}

impl<F: FnOnce()> Drop for FnDropGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::prepare_2_ioslices;

    #[test]
    fn test_seq_nr_offset() {
        use super::seq_nr_offset;

        // no wraps
        assert_eq!(seq_nr_offset(2, 1, 1024), 1);
        assert_eq!(seq_nr_offset(1, 1, 1024), 0);
        assert_eq!(seq_nr_offset(0, 1, 1024), -1);

        // new wraps within tolerance
        assert_eq!(seq_nr_offset(0, u16::MAX, 1024), 1);
        assert_eq!(seq_nr_offset(1023, u16::MAX, 1024), 1024);

        // old wraps within tolerance
        assert_eq!(seq_nr_offset(u16::MAX, 0, 1024), -1);
        assert_eq!(seq_nr_offset(u16::MAX, 1023, 1024), -1024);

        // new wraps outside tolerance
        assert_eq!(
            seq_nr_offset(1024, u16::MAX, 1024),
            -(u16::MAX as isize - 1024)
        );

        // old wraps outside tolerance
        assert_eq!(
            seq_nr_offset(u16::MAX, 1024, 1024),
            u16::MAX as isize - 1024
        );
    }

    #[test]
    fn test_prepare_2_ioslices() {
        // Test empty request
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 0).unwrap();
        assert_eq!(result, [&[][..] as &[u8], &[][..]]);

        // Test single slice scenarios
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 2).unwrap();
        assert_eq!(result, [&[1, 2][..], &[][..]]);

        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 1, 2).unwrap();
        assert_eq!(result, [&[2, 3][..], &[][..]]);

        // Test cross-slice scenarios
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 2, 2).unwrap();
        assert_eq!(result, [&[3][..], &[4][..]]);

        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 3, 2).unwrap();
        assert_eq!(result, [&[][..] as &[u8], &[4, 5][..]]);

        // Test full length
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 6).unwrap();
        assert_eq!(result, [&[1, 2, 3][..], &[4, 5, 6][..]]);

        // Test error cases
        assert!(prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 7, 1).is_err()); // offset too large
        assert!(prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 7).is_err()); // length too large
        assert!(prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 5, 2).is_err()); // offset + length too large
    }
}
