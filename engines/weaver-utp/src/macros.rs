// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

macro_rules! log_every_ms {
    ($dur:expr, $level:expr, $($rest:tt)*) => {
        static LAST_RUN: ::std::sync::atomic::AtomicU64 = ::std::sync::atomic::AtomicU64::new(0);
        static EVENT_COUNT: ::std::sync::atomic::AtomicU64 =
            ::std::sync::atomic::AtomicU64::new(0);

        EVENT_COUNT.fetch_add(1, ::std::sync::atomic::Ordering::Relaxed);

        if let Ok(now) = std::time::SystemTime::now().duration_since(::std::time::UNIX_EPOCH) {
            let last = LAST_RUN.load(::std::sync::atomic::Ordering::Relaxed);
            let now = now.as_millis() as u64;

            if now.saturating_sub(last) > $dur {
                if let Ok(_) = LAST_RUN.compare_exchange_weak(
                    last,
                    now,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                ) {
                    // Reset the counter after getting its value
                    let events_since_last =
                        EVENT_COUNT.swap(0, ::std::sync::atomic::Ordering::Relaxed).saturating_sub(1);
                    tracing::event!($level, skipped_logs=events_since_last, $($rest)*);
                }
            }
        }
    };
}

macro_rules! non_zero_const {
    ($v:expr) => {
        const {
            match std::num::NonZero::new($v) {
                Some(v) => v,
                None => unreachable!(),
            }
        }
    };
}

#[allow(unused)]
macro_rules! trace_dbg {
    ($e:expr) => {{
        let expr = $e;
        trace!(?expr);
        expr
    }};
}

#[allow(unused)]
macro_rules! trace_every_ms {
    ($dur:expr, $($rest:tt)*) => {
        log_every_ms!($dur, tracing::Level::TRACE, $($rest)*);
    };
}

#[allow(unused)]
macro_rules! debug_every_ms {
    ($dur:expr, $($rest:tt)*) => {
        log_every_ms!($dur, tracing::Level::DEBUG, $($rest)*);
    };
}

#[allow(unused)]
macro_rules! warn_every_ms {
    ($dur:expr, $($rest:tt)*) => {
        log_every_ms!($dur, tracing::Level::WARN, $($rest)*);
    };
}

macro_rules! log_every_ms_if_changed {
    ($dur:expr, $level:expr, $name:expr, $obj:expr, $calc:expr, $maybe_change:expr) => {
        crate::utils::run_before_and_after_if_changed($obj, $calc, $maybe_change, |_, before, after| {
            log_every_ms!($dur, $level, before=?before, after=?after, "{} changed", $name);
        });
    };
}

macro_rules! log_if_changed {
    ($level:expr, $name:expr, $obj:expr, $calc:expr, $maybe_change:expr) => {
        crate::utils::run_before_and_after_if_changed($obj, $calc, $maybe_change, |_, before, after| {
            tracing::event!($level, before=?before, after=?after, "{} changed", $name);
        });
    };
}

// Create a mock header that can be used to compare with others
#[cfg(test)]
macro_rules! cmphead {
    // Recursive case - multiple fields
    ($htype:expr, $($rest_name:ident=$rest_value:expr),+) => {
        crate::test_util::cmphead::CmpUtpHeader {
            htype: $htype,
            $($rest_name: Some($rest_value.into())),+,
            ..Default::default()
        }
    };
}

macro_rules! try_shrink_or_neg {
    ($v:expr) => {
        $v.try_into().unwrap_or(-1)
    };
}
