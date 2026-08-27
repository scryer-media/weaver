const OPEN_FILE_LIMIT_TARGETS: [u64; 4] = [65_536, 16_384, 4_096, 1_024];

/// Raise the process open-file soft limit toward a practical ceiling.
///
/// macOS GUI processes commonly inherit a soft limit of 256 even when the
/// kernel hard limit is much higher. Download sockets, direct-store writers,
/// and a large PAR2 set can legitimately exceed that. Never cross the
/// operator's hard limit, and keep running with the inherited limit when the
/// platform refuses every candidate.
#[allow(clippy::unnecessary_cast)]
pub fn raise_open_file_limit() -> u64 {
    #[cfg(unix)]
    // SAFETY: `rlimit` is an integer-only C struct. `getrlimit` and
    // `setrlimit` only read the live stack values passed to them.
    unsafe {
        let mut limit: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return 0;
        }

        let inherited = limit.rlim_cur;
        for target in OPEN_FILE_LIMIT_TARGETS {
            let wanted = if limit.rlim_max == libc::RLIM_INFINITY {
                target as libc::rlim_t
            } else {
                (target as libc::rlim_t).min(limit.rlim_max)
            };
            if wanted <= limit.rlim_cur {
                continue;
            }
            let mut next = limit;
            next.rlim_cur = wanted;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &next) == 0 {
                return wanted as u64;
            }
        }
        inherited as u64
    }

    #[cfg(not(unix))]
    {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_file_limit_targets_step_down_to_portable_floor() {
        assert_eq!(OPEN_FILE_LIMIT_TARGETS, [65_536, 16_384, 4_096, 1_024]);
        assert!(
            OPEN_FILE_LIMIT_TARGETS
                .windows(2)
                .all(|pair| pair[0] > pair[1])
        );
    }
}
