/// Minimum LZ window allocation used by official UnRAR for modern unpackers.
///
/// RAR5 headers can declare 128 KiB, but `Unpack::Init` allocates at least
/// 256 KiB so filter accounting always has enough room.
pub const UNRAR_MIN_LZ_WINDOW_SIZE: u64 = 0x40000;

/// Maximum dictionary official UnRAR permits for extraction.
pub const UNRAR_UNPACK_MAX_DICT_SIZE: u64 = 0x1000000000;

/// Maximum RAR5 header body size accepted by official UnRAR.
pub const UNRAR_RAR5_MAX_HEADER_BODY: u64 = 0x200000;

/// Weaver's finite anti-abuse ceiling for a single packed or unpacked member.
pub const WEAVER_MAX_MEMBER_DATA_SIZE: u64 = 500 * 1024 * 1024 * 1024;

/// Configurable limits for archive processing to prevent resource exhaustion.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum header body size in bytes (default 2 MiB, matching UnRAR).
    pub max_header_size: u64,
    /// Maximum single data segment size in bytes.
    pub max_data_segment: u64,
    /// Maximum unpacked output size in bytes.
    pub max_unpacked_size: u64,
    /// Maximum dictionary size in bytes (default 256 MB).
    pub max_dict_size: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_header_size: UNRAR_RAR5_MAX_HEADER_BODY,
            max_data_segment: WEAVER_MAX_MEMBER_DATA_SIZE,
            max_unpacked_size: WEAVER_MAX_MEMBER_DATA_SIZE,
            max_dict_size: 256 * 1024 * 1024, // 256 MB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = Limits::default();
        assert_eq!(limits.max_header_size, UNRAR_RAR5_MAX_HEADER_BODY);
        assert_eq!(limits.max_data_segment, WEAVER_MAX_MEMBER_DATA_SIZE);
        assert_eq!(limits.max_unpacked_size, WEAVER_MAX_MEMBER_DATA_SIZE);
        assert_eq!(limits.max_dict_size, 256 * 1024 * 1024);
    }

    #[test]
    fn default_member_data_limit_covers_large_media_members() {
        let observed_bluray_member_size = 68_325_814_272;
        assert!(observed_bluray_member_size <= Limits::default().max_unpacked_size);
        assert_eq!(WEAVER_MAX_MEMBER_DATA_SIZE, 500 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_custom_limits() {
        let limits = Limits {
            max_header_size: 1024,
            max_data_segment: 2048,
            max_unpacked_size: 4096,
            max_dict_size: 8192,
        };
        assert_eq!(limits.max_header_size, 1024);
        assert_eq!(limits.max_data_segment, 2048);
        assert_eq!(limits.max_unpacked_size, 4096);
        assert_eq!(limits.max_dict_size, 8192);
    }
}
