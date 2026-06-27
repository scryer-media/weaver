/// Minimum LZ window allocation used by official UnRAR for modern unpackers.
///
/// RAR5 headers can declare 128 KiB, but `Unpack::Init` allocates at least
/// 256 KiB so filter accounting always has enough room.
pub const UNRAR_MIN_LZ_WINDOW_SIZE: u64 = 0x40000;

/// Maximum dictionary official UnRAR permits for extraction.
pub const UNRAR_UNPACK_MAX_DICT_SIZE: u64 = 0x1000000000;

/// Maximum RAR5 header body size accepted by official UnRAR.
pub const UNRAR_RAR5_MAX_HEADER_BODY: u64 = 0x200000;

/// Configurable limits for archive processing to prevent resource exhaustion.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum header body size in bytes (default 2 MiB, matching UnRAR).
    pub max_header_size: u64,
    /// Maximum single data segment size in bytes (default 2 GB).
    pub max_data_segment: u64,
    /// Maximum unpacked output size in bytes (default 4 GB).
    pub max_unpacked_size: u64,
    /// Maximum dictionary size in bytes (default 256 MB).
    pub max_dict_size: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_header_size: UNRAR_RAR5_MAX_HEADER_BODY,
            max_data_segment: 2 * 1024 * 1024 * 1024, // 2 GB
            max_unpacked_size: 4 * 1024 * 1024 * 1024, // 4 GB
            max_dict_size: 256 * 1024 * 1024,         // 256 MB
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
        assert_eq!(limits.max_data_segment, 2 * 1024 * 1024 * 1024);
        assert_eq!(limits.max_unpacked_size, 4 * 1024 * 1024 * 1024);
        assert_eq!(limits.max_dict_size, 256 * 1024 * 1024);
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
