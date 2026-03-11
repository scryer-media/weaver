/// Configurable limits for archive processing to prevent resource exhaustion.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum header body size in bytes (default 16 MB).
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
            max_header_size: 16 * 1024 * 1024,         // 16 MB
            max_data_segment: 2 * 1024 * 1024 * 1024,  // 2 GB
            max_unpacked_size: 4 * 1024 * 1024 * 1024, // 4 GB
            max_dict_size: 256 * 1024 * 1024,          // 256 MB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = Limits::default();
        assert_eq!(limits.max_header_size, 16 * 1024 * 1024);
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
