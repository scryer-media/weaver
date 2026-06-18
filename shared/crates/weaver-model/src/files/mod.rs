mod archive_name;
mod classify;
mod sanitize;

pub use archive_name::archive_base_name;
pub use classify::FileRole;
pub(crate) use sanitize::role_filename_view;
pub use sanitize::{
    allocate_unique_download_filename, download_filename_collision_key,
    forget_reserved_download_filename, reserve_download_filename, sanitize_download_filename,
    unique_download_filenames,
};
