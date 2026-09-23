//! The one seam the two container families meet at.
//!
//! The router asks its layout exactly two questions — "which members does this
//! archive have" and "where do these physical volume bytes belong" — and every
//! other consumer in the subsystem reaches the layout through one of those two
//! answers. Both are pure coordinates: a member name and extent, a slice that
//! says member, envelope or unroutable. Nothing downstream of them is
//! format-aware, which is why a second family is an arm here rather than a
//! second router, a second routing path, a second provider and a second
//! finalization.

use super::sevenz::SevenZipLayout;
use unrar_rs::{MappedSlice, StoredLayoutBuilder, StoredMember};

/// The layout engine reading this set's containers.
pub(super) enum SetLayout {
    /// RAR, grown volume by volume as each one's headers are walked.
    Rar(StoredLayoutBuilder),
    /// 7z, built once and whole from the container's end header.
    SevenZip(SevenZipLayout),
}

impl SetLayout {
    pub(super) fn members(&self) -> &[StoredMember] {
        match self {
            Self::Rar(layout) => layout.members(),
            Self::SevenZip(layout) => layout.members(),
        }
    }

    pub(super) fn map_physical_range(
        &self,
        volume: u32,
        offset: u64,
        len: u64,
    ) -> Vec<MappedSlice> {
        match self {
            Self::Rar(layout) => layout.map_physical_range(volume, offset, len),
            Self::SevenZip(layout) => layout.map_physical_range(volume, offset, len),
        }
    }

    /// The RAR builder, for the paths that grow it. `None` for a 7z set, whose
    /// layout is never added to after it exists.
    pub(super) fn rar(&self) -> Option<&StoredLayoutBuilder> {
        match self {
            Self::Rar(layout) => Some(layout),
            Self::SevenZip(_) => None,
        }
    }

    pub(super) fn rar_mut(&mut self) -> Option<&mut StoredLayoutBuilder> {
        match self {
            Self::Rar(layout) => Some(layout),
            Self::SevenZip(_) => None,
        }
    }

    pub(super) fn sevenz(&self) -> Option<&SevenZipLayout> {
        match self {
            Self::Rar(_) => None,
            Self::SevenZip(layout) => Some(layout),
        }
    }

    /// A short label for diagnostics — the archive format for a RAR set, the
    /// family for a 7z one, which has only the one.
    pub(super) fn label(&self) -> LayoutLabel {
        match self {
            Self::Rar(layout) => LayoutLabel(format!("{:?}", layout.format())),
            Self::SevenZip(_) => LayoutLabel("SevenZip".to_string()),
        }
    }
}

/// A layout label in a diagnostic line, written as the bare name a format enum
/// would have printed rather than as a quoted string.
pub(super) struct LayoutLabel(String);

impl std::fmt::Debug for LayoutLabel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}
