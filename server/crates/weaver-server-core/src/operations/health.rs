use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryBrowseEntry {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryBrowseListing {
    pub current_path: String,
    pub parent_path: Option<String>,
    pub entries: Vec<DirectoryBrowseEntry>,
}

pub fn browse_directories(path: &Path) -> Result<DirectoryBrowseListing, String> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("failed to read directory metadata: {error}"))?;
    if !metadata.is_dir() {
        return Err("path is not a directory".to_string());
    }

    let mut entries = std::fs::read_dir(path)
        .map_err(|error| format!("failed to read directory: {error}"))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let entry_path = entry.path();
            let metadata = std::fs::metadata(&entry_path).ok()?;
            if !metadata.is_dir() {
                return None;
            }
            Some(DirectoryBrowseEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                path: entry_path.to_string_lossy().into_owned(),
            })
        })
        .collect::<Vec<_>>();

    entries.sort_by(|left, right| {
        left.name
            .to_ascii_lowercase()
            .cmp(&right.name.to_ascii_lowercase())
            .then_with(|| left.name.cmp(&right.name))
    });

    let current_path = path.to_string_lossy().into_owned();
    let parent_path = PathBuf::from(path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
        .filter(|parent| parent != &current_path);

    Ok(DirectoryBrowseListing {
        current_path,
        parent_path,
        entries,
    })
}
