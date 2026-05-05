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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateDirectoryError {
    InvalidInput(&'static str),
    Conflict(String),
    Internal(String),
}

pub fn browse_directories(path: &Path) -> Result<DirectoryBrowseListing, String> {
    let path = nearest_existing_directory(path)?;
    directory_listing_for_path(&path)
}

pub fn create_directory(
    path: &Path,
    name: &str,
) -> Result<DirectoryBrowseListing, CreateDirectoryError> {
    let parent = existing_directory(path).map_err(CreateDirectoryError::Internal)?;
    let folder_name = name.trim();

    if folder_name.is_empty() {
        return Err(CreateDirectoryError::InvalidInput(
            "folder name must not be empty",
        ));
    }
    if folder_name == "." || folder_name == ".." {
        return Err(CreateDirectoryError::InvalidInput(
            "folder name must not be '.' or '..'",
        ));
    }
    if folder_name.contains('/') || folder_name.contains('\\') {
        return Err(CreateDirectoryError::InvalidInput(
            "folder name must not contain path separators",
        ));
    }

    let target = parent.join(folder_name);
    match std::fs::create_dir(&target) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(CreateDirectoryError::Conflict(format!(
                "folder '{folder_name}' already exists"
            )));
        }
        Err(error) => {
            return Err(CreateDirectoryError::Internal(format!(
                "failed to create directory: {error}"
            )));
        }
    }

    directory_listing_for_path(&target).map_err(CreateDirectoryError::Internal)
}

fn nearest_existing_directory(path: &Path) -> Result<PathBuf, String> {
    let mut candidate = path.to_path_buf();

    loop {
        match std::fs::metadata(&candidate) {
            Ok(metadata) => {
                if metadata.is_dir() {
                    return Ok(candidate);
                }
                return Err("path is not a directory".to_string());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !candidate.pop() {
                    return Err(format!("failed to read directory metadata: {error}"));
                }
            }
            Err(error) => return Err(format!("failed to read directory metadata: {error}")),
        }
    }
}

fn existing_directory(path: &Path) -> Result<PathBuf, String> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("failed to read directory metadata: {error}"))?;
    if !metadata.is_dir() {
        return Err("path is not a directory".to_string());
    }
    Ok(path.to_path_buf())
}

fn directory_listing_for_path(path: &Path) -> Result<DirectoryBrowseListing, String> {
    let path = existing_directory(path)?;

    let mut entries = std::fs::read_dir(&path)
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
    let parent_path = PathBuf::from(&path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
        .filter(|parent| parent != &current_path);

    Ok(DirectoryBrowseListing {
        current_path,
        parent_path,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_directory_creates_folder_and_returns_created_listing() {
        let tempdir = tempfile::TempDir::new().expect("temp dir");

        let listing = create_directory(tempdir.path(), "alpha").expect("folder should be created");

        assert_eq!(
            listing.current_path,
            tempdir.path().join("alpha").to_string_lossy().as_ref()
        );
        assert!(tempdir.path().join("alpha").is_dir());
    }

    #[test]
    fn create_directory_rejects_path_separators() {
        let tempdir = tempfile::TempDir::new().expect("temp dir");

        let error = create_directory(tempdir.path(), "nested/child")
            .expect_err("nested paths should be rejected");

        assert_eq!(
            error,
            CreateDirectoryError::InvalidInput("folder name must not contain path separators")
        );
    }
}
