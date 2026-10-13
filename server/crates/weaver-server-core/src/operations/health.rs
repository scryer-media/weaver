use std::fmt;
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
    // Directories only.
    pub entries: Vec<DirectoryBrowseEntry>,
    // Regular files directly inside the directory; empty unless asked for.
    pub files: Vec<DirectoryBrowseEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateDirectoryError {
    InvalidInput(&'static str),
    Conflict(String),
    Internal(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowseDirectoryError {
    InvalidInput(String),
    Internal(String),
}

impl fmt::Display for BrowseDirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrowseDirectoryError::InvalidInput(message)
            | BrowseDirectoryError::Internal(message) => f.write_str(message),
        }
    }
}

impl From<String> for BrowseDirectoryError {
    fn from(message: String) -> Self {
        if message == "Directory is not readable" {
            Self::InvalidInput(message)
        } else {
            Self::Internal(message)
        }
    }
}

pub fn browse_directories(
    path: &Path,
    include_files: bool,
) -> Result<DirectoryBrowseListing, BrowseDirectoryError> {
    let path = requested_directory(path)?;
    directory_listing_for_path(&path, include_files).map_err(BrowseDirectoryError::from)
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

    directory_listing_for_path(&target, false).map_err(CreateDirectoryError::Internal)
}

fn requested_directory(path: &Path) -> Result<PathBuf, BrowseDirectoryError> {
    if !path.is_absolute() {
        return Err(BrowseDirectoryError::InvalidInput(
            "path must be absolute".to_string(),
        ));
    }

    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(path.to_path_buf()),
        Ok(_) => Err(BrowseDirectoryError::InvalidInput(
            "Path is not a directory".to_string(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(
            BrowseDirectoryError::InvalidInput("Directory does not exist".to_string()),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Err(
            BrowseDirectoryError::InvalidInput("Directory is not readable".to_string()),
        ),
        Err(error) => Err(BrowseDirectoryError::Internal(format!(
            "failed to read directory metadata: {error}"
        ))),
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

fn by_name(left: &DirectoryBrowseEntry, right: &DirectoryBrowseEntry) -> std::cmp::Ordering {
    left.name
        .to_ascii_lowercase()
        .cmp(&right.name.to_ascii_lowercase())
        .then_with(|| left.name.cmp(&right.name))
}

fn directory_listing_for_path(
    path: &Path,
    include_files: bool,
) -> Result<DirectoryBrowseListing, String> {
    let path = existing_directory(path)?;

    let mut entries = Vec::new();
    let mut files = Vec::new();
    let children = std::fs::read_dir(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            "Directory is not readable".to_string()
        } else {
            format!("failed to read directory: {error}")
        }
    })?;
    for entry in children.filter_map(|entry| entry.ok()) {
        let entry_path = entry.path();
        // Judged through the link, so a link to a directory is a directory and
        // a link to a file is a file.
        let Ok(metadata) = std::fs::metadata(&entry_path) else {
            continue;
        };
        let listed = if metadata.is_dir() {
            &mut entries
        } else if include_files && metadata.is_file() {
            &mut files
        } else {
            continue;
        };
        listed.push(DirectoryBrowseEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            path: entry_path.to_string_lossy().into_owned(),
        });
    }

    entries.sort_by(by_name);
    files.sort_by(by_name);

    let current_path = path.to_string_lossy().into_owned();
    let parent_path = PathBuf::from(&path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
        .filter(|parent| parent != &current_path);

    Ok(DirectoryBrowseListing {
        current_path,
        parent_path,
        entries,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_directory_creates_folder_and_returns_created_listing() {
        let tempdir = tempfile::TempDir::new().expect("temp dir");

        let listing = create_directory(tempdir.path(), "alpha").expect("folder should be created");

        // Annotated because two `typed-path` versions in the graph make a bare
        // `as_ref()` ambiguous here.
        let expected_dir = tempdir.path().join("alpha");
        let expected_path: &str = &expected_dir.to_string_lossy();
        assert_eq!(listing.current_path, expected_path);
        assert!(listing.entries.is_empty());
        assert!(listing.files.is_empty());
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

    fn names(entries: &[DirectoryBrowseEntry]) -> Vec<&str> {
        entries.iter().map(|entry| entry.name.as_str()).collect()
    }

    fn mixed_directory() -> tempfile::TempDir {
        let tempdir = tempfile::TempDir::new().expect("temp dir");
        for folder in ["Zulu", "alpha"] {
            std::fs::create_dir(tempdir.path().join(folder)).expect("folder");
        }
        for file in ["banana.txt", "Date.txt", "Cherry.txt", "apple.txt"] {
            std::fs::write(tempdir.path().join(file), b"x").expect("file");
        }
        std::fs::write(tempdir.path().join("alpha").join("nested.txt"), b"x").expect("nested");
        tempdir
    }

    #[test]
    fn files_are_listed_only_when_asked_for() {
        let tempdir = mixed_directory();

        let folders_only = browse_directories(tempdir.path(), false).expect("listing");
        assert_eq!(names(&folders_only.entries), ["alpha", "Zulu"]);
        assert!(folders_only.files.is_empty());

        let with_files = browse_directories(tempdir.path(), true).expect("listing");
        assert_eq!(with_files.entries, folders_only.entries);
        assert_eq!(
            names(&with_files.files),
            ["apple.txt", "banana.txt", "Cherry.txt", "Date.txt"]
        );
        let expected_file = tempdir.path().join("banana.txt");
        let expected_path: &str = &expected_file.to_string_lossy();
        assert_eq!(with_files.files[1].path, expected_path);
    }

    #[cfg(unix)]
    #[test]
    fn a_link_is_listed_as_what_it_points_to() {
        let tempdir = mixed_directory();
        let root = tempdir.path();
        std::os::unix::fs::symlink(root.join("banana.txt"), root.join("link-to-file")).unwrap();
        std::os::unix::fs::symlink(root.join("alpha"), root.join("link-to-folder")).unwrap();
        std::os::unix::fs::symlink(root.join("gone"), root.join("link-to-nothing")).unwrap();

        let listing = browse_directories(root, true).expect("listing");

        assert_eq!(names(&listing.entries), ["alpha", "link-to-folder", "Zulu"]);
        assert_eq!(
            names(&listing.files),
            [
                "apple.txt",
                "banana.txt",
                "Cherry.txt",
                "Date.txt",
                "link-to-file"
            ]
        );
    }
}
