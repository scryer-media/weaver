use std::collections::HashSet;

pub fn sanitize_download_filename(name: &str) -> String {
    let mut sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_control()
                || matches!(ch, '"' | '*' | '/' | ':' | '<' | '>' | '?' | '\\' | '|')
            {
                '_'
            } else {
                ch
            }
        })
        .collect::<String>();

    sanitized = sanitized.trim_end_matches([' ', '.']).to_string();
    if sanitized.is_empty() {
        return "unknown".to_string();
    }

    let basename = sanitized
        .split_once('.')
        .map_or(sanitized.as_str(), |(basename, _)| basename);
    if is_windows_device_name(&sanitized) || is_windows_device_name(basename) {
        sanitized.insert(0, '_');
    }

    sanitized
}

pub fn unique_download_filenames<'a, I>(names: I) -> Vec<String>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut occupied = HashSet::new();
    names
        .into_iter()
        .map(|name| allocate_unique_download_filename(name, &mut occupied))
        .collect()
}

pub fn allocate_unique_download_filename(
    name: &str,
    occupied_keys: &mut HashSet<String>,
) -> String {
    let sanitized = sanitize_download_filename(name);
    let key = download_filename_collision_key(&sanitized);
    if occupied_keys.insert(key) {
        return sanitized;
    }

    let mut duplicate = 1usize;
    loop {
        let candidate = duplicate_filename_candidate(&sanitized, duplicate);
        let key = download_filename_collision_key(&candidate);
        if occupied_keys.insert(key) {
            return candidate;
        }
        duplicate += 1;
    }
}

pub fn reserve_download_filename(name: &str, occupied_keys: &mut HashSet<String>) {
    occupied_keys.insert(download_filename_collision_key(name));
}

pub fn forget_reserved_download_filename(name: &str, occupied_keys: &mut HashSet<String>) {
    occupied_keys.remove(&download_filename_collision_key(name));
}

pub fn download_filename_collision_key(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub(crate) fn role_filename_view(name: &str) -> &str {
    name.trim_end_matches(|ch: char| {
        ch.is_ascii_whitespace() || matches!(ch, '"' | '\'' | '`' | '_' | '.')
    })
}

fn duplicate_filename_candidate(name: &str, duplicate: usize) -> String {
    let split = duplicate_extension_split(name);
    let (stem, extension) = name.split_at(split);
    format!("{stem}.duplicate{duplicate}{extension}")
}

fn duplicate_extension_split(name: &str) -> usize {
    let Some(extension) = name.rfind('.').filter(|index| *index > 0) else {
        return name.len();
    };

    if name[extension..].eq_ignore_ascii_case(".par2") {
        let stem = &name[..extension];
        if let Some(vol_extension) = stem.rfind('.').filter(|index| *index > 0)
            && stem[vol_extension..]
                .get(..4)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(".vol"))
        {
            return vol_extension;
        }
    }

    extension
}

fn is_windows_device_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}
