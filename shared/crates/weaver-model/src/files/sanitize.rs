use std::collections::HashSet;

pub const DOWNLOAD_FILENAME_MAX_BYTES: usize = 240;

pub fn sanitize_download_filename(name: &str) -> String {
    sanitize_path_component(name)
}

pub fn sanitize_path_component(name: &str) -> String {
    let sanitized = name
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

    finalize_sanitized_component(sanitized)
}

pub fn path_component_with_suffix(component: &str, suffix: &str) -> String {
    let sanitized_suffix = sanitize_suffix_fragment(suffix);
    finalize_sanitized_component(bounded_filename_from_parts(
        component,
        &sanitized_suffix,
        "",
    ))
}

fn finalize_sanitized_component(mut sanitized: String) -> String {
    sanitized = trim_for_filesystem_component(&sanitized).to_string();
    if sanitized.is_empty() {
        sanitized = "unknown".to_string();
    }

    sanitized = truncate_filename_component(&sanitized);
    sanitized = trim_for_filesystem_component(&sanitized).to_string();
    if sanitized.is_empty() {
        sanitized = "unknown".to_string();
    }

    if component_is_windows_device_name(&sanitized) {
        sanitized.insert(0, '_');
        sanitized = truncate_filename_component(&sanitized);
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
    bounded_filename_from_parts(stem, &format!(".duplicate{duplicate}"), extension)
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

fn component_is_windows_device_name(name: &str) -> bool {
    let basename = name.split_once('.').map_or(name, |(basename, _)| basename);
    is_windows_device_name(name) || is_windows_device_name(basename)
}

fn truncate_filename_component(name: &str) -> String {
    if name.len() <= DOWNLOAD_FILENAME_MAX_BYTES {
        return name.to_string();
    }

    let split = duplicate_extension_split(name);
    let (stem, extension) = name.split_at(split);
    bounded_filename_from_parts(stem, "", extension)
}

fn bounded_filename_from_parts(stem: &str, infix: &str, extension: &str) -> String {
    let total_len = stem
        .len()
        .saturating_add(infix.len())
        .saturating_add(extension.len());
    if total_len <= DOWNLOAD_FILENAME_MAX_BYTES {
        return format!("{stem}{infix}{extension}");
    }

    let suffix_len = infix.len().saturating_add(extension.len());
    if suffix_len >= DOWNLOAD_FILENAME_MAX_BYTES {
        return truncate_to_utf8_boundary(
            &format!("{stem}{infix}{extension}"),
            DOWNLOAD_FILENAME_MAX_BYTES,
        );
    }

    let stem_budget = DOWNLOAD_FILENAME_MAX_BYTES - suffix_len;
    let mut bounded_stem = truncate_to_utf8_boundary(stem, stem_budget);
    bounded_stem = trim_after_truncation(&bounded_stem).to_string();
    if bounded_stem.is_empty() {
        bounded_stem = "unknown".to_string();
        if bounded_stem.len() > stem_budget {
            bounded_stem = truncate_to_utf8_boundary(&bounded_stem, stem_budget);
        }
    }

    format!("{bounded_stem}{infix}{extension}")
}

fn truncate_to_utf8_boundary(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn trim_for_filesystem_component(value: &str) -> &str {
    value.trim_end_matches([' ', '.'])
}

fn trim_after_truncation(value: &str) -> &str {
    value.trim_end_matches(|ch: char| ch.is_ascii_whitespace() || matches!(ch, '.' | '-' | '_'))
}

fn sanitize_suffix_fragment(suffix: &str) -> String {
    suffix
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
        .collect()
}
