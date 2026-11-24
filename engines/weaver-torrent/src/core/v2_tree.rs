// SPDX-License-Identifier: GPL-3.0-or-later

//! The BEP 52 `file tree` of a v2 info dict, read in the order it is
//! written. Reading keeps the tree's shape and leaves its values raw; the
//! rules on those values live in the metainfo's validation.

use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_derive::Deserialize as DeriveDeserialize;

use crate::bencode::WithRawBytes;
use crate::bencode::raw_value::TAG as RAW_TAG;
use crate::buffers::ByteBuf;
use crate::clone_to_owned::CloneToOwned;

/// Directories a file tree may nest (libtorrent
/// `load_torrent_limits::max_directory_depth`).
pub(crate) const MAX_DIRECTORY_DEPTH: usize = 100;

/// Bytes of a path element kept before it is cut (the 240 in libtorrent
/// `sanitize_append_path_element`).
const MAX_ELEMENT_BYTES: usize = 240;

/// A file of the tree as written: its path of raw keys and the raw values
/// of its leaf dict.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TreeLeaf {
    pub path: Vec<Vec<u8>>,
    pub length: Option<i64>,
    pub pieces_root: Option<Vec<u8>>,
    pub attr: Option<Vec<u8>>,
}

/// The `file tree` dict: its files in tree order, and the bytes it was read
/// from, which are written back unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTree<Buf> {
    pub files: Vec<TreeLeaf>,
    /// The tree is a single file at its top, BEP 52's single-file form.
    pub single_file: bool,
    raw: Buf,
}

impl<Buf: Serialize> Serialize for FileTree<Buf> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The bytes as they were read, without a string prefix.
        serializer.serialize_newtype_struct(RAW_TAG, &self.raw)
    }
}

impl<'de, Buf: Deserialize<'de>> Deserialize<'de> for FileTree<Buf> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let WithRawBytes { data, raw_bytes } =
            WithRawBytes::<Walked, Buf>::deserialize(deserializer)?;
        Ok(FileTree {
            files: data.files,
            single_file: data.single_file,
            raw: raw_bytes,
        })
    }
}

impl<Buf: CloneToOwned> CloneToOwned for FileTree<Buf> {
    type Target = FileTree<Buf::Target>;

    fn clone_to_owned(&self, within_buffer: Option<&bytes::Bytes>) -> Self::Target {
        FileTree {
            files: self.files.clone(),
            single_file: self.single_file,
            raw: self.raw.clone_to_owned(within_buffer),
        }
    }
}

struct Walked {
    files: Vec<TreeLeaf>,
    single_file: bool,
}

impl<'de> Deserialize<'de> for Walked {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut files = Vec::new();
        let top = deserializer.deserialize_map(Directory {
            path: &mut Vec::new(),
            files: &mut files,
            depth: 1,
        })?;
        Ok(Walked {
            single_file: top.entries == 1 && top.leaves == 1,
            files,
        })
    }
}

/// What a directory held directly.
#[derive(Default)]
struct Entries {
    entries: usize,
    leaves: usize,
}

/// A directory: a dict whose keys are names, strictly ascending.
struct Directory<'a> {
    path: &'a mut Vec<Vec<u8>>,
    files: &'a mut Vec<TreeLeaf>,
    depth: usize,
}

impl<'de> Visitor<'de> for Directory<'_> {
    type Value = Entries;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a file tree directory")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Entries, A::Error> {
        walk_directory(&mut map, None, self.path, self.files, self.depth)
    }
}

/// The entries of a directory from `first`, a name already read.
fn walk_directory<'de, A: MapAccess<'de>>(
    map: &mut A,
    first: Option<&'de [u8]>,
    path: &mut Vec<Vec<u8>>,
    files: &mut Vec<TreeLeaf>,
    depth: usize,
) -> Result<Entries, A::Error> {
    use serde::de::Error;
    let mut seen = Entries::default();
    let mut next = first;
    loop {
        let name = match next.take() {
            Some(name) => name,
            None => match map.next_key::<ByteBuf<'de>>()? {
                Some(name) => name.0,
                None => return Ok(seen),
            },
        };
        // libtorrent `extract_files2`: a node holding both a file and
        // children reaches here as a directory with an empty name.
        if name.is_empty() {
            return Err(A::Error::custom("file tree entry with an empty name"));
        }
        path.push(name.to_vec());
        let is_leaf = map.next_value_seed(Node {
            path: &mut *path,
            files: &mut *files,
            depth,
        })?;
        path.pop();
        seen.entries += 1;
        seen.leaves += usize::from(is_leaf);
    }
}

/// The value under a name: a leaf `{"": {...}}` or a directory.
struct Node<'a> {
    path: &'a mut Vec<Vec<u8>>,
    files: &'a mut Vec<TreeLeaf>,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Node<'_> {
    type Value = bool;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Node<'_> {
    type Value = bool;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a file tree node")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        use serde::de::Error;
        let Some(first) = map.next_key::<ByteBuf<'de>>()? else {
            // An empty directory holds nothing.
            return Ok(false);
        };
        if !first.0.is_empty() {
            if self.depth > MAX_DIRECTORY_DEPTH {
                return Err(A::Error::custom("file tree nested too deep"));
            }
            walk_directory(
                &mut map,
                Some(first.0),
                self.path,
                self.files,
                self.depth + 1,
            )?;
            return Ok(false);
        }
        let leaf: Leaf<'de> = map.next_value()?;
        if map.next_key::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("file tree entry with an empty name"));
        }
        self.files.push(TreeLeaf {
            path: self.path.clone(),
            length: leaf.length,
            pieces_root: leaf.pieces_root.map(|r| r.0.to_vec()),
            attr: leaf.attr.map(|a| a.0.to_vec()),
        });
        Ok(true)
    }
}

#[derive(DeriveDeserialize)]
struct Leaf<'a> {
    #[serde(default)]
    length: Option<i64>,
    #[serde(rename = "pieces root", borrow, default)]
    pieces_root: Option<ByteBuf<'a>>,
    #[serde(borrow, default)]
    attr: Option<ByteBuf<'a>>,
}

/// A path element as libtorrent stores it (`sanitize_append_path_element`
/// with the element forced): leading separators dropped, separators,
/// NUL and invisible formatting characters removed, invalid UTF-8 and
/// control characters replaced by `_`, a name of one or two dots, or of
/// nothing, replaced by `_`, and a name cut at 240 bytes, keeping an
/// extension of up to nine bytes.
pub fn sanitize_component(raw: &[u8]) -> String {
    sanitize_component_with(raw, NameRules::PORTABLE)
}

/// [`sanitize_component`] under `rules`.
pub fn sanitize_component_with(raw: &[u8], rules: NameRules) -> String {
    sanitize_element(raw, rules).unwrap_or_else(|| "_".to_owned())
}

/// A path element that is not forced, as libtorrent stores the torrent's
/// `name` (`extract_single_file` with `top_level`): `None` where it drops
/// the element whole.
pub fn sanitize_name_with(raw: &[u8], rules: NameRules) -> Option<String> {
    sanitize_element(raw, rules)
}

/// The name libtorrent knows a torrent by (`torrent_info::name`):
/// `name.utf-8`, or else `name`, sanitized as a path element that is not
/// forced. What that drops whole, a name of nothing but up to two dots once
/// sanitized, gives way to `fallback`, the hex v1 info hash in libtorrent,
/// as does a torrent with neither name. It is the same name on every
/// platform.
pub fn torrent_name(raw: Option<&[u8]>, fallback: impl FnOnce() -> String) -> String {
    raw.and_then(|raw| sanitize_element(raw, NameRules::PORTABLE))
        .unwrap_or_else(fallback)
}

/// The rules a path element is cleaned under beyond the ones every
/// platform shares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NameRules {
    /// Windows' own: `?<>"|*:` replaced by `_`, trailing spaces and dots
    /// stripped, and a device name (CON, NUL, COM1, ...) made a plain one
    /// (libtorrent torrent_info.cpp:95-98 and :323-342, v2.0.15).
    pub windows: bool,
}

impl NameRules {
    /// The rules every platform shares.
    pub const PORTABLE: Self = Self { windows: false };
    /// The rules of the platform Weaver runs on, which name its files on
    /// disk.
    pub const HOST: Self = Self {
        windows: cfg!(windows),
    };
}

/// Names Windows opens as a device rather than a file wherever they stand
/// in a path, alone or before any extension (libtorrent
/// torrent_info.cpp:205-211, v2.0.15, plus the console's own `CONIN$` and
/// `CONOUT$`).
const WINDOWS_DEVICE_NAMES: [&str; 27] = [
    "con", "prn", "aux", "clock$", "nul", "conin$", "conout$", "com0", "com1", "com2", "com3",
    "com4", "com5", "com6", "com7", "com8", "com9", "lpt0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5",
    "lpt6", "lpt7", "lpt8", "lpt9",
];

/// Whether Windows takes `element` for a device: its part before the first
/// dot, trailing spaces dropped, is a device name, matched without regard
/// to case. libtorrent looks only before the last dot, and only where it
/// opens files without `\\?\` paths (torrent_info.cpp:199-226, v2.0.15);
/// Weaver opens plain paths, where `CON.tar.gz` is the console as much as
/// `CON` is.
fn is_windows_device(element: &str) -> bool {
    let stem = element.split('.').next().unwrap_or(element);
    let stem = stem.trim_end_matches(' ');
    WINDOWS_DEVICE_NAMES
        .iter()
        .any(|device| stem.eq_ignore_ascii_case(device))
}

/// Characters Windows refuses in a name, replaced by `_` (libtorrent
/// `valid_path_character`, torrent_info.cpp:95-98, v2.0.15; backspace is
/// a control character, replaced on every platform).
fn is_windows_invalid(c: char) -> bool {
    matches!(c, '?' | '<' | '>' | '"' | '|' | '*' | ':')
}

/// [`sanitize_component_with`], `None` where libtorrent drops an element it
/// does not force: one of nothing but up to two dots once sanitized, or,
/// under Windows rules, of nothing once its trailing spaces and dots are
/// stripped. An empty element is `_` either way.
fn sanitize_element(raw: &[u8], rules: NameRules) -> Option<String> {
    let mut out = sanitize_portable(raw)?;
    if rules.windows {
        // libtorrent torrent_info.cpp:323-342, v2.0.15.
        if out.contains(is_windows_invalid) {
            out = out
                .chars()
                .map(|c| if is_windows_invalid(c) { '_' } else { c })
                .collect();
        }
        let kept = out.trim_end_matches([' ', '.']).len();
        out.truncate(kept);
        if out.is_empty() {
            return None;
        }
        if is_windows_device(&out) {
            out.insert(0, '_');
        }
    }
    Some(out)
}

fn sanitize_portable(raw: &[u8]) -> Option<String> {
    if raw.is_empty() {
        return Some("_".to_owned());
    }
    let mut element = raw;
    while let [b'/', tail @ ..] = element {
        element = tail;
    }
    let mut out = String::with_capacity(element.len());
    let (mut added, mut dots, mut found_extension) = (0, 0, false);
    let mut at = 0;
    while at < element.len() {
        let (point, len) = next_code_point(&element[at..]);
        match point {
            Some(c) if is_filtered(c) => {}
            Some(c) if is_valid(c) => {
                out.push(c);
                added += len;
                dots += usize::from(c == '.');
                if added >= MAX_ELEMENT_BYTES && !found_extension {
                    let floor = (element.len().saturating_sub(10)).max(at);
                    match (floor + 1..element.len())
                        .rev()
                        .find(|&j| element[j] == b'.')
                    {
                        Some(dot) => {
                            found_extension = true;
                            at = dot;
                            continue;
                        }
                        None => break,
                    }
                }
            }
            _ => {
                out.push('_');
                added += 1;
            }
        }
        at += len;
    }
    if added == dots && added <= 2 {
        return None;
    }
    Some(out)
}

/// The code point at the start of `bytes` and the bytes it spans; `None`
/// for an invalid sequence (libtorrent `parse_utf8_codepoint`).
fn next_code_point(bytes: &[u8]) -> (Option<char>, usize) {
    let len = match bytes[0] {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf7 => 4,
        _ => return (None, 1),
    };
    if len > bytes.len() {
        return (None, bytes.len());
    }
    let point = std::str::from_utf8(&bytes[..len])
        .ok()
        .and_then(|s| s.chars().next());
    (point, len)
}

/// Characters dropped from a name (libtorrent `filter_path_character`).
fn is_filtered(c: char) -> bool {
    matches!(
        c,
        '/' | '\\'
            | '\0'
            | '\u{061c}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{2069}'
            | '\u{feff}'
    )
}

/// Characters kept in a name (libtorrent `valid_path_character`).
fn is_valid(c: char) -> bool {
    !matches!(c, '\u{0}'..='\u{1f}' | '\u{7f}'..='\u{9f}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::from_bytes;
    use crate::buffers::ByteBuf;

    fn tree(raw: &[u8]) -> Result<FileTree<ByteBuf<'_>>, crate::bencode::DeserializeError> {
        from_bytes::<FileTree<ByteBuf>>(raw).map_err(|e| e.into_kind())
    }

    #[test]
    fn a_tree_reads_in_order_and_writes_back_unchanged() {
        let raw = b"d1:ad1:bd0:d6:lengthi3e11:pieces root1:xee1:cd0:d6:lengthi0eeee1:dd0:d4:attr1:x6:lengthi5eeee";
        let t = tree(raw).unwrap();
        let paths: Vec<_> = t.files.iter().map(|f| f.path.clone()).collect();
        assert_eq!(
            paths,
            vec![
                vec![b"a".to_vec(), b"b".to_vec()],
                vec![b"a".to_vec(), b"c".to_vec()],
                vec![b"d".to_vec()],
            ]
        );
        assert_eq!(t.files[0].length, Some(3));
        assert_eq!(t.files[0].pieces_root.as_deref(), Some(&b"x"[..]));
        assert_eq!(t.files[2].attr.as_deref(), Some(&b"x"[..]));
        assert!(!t.single_file);
        let mut out = Vec::new();
        crate::bencode::bencode_serialize_to_writer(&t, &mut out).unwrap();
        assert_eq!(out, raw);

        assert!(tree(b"d1:fd0:d6:lengthi3eeee").unwrap().single_file);
        assert!(!tree(b"d1:dd1:fd0:d6:lengthi3eeeee").unwrap().single_file);
    }

    #[test]
    fn a_node_with_a_file_and_children_is_refused() {
        assert!(tree(b"d1:ad0:d6:lengthi0ee1:bd0:d6:lengthi0eeeee").is_err());
        // An empty name anywhere is refused.
        assert!(tree(b"d0:d0:d6:lengthi0eeee").is_err());
        // A node that is not a dict is refused.
        assert!(tree(b"d1:ai3ee").is_err());
    }

    #[test]
    fn a_tree_nested_past_the_limit_is_refused() {
        // `directories` directories deep, then a file.
        fn nested(directories: usize) -> Vec<u8> {
            let mut raw = b"d1:a".repeat(directories + 1);
            raw.extend_from_slice(b"d0:d6:lengthi0eee");
            raw.extend(std::iter::repeat_n(b'e', directories + 1));
            raw
        }
        fn read(raw: &[u8]) -> Result<usize, crate::bencode::DeserializeError> {
            let mut de =
                crate::bencode::BencodeDeserializer::new_from_buf(raw).with_max_depth(1000);
            FileTree::<ByteBuf>::deserialize(&mut de).map(|t| t.files.len())
        }
        assert_eq!(read(&nested(MAX_DIRECTORY_DEPTH)).unwrap(), 1);
        assert!(read(&nested(MAX_DIRECTORY_DEPTH + 1)).is_err());
    }

    #[test]
    fn names_are_sanitized_as_libtorrent_stores_them() {
        // test_torrent_info.cpp `v2_invalid_filename.torrent`.
        assert_eq!(sanitize_component(b"\x82est/MB"), "_estMB");
        // test_torrent_info.cpp `sanitize_path`, forced elements.
        assert_eq!(sanitize_component(b"/est1MB"), "est1MB");
        assert_eq!(sanitize_component(b"."), "_");
        assert_eq!(sanitize_component(b".."), "_");
        assert_eq!(sanitize_component(b""), "_");
        assert_eq!(sanitize_component(b"..."), "...");
        assert_eq!(sanitize_component(b"\xe2\x80\x8b"), "_");
        assert_eq!(sanitize_component(b"a\x01b"), "a_b");
        assert_eq!(sanitize_component(b"a\\b\0c"), "abc");
        assert_eq!(sanitize_component("\u{202e}gpj.exe".as_bytes()), "gpj.exe");
        assert_eq!(sanitize_component("caf\u{e9}".as_bytes()), "caf\u{e9}");
    }

    #[test]
    fn a_torrent_is_named_as_libtorrent_names_it() {
        let name = |raw: Option<&[u8]>| torrent_name(raw, || "hash".to_owned());
        // test_torrent_info.cpp `sanitize_path`, elements not forced.
        assert_eq!(name(Some(b"\0\0\xed\0\x80")), "_");
        assert_eq!(name(Some(b"/a/")), "a");
        assert_eq!(name(Some(b"a...b")), "a...b");
        // What an element not forced drops whole leaves the torrent its
        // hex info hash.
        for dropped in [&b"."[..], b"..", b"/..", b"/", b"\xe2\x80\x8b"] {
            assert_eq!(name(Some(dropped)), "hash", "{dropped:?}");
        }
        assert_eq!(name(Some(b"")), "_");
        assert_eq!(name(None), "hash");
        assert_eq!(name(Some("caf\u{e9}.mkv".as_bytes())), "caf\u{e9}.mkv");
    }

    #[test]
    fn names_are_cleaned_under_windows_rules_on_every_platform() {
        const WINDOWS: NameRules = NameRules { windows: true };
        let forced = |raw: &[u8]| sanitize_component_with(raw, WINDOWS);
        let name = |raw: &[u8]| sanitize_name_with(raw, WINDOWS);
        // libtorrent `valid_path_character` on Windows.
        assert_eq!(forced(b"a?b<c>d\"e|f*g:h"), "a_b_c_d_e_f_g_h");
        assert_eq!(forced(b"a\x08b"), "a_b");
        // test_torrent_info.cpp `sanitize_path_trailing_dots`, Windows.
        assert_eq!(forced(b"abc..."), "abc");
        assert_eq!(forced(b"abc. . "), "abc");
        assert_eq!(forced(b"... "), "_");
        assert_eq!(name(b"... "), None);
        // Shared rules still hold.
        assert_eq!(forced("\u{202e}gpj.exe\0".as_bytes()), "gpj.exe");
        assert_eq!(forced(b"a\x01b"), "a_b");
        // A device name never stays one, before any extension and in any
        // case; a name that only starts like one is left alone.
        for device in [
            &b"CON"[..],
            b"con.txt",
            b"Con.tar.gz",
            b"NUL",
            b"com1.log",
            b"LPT9",
            b"aux ",
            b"CONOUT$",
            b"clock$.x",
        ] {
            let cleaned = forced(device);
            assert!(cleaned.starts_with('_'), "{device:?} -> {cleaned}");
            assert!(!is_windows_device(&cleaned), "{device:?} -> {cleaned}");
        }
        assert_eq!(forced(b"CON"), "_CON");
        assert_eq!(forced(b"con.txt"), "_con.txt");
        assert_eq!(forced(b"console.txt"), "console.txt");
        assert_eq!(forced(b"com10"), "com10");
        assert_eq!(forced(b"xnul"), "xnul");
        // The portable rules keep all of these.
        assert_eq!(sanitize_component(b"CON"), "CON");
        assert_eq!(sanitize_component(b"a?b:"), "a?b:");
        assert_eq!(sanitize_component(b"abc. "), "abc. ");
    }

    #[test]
    fn a_long_name_is_cut_keeping_its_extension() {
        // test_torrent_info.cpp `sanitize_path_truncate` and
        // `sanitize_path_truncate_utf`.
        let tens = "abcdefghi_".repeat(24);
        assert_eq!(sanitize_component("abcdefghi_".repeat(25).as_bytes()), tens);
        let long = format!(
            "{}abcdefghi_abcdefghi_abcdefghi_abcdefghi_abcde.test",
            "abcdefghi_".repeat(20)
        );
        assert_eq!(sanitize_component(long.as_bytes()), format!("{tens}.test"));
        let dash = "\u{2014}";
        let utf = format!("{}abcdefghi{dash}abcde.jpg", "abcdefghi_".repeat(23));
        assert_eq!(
            sanitize_component(utf.as_bytes()),
            format!("{}abcdefghi{dash}.jpg", "abcdefghi_".repeat(23))
        );
        // test_torrent_info.cpp `sanitize_path_trailing_dots`, not Windows.
        assert_eq!(sanitize_component(b"abc..."), "abc...");
        assert_eq!(sanitize_component(b"abc."), "abc.");
    }
}
