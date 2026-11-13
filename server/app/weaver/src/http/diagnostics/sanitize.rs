// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The filter that keeps every name in the diagnostics package on the host.
//
// The package is attached to a public bug report, so it may not name the
// operator, their machine, their providers or what they downloaded. A name is
// never blanked: it becomes a stand-in that is the same everywhere in the
// package, so a maintainer can still follow server 2 (`provider-2`) or job 17
// (`job-17`, its third file `job-17/file-3.mkv`) through every file.
//
// One table, one rule, one pass:
//
// - The table is read from the database once, after the package is
//   collected: the servers, categories, schedules, proxies, proxy pools,
//   egress interfaces, scripts, feeds, the login and every account; every
//   active job and the 2,000 most recently finished, with their names,
//   files, metadata, NZB title and posted files, and the title the API
//   derives from each name; and the host's directories, user and host name.
//   Older history is only counted, in the manifest. Any read, parse or build
//   failure leaves every component out.
// - The structure decides first. A string under a field that carries a name
//   (its field name ends in `name`, `file`, `path`, `dir`, `member`, `set`,
//   `title`, `volume`, `archive`, `release`, `label`, `user`, `host` and the
//   like, in a JSON file, a JSON log record, a Prometheus label or a
//   `key=value` text field) is replaced whole, whether the table holds the
//   name or not: one the table does not hold gets a `name-N` (`host-N` for a
//   host) the first time any field shows it. A file keeps the segments that
//   name its kind on a stand-in for its stem, so one set reads as one name in
//   every recovery and archive kind (`name-3.part02.rar`, `name-3.r04`,
//   `name-3.vol01+02.par2`, `name-3.vol0-1.par3`, `name-3.part01.rev`,
//   `name-3.sfv`, `name-3.7z.001`). A path is replaced component by
//   component, and so is every component after a download directory in
//   running text, and every path under a drive no directory names.
// - Then running text, by kind. A distinctive name (two words or more, or
//   letters and digits mixed) is replaced wherever it stands as a word of
//   its own, in any ASCII case. A plain one-word name ("runtime", "Audio")
//   reads like Weaver's own prose, so it is replaced only where the
//   structure puts a name. A value (a category, a metadata field) is
//   replaced only as a whole string. A directory is replaced only up to a
//   path boundary. When two names share a spelling, the first stand-in
//   wins; jobs that share a name get one stand-in naming every one of them
//   (`job-12+40`). Every other URL host in the package is a name, `host-N`.
// - The pass: one leftmost-longest automaton over every text component; a
//   JSON log line is rewritten value by value so it stays valid JSON. A JSON
//   key is matched as text but never as a plain name, and a key the GraphQL
//   schema declares is kept. After it, IP and MAC addresses, e-mail
//   addresses, and URL credentials, query strings and paths after a
//   replaced host become their shape.
//
// What it cannot catch: a plain one-word name, or a name the table does not
// hold, inside running text outside any field or path; a category or
// metadata value inside running text; a name written in another Unicode
// normal form or a non-ASCII case; a path written from an unknown root
// other than a drive; a message-ID whose domain has no dot.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{BufReader, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, Anchored, Input, MatchKind, StartKind};
use serde_json::Value;
use sha2::{Digest, Sha256};
use weaver_nzb::NzbNames;
use weaver_server_core::Database;
use weaver_server_core::bandwidth::ScheduleEntry;
use weaver_server_core::categories::CategoryConfig;
use weaver_server_core::jobs::diagnostic_names::{DiagnosticNzbSource, JobNameRecord};
use weaver_server_core::proxies::{EgressBinding, EgressInterface, ProxyPool, ProxyProfile};
use weaver_server_core::rss::RssFeedRow;
use weaver_server_core::servers::ServerConfig;

use super::archive::{Component, Form};

// A name shorter than this is replaced only as a whole string: as a word of
// running text, "a", "tv" or "nas" is indistinguishable from prose.
const MIN_WORD_CHARS: usize = 4;

const IP_STAND_IN: &str = "<ip>";
const MAC_STAND_IN: &str = "<mac>";
const EMAIL_STAND_IN: &str = "<email>";
const USERINFO_STAND_IN: &str = "<userinfo>";
const QUERY_STAND_IN: &str = "<query>";
const PATH_STAND_IN: &str = "/<path>";
// What the log file's own name becomes: the name it has by default.
const LOG_FILE_STAND_IN: &str = "weaver.log";
const LOG_STEM_STAND_IN: &str = "weaver";

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

// What the manifest says about the stand-ins, so a reader knows the names in
// the package are not real.
pub(super) const SANITIZED_NOTE: &str = "Provider, proxy, egress, schedule, script, feed, \
     job, file, title, category, metadata, directory, login, user and host names are stable \
     stand-ins: the same name is the same stand-in in every file, and a spelling two names \
     share is the first one's stand-in. Names are read for every active job and the 2,000 most \
     recently finished; history_jobs_total counts the whole history. Every field that carries \
     a name is replaced whole, and a name the table does not hold there becomes a name-N \
     (host-N for a host) stand-in; jobs sharing one name share a job-A+B stand-in. A one-word \
     name is replaced only in such fields and in paths, never in running text. A category or \
     metadata value is replaced only where it is a whole string value; any other URL host is \
     a host-N stand-in. A file stand-in keeps the segments that name its kind (extension, \
     part, volume and recovery markers), so one set reads as one stand-in. IP, MAC \
     and e-mail addresses, and URL credentials, query strings and paths after a replaced host \
     are replaced by their shape. Configuration \
     files are numbered, and a log starts at its first whole line when truncated and ends at \
     its last whole line.";

// The names of the machine the package is built on.
#[derive(Clone)]
pub(super) struct LocalNames {
    pub(super) data_dir: PathBuf,
    pub(super) intermediate_dir: PathBuf,
    pub(super) complete_dir: PathBuf,
    pub(super) watch_dir: Option<PathBuf>,
    pub(super) log_file: Option<PathBuf>,
    pub(super) home_dir: Option<PathBuf>,
    pub(super) user: Option<String>,
    pub(super) hostname: Option<String>,
}

impl LocalNames {
    // The names of the running host, around the configured directories.
    pub(super) fn current(
        data_dir: PathBuf,
        intermediate_dir: PathBuf,
        complete_dir: PathBuf,
        watch_dir: Option<PathBuf>,
    ) -> Self {
        let home_dir = std::env::home_dir();
        let user = ["USER", "USERNAME", "LOGNAME"]
            .iter()
            .find_map(|variable| std::env::var(variable).ok())
            .filter(|user| !user.is_empty())
            .or_else(|| {
                home_dir
                    .as_deref()
                    .and_then(Path::file_name)
                    .map(|name| name.to_string_lossy().into_owned())
            });
        Self {
            data_dir,
            intermediate_dir,
            complete_dir,
            watch_dir,
            log_file: weaver_server_core::runtime::log_buffer::log_file_path()
                .map(Path::to_path_buf),
            home_dir,
            user,
            hostname: local_hostname(),
        }
    }

    // Every directory the host names, with its stand-in.
    fn dirs(&self) -> Vec<(&Path, &'static str)> {
        let log_dir = self.log_file.as_deref().and_then(Path::parent);
        [
            (Some(self.data_dir.as_path()), "<data>"),
            (Some(self.intermediate_dir.as_path()), "<intermediate>"),
            (Some(self.complete_dir.as_path()), "<complete>"),
            (self.watch_dir.as_deref(), "<watch>"),
            (log_dir, "<logs>"),
            (self.home_dir.as_deref(), "<home>"),
        ]
        .into_iter()
        .filter_map(|(dir, stand_in)| dir.map(|dir| (dir, stand_in)))
        .collect()
    }
}

// A directory as configured and as the filesystem resolves it, without a
// trailing separator.
fn dir_spellings(dir: &Path) -> Vec<String> {
    let trim = |dir: &Path| {
        dir.to_string_lossy()
            .trim_end_matches(['/', '\\'])
            .to_string()
    };
    let mut spellings = vec![trim(dir)];
    if let Ok(canonical) = std::fs::canonicalize(dir) {
        let canonical = trim(&canonical);
        if !spellings.contains(&canonical) {
            spellings.push(canonical);
        }
    }
    spellings.retain(|spelling| !spelling.is_empty());
    spellings
}

#[cfg(unix)]
fn local_hostname() -> Option<String> {
    let mut buffer = [0u8; 256];
    // SAFETY: the pointer and length describe `buffer`, which outlives the
    // call, and `gethostname` writes at most that many bytes into it.
    let status = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) };
    if status != 0 {
        return None;
    }
    let end = buffer
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(buffer.len());
    String::from_utf8(buffer[..end].to_vec())
        .ok()
        .filter(|name| !name.is_empty())
}

#[cfg(not(unix))]
fn local_hostname() -> Option<String> {
    std::env::var("COMPUTERNAME").ok()
}

// Every name the database holds, read once for the table.
#[derive(Debug, Default)]
pub(super) struct StoredNames {
    pub(super) jobs: BTreeMap<u64, JobNameRecord>,
    // The title and posted file names of each job's persisted NZB.
    pub(super) posted: BTreeMap<u64, NzbNames>,
    // Every finished job the history holds, read or not.
    pub(super) history_total: u64,
    pub(super) categories: Vec<CategoryConfig>,
    pub(super) logins: Vec<String>,
    pub(super) servers: Vec<ServerConfig>,
    pub(super) schedules: Vec<ScheduleEntry>,
    pub(super) proxies: Vec<ProxyProfile>,
    pub(super) egress: Vec<EgressInterface>,
    pub(super) pools: Vec<ProxyPool>,
    pub(super) feeds: Vec<RssFeedRow>,
    pub(super) script_dirs: Vec<String>,
    // Every script file in the scripts directory, every script an instance
    // runs, and every instance's own name.
    pub(super) scripts: Vec<String>,
}

// Reads every table source the database and the host hold. One that cannot
// be read fails the read, and with it the package. Blocking: run it off the
// async runtime.
pub(super) fn read_stored_names(db: &Database) -> Result<StoredNames, String> {
    fn failed(source: &str, error: impl std::fmt::Display) -> String {
        format!("the stand-in table could not read {source}: {error}")
    }

    let servers = db
        .list_servers()
        .map_err(|error| failed("the servers", error))?;
    let jobs = db
        .diagnostic_job_names()
        .map_err(|error| failed("the job names", error))?;

    // A page at a time, so no more than one page of NZBs is held at once.
    let mut posted = BTreeMap::new();
    for source in [DiagnosticNzbSource::Active, DiagnosticNzbSource::History] {
        let mut after = 0;
        loop {
            let page = db
                .diagnostic_persisted_nzbs(source, after)
                .map_err(|error| failed("the persisted NZBs", error))?;
            let Some(last) = page.last().map(|nzb| nzb.job_id) else {
                break;
            };
            for nzb in page {
                if let Some(document) = nzb.nzb.filter(|document| !document.is_empty()) {
                    let names = posted_names(&document).map_err(|error| {
                        failed(&format!("the NZB of job {}", nzb.job_id), error)
                    })?;
                    posted.insert(nzb.job_id, names);
                }
            }
            after = last;
        }
    }

    let script_dir = db
        .diagnostic_script_directory()
        .map_err(|error| failed("the scripts directory", error))?;
    let mut scripts = Vec::new();
    if let Some(dir) = &script_dir {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|error| failed("the scripts directory", error))?;
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if !name.starts_with('.') {
                        scripts.push(name);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(failed("the scripts directory", error)),
        }
    }
    for instance in db
        .script_instances()
        .map_err(|error| failed("the script instances", error))?
    {
        scripts.push(instance.script.as_str().to_string());
        scripts.push(instance.name);
    }

    Ok(StoredNames {
        jobs,
        posted,
        history_total: db
            .diagnostic_history_total()
            .map_err(|error| failed("the history", error))?,
        categories: db
            .list_categories()
            .map_err(|error| failed("the categories", error))?,
        logins: db
            .get_auth_credentials()
            .map_err(|error| failed("the login", error))?
            .map(|credentials| credentials.username)
            .into_iter()
            .collect(),
        servers,
        schedules: db
            .list_schedules()
            .map_err(|error| failed("the schedules", error))?,
        proxies: db
            .list_proxy_profiles()
            .map_err(|error| failed("the proxy profiles", error))?,
        egress: db
            .list_egress_interfaces()
            .map_err(|error| failed("the egress interfaces", error))?,
        pools: db
            .list_proxy_pools()
            .map_err(|error| failed("the proxy pools", error))?,
        feeds: db
            .list_rss_feeds()
            .map_err(|error| failed("the RSS feeds", error))?,
        script_dirs: script_dir.into_iter().collect(),
        scripts,
    })
}

// The title and file names of a persisted NZB, read without its segments.
fn posted_names(nzb: &[u8]) -> Result<NzbNames, String> {
    if nzb.starts_with(&ZSTD_MAGIC) {
        let decoder = zstd::stream::read::Decoder::new(nzb).map_err(|error| error.to_string())?;
        weaver_nzb::parse_nzb_names_reader(BufReader::new(decoder))
    } else {
        weaver_nzb::parse_nzb_names_reader(nzb)
    }
    .map_err(|error| error.to_string())
}

// Sanitizes every component in place, its member name included, and returns
// how many stand-ins the table holds. A table that cannot be built fails the
// package. `schema_keys` are the field names the GraphQL schema declares.
// Blocking: run it off the async runtime.
pub(super) fn sanitize_package(
    components: &mut [Component],
    local: &LocalNames,
    stored: &StoredNames,
    schema_keys: HashSet<String>,
) -> Result<usize, String> {
    components.iter_mut().for_each(open_gzip);
    let mut table = StandIns::build(components, local, stored)?;
    table.schema_keys = schema_keys;
    for component in components.iter_mut() {
        table.apply(component);
    }
    Ok(table.stand_in_count())
}

// Every component left out because the table could not be built, named
// through the host's own names, and the reason, sanitized the same way, as
// one more. `code`, when the failure has one, is carried by each. A
// component that names nothing by construction ships in its name-free form
// instead, with its own reason.
pub(super) fn left_out(
    local: &LocalNames,
    components: Vec<Component>,
    reason: &str,
    code: Option<&'static str>,
) -> Vec<Component> {
    let table = StandIns::build(&[], local, &StoredNames::default()).ok();
    let sanitize = |text: &str| match &table {
        Some(table) => table.sanitize_str(text),
        None => "the stand-in table could not be built".to_string(),
    };
    let mut kept: Vec<Component> = components
        .into_iter()
        .map(|component| match component.without_table {
            Some(outcome) => Component {
                outcome,
                without_table: None,
                ..component
            },
            None => {
                let name = match &table {
                    Some(table) => table.member_name(&component.name),
                    None => "component".to_string(),
                };
                Component {
                    reason: code,
                    ..Component::failed(
                        name,
                        "left out because the stand-in table could not be built",
                    )
                }
            }
        })
        .collect();
    kept.push(Component {
        reason: code,
        ..Component::failed("stand-in-table", sanitize(reason))
    });
    kept
}

// What `left_out` needs of a component, kept aside before the pass takes the
// components, so a pass that stops still leaves out the right ones.
pub(super) fn shell_of(component: &Component) -> Component {
    Component {
        name: component.name.clone(),
        form: component.form,
        outcome: Err(String::new()),
        reason: component.reason,
        without_table: component.without_table.clone(),
    }
}

// Drops everything before the first whole line of a text that was cut at an
// arbitrary byte, so no name survives as a fragment the table cannot match.
pub(super) fn from_first_line(text: &mut Vec<u8>) {
    match text.iter().position(|byte| *byte == b'\n') {
        Some(newline) => {
            text.drain(..=newline);
        }
        None => text.clear(),
    }
}

// Drops everything after the last whole line of a log read while it is
// written, so a line cut mid-name does not leave as a fragment.
pub(super) fn to_last_line(text: &mut Vec<u8>) {
    let end = text
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |newline| newline + 1);
    text.truncate(end);
}

// A rotated log is sanitized as the text it holds. Like a plain log, an
// oversized one contributes its last `MAX_LOG_BYTES` of text, from the first
// whole line on.
fn open_gzip(component: &mut Component) {
    if component.form != Form::Gzip {
        return;
    }
    if let Ok(data) = &component.outcome {
        component.outcome = gunzip_tail(data);
    }
    component.form = Form::GzipText;
}

fn gunzip_tail(data: &[u8]) -> Result<Vec<u8>, String> {
    let limit = usize::try_from(super::MAX_LOG_BYTES).unwrap_or(usize::MAX);
    let mut decoder = flate2::read::MultiGzDecoder::new(data);
    let mut text = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut truncated = false;
    loop {
        let read = decoder.read(&mut chunk).map_err(|error| {
            format!("left out because it could not be decompressed for sanitizing: {error}")
        })?;
        if read == 0 {
            break;
        }
        text.extend_from_slice(&chunk[..read]);
        if text.len() > limit.saturating_mul(2) {
            text.drain(..text.len() - limit);
            truncated = true;
        }
    }
    if text.len() > limit {
        text.drain(..text.len() - limit);
        truncated = true;
    }
    if truncated {
        from_first_line(&mut text);
    }
    Ok(text)
}

// The domain a host is registered under: its last two labels, or three
// under a two-letter country code with a short second level (`co.uk`).
// `None` when the host is that domain already, or an address.
fn registrable_domain(host: &str) -> Option<String> {
    if host.parse::<IpAddr>().is_ok() {
        return None;
    }
    let labels: Vec<&str> = host.split('.').filter(|label| !label.is_empty()).collect();
    let keep = match labels.as_slice() {
        [.., second, top] if top.len() == 2 && second.len() <= 3 => 3,
        _ => 2,
    };
    (labels.len() > keep).then(|| labels[labels.len() - keep..].join("."))
}

// A certificate's SHA-256 fingerprint as the API spells it.
fn certificate_fingerprint(der: &[u8]) -> String {
    Sha256::digest(der)
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

// The host of the URL whose authority starts `text`.
fn url_host(text: &[u8]) -> Option<String> {
    let end = text
        .iter()
        .position(|byte| ends_url(*byte) || matches!(byte, b'/' | b'?' | b'#'))
        .unwrap_or(text.len());
    let authority = &text[..end];
    let host_start = authority
        .iter()
        .rposition(|byte| *byte == b'@')
        .map_or(0, |at| at + 1);
    let host: Vec<u8> = authority[host_start..]
        .iter()
        .copied()
        .take_while(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        .collect();
    String::from_utf8(host).ok().filter(|host| !host.is_empty())
}

// Every URL host `text` names that is a name: not `localhost`, not an
// address.
fn url_hosts(text: &[u8]) -> Vec<String> {
    let mut hosts = Vec::new();
    let mut from = 0;
    while let Some(offset) = find(&text[from..], b"://") {
        let separator = from + offset;
        from = separator + 3;
        if separator == 0 || !text[separator - 1].is_ascii_alphanumeric() {
            continue;
        }
        if let Some(host) = url_host(&text[from..]).filter(|host| {
            !host.eq_ignore_ascii_case("localhost")
                && host.parse::<IpAddr>().is_err()
                && host.chars().any(char::is_alphabetic)
        }) {
            hosts.push(host);
        }
    }
    hosts
}

// Whether a spelling is safe to match inside running text. A name that is one
// plain word ("runtime", "System", "Audio") or shorter than `MIN_WORD_CHARS`
// reads the same as Weaver's own prose, so it is replaced only where the
// structure says a name stands: a field that carries names, or a path
// component. A name of two or more words, or one that mixes letters and
// digits, is matched as a word anywhere.
fn is_distinctive(name: &str) -> bool {
    if name.chars().count() < MIN_WORD_CHARS {
        return false;
    }
    let runs = name
        .split(|character: char| !character.is_alphanumeric())
        .filter(|run| !run.is_empty())
        .count();
    let letters = name.chars().any(char::is_alphabetic);
    let digits = name.chars().any(|character| character.is_ascii_digit());
    runs >= 2 || (letters && digits)
}

// What a field's string value is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FieldKind {
    // A release, file, folder, path, account or other operator name.
    Name,
    // A host name, with or without a port.
    Host,
}

// The last word of a field name that says its value is a name. Every field
// that carries a release, file, folder, path, account, script or feed name is
// named after what it holds, so the field, not the value, decides.
const NAME_HEADS: &[&str] = &[
    "name",
    "names",
    "filename",
    "filenames",
    "file",
    "files",
    "path",
    "paths",
    "dir",
    "dirs",
    "directory",
    "folder",
    "member",
    "members",
    "title",
    "set",
    "sets",
    "volume",
    "volumes",
    "archive",
    "archives",
    "release",
    "dest",
    "destination",
    "src",
    "root",
    "label",
    "labels",
    "user",
    "username",
    "login",
    "account",
    "script",
    "scripts",
    "feed",
    "egress",
    "output",
    "stem",
    "basename",
    "group",
    "service",
    "executable",
];

// Whole field names that carry names without saying so: a recovery record's
// `as`, `over` and the member lists of a repair verdict.
const NAME_KEYS: &[&str] = &[
    "as",
    "over",
    "from",
    "to",
    "whole",
    "damaged",
    "unrepaired",
    "withheld",
    "normalizedkey",
];

const HOST_HEADS: &[&str] = &["host", "hostname", "address", "addr", "peer", "server"];

// A field name's words, lowercased: `outputDir`, `output_dir` and
// `output-dir` are all `output dir`.
fn key_words(key: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut previous_lower = false;
    for character in key.chars() {
        if !character.is_alphanumeric() {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            previous_lower = false;
            continue;
        }
        if character.is_uppercase() && previous_lower && !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
        previous_lower = character.is_lowercase() || character.is_ascii_digit();
        word.extend(character.to_lowercase());
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

fn field_kind(key: &str) -> Option<FieldKind> {
    let words = key_words(key);
    if NAME_KEYS.contains(&words.concat().as_str()) {
        return Some(FieldKind::Name);
    }
    let last = words.last()?.as_str();
    if HOST_HEADS.contains(&last) {
        Some(FieldKind::Host)
    } else if NAME_HEADS.contains(&last) {
        Some(FieldKind::Name)
    } else {
        None
    }
}

// Whether a field value is a name at all: a number, an address, a word of no
// letters, a boolean or a stand-in already is not.
fn worth_a_stand_in(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed.chars().any(char::is_alphabetic)
        && trimmed.parse::<IpAddr>().is_err()
        && trimmed.parse::<std::net::SocketAddr>().is_err()
        && !(trimmed.starts_with('<') && trimmed.ends_with('>'))
        && !["true", "false", "none", "null"]
            .iter()
            .any(|word| trimmed.eq_ignore_ascii_case(word))
}

fn is_host_shaped(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn is_drive(component: &str) -> bool {
    let bytes = component.as_bytes();
    bytes.len() == 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

// Whether a path is written from a root: `/x`, `\x`, `C:\x` or `C:/x`.
fn is_rooted(path: &str) -> bool {
    let bytes = path.as_bytes();
    matches!(bytes.first(), Some(b'/' | b'\\'))
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'))
}

// One segment of a file name that names its kind rather than the file: an
// extension the engines know, a recovery volume (`.rev`), a split part
// (`.z01`, `.001`), or a volume marker (`.part02`, `.vol003+04`,
// `.vol003-04`).
fn is_kind_segment(segment: &str) -> bool {
    let lowered = segment.to_ascii_lowercase();
    let numbered_after = |prefix: &str| {
        lowered.strip_prefix(prefix).is_some_and(|number| {
            !number.is_empty()
                && number.starts_with(|character: char| character.is_ascii_digit())
                && number
                    .chars()
                    .all(|character| character.is_ascii_digit() || matches!(character, '+' | '-'))
        })
    };
    if lowered == "rev" || numbered_after("part") || numbered_after("vol") {
        return true;
    }
    if lowered.len() >= 3
        && lowered.starts_with(['r', 's', 'z'])
        && lowered[1..].bytes().all(|byte| byte.is_ascii_digit())
    {
        return true;
    }
    let kind = weaver_nzb::analysis::file_ext(&format!(".{segment}"));
    kind != weaver_nzb::analysis::FileExt::NONE && kind != weaver_nzb::analysis::FileExt::OTHER
}

// Where a file name's stem may end: after each kind segment peeled from the
// end, longest stem first. `Name.mkv.vol003+04.par2` may end at `Name.mkv.vol003+04`,
// `Name.mkv` or `Name`. Empty for a name with no kind segment.
fn stem_cuts(name: &str) -> Vec<usize> {
    let mut cuts = Vec::new();
    let mut cut = name.len();
    while let Some(dot) = name[..cut].rfind('.') {
        if dot == 0 || !is_kind_segment(&name[dot + 1..cut]) {
            break;
        }
        cut = dot;
        cuts.push(cut);
    }
    cuts
}

// The names the structure finds that the table does not hold: a member a
// finished job no longer stores, a recovery set, a folder under a download
// directory. Each gets one stand-in for the package, and a file keeps its
// kind segments on a stand-in for its stem, so every member of one set reads
// as one name (`name-3.part02.rar`, `name-3.vol01+02.par2`, `name-3.sfv`).
#[derive(Default)]
struct Registry {
    found: HashMap<String, String>,
    stand_ins: HashSet<String>,
    names: usize,
    hosts: usize,
}

impl Registry {
    fn name(&mut self, table: &Table, value: &str) -> String {
        let lowered = value.to_ascii_lowercase();
        if let Some(stand_in) = self.found.get(&lowered) {
            return stand_in.clone();
        }
        let cuts = stem_cuts(value);
        // A stem the table or an earlier name already holds keeps its
        // stand-in: the job's own name, a posted file's stem.
        let known = cuts.iter().find_map(|cut| {
            let stem = &lowered[..*cut];
            table
                .names
                .get(stem)
                .or_else(|| self.found.get(stem))
                .map(|stand_in| format!("{stand_in}{}", &value[*cut..]))
        });
        let stand_in = known.unwrap_or_else(|| {
            let cut = cuts.last().copied().unwrap_or(value.len());
            self.names += 1;
            let stem = format!("name-{}", self.names);
            self.found.insert(lowered[..cut].to_string(), stem.clone());
            self.stand_ins.insert(stem.clone());
            format!("{stem}{}", &value[cut..])
        });
        self.found.insert(lowered, stand_in.clone());
        self.stand_ins.insert(stand_in.clone());
        stand_in
    }

    fn host(&mut self, value: &str) -> String {
        let lowered = value.to_ascii_lowercase();
        if let Some(stand_in) = self.found.get(&lowered) {
            return stand_in.clone();
        }
        self.hosts += 1;
        let stand_in = format!("host-{}", self.hosts);
        self.found.insert(lowered, stand_in.clone());
        self.stand_ins.insert(stand_in.clone());
        stand_in
    }
}

// Where a spelling is matched.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    // A value (a category, a metadata field): only as a whole string.
    Value,
    // A name: wherever a name stands, and as a word of running text when it
    // is distinctive.
    Name,
    // A login, user, account or host name: as a name, and as a word of
    // running text from `MIN_WORD_CHARS` on even when it is one plain word,
    // since it names the operator and its stand-in (`<login>`) cannot be
    // mistaken for a job.
    Identity,
    // A directory whose contents are Weaver's own (the data, log and
    // scripts directories): up to a path boundary.
    Dir,
    // A directory that holds downloads: up to a path boundary, and every
    // component after it is a name.
    ContentDir,
}

// The one table: every name's spelling, ASCII-lowercased, and its stand-in.
#[derive(Default)]
struct Table {
    // The spellings the automaton matches, and for each its stand-in and
    // scope (`Name`, `Dir` or `ContentDir`).
    keys: Vec<String>,
    matched: Vec<(String, Scope)>,
    index: HashSet<String>,
    // Every spelling's first stand-in, whatever its scope.
    first: HashMap<String, String>,
    // The names, matched where a name stands.
    names: HashMap<String, String>,
    // The values, matched only as a whole string.
    values: HashMap<String, String>,
    stand_ins: HashSet<String>,
    // Every directory spelling with its stand-in and whether it holds
    // downloads, and how many were numbered `dir-N`.
    dir_keys: Vec<(String, String, bool)>,
    dirs: usize,
}

impl Table {
    fn name(&mut self, value: &str, stand_in: &str) -> bool {
        self.insert(value, stand_in, Scope::Name)
    }

    fn identity(&mut self, value: &str, stand_in: &str) -> bool {
        self.insert(value, stand_in, Scope::Identity)
    }

    fn dir(&mut self, value: &str, stand_in: &str, content: bool) -> bool {
        let scope = if content {
            Scope::ContentDir
        } else {
            Scope::Dir
        };
        self.insert(value, stand_in, scope)
    }

    fn whole(&mut self, value: &str, stand_in: &str) -> bool {
        self.insert(value, stand_in, Scope::Value)
    }

    // A directory outside every other the table holds, as `dir-N`. Only a
    // directory written from a root: a relative "parent" is a piece of a
    // name, not a place (`[1` of a name holding `[1/5]`).
    fn numbered_dir(&mut self, dir: &Path) {
        let spelling = dir.to_string_lossy();
        if !is_rooted(&spelling) {
            return;
        }
        let lowered = spelling.to_ascii_lowercase();
        if self.dir_keys.iter().any(|(key, _, _)| {
            lowered
                .strip_prefix(key.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '\\']))
        }) {
            return;
        }
        let stand_in = format!("dir-{}", self.dirs + 1);
        let mut added = false;
        for spelling in dir_spellings(dir) {
            added |= self.dir(&spelling, &stand_in, true);
        }
        self.dirs += usize::from(added);
    }

    // A spelling's stand-in is the first one stored for it; where it matches
    // is the entry's own scope. Says whether the spelling was new.
    fn insert(&mut self, value: &str, stand_in: &str, scope: Scope) -> bool {
        let key = value.to_ascii_lowercase();
        if value.trim().is_empty() || value == stand_in {
            return false;
        }
        let first = self.first.get(&key).cloned();
        let new = first.is_none();
        let stand_in = first.unwrap_or_else(|| stand_in.to_string());
        self.first.insert(key.clone(), stand_in.clone());
        self.stand_ins.insert(stand_in.clone());
        match scope {
            Scope::Value => {
                self.values.entry(key).or_insert(stand_in);
            }
            Scope::Name | Scope::Identity => {
                self.names.entry(key.clone()).or_insert(stand_in.clone());
                let identity = scope == Scope::Identity && value.chars().count() >= MIN_WORD_CHARS;
                if identity || is_distinctive(value) {
                    self.automaton_key(key, &stand_in, Scope::Name);
                }
            }
            Scope::Dir | Scope::ContentDir => {
                self.dir_keys
                    .push((key.clone(), stand_in.clone(), scope == Scope::ContentDir));
                self.automaton_key(key, &stand_in, scope);
            }
        }
        new
    }

    // A log record writes the name JSON-escaped, and its text line quotes a
    // value holding a space with Rust's escapes: a backslash doubled, a quote
    // escaped. Each spelling gets the same stand-in.
    fn automaton_key(&mut self, key: String, stand_in: &str, scope: Scope) {
        for spelling in escaped_spellings(&key).into_iter().chain([key]) {
            if self.index.insert(spelling.clone()) {
                self.keys.push(spelling);
                self.matched.push((stand_in.to_string(), scope));
            }
        }
    }

    // The longest directory a path starts with, up to a path boundary: how
    // long its spelling is, its stand-in, and whether it holds downloads.
    fn dir_prefix(&self, lowered: &str) -> Option<(usize, &str, bool)> {
        self.dir_keys
            .iter()
            .filter(|(key, _, _)| {
                lowered
                    .strip_prefix(key.as_str())
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '\\']))
            })
            .max_by_key(|(key, _, _)| key.len())
            .map(|(key, stand_in, content)| (key.len(), stand_in.as_str(), *content))
    }
}

// A name as a JSON string and as a Rust `{:?}` string spell it, without the
// quotes, where that differs from the name itself.
fn escaped_spellings(key: &str) -> Vec<String> {
    let unquote = |quoted: String| quoted[1..quoted.len() - 1].to_string();
    let mut spellings = Vec::new();
    for spelling in [
        serde_json::to_string(key).map(unquote).unwrap_or_default(),
        unquote(format!("{key:?}")),
    ] {
        if spelling != key && !spellings.contains(&spelling) {
            spellings.push(spelling);
        }
    }
    spellings
}

// The table for one package, its automaton, and the pass that applies it.
pub(super) struct StandIns {
    table: Table,
    automaton: Option<AhoCorasick>,
    // The names the structure found that the table does not hold.
    registry: RefCell<Registry>,
    // The log file's own name and stem, which the log members are named by.
    log_names: Vec<(String, &'static str)>,
    // The field names the GraphQL schema declares, kept as JSON keys.
    schema_keys: HashSet<String>,
}

impl StandIns {
    // Builds the table from every source, in a fixed order, each numbered by
    // its own id where it has one, so a spelling two names share goes to the
    // same one every time. Then reads every field that carries a name in the
    // package, so a name the table does not hold gets its stand-in before any
    // running text is matched, and is matched there too.
    fn build(
        components: &[Component],
        local: &LocalNames,
        stored: &StoredNames,
    ) -> Result<Self, String> {
        let mut table = Table::default();
        let mut registry = Registry::default();

        // Providers, by server id, also known by `host:port` and the domain
        // their host is registered under.
        let mut servers: Vec<&ServerConfig> = stored.servers.iter().collect();
        servers.sort_by_key(|server| server.id);
        for server in servers {
            let stand_in = format!("provider-{}", server.id);
            let host = server.host.as_str();
            table.name(&format!("{host}:{}", server.port), &stand_in);
            table.identity(host, &stand_in);
            if let Some(domain) = registrable_domain(host) {
                table.identity(&domain, &stand_in);
            }
            let certificate = format!("{stand_in}/certificate");
            if let Some(der) = &server.tls_name_mismatch_certificate_der {
                table.name(&certificate_fingerprint(der), &certificate);
            }
            if let Some(path) = &server.tls_ca_cert {
                table.dir(&path.to_string_lossy(), &certificate, false);
            }
            if let Some(username) = &server.username {
                table.identity(username, &format!("{stand_in}/account"));
            }
        }

        // The host's directories, then each category's.
        for (dir, stand_in) in local.dirs() {
            let content = !matches!(stand_in, "<data>" | "<logs>");
            for spelling in dir_spellings(dir) {
                table.dir(&spelling, stand_in, content);
            }
        }
        let mut next_category = 1u64;
        for category in &stored.categories {
            let id = u64::from(category.id);
            next_category = next_category.max(id + 1);
            let stand_in = format!("category-{id}");
            table.whole(&category.name, &stand_in);
            for alias in category.aliases.split(',') {
                let alias: String = alias
                    .chars()
                    .filter(|character| !matches!(character, '*' | '?'))
                    .collect();
                table.whole(alias.trim(), &stand_in);
            }
            if let Some(dest_dir) = &category.dest_dir {
                table.numbered_dir(Path::new(dest_dir));
            }
        }

        // Jobs, by id. Jobs that share a name share its stand-in, and it
        // names every one of them, so neither reads as the other.
        let ids: BTreeSet<u64> = stored
            .jobs
            .keys()
            .chain(stored.posted.keys())
            .copied()
            .collect();
        let mut owners: HashMap<String, Vec<u64>> = HashMap::new();
        for id in &ids {
            if let Some(name) = stored.jobs.get(id).and_then(|record| record.name.as_ref()) {
                owners
                    .entry(name.to_ascii_lowercase())
                    .or_default()
                    .push(*id);
            }
        }
        let no_record = JobNameRecord::default();
        let no_nzb = NzbNames::default();
        for id in ids {
            let record = stored.jobs.get(&id).unwrap_or(&no_record);
            let nzb = stored.posted.get(&id).unwrap_or(&no_nzb);
            let job = match record
                .name
                .as_ref()
                .and_then(|name| owners.get(&name.to_ascii_lowercase()))
            {
                Some(sharing) if sharing.len() > 1 => format!(
                    "job-{}",
                    sharing
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join("+")
                ),
                _ => format!("job-{id}"),
            };

            let nzb_path = record.nzb_path.as_deref().map(Path::new);
            let output_dir = record.output_dir.as_deref().map(Path::new);
            let mut names: Vec<String> = Vec::new();
            for name in [
                record.name.clone(),
                nzb_path.and_then(Path::file_stem).map(os_string),
                output_dir.and_then(Path::file_name).map(os_string),
            ]
            .into_iter()
            .flatten()
            {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
            for name in &names {
                table.name(name, &job);
            }

            // The NZB title, the names duplicate detection keeps, and the
            // title the API derives from each distinct name, parsed once.
            let mut titles: Vec<String> = nzb.title.iter().cloned().collect();
            titles.extend(record.normalized_names.iter().cloned());
            titles.extend(record.keys.iter().cloned());
            let mut parsed: Vec<&String> = Vec::new();
            for name in names.iter().chain(nzb.title.as_ref()) {
                if parsed.contains(&name) {
                    continue;
                }
                parsed.push(name);
                let derived = weaver_server_api::release_display_names(
                    name,
                    &record.metadata,
                    record.category.as_deref(),
                );
                titles.extend(derived);
            }
            let mut count = 0;
            for title in &titles {
                count += usize::from(table.name(title, &format!("{job}/title-{}", count + 1)));
            }

            if let Some(category) = &record.category
                && table.whole(category, &format!("category-{next_category}"))
            {
                next_category += 1;
            }

            // Posted names first, so a file is numbered by its place in the
            // NZB. A file is named by its stem with its kind segments kept,
            // so every volume and recovery file of one set reads as one name
            // (`job-17/file-1.part02.rar`, `job-17/file-1.vol01+02.par2`),
            // and a stem that is the job's own name reads as the job
            // (`job-17.mkv`). A file is named both with and without the
            // folder the NZB or the archive put it in.
            let mut seen = HashSet::new();
            let mut stems = 0;
            for file in nzb.file_names.iter().chain(&record.files) {
                if !seen.insert(file.as_str()) {
                    continue;
                }
                let base = file.rsplit(['/', '\\']).next().unwrap_or(file);
                let cuts = stem_cuts(base);
                let known = cuts.iter().find_map(|cut| {
                    table
                        .names
                        .get(&base[..*cut].to_ascii_lowercase())
                        .map(|stand_in| (*cut, stand_in.clone()))
                });
                let (cut, stem) = known.unwrap_or_else(|| {
                    let cut = cuts.last().copied().unwrap_or(base.len());
                    stems += 1;
                    let stem = format!("{job}/file-{stems}");
                    if cut < base.len() {
                        table.name(&base[..cut], &stem);
                    }
                    (cut, stem)
                });
                let stand_in = format!("{stem}{}", &base[cut..]);
                table.name(file, &stand_in);
                table.name(base, &stand_in);
            }
            if let Some(hash) = &record.hash {
                table.name(hash, &format!("{job}/hash"));
            }

            // Metadata after the names and files, so a value that is one of
            // them keeps that stand-in. A value is matched only whole.
            let mut count = 0;
            for (_, value) in &record.metadata {
                count += usize::from(table.whole(value, &format!("{job}/meta-{}", count + 1)));
            }

            // An output or NZB directory outside every other one would carry
            // its parent folders out whole.
            for path in [output_dir, nzb_path].into_iter().flatten() {
                if let Some(parent) = path.parent() {
                    table.numbered_dir(parent);
                }
            }
        }

        // Schedules, egress interfaces, proxies, pools, scripts and feeds.
        let mut count = 0;
        for schedule in &stored.schedules {
            count += usize::from(table.name(&schedule.label, &format!("schedule-{}", count + 1)));
        }
        for egress in &stored.egress {
            let stand_in = format!("egress-{}", egress.id);
            table.name(&egress.name, &stand_in);
            if let EgressBinding::Interface { name } = &egress.binding {
                table.name(name, &stand_in);
            }
        }
        for proxy in &stored.proxies {
            let stand_in = format!("proxy-{}", proxy.id);
            table.name(&proxy.name, &stand_in);
            table.identity(&proxy.host, &stand_in);
            if let Some(username) = &proxy.secrets.username {
                table.identity(username, &format!("{stand_in}/account"));
            }
            let tunnel_key = proxy.wireguard_spec().ok().map(|spec| spec.public_key());
            for key in [
                &proxy.peer_public_key,
                &proxy.host_key_fingerprint,
                &tunnel_key,
            ]
            .into_iter()
            .flatten()
            {
                table.name(key, &format!("{stand_in}/key"));
            }
        }
        for pool in &stored.pools {
            table.name(&pool.name, &format!("proxy-pool-{}", pool.id));
        }
        let scripts: BTreeSet<&String> = stored.scripts.iter().collect();
        for (index, script) in scripts.into_iter().enumerate() {
            table.name(script, &format!("script-{}", index + 1));
        }
        for dir in &stored.script_dirs {
            for spelling in dir_spellings(Path::new(dir)) {
                table.dir(&spelling, "<scripts>", false);
            }
        }
        for feed in &stored.feeds {
            let stand_in = format!("feed-{}", feed.id);
            table.name(&feed.name, &stand_in);
            if let Some(username) = &feed.username {
                table.identity(username, &format!("{stand_in}/account"));
            }
            if let Some(host) = feed
                .url
                .split_once("://")
                .and_then(|(_, rest)| url_host(rest.as_bytes()))
            {
                table.name(&host, &format!("{stand_in}-host"));
            }
        }

        // The login, the user, the log file's name and the host name.
        for login in &stored.logins {
            table.identity(login, "<login>");
        }
        if let Some(user) = &local.user {
            table.identity(user, "<user>");
        }
        let mut log_names = Vec::new();
        if let Some(log_file) = &local.log_file {
            if let Some(name) = log_file.file_name() {
                log_names.push((os_string(name), LOG_FILE_STAND_IN));
            }
            if let Some(stem) = log_file.file_stem() {
                log_names.push((os_string(stem), LOG_STEM_STAND_IN));
            }
        }
        for (name, stand_in) in &log_names {
            table.name(name, stand_in);
        }
        if let Some(hostname) = &local.hostname {
            table.identity(hostname, "<hostname>");
            if let Some((short, _)) = hostname.split_once('.') {
                table.identity(short, "<hostname>");
            }
        }

        // Every other URL host the package names.
        for data in components
            .iter()
            .filter_map(|component| component.outcome.as_ref().ok())
        {
            for host in url_hosts(data) {
                if !table.first.contains_key(&host.to_ascii_lowercase()) {
                    let stand_in = registry.host(&host);
                    table.name(&host, &stand_in);
                }
            }
        }

        // Every field that carries a name, read once with no automaton, so
        // each name the table does not hold has its stand-in before the pass.
        let mut stand_ins = Self {
            table,
            automaton: None,
            registry: RefCell::new(registry),
            log_names,
            schema_keys: HashSet::new(),
        };
        for component in components {
            if let Ok(data) = &component.outcome {
                let _ = stand_ins.sanitize_form(component.form, data);
            }
        }
        // A found name that is distinctive is matched in running text too.
        let mut found: Vec<(String, String)> = stand_ins
            .registry
            .borrow()
            .found
            .iter()
            .map(|(name, stand_in)| (name.clone(), stand_in.clone()))
            .collect();
        found.sort();
        for (name, stand_in) in found {
            if !stand_ins.table.first.contains_key(&name) && is_distinctive(&name) {
                stand_ins.table.name(&name, &stand_in);
            }
        }

        let table = &stand_ins.table;
        stand_ins.automaton = if table.keys.is_empty() {
            None
        } else {
            Some(
                AhoCorasickBuilder::new()
                    .match_kind(MatchKind::LeftmostLongest)
                    .ascii_case_insensitive(true)
                    .start_kind(StartKind::Both)
                    .build(&table.keys)
                    .map_err(|error| format!("the stand-in table could not be built: {error}"))?,
            )
        };
        Ok(stand_ins)
    }

    // How many stand-ins the package allocated.
    fn stand_in_count(&self) -> usize {
        let registry = self.registry.borrow();
        self.table.stand_ins.len()
            + registry
                .stand_ins
                .iter()
                .filter(|stand_in| !self.table.stand_ins.contains(*stand_in))
                .count()
    }

    // A member is named by the package, or for a log, by the log file: only
    // the log file's own name is the operator's, and it becomes the default
    // name.
    fn member_name(&self, name: &str) -> String {
        self.log_names
            .iter()
            .find_map(|(log_name, stand_in)| {
                name.strip_prefix(log_name.as_str())
                    .map(|rest| format!("{stand_in}{rest}"))
            })
            .unwrap_or_else(|| name.to_string())
    }

    // Sanitizes one component in place, whatever its form, and its member
    // name. A component that cannot be read in its form is left out with that
    // reason rather than passed through unsanitized.
    fn apply(&self, component: &mut Component) {
        component.name = self.member_name(&component.name);
        let outcome = std::mem::replace(&mut component.outcome, Ok(Vec::new()));
        component.outcome = match outcome {
            Err(error) => Err(self.sanitize_str(&error)),
            Ok(data) => self.sanitize_form(component.form, &data),
        };
    }

    fn sanitize_form(&self, form: Form, data: &[u8]) -> Result<Vec<u8>, String> {
        match form {
            Form::Text => Ok(self.sanitize_text(data)),
            Form::Json => self.sanitize_json(data),
            Form::Toml => self.sanitize_toml(data),
            Form::GzipText => self.sanitize_gzip(data),
            // Only text is sanitized; a stream not opened is not sent.
            Form::Gzip => Err("left out because it was not decompressed for sanitizing".into()),
        }
    }

    fn sanitize_json(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut value: Value = serde_json::from_slice(data).map_err(|error| {
            self.sanitize_str(&format!(
                "left out because it could not be read for sanitizing: {error}"
            ))
        })?;
        self.sanitize_json_value(&mut value, None);
        serde_json::to_vec_pretty(&value).map_err(|error| self.sanitize_str(&error.to_string()))
    }

    // A value is read in the light of the field that holds it: an array's
    // items are its field's.
    fn sanitize_json_value(&self, value: &mut Value, key: Option<&str>) {
        match value {
            Value::String(text) => *text = self.sanitize_value(key, text),
            Value::Array(items) => items
                .iter_mut()
                .for_each(|item| self.sanitize_json_value(item, key)),
            Value::Object(map) => {
                let fields = std::mem::take(map);
                for (key, mut child) in fields {
                    self.sanitize_json_value(&mut child, Some(&key));
                    let key = self.sanitize_key(key, |key| map.contains_key(key));
                    map.insert(key, child);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    // A string under its field: a field that carries a name is replaced by
    // the name's stand-in whole, whether the table holds the name or not;
    // any other string by the value rule and the text pass.
    fn sanitize_value(&self, key: Option<&str>, text: &str) -> String {
        match key.and_then(field_kind) {
            Some(kind) => match self.resolve_field(text, kind) {
                Some(resolved) => self.shapes(resolved.as_bytes()),
                None => self.sanitize_string(text),
            },
            None => self.sanitize_string(text),
        }
    }

    // A whole string outside a name field: a whole-string value it is
    // (`Some("x")` included), or the text pass.
    fn sanitize_string(&self, text: &str) -> String {
        if let Some(inner) = text
            .strip_prefix("Some(\"")
            .and_then(|rest| rest.strip_suffix("\")"))
            && let Some(stand_in) = self.table.values.get(&inner.to_ascii_lowercase())
        {
            return format!("Some({stand_in:?})");
        }
        match self.table.values.get(&text.to_ascii_lowercase()) {
            Some(stand_in) => stand_in.clone(),
            // A value can carry rendered fields of its own, as a replayed
            // line does: each is rewritten under its key.
            None => String::from_utf8(self.sanitize_fields_line(text.as_bytes()))
                .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned()),
        }
    }

    // A name field's value: its stand-in, built through the wrappers a log
    // field writes (`Some(..)`, a quoted string, a list of names), a path
    // component by component, or `None` when the value is not a name at all
    // (a number, an address, a sentence in a host field).
    fn resolve_field(&self, value: &str, kind: FieldKind) -> Option<String> {
        if let Some(inner) = value
            .strip_prefix("Some(")
            .and_then(|rest| rest.strip_suffix(')'))
        {
            return self
                .resolve_field(inner, kind)
                .map(|inner| format!("Some({inner})"));
        }
        if value.len() >= 2
            && value.starts_with('"')
            && value.ends_with('"')
            && let Ok(inner) = serde_json::from_str::<String>(value)
        {
            let resolved = self
                .resolve_field(&inner, kind)
                .unwrap_or_else(|| self.sanitize_string(&inner));
            return Some(format!("{resolved:?}"));
        }
        if value.starts_with('[')
            && value.ends_with(']')
            && let Ok(items) = serde_json::from_str::<Vec<String>>(value)
        {
            let items: Vec<String> = items
                .iter()
                .map(|item| {
                    let resolved = self
                        .resolve_field(item, kind)
                        .unwrap_or_else(|| self.sanitize_string(item));
                    format!("{resolved:?}")
                })
                .collect();
            return Some(format!("[{}]", items.join(", ")));
        }
        let lowered = value.to_ascii_lowercase();
        if let Some(stand_in) = self.table.first.get(&lowered) {
            return Some(stand_in.clone());
        }
        if let Some(stand_in) = self.registry.borrow().found.get(&lowered) {
            return Some(stand_in.clone());
        }
        if !worth_a_stand_in(value) {
            return None;
        }
        match kind {
            FieldKind::Host => self.resolve_host(value),
            FieldKind::Name if value.contains(['/', '\\']) => Some(self.resolve_path(value)),
            FieldKind::Name => Some(self.registry.borrow_mut().name(&self.table, value)),
        }
    }

    // A host field: a host name's stand-in, its port kept. A value that is
    // not shaped like a host (`server:2 leg 0`) is left to the text pass.
    fn resolve_host(&self, value: &str) -> Option<String> {
        if let Some((host, port)) = value.rsplit_once(':')
            && !host.contains(':')
            && !port.is_empty()
            && port.bytes().all(|byte| byte.is_ascii_digit())
        {
            return self.resolve_host(host).map(|host| format!("{host}:{port}"));
        }
        if !is_host_shaped(value) || !worth_a_stand_in(value) {
            return None;
        }
        if let Some(stand_in) = self.table.first.get(&value.to_ascii_lowercase()) {
            return Some(stand_in.clone());
        }
        Some(self.registry.borrow_mut().host(value))
    }

    // A path: the directory it starts in, then every component after it. A
    // component under a download directory, or under no directory the table
    // holds, is a name; under Weaver's own directories only a name the table
    // holds is replaced.
    fn resolve_path(&self, value: &str) -> String {
        let lowered = value.to_ascii_lowercase();
        let (mut out, rest, content) = match self.table.dir_prefix(&lowered) {
            Some((length, stand_in, content)) => (stand_in.to_string(), &value[length..], content),
            None => (String::new(), value, true),
        };
        let mut first = out.is_empty();
        let mut piece = String::new();
        for character in rest.chars() {
            if matches!(character, '/' | '\\') {
                out.push_str(&self.resolve_component(&piece, content, first));
                piece.clear();
                first = false;
                out.push(character);
            } else {
                piece.push(character);
            }
        }
        out.push_str(&self.resolve_component(&piece, content, first));
        out
    }

    // One path component: its stand-in, or as it stands when it is a drive,
    // `.` or `..`, not a name, or under Weaver's own directories.
    fn resolve_component(&self, component: &str, content: bool, first: bool) -> String {
        if component.is_empty()
            || component == "."
            || component == ".."
            || (first && is_drive(component))
        {
            return component.to_string();
        }
        let lowered = component.to_ascii_lowercase();
        if let Some(stand_in) = self.table.first.get(&lowered) {
            return stand_in.clone();
        }
        if let Some(stand_in) = self.registry.borrow().found.get(&lowered) {
            return stand_in.clone();
        }
        if !content || !worth_a_stand_in(component) {
            return component.to_string();
        }
        self.registry.borrow_mut().name(&self.table, component)
    }

    fn sanitize_toml(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let text = std::str::from_utf8(data)
            .map_err(|_| "left out because it is not valid UTF-8".to_string())?;
        // A parse error quotes the line it failed on, so only its kind is
        // kept.
        let mut value: toml::Value = toml::from_str(text).map_err(|error| {
            self.sanitize_str(&format!(
                "left out because it could not be read for sanitizing: {}",
                error.message()
            ))
        })?;
        self.sanitize_toml_value(&mut value, None);
        toml::to_string_pretty(&value)
            .map(String::into_bytes)
            .map_err(|error| {
                self.sanitize_str(&format!(
                    "left out because it could not be re-serialized: {error}"
                ))
            })
    }

    fn sanitize_toml_value(&self, value: &mut toml::Value, key: Option<&str>) {
        match value {
            toml::Value::String(text) => *text = self.sanitize_value(key, text),
            toml::Value::Array(items) => items
                .iter_mut()
                .for_each(|item| self.sanitize_toml_value(item, key)),
            toml::Value::Table(table) => {
                let fields = std::mem::take(table);
                for (key, mut child) in fields {
                    self.sanitize_toml_value(&mut child, Some(&key));
                    let key = self.sanitize_key(key, |key| table.contains_key(key));
                    table.insert(key, child);
                }
            }
            toml::Value::Integer(_)
            | toml::Value::Float(_)
            | toml::Value::Boolean(_)
            | toml::Value::Datetime(_) => {}
        }
    }

    // A key the GraphQL schema declares is kept as it is. Any other key is
    // sanitized as a string is, and kept distinct from the keys already
    // written.
    fn sanitize_key(&self, key: String, taken: impl Fn(&str) -> bool) -> String {
        if self.schema_keys.contains(&key) {
            return key;
        }
        let sanitized = self.sanitize_string(&key);
        let mut candidate = sanitized.clone();
        let mut count = 1;
        while taken(&candidate) {
            count += 1;
            candidate = format!("{sanitized} ({count})");
        }
        candidate
    }

    fn sanitize_gzip(&self, text: &[u8]) -> Result<Vec<u8>, String> {
        let sanitized = self.sanitize_text(text);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(&sanitized)
            .and_then(|()| encoder.finish())
            .map_err(|error| format!("left out because it could not be compressed again: {error}"))
    }

    // A text file, a line at a time, by the line's own structure: a JSON log
    // record value by value under its field, so the line stays valid JSON; a
    // Prometheus sample label by label; any other line its `key=value`
    // fields, then the text pass.
    fn sanitize_text(&self, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len());
        for line in data.split_inclusive(|byte| *byte == b'\n') {
            let (body, ending) = match line.strip_suffix(b"\n") {
                Some(body) => match body.strip_suffix(b"\r") {
                    Some(body) => (body, &b"\r\n"[..]),
                    None => (body, &b"\n"[..]),
                },
                None => (line, &b""[..]),
            };
            let structured = std::str::from_utf8(body).ok().and_then(|text| {
                self.sanitize_json_line(text)
                    .or_else(|| self.sanitize_prometheus_line(text))
            });
            match structured {
                Some(text) => out.extend_from_slice(text.as_bytes()),
                None => out.extend_from_slice(&self.sanitize_fields_line(body)),
            }
            out.extend_from_slice(ending);
        }
        out
    }

    // A JSON object on one line, its string values rewritten in place under
    // their fields; keys, numbers, order and layout as they were. `None` when
    // the line is not one valid JSON object.
    fn sanitize_json_line(&self, line: &str) -> Option<String> {
        if !line.trim_start().starts_with('{') {
            return None;
        }
        serde_json::from_str::<serde::de::IgnoredAny>(line).ok()?;
        let bytes = line.as_bytes();
        let mut out = String::with_capacity(line.len());
        let mut copied = 0;
        let mut index = 0;
        // Each open object or array, and the field its values are under.
        let mut frames: Vec<Option<String>> = Vec::new();
        while index < bytes.len() {
            match bytes[index] {
                b'{' => frames.push(None),
                b'[' => {
                    let key = frames.last().cloned().flatten();
                    frames.push(key);
                }
                b'}' | b']' => {
                    frames.pop();
                }
                b'"' => {
                    let end = json_string_end(bytes, index)?;
                    let text: String = serde_json::from_str(&line[index..end]).ok()?;
                    let mut next = end;
                    while next < bytes.len() && bytes[next].is_ascii_whitespace() {
                        next += 1;
                    }
                    if bytes.get(next) == Some(&b':') {
                        if let Some(frame) = frames.last_mut() {
                            *frame = Some(text);
                        }
                    } else {
                        let key = frames.last().cloned().flatten();
                        let sanitized = self.sanitize_value(key.as_deref(), &text);
                        if sanitized != text {
                            out.push_str(&line[copied..index]);
                            out.push_str(&serde_json::to_string(&sanitized).ok()?);
                            copied = end;
                        }
                    }
                    index = end;
                    continue;
                }
                _ => {}
            }
            index += 1;
        }
        out.push_str(&line[copied..]);
        Some(out)
    }

    // A Prometheus sample, each label value under its label name. `None`
    // for any other line, a comment included.
    fn sanitize_prometheus_line(&self, line: &str) -> Option<String> {
        let open = line.find('{')?;
        let name = &line[..open];
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':'))
        {
            return None;
        }
        let bytes = line.as_bytes();
        let mut out = String::with_capacity(line.len());
        out.push_str(&line[..=open]);
        let mut index = open + 1;
        loop {
            match bytes.get(index)? {
                b'}' => break,
                b',' => {
                    out.push(',');
                    index += 1;
                    continue;
                }
                _ => {}
            }
            let equals = index + line[index..].find('=')?;
            let label = &line[index..equals];
            if bytes.get(equals + 1) != Some(&b'"') {
                return None;
            }
            let mut value = String::new();
            let mut cursor = equals + 2;
            loop {
                match bytes.get(cursor)? {
                    b'"' => break,
                    b'\\' => {
                        match bytes.get(cursor + 1)? {
                            b'n' => value.push('\n'),
                            other => value.push(char::from(*other)),
                        }
                        cursor += 2;
                    }
                    _ => {
                        let character = line[cursor..].chars().next()?;
                        value.push(character);
                        cursor += character.len_utf8();
                    }
                }
            }
            let sanitized = self.sanitize_value(Some(label), &value);
            out.push_str(label);
            out.push_str("=\"");
            out.push_str(
                &sanitized
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n"),
            );
            out.push('"');
            index = cursor + 1;
        }
        out.push_str(&line[index..]);
        Some(out)
    }

    // A text's `key=value` and `key="value"` fields under their keys, a key
    // standing after whitespace or at the start; everything between them
    // through the text pass.
    fn sanitize_fields_line(&self, line: &[u8]) -> Vec<u8> {
        let Ok(text) = std::str::from_utf8(line) else {
            return self.sanitize_bytes(line);
        };
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(line.len());
        let mut copied = 0;
        let mut index = 0;
        while let Some(offset) = text[index..].find('=') {
            let equals = index + offset;
            index = equals + 1;
            let key_start = text[..equals]
                .rfind(|character: char| {
                    !(character.is_ascii_alphanumeric() || matches!(character, '_' | '.'))
                })
                .map_or(0, |space| space + 1);
            if key_start == equals
                || key_start < copied
                || (key_start > 0 && !bytes[key_start - 1].is_ascii_whitespace())
            {
                continue;
            }
            let Some(kind) = field_kind(&text[key_start..equals]) else {
                continue;
            };
            let (value, end) = if bytes.get(equals + 1) == Some(&b'"') {
                let Some(end) = json_string_end(bytes, equals + 1) else {
                    continue;
                };
                (&text[equals + 1..end], end)
            } else {
                let end = text[equals + 1..]
                    .find(char::is_whitespace)
                    .map_or(text.len(), |space| equals + 1 + space);
                (&text[equals + 1..end], end)
            };
            if let Some(resolved) = self.resolve_field(value, kind) {
                out.extend_from_slice(&self.sanitize_bytes(&bytes[copied..equals + 1]));
                out.extend_from_slice(self.shapes(resolved.as_bytes()).as_bytes());
                copied = end;
                index = end;
            }
        }
        out.extend_from_slice(&self.sanitize_bytes(&bytes[copied..]));
        out
    }

    fn sanitize_str(&self, text: &str) -> String {
        // Every substitution replaces whole names with valid UTF-8, and every
        // shape rule matches and replaces ASCII only, so valid UTF-8 stays
        // valid.
        String::from_utf8(self.sanitize_bytes(text.as_bytes()))
            .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
    }

    // The text pass: the table, the paths under a drive no directory names,
    // then the shapes.
    fn sanitize_bytes(&self, text: &[u8]) -> Vec<u8> {
        let shaped = self.shapes(&self.drive_paths(&self.substitute(text)));
        shaped.into_bytes()
    }

    // IP and MAC addresses, e-mail addresses, and URL credentials, query
    // strings and paths after a replaced host, as their shape.
    fn shapes(&self, text: &[u8]) -> String {
        let shaped = redact_ips(&redact_macs(&redact_emails(&self.redact_url_parts(text))));
        String::from_utf8(shaped)
            .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
    }

    // Replaces every name in `text`: the longest at the leftmost place that
    // the rule lets stand. After a download directory, each path component
    // that follows is a name.
    fn substitute(&self, text: &[u8]) -> Vec<u8> {
        let Some(automaton) = &self.automaton else {
            return text.to_vec();
        };
        let mut out = Vec::with_capacity(text.len());
        let mut copied = 0;
        let mut at = 0;
        while at < text.len() {
            let Some(found) = automaton.find(Input::new(text).span(at..text.len())) else {
                break;
            };
            let start = found.start();
            let Some((end, name)) = self.fitting_at(
                automaton,
                text,
                start,
                found.end(),
                found.pattern().as_usize(),
            ) else {
                at = start + 1;
                continue;
            };
            out.extend_from_slice(&text[copied..start]);
            let (stand_in, scope) = &self.table.matched[name];
            out.extend_from_slice(stand_in.as_bytes());
            let end = if *scope == Scope::ContentDir {
                self.path_tail(text, end, &mut out)
            } else {
                end
            };
            copied = end;
            at = end;
        }
        out.extend_from_slice(&text[copied..]);
        out
    }

    // Every path under a drive that no directory in the table names
    // (`D:\Other\Some.Name\x.mkv`): the drive kept, each component a name.
    fn drive_paths(&self, text: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(text.len());
        let mut copied = 0;
        let mut index = 0;
        while index + 2 < text.len() {
            let rooted = text[index].is_ascii_alphabetic()
                && text[index + 1] == b':'
                && matches!(text[index + 2], b'/' | b'\\')
                && (index == 0 || !is_word_byte(text[index - 1]));
            if !rooted {
                index += 1;
                continue;
            }
            out.extend_from_slice(&text[copied..index + 2]);
            let end = self.path_tail(text, index + 2, &mut out);
            copied = end;
            index = end.max(index + 1);
        }
        out.extend_from_slice(&text[copied..]);
        out
    }

    // The components of a path that goes on at `at` in running text, each
    // written to `out` as a name, up to where the path ends: a quote, a
    // control character, `: `, `, `, `; `, or a full stop before a space or
    // the end. Returns where it ended.
    fn path_tail(&self, text: &[u8], mut at: usize, out: &mut Vec<u8>) -> usize {
        loop {
            let mut component_start = at;
            while component_start < text.len() && matches!(text[component_start], b'/' | b'\\') {
                component_start += 1;
            }
            if component_start == at {
                return at;
            }
            let mut end = component_start;
            while end < text.len() && !ends_path(text, end) {
                end += 1;
            }
            if end > component_start
                && text[end - 1] == b'.'
                && (end == text.len() || text[end].is_ascii_whitespace())
            {
                end -= 1;
            }
            if end == component_start {
                return at;
            }
            let Ok(component) = std::str::from_utf8(&text[component_start..end]) else {
                return at;
            };
            out.extend_from_slice(&text[at..component_start]);
            out.extend_from_slice(self.resolve_component(component, true, false).as_bytes());
            at = end;
        }
    }

    // The longest match at `start` that the rule lets stand, and where it
    // ends, trying shorter ones at the same place while one does not.
    fn fitting_at(
        &self,
        automaton: &AhoCorasick,
        text: &[u8],
        start: usize,
        mut end: usize,
        mut name: usize,
    ) -> Option<(usize, usize)> {
        loop {
            let stands = if self.table.matched[name].1 == Scope::Name {
                !(word_after(text, start) && word_before(text, start))
                    && !(word_before(text, end) && word_after(text, end))
            } else {
                !path_glued_before(text, start) && !path_glued_after(text, end)
            };
            if stands {
                return Some((end, name));
            }
            if end <= start + 1 {
                return None;
            }
            let shorter = automaton.find(
                Input::new(text)
                    .range(start..end - 1)
                    .anchored(Anchored::Yes),
            )?;
            end = shorter.end();
            name = shorter.pattern().as_usize();
        }
    }

    // Whether a URL host is one the table or the IP rule replaces.
    fn is_replaced_host(&self, authority: &[u8]) -> bool {
        let Ok(authority) = std::str::from_utf8(authority) else {
            return false;
        };
        let host = if let Some(bracketed) = authority.strip_prefix('[') {
            bracketed.split(']').next().unwrap_or(bracketed)
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) if port.bytes().all(|byte| byte.is_ascii_digit()) => host,
                _ => authority,
            }
        };
        self.table.stand_ins.contains(host)
            || self.registry.borrow().stand_ins.contains(host)
            || host.parse::<IpAddr>().is_ok()
    }

    // Replaces the credentials (`user:pass@`) and the query string of every
    // URL, and its path when its host is replaced.
    fn redact_url_parts(&self, text: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(text.len());
        let mut copied = 0;
        let mut from = 0;
        while let Some(offset) = find(&text[from..], b"://") {
            let separator = from + offset;
            let start = separator + 3;
            from = start;
            if separator == 0 || !text[separator - 1].is_ascii_alphanumeric() {
                continue;
            }
            let span = |from: usize, to: usize, stop: &dyn Fn(u8) -> bool| {
                from + text[from..to]
                    .iter()
                    .position(|byte| stop(*byte))
                    .unwrap_or(to - from)
            };
            let end = span(start, text.len(), &ends_url);
            let authority_end = span(start, end, &|byte| matches!(byte, b'/' | b'?' | b'#'));
            let path_end = span(authority_end, end, &|byte| matches!(byte, b'?' | b'#'));

            out.extend_from_slice(&text[copied..start]);
            let mut host_start = start;
            if let Some(at) = text[start..authority_end]
                .iter()
                .rposition(|byte| *byte == b'@')
            {
                out.extend_from_slice(USERINFO_STAND_IN.as_bytes());
                host_start = start + at;
            }
            out.extend_from_slice(&text[host_start..authority_end]);
            let host = text[host_start..authority_end]
                .strip_prefix(b"@")
                .unwrap_or(&text[host_start..authority_end]);
            if path_end > authority_end + 1 && self.is_replaced_host(host) {
                out.extend_from_slice(PATH_STAND_IN.as_bytes());
            } else {
                out.extend_from_slice(&text[authority_end..path_end]);
            }
            let mut cursor = path_end;
            if path_end < end && text[path_end] == b'?' {
                let query_end = span(path_end + 1, end, &|byte| byte == b'#');
                out.push(b'?');
                if query_end > path_end + 1 {
                    out.extend_from_slice(QUERY_STAND_IN.as_bytes());
                }
                cursor = query_end;
            }
            out.extend_from_slice(&text[cursor..end]);
            copied = end;
            from = end;
        }
        out.extend_from_slice(&text[copied..]);
        out
    }
}

fn os_string(value: &std::ffi::OsStr) -> String {
    value.to_string_lossy().into_owned()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// Whether the character that ends right before `at` is a word character.
fn word_before(text: &[u8], at: usize) -> bool {
    let from = at.saturating_sub(4);
    (from..at)
        .find_map(|start| std::str::from_utf8(&text[start..at]).ok())
        .and_then(|tail| tail.chars().next_back())
        .is_some_and(is_word_char)
}

// Whether the character that starts at `at` is a word character.
fn word_after(text: &[u8], at: usize) -> bool {
    let to = text.len().min(at + 4);
    (at + 1..=to)
        .find_map(|end| std::str::from_utf8(&text[at..end]).ok())
        .and_then(|head| head.chars().next())
        .is_some_and(is_word_char)
}

fn is_word_char(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

// Whether a directory match is glued to more of a name before it: `/data`
// does not stand inside `/mnt/data` or `x-/data`.
fn path_glued_before(text: &[u8], at: usize) -> bool {
    word_before(text, at)
        || at
            .checked_sub(1)
            .is_some_and(|before| matches!(text[before], b'-' | b'.') && word_before(text, before))
}

// Whether a directory match goes on as more of the same name: `/data` does
// not stand inside `/data2/x` or `/data.old`, but does before a full stop.
fn path_glued_after(text: &[u8], at: usize) -> bool {
    word_after(text, at)
        || text
            .get(at)
            .is_some_and(|after| matches!(after, b'-' | b'.') && word_after(text, at + 1))
}

// Where a URL ends in running text. Angle brackets do not end one: the table
// pass runs first, and its stand-ins (`<home>`) may already sit inside it.
fn ends_url(byte: u8) -> bool {
    byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\'' | b'`')
}

// Where a path component in running text ends: at a separator, a quote, a
// control character, or `: `, `, ` and `; ` (an error's own text follows a
// path that way; a Windows name holds no colon).
fn ends_path(text: &[u8], at: usize) -> bool {
    let byte = text[at];
    byte < 0x20
        || matches!(byte, b'/' | b'\\' | b'"' | b'\'' | b'`')
        || (matches!(byte, b':' | b',' | b';') && text.get(at + 1) == Some(&b' '))
}

// Where the JSON or `{:?}` string that opens at `open` ends, after its
// closing quote.
fn json_string_end(bytes: &[u8], open: usize) -> Option<usize> {
    let mut index = open + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn is_email_local(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'%' | b'+' | b'-')
}

fn is_email_domain(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-')
}

// A domain with at least one dot and an alphabetic top level.
fn is_email_domain_name(domain: &[u8]) -> bool {
    let Some(dot) = domain.iter().rposition(|byte| *byte == b'.') else {
        return false;
    };
    let top = &domain[dot + 1..];
    dot > 0 && top.len() >= 2 && top.iter().all(u8::is_ascii_alphabetic)
}

// Replaces every `local@domain.tld`.
fn redact_emails(text: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len());
    let mut copied = 0;
    let mut from = 0;
    while let Some(offset) = text[from..].iter().position(|byte| *byte == b'@') {
        let at = from + offset;
        let mut start = at;
        while start > copied && is_email_local(text[start - 1]) {
            start -= 1;
        }
        while start < at && text[start] == b'.' {
            start += 1;
        }
        let mut end = at + 1;
        while end < text.len() && is_email_domain(text[end]) {
            end += 1;
        }
        while end > at + 1 && matches!(text[end - 1], b'.' | b'-') {
            end -= 1;
        }
        if start < at && is_email_domain_name(&text[at + 1..end]) {
            out.extend_from_slice(&text[copied..start]);
            out.extend_from_slice(EMAIL_STAND_IN.as_bytes());
            copied = end;
            from = end;
        } else {
            from = at + 1;
        }
    }
    out.extend_from_slice(&text[copied..]);
    out
}

// Replaces every MAC address written as six two-digit hex groups joined by
// `:` or `-`, standing on its own.
fn redact_macs(text: &[u8]) -> Vec<u8> {
    const LENGTH: usize = 17;
    let is_mac = |candidate: &[u8]| {
        let separator = candidate[2];
        matches!(separator, b':' | b'-')
            && candidate.chunks(3).all(|group| {
                group[0].is_ascii_hexdigit()
                    && group[1].is_ascii_hexdigit()
                    && group.get(2).is_none_or(|byte| *byte == separator)
            })
    };
    let glued = |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'-' | b'_');
    let mut out = Vec::with_capacity(text.len());
    let mut index = 0;
    while index < text.len() {
        if index + LENGTH <= text.len()
            && (index == 0 || !glued(text[index - 1]))
            && text.get(index + LENGTH).is_none_or(|byte| !glued(*byte))
            && is_mac(&text[index..index + LENGTH])
        {
            out.extend_from_slice(MAC_STAND_IN.as_bytes());
            index += LENGTH;
        } else {
            out.push(text[index]);
            index += 1;
        }
    }
    out
}

fn is_ip_byte(byte: u8) -> bool {
    byte.is_ascii_hexdigit() || matches!(byte, b':' | b'.')
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

// The address inside one run of address characters, as a byte range of the
// run. A trailing port (`1.2.3.4:563`) is kept outside the range.
fn address_span(run: &[u8]) -> Option<usize> {
    let mut end = run.len();
    while end > 0 && matches!(run[end - 1], b'.' | b':') {
        end -= 1;
    }
    let candidate = std::str::from_utf8(&run[..end]).ok()?;
    if candidate.contains(':')
        && candidate.bytes().any(|byte| byte.is_ascii_hexdigit())
        && candidate.parse::<Ipv6Addr>().is_ok()
    {
        return Some(end);
    }
    if candidate.parse::<Ipv4Addr>().is_ok() {
        return Some(end);
    }
    let (host, port) = candidate.rsplit_once(':')?;
    (!port.is_empty()
        && port.bytes().all(|byte| byte.is_ascii_digit())
        && host.parse::<Ipv4Addr>().is_ok())
    .then_some(host.len())
}

// An IPv4 address a run carries after one of its colons (`addr:1.2.3.4`), as
// its start and length in the run.
fn ipv4_after_colon(run: &[u8]) -> Option<(usize, usize)> {
    run.iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b':')
        .find_map(|(colon, _)| {
            let rest = &run[colon + 1..];
            let length = address_span(rest)?;
            std::str::from_utf8(&rest[..length])
                .ok()?
                .parse::<Ipv4Addr>()
                .ok()
                .map(|_| (colon + 1, length))
        })
}

// Replaces every IPv4 and IPv6 literal that stands as a word of its own, or
// that follows a word through a colon (`addr:203.0.113.9`).
fn redact_ips(text: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len());
    let mut copied = 0;
    let mut index = 0;
    while index < text.len() {
        if !is_ip_byte(text[index]) {
            index += 1;
            continue;
        }
        let start = index;
        while index < text.len() && is_ip_byte(text[index]) {
            index += 1;
        }
        // `v1.2.3.4` and `core::fade` are not addresses: a run glued to a
        // word is part of that word.
        if index < text.len() && is_word_byte(text[index]) {
            continue;
        }
        let run = &text[start..index];
        let glued = start > 0 && is_word_byte(text[start - 1]);
        let span = if glued {
            None
        } else {
            address_span(run).map(|length| (0, length))
        }
        .or_else(|| ipv4_after_colon(run));
        if let Some((offset, length)) = span {
            out.extend_from_slice(&text[copied..start + offset]);
            out.extend_from_slice(IP_STAND_IN.as_bytes());
            copied = start + offset + length;
        }
    }
    out.extend_from_slice(&text[copied..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(hostname: &str, data_dir: &str) -> LocalNames {
        LocalNames {
            data_dir: PathBuf::from(data_dir),
            intermediate_dir: PathBuf::from(format!("{data_dir}/intermediate")),
            complete_dir: PathBuf::from(format!("{data_dir}/complete")),
            watch_dir: None,
            log_file: None,
            home_dir: None,
            user: None,
            hostname: Some(hostname.to_string()),
        }
    }

    fn table(stored: &StoredNames) -> StandIns {
        StandIns::build(&[], &local("host-name", "/srv/weaver-data"), stored).expect("table")
    }

    fn job(id: u64, record: JobNameRecord) -> StoredNames {
        StoredNames {
            jobs: BTreeMap::from([(id, record)]),
            ..StoredNames::default()
        }
    }

    fn json(table: &StandIns, text: &str) -> Value {
        let json = table.sanitize_json(text.as_bytes()).expect("json");
        serde_json::from_slice(&json).expect("json")
    }

    fn shapes(text: &str) -> String {
        table(&StoredNames::default()).sanitize_str(text)
    }

    // A text component's lines, as the package sanitizes them.
    fn lines(table: &StandIns, text: &str) -> String {
        String::from_utf8(table.sanitize_text(text.as_bytes())).expect("utf-8")
    }

    // Rule 1: a name of four or more characters, in any ASCII case, where it
    // stands as a word of its own.
    #[test]
    fn a_long_name_is_replaced_as_a_word_in_any_case() {
        let stored = job(
            4,
            JobNameRecord {
                name: Some("Olaf.Show.S01E01".to_string()),
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        assert_eq!(
            table.sanitize_str("got Olaf.Show.S01E01, OLAF.SHOW.S01E01. xOlaf.Show.S01E01"),
            "got job-4, job-4. xOlaf.Show.S01E01"
        );
        // The title the API derives from the name is a name too.
        let title = table.sanitize_str("title Olaf Show");
        assert!(title.starts_with("title job-4/title-"), "{title}");
    }

    // Rule 2: a name under four characters, a login or host name included,
    // only as a whole string.
    #[test]
    fn a_short_name_is_replaced_only_as_a_whole_string() {
        let stored = StoredNames {
            logins: vec!["a".to_string()],
            ..StoredNames::default()
        };
        let table =
            StandIns::build(&[], &local("nas.lan", "/srv/weaver-data"), &stored).expect("table");
        assert_eq!(
            table.sanitize_str("a login on nas.lan, host nas"),
            "a login on <hostname>, host nas"
        );
        let value = json(&table, r#"{"login": "a", "host": "NAS", "text": "a nas"}"#);
        assert_eq!(value["login"], "<login>");
        assert_eq!(value["host"], "<hostname>");
        assert_eq!(value["text"], "a nas");
    }

    // Rule 3: a directory up to a path boundary; a full stop ending a
    // sentence is one.
    #[test]
    fn a_directory_is_replaced_up_to_a_path_boundary() {
        let table = table(&StoredNames::default());
        assert_eq!(
            table.sanitize_str(
                "/srv/weaver-data/x /srv/weaver-data2/x /srv/weaver-data.old /srv/weaver-data-old \
                 /mnt/srv/weaver-data \"/srv/weaver-data\""
            ),
            "<data>/x /srv/weaver-data2/x /srv/weaver-data.old /srv/weaver-data-old \
             /mnt/srv/weaver-data \"<data>\""
        );
        assert_eq!(
            table.sanitize_str("wrote to /srv/weaver-data/complete."),
            "wrote to <complete>."
        );
    }

    // Rule 4: a numeric metadata value only as a whole string.
    #[test]
    fn a_numeric_metadata_value_is_replaced_only_as_a_whole_string() {
        let stored = job(
            6,
            JobNameRecord {
                metadata: vec![("tvdb_id".to_string(), "81189".to_string())],
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        let value = json(
            &table,
            r#"{"value": "81189", "text": "id 81189", "n": 81189}"#,
        );
        assert_eq!(value["value"], "job-6/meta-1");
        assert_eq!(value["text"], "id 81189");
        assert_eq!(value["n"], 81189);
    }

    // Two names of one spelling: the first stand-in wins, so an unconfigured
    // category that is also an earlier job's metadata value still leaves no
    // trace.
    #[test]
    fn the_first_stand_in_of_a_shared_spelling_wins() {
        let mut stored = job(
            3,
            JobNameRecord {
                metadata: vec![("tag".to_string(), "Docu".to_string())],
                ..JobNameRecord::default()
            },
        );
        stored.jobs.insert(
            5,
            JobNameRecord {
                category: Some("docu".to_string()),
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        let value = json(
            &table,
            r#"{"category": "docu", "tag": "Docu", "paused": "true"}"#,
        );
        assert_eq!(value["category"], "job-3/meta-1");
        assert_eq!(value["tag"], "job-3/meta-1");
        assert_eq!(value["paused"], "true");
        // A value is not a name: in running text it stays.
        assert_eq!(
            table.sanitize_str("job 5 in docu, paused=true"),
            "job 5 in docu, paused=true"
        );
    }

    // A name spelled like an earlier value is matched as a name, under the
    // value's stand-in.
    #[test]
    fn a_name_spelled_like_an_earlier_value_keeps_its_stand_in() {
        let mut stored = job(
            3,
            JobNameRecord {
                metadata: vec![("series".to_string(), "Docu Series".to_string())],
                ..JobNameRecord::default()
            },
        );
        stored.jobs.insert(
            5,
            JobNameRecord {
                name: Some("Docu Series".to_string()),
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        assert_eq!(
            table.sanitize_str("INFO Docu Series done"),
            "INFO job-3/meta-1 done"
        );
    }

    // A key the GraphQL schema declares is the schema's, even when a name
    // spells it; any other key is sanitized.
    #[test]
    fn a_schema_key_is_kept_and_a_free_key_is_sanitized() {
        let stored = job(
            1,
            JobNameRecord {
                name: Some("progress".to_string()),
                ..JobNameRecord::default()
            },
        );
        let mut table = table(&stored);
        table.schema_keys = super::super::graphql::SchemaShape::current()
            .expect("schema")
            .field_names();
        assert!(table.schema_keys.contains("progress"));
        let value = json(
            &table,
            r#"{"name": "progress", "progress": "progress", "progress of x": 1}"#,
        );
        // A plain word is replaced only where a name stands, never in a key,
        // a schema value or running text.
        assert_eq!(value["name"], "job-1");
        assert_eq!(value["progress"], "progress");
        assert_eq!(value["progress of x"], 1);
    }

    const RELEASE: &str = "Brindle.Quay.S02E07.1080p-GRPX";

    fn release_job() -> StoredNames {
        let files = [
            // PAR2 index and volumes, plus and dash forms.
            "Brindle.Quay.S02E07.1080p-GRPX.par2",
            "Brindle.Quay.S02E07.1080p-GRPX.vol000+01.par2",
            "Brindle.Quay.S02E07.1080p-GRPX.vol03-07.par2",
            // PAR3 index and volume.
            "Brindle.Quay.S02E07.1080p-GRPX.par3",
            "Brindle.Quay.S02E07.1080p-GRPX.vol00+04.par3",
            // RAR volumes, new and old naming, which carry recovery records,
            // and their .rev volumes in both namings.
            "Brindle.Quay.S02E07.1080p-GRPX.part01.rar",
            "Brindle.Quay.S02E07.1080p-GRPX.part02.rar",
            "Brindle.Quay.S02E07.1080p-GRPX.part02.rev",
            "brq-s02e07.rar",
            "brq-s02e07.r00",
            "brq-s02e07.rev",
            // SFV.
            "Brindle.Quay.S02E07.1080p-GRPX.sfv",
            // 7z, zip and split sets.
            "brq-s02e07.7z.001",
            "brq-s02e07.zip",
            "brq-s02e07.z01",
            "Brindle.Quay.S02E07.1080p-GRPX.mkv.001",
        ];
        job(
            9,
            JobNameRecord {
                name: Some(RELEASE.to_string()),
                files: files.iter().map(|file| file.to_string()).collect(),
                ..JobNameRecord::default()
            },
        )
    }

    // Every recovery kind and archive set takes the one stand-in of the name
    // it is built from, its kind suffix kept, in every form it reaches the
    // bundle: a log field, a JSON value, running text, a path, and the names
    // an SFV lists.
    #[test]
    fn every_recovery_kind_takes_the_release_stand_in() {
        let table = table(&release_job());
        let text = lines(
            &table,
            concat!(
                "INFO verifying set=Brindle.Quay.S02E07.1080p-GRPX.par2 ",
                "file=Brindle.Quay.S02E07.1080p-GRPX.vol000+01.par2\n",
                "INFO recovery volume Brindle.Quay.S02E07.1080p-GRPX.vol03-07.par2 arrived\n",
                "INFO par3 index member=\"Brindle.Quay.S02E07.1080p-GRPX.par3\" ",
                "over=Brindle.Quay.S02E07.1080p-GRPX.vol00+04.par3\n",
                "INFO recovery record in Brindle.Quay.S02E07.1080p-GRPX.part02.rar repaired ",
                "/srv/weaver-data/complete/Brindle.Quay.S02E07.1080p-GRPX/",
                "Brindle.Quay.S02E07.1080p-GRPX.part01.rar\n",
                "INFO rev rebuilt as=Brindle.Quay.S02E07.1080p-GRPX.part02.rev ",
                "from=brq-s02e07.rev whole=brq-s02e07.r00 dest=brq-s02e07.rar\n",
                "; Brindle.Quay.S02E07.1080p-GRPX.sfv\n",
                "Brindle.Quay.S02E07.1080p-GRPX.part01.rar 0a1b2c3d\n",
                "brq-s02e07.r00 4e5f6a7b\n",
                "INFO sets 7z=brq-s02e07.7z.001 zip=brq-s02e07.zip z=brq-s02e07.z01 ",
                "split=Brindle.Quay.S02E07.1080p-GRPX.mkv.001\n",
                r#"{"level":"INFO","fields":{"message":"repaired","set":"brq-s02e07.7z.001","#,
                r#""volume":"Brindle.Quay.S02E07.1080p-GRPX.vol00+04.par3"}}"#,
            ),
        );
        for real in [
            "Brindle", "brindle", "Quay", "GRPX", "brq", "s02e07", "S02E07",
        ] {
            assert!(!text.contains(real), "{real} survived:\n{text}");
        }
        for kept in [
            "job-9.par2",
            "job-9.vol000+01.par2",
            "job-9.vol03-07.par2",
            "job-9.par3",
            "job-9.vol00+04.par3",
            "job-9.part01.rar",
            "job-9.part02.rar",
            "job-9.part02.rev",
            "job-9.sfv",
            "job-9.mkv.001",
            "<complete>/job-9/job-9.part01.rar",
        ] {
            assert!(text.contains(kept), "{kept} missing:\n{text}");
        }
        // The second set's stem has one stand-in across its volumes, its
        // .rev, its SFV entry and the 7z, zip and split sets built on it.
        let stem = text
            .split("dest=")
            .nth(1)
            .and_then(|tail| tail.split(".rar").next())
            .expect("dest")
            .to_string();
        assert!(stem.starts_with("job-9/file-"), "{stem}");
        for kept in [".rev", ".r00", ".7z.001", ".zip", ".z01"] {
            assert!(
                text.contains(&format!("{stem}{kept}")),
                "{stem}{kept}:\n{text}"
            );
        }
        assert!(text.contains(&format!("{stem}.r00 4e5f6a7b")), "{text}");
        let json_line = text.lines().last().expect("json line");
        let value: Value = serde_json::from_str(json_line).expect("valid JSON");
        assert_eq!(value["fields"]["set"], format!("{stem}.7z.001"));
        assert_eq!(value["fields"]["volume"], "job-9.vol00+04.par3");
    }

    // A recovery or archive file the table never saw, named in a field, takes
    // the stand-in of the stem it shares with a known file.
    #[test]
    fn an_unseen_volume_takes_its_set_stand_in() {
        let table = table(&release_job());
        let text = lines(
            &table,
            "INFO fetched file=Brindle.Quay.S02E07.1080p-GRPX.vol127+64.par2 \
             other=brq-s02e07.part09.rev",
        );
        assert!(text.contains("file=job-9.vol127+64.par2"), "{text}");
        assert!(!text.contains("brq"), "{text}");
        assert!(text.contains(".part09.rev"), "{text}");
    }

    // Ordinary text stays as it was: a job named with a plain word does not
    // touch prose, keys, enum values or numbers, and a bracket list stays.
    #[test]
    fn a_plain_job_name_leaves_ordinary_text_alone() {
        let mut stored = job(
            1,
            JobNameRecord {
                name: Some("runtime".to_string()),
                ..JobNameRecord::default()
            },
        );
        stored.jobs.insert(
            2,
            JobNameRecord {
                name: Some("System".to_string()),
                nzb_path: Some("/srv/weaver-data/watch/[1/5] System.nzb".to_string()),
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        let line = concat!(
            r#"{"level":"WARN","fields":{"message":"runtime pressure on System egress","#,
            r#""runtime_ms":12,"state":"up","ids":"[1, 2]","name":"runtime"}}"#,
        );
        let text = lines(&table, line);
        let value: Value = serde_json::from_str(&text).expect("valid JSON");
        assert_eq!(
            value["fields"]["message"],
            "runtime pressure on System egress"
        );
        assert_eq!(value["fields"]["runtime_ms"], 12);
        assert_eq!(value["fields"]["state"], "up");
        assert_eq!(value["fields"]["ids"], "[1, 2]");
        assert_eq!(value["fields"]["name"], "job-1");
        assert_eq!(
            lines(&table, "INFO egress System up, runtime 3 s, picks [1, 2]"),
            "INFO egress System up, runtime 3 s, picks [1, 2]"
        );
    }

    // Two jobs that share a name share one stand-in naming both.
    #[test]
    fn jobs_sharing_a_name_share_one_stand_in() {
        let mut stored = job(
            17640,
            JobNameRecord {
                name: Some("Olaf.Twin.S01E02".to_string()),
                ..JobNameRecord::default()
            },
        );
        stored.jobs.insert(
            17635,
            JobNameRecord {
                name: Some("Olaf.Twin.S01E02".to_string()),
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        let text = lines(&table, "INFO name=Olaf.Twin.S01E02 again Olaf.Twin.S01E02");
        assert_eq!(text, "INFO name=job-17635+17640 again job-17635+17640");
    }

    // Names the table never saw, in the fields that carry them, are replaced
    // with generated stand-ins, one per name, used for every later mention.
    #[test]
    fn an_unseen_name_in_a_name_field_gets_one_stand_in() {
        let log = concat!(
            "INFO set=Zephyr.Lantern.2019 member=\"Zephyr Lantern.mkv\" ",
            "releaseGroup=QRKS streamingService=NFLXQ\n",
            "INFO again Zephyr.Lantern.2019 failed: ",
            "/srv/weaver-data/complete/Zephyr.Lantern.2019/Zephyr Lantern.mkv: short\n",
            "weaver_job_bytes{job_name=\"Zephyr.Lantern.2019\",state=\"up\"} 3\n",
        );
        // The package reads every component once to find the names its
        // fields carry, then replaces them wherever they stand.
        let table = StandIns::build(
            &[Component::text("weaver.log", log)],
            &local("host-name", "/srv/weaver-data"),
            &StoredNames::default(),
        )
        .expect("table");
        let text = lines(&table, log);
        for real in ["Zephyr", "Lantern", "QRKS", "NFLXQ"] {
            assert!(!text.contains(real), "{real} survived:\n{text}");
        }
        let stand_in = text
            .split("set=")
            .nth(1)
            .and_then(|tail| tail.split(' ').next())
            .expect("set")
            .to_string();
        assert!(stand_in.starts_with("name-"), "{stand_in}");
        assert!(text.contains(&format!("again {stand_in} failed")), "{text}");
        assert!(text.contains(&format!("job_name=\"{stand_in}\"")), "{text}");
        assert!(text.contains("state=\"up\"} 3"), "{text}");
        assert!(text.contains(": short"), "{text}");
    }

    #[test]
    fn a_distinctive_name_is_replaced_in_a_free_key() {
        let stored = job(
            1,
            JobNameRecord {
                name: Some("Olaf.Progress.S01".to_string()),
                ..JobNameRecord::default()
            },
        );
        let table = table(&stored);
        let value = json(&table, r#"{"Olaf.Progress.S01 of x": 1}"#);
        assert_eq!(value["job-1 of x"], 1);
    }

    #[test]
    fn ip_literals_become_their_shape() {
        assert_eq!(
            shapes("dialing 203.0.113.9:563 now"),
            "dialing <ip>:563 now"
        );
        assert_eq!(shapes("peer [2001:db8::1]:563"), "peer [<ip>]:563");
        assert_eq!(shapes("addr:203.0.113.9 up"), "addr:<ip> up");
        for text in [
            "at 12:34:56.789 the run ended",
            "weaver_server_core::pipeline::graph",
            "release v1.2.3.4",
            "version 0.16.6",
        ] {
            assert_eq!(shapes(text), text, "{text}");
        }
    }

    #[test]
    fn mac_and_email_addresses_become_their_shape() {
        assert_eq!(shapes("mac aa:bb:cc:dd:ee:ff up"), "mac <mac> up");
        assert_eq!(
            shapes("at 12:34:56 the run ended"),
            "at 12:34:56 the run ended"
        );
        assert_eq!(
            shapes("poster someone.else+x@mail.example.org."),
            "poster <email>."
        );
        assert_eq!(shapes("no domain@here"), "no domain@here");
    }

    #[test]
    fn url_credentials_queries_and_replaced_host_paths_become_their_shape() {
        assert_eq!(
            shapes("fetch https://someone:hunter2@indexer.example/api?apikey=abc#frag done"),
            "fetch https://<userinfo>@indexer.example/api?<query>#frag done"
        );
        assert_eq!(
            shapes("https://198.51.100.7:8080/files/x"),
            "https://<ip>:8080/<path>"
        );
    }

    // A URL host no other source names is a name of its own.
    #[test]
    fn every_other_url_host_becomes_a_host_stand_in() {
        let log = "GET https://tracker.olaf.example/announce?k=1 and http://localhost:9090/x\n";
        let table = StandIns::build(
            &[Component::text("weaver.log", log)],
            &local("host-name", "/srv/weaver-data"),
            &StoredNames::default(),
        )
        .expect("table");
        assert_eq!(
            table.sanitize_str(&format!("{log}TRACKER.OLAF.EXAMPLE answered")),
            "GET https://host-1/<path>?<query> and http://localhost:9090/x\nhost-1 answered"
        );
    }

    #[test]
    fn a_cut_log_keeps_only_whole_lines() {
        let mut text = b"lease.2020.mkv done\nnext line\n".to_vec();
        from_first_line(&mut text);
        assert_eq!(text, b"next line\n");
        let mut text = b"INFO first\nINFO wrote Some.Secret.Na".to_vec();
        to_last_line(&mut text);
        assert_eq!(text, b"INFO first\n");

        let limit = usize::try_from(super::super::MAX_LOG_BYTES).expect("limit");
        let line = b"INFO Some.Secret.Name.mkv written\n";
        let mut text = Vec::with_capacity(limit + 4 * line.len());
        while text.len() <= limit + 2 * line.len() {
            text.extend_from_slice(line);
        }
        text.extend_from_slice(b"INFO last line\n");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&text).expect("gzip");
        let tail = gunzip_tail(&encoder.finish().expect("gzip")).expect("tail");
        assert!(tail.len() <= limit);
        assert!(tail.starts_with(line));
        assert!(tail.ends_with(b"INFO last line\n"));
    }

    #[test]
    fn file_suffix_keeps_only_what_the_engines_act_on() {
        // What a stand-in keeps of a name: every kind segment, so the shape
        // of a set stays readable. Any other suffix names the file rather
        // than its kind, so it is dropped.
        fn file_suffix(name: &str) -> String {
            stem_cuts(name)
                .last()
                .map_or_else(String::new, |cut| name[*cut..].to_string())
        }
        assert_eq!(file_suffix("Some.Name.part01.rar"), ".part01.rar");
        assert_eq!(file_suffix("Some.Name.vol03+04.par2"), ".vol03+04.par2");
        assert_eq!(file_suffix("Some.Name.7z.001"), ".7z.001");
        assert_eq!(file_suffix("Some Name.MKV"), ".MKV");
        assert_eq!(file_suffix("Some.Name.2020"), "");
        assert_eq!(file_suffix("Some.Name.Olaf.001"), ".001");
    }

    #[test]
    fn a_provider_domain_is_its_last_two_labels() {
        assert_eq!(
            registrable_domain("news.real-provider.example").as_deref(),
            Some("real-provider.example")
        );
        assert_eq!(
            registrable_domain("eu.news.provider.co.uk").as_deref(),
            Some("provider.co.uk")
        );
        assert_eq!(registrable_domain("provider.example"), None);
    }
}

// The log file is JSON records by default. A Windows directory reaches it with
// its backslashes doubled, as JSON escapes them, and a name holding a quote
// with the quote escaped; the text lines built from those records quote a
// value holding a space with Rust's escapes. Every spelling is replaced.
#[cfg(test)]
mod json_log_spellings {
    use super::*;

    #[test]
    fn a_windows_directory_in_a_json_record_is_replaced_as_in_a_text_line() {
        let local = LocalNames {
            data_dir: PathBuf::from(r"D:\Media\WeaverData"),
            intermediate_dir: PathBuf::from(r"D:\Media\WeaverData\intermediate"),
            complete_dir: PathBuf::from(r"D:\Media\WeaverData\complete"),
            watch_dir: None,
            log_file: None,
            home_dir: None,
            user: None,
            hostname: Some("host-name".to_string()),
        };
        let table = StandIns::build(&[], &local, &StoredNames::default()).expect("table");
        let path = r"D:\Media\WeaverData\complete\Some.Show";
        // The old text line: `path=D:\Media\...` as `%` wrote it.
        let text_line = format!("2026-11-03T08:10:00.123456-07:00  INFO weaver: moved path={path}");
        let text = table.sanitize_str(&text_line);
        assert!(!text.contains("WeaverData"), "text line: {text}");
        // The JSON record JsonRecord writes for the same event.
        let record = format!(
            r#"{{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver","fields":{{"message":"moved","path":{}}}}}"#,
            serde_json::to_string(path).unwrap()
        );
        let json = table.sanitize_str(&record);
        assert!(!json.contains("WeaverData"), "json record: {json}");
    }

    #[test]
    fn a_windows_path_with_a_space_is_replaced_in_a_quoted_text_line() {
        let local = LocalNames {
            data_dir: PathBuf::from(r"D:\My Media\WeaverData"),
            intermediate_dir: PathBuf::from(r"D:\My Media\WeaverData\intermediate"),
            complete_dir: PathBuf::from(r"D:\My Media\WeaverData\complete"),
            watch_dir: None,
            log_file: None,
            home_dir: None,
            user: None,
            hostname: Some("host-name".to_string()),
        };
        let table = StandIns::build(&[], &local, &StoredNames::default()).expect("table");
        let path = r"D:\My Media\WeaverData\complete\Some.Show";
        let line = format!("2026-11-03T08:10:00.123456-07:00  INFO weaver: moved path={path:?}");
        let text = table.sanitize_str(&line);
        assert!(!text.contains("WeaverData"), "text line: {text}");
        assert!(
            text.contains(r#"path="<complete>\\name-1""#),
            "text line: {text}"
        );
    }

    #[test]
    fn a_name_holding_a_quote_and_a_backslash_is_replaced_in_a_json_record() {
        let name = r#"Olaf "Live" \ Encore.S01E01"#;
        let stored = StoredNames {
            jobs: BTreeMap::from([(
                4,
                JobNameRecord {
                    name: Some(name.to_string()),
                    ..JobNameRecord::default()
                },
            )]),
            ..StoredNames::default()
        };
        let table = StandIns::build(&[], &local_unix(), &stored).expect("table");
        let record = format!(
            r#"{{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver","fields":{{"message":"job added","name":{}}}}}"#,
            serde_json::to_string(name).unwrap()
        );
        let json = table.sanitize_str(&record);
        assert!(!json.contains("Olaf"), "json record: {json}");
        assert!(json.contains(r#""name":"job-4""#), "json record: {json}");
        let line =
            format!("2026-11-03T08:10:00.123456-07:00  INFO weaver: job added name={name:?}");
        let text = table.sanitize_str(&line);
        assert!(text.ends_with(r#"name="job-4""#), "text line: {text}");
    }

    fn local_unix() -> LocalNames {
        LocalNames {
            data_dir: PathBuf::from("/srv/weaver-data"),
            intermediate_dir: PathBuf::from("/srv/weaver-data/intermediate"),
            complete_dir: PathBuf::from("/srv/weaver-data/complete"),
            watch_dir: None,
            log_file: None,
            home_dir: None,
            user: None,
            hostname: Some("host-name".to_string()),
        }
    }
}
