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
// - The rule is by kind. A name (a host, login, account, directory, job,
//   file or title) of four characters or more is replaced wherever it stands
//   as a word of its own, in any ASCII case; a shorter one only as a whole
//   JSON or TOML string. A value (a category, a metadata field, a number) is
//   replaced only as a whole JSON or TOML string. A directory is replaced only
//   up to a path boundary. When two names share a spelling, the first
//   stand-in wins. Every other URL host in the package is a name, `host-N`.
// - The pass: one leftmost-longest automaton over every text component
//   (logs, opened gzip, JSON and TOML values and free keys). A JSON key the
//   GraphQL schema declares is kept. After it, IP and MAC addresses, e-mail
//   addresses, and URL credentials, query strings and paths after a
//   replaced host become their shape.
//
// What it cannot catch: a finished job past the 2,000, a deleted job, and a
// member name a finished job no longer stores, that a log still names; a
// category or metadata value inside running text; a name written in another
// Unicode normal form or a non-ASCII case; a name under four characters
// inside running text; a message-ID whose domain has no dot.

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
     recently finished; history_jobs_total counts the whole history. A log can still name an \
     older or deleted job, or a member name a finished job no longer stores. A category or \
     metadata value is replaced only where it is a whole string value; any other URL host is \
     a host-N stand-in. A file stand-in keeps only an extension the engines act on. IP, MAC \
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
    Ok(table.table.stand_ins.len())
}

// Every component left out because the table could not be built, named
// through the host's own names, and the reason, sanitized the same way, as
// one more.
pub(super) fn left_out(
    local: &LocalNames,
    names: impl Iterator<Item = String>,
    reason: &str,
) -> Vec<Component> {
    let table = StandIns::build(&[], local, &StoredNames::default()).ok();
    let sanitize = |text: &str| match &table {
        Some(table) => table.sanitize_str(text),
        None => "the stand-in table could not be built".to_string(),
    };
    let mut components: Vec<Component> = names
        .map(|name| {
            let name = match &table {
                Some(table) => table.member_name(&name),
                None => "component".to_string(),
            };
            Component::failed(
                name,
                "left out because the stand-in table could not be built",
            )
        })
        .collect();
    components.push(Component::failed("stand-in-table", sanitize(reason)));
    components
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

// The trailing part of a file name the engines act on: an extension they
// know, and the volume marker or container extension in front of a numbered
// one (`.part01.rar`, `.vol03+04.par2`, `.7z.001`). Any other suffix names
// the file rather than its kind, so it is dropped.
fn file_suffix(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let segments: Vec<&str> = base.split('.').collect();
    let Some((&last, rest)) = segments.split_last() else {
        return String::new();
    };
    let known = |segment: &str| {
        let kind = weaver_nzb::analysis::file_ext(&format!(".{segment}"));
        kind != weaver_nzb::analysis::FileExt::NONE && kind != weaver_nzb::analysis::FileExt::OTHER
    };
    if rest.is_empty() || !known(last) {
        return String::new();
    }
    let Some((&previous, stem)) = rest.split_last() else {
        return format!(".{last}");
    };
    let lowered = previous.to_ascii_lowercase();
    let volume_marker = ["part", "vol"].iter().any(|marker| {
        lowered.strip_prefix(marker).is_some_and(|number| {
            !number.is_empty()
                && number
                    .chars()
                    .all(|character| character.is_ascii_digit() || matches!(character, '+' | '-'))
        })
    });
    let numbered = last.chars().all(|character| character.is_ascii_digit());
    if !stem.is_empty() && (volume_marker || (numbered && known(previous))) {
        format!(".{previous}.{last}")
    } else {
        format!(".{last}")
    }
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

// The one table: every name's spelling, ASCII-lowercased, and its stand-in.
#[derive(Default)]
struct Table {
    // The spellings the automaton matches, and for each its stand-in and
    // whether it is a directory.
    keys: Vec<String>,
    names: Vec<(String, bool)>,
    index: HashSet<String>,
    // The names matched only as a whole JSON or TOML string.
    whole: HashMap<String, String>,
    stand_ins: HashSet<String>,
    // Every directory spelling, and how many were numbered `dir-N`.
    dir_keys: Vec<String>,
    dirs: usize,
}

impl Table {
    // Adds a name under the one rule, and says whether it was new.
    fn name(&mut self, value: &str, stand_in: &str) -> bool {
        if value.chars().count() < MIN_WORD_CHARS {
            self.insert(value, stand_in, Scope::Whole)
        } else {
            self.insert(value, stand_in, Scope::Word)
        }
    }

    fn dir(&mut self, value: &str, stand_in: &str) -> bool {
        self.insert(value, stand_in, Scope::Dir)
    }

    fn whole(&mut self, value: &str, stand_in: &str) -> bool {
        self.insert(value, stand_in, Scope::Whole)
    }

    // A directory outside every other the table holds, as `dir-N`.
    fn numbered_dir(&mut self, dir: &Path) {
        let lowered = dir.to_string_lossy().to_ascii_lowercase();
        if self.dir_keys.iter().any(|key| {
            lowered
                .strip_prefix(key.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '\\']))
        }) {
            return;
        }
        let stand_in = format!("dir-{}", self.dirs + 1);
        let mut added = false;
        for spelling in dir_spellings(dir) {
            added |= self.dir(&spelling, &stand_in);
        }
        self.dirs += usize::from(added);
    }

    // A spelling's stand-in is the first one stored for it, as a whole
    // string or in the automaton; where it matches is the entry's own scope.
    // Says whether the spelling was new.
    fn insert(&mut self, value: &str, stand_in: &str, scope: Scope) -> bool {
        let key = value.to_ascii_lowercase();
        if value.trim().is_empty() || value == stand_in || self.index.contains(&key) {
            return false;
        }
        let first = self.whole.get(&key).cloned();
        let new = first.is_none();
        let stand_in = first.unwrap_or_else(|| stand_in.to_string());
        match scope {
            Scope::Whole if new => {
                self.whole.insert(key, stand_in.clone());
            }
            Scope::Whole => return false,
            Scope::Word | Scope::Dir => {
                let dir = scope == Scope::Dir;
                if dir {
                    self.dir_keys.push(key.clone());
                }
                self.index.insert(key.clone());
                self.keys.push(key);
                self.names.push((stand_in.clone(), dir));
            }
        }
        self.stand_ins.insert(stand_in);
        new
    }
}

// Where a spelling is matched: only as a whole JSON or TOML string, as a
// word of running text, or as a directory up to a path boundary.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Whole,
    Word,
    Dir,
}

// The table for one package, its automaton, and the pass that applies it.
pub(super) struct StandIns {
    table: Table,
    automaton: Option<AhoCorasick>,
    // The log file's own name and stem, which the log members are named by.
    log_names: Vec<(String, &'static str)>,
    // The field names the GraphQL schema declares, kept as JSON keys.
    schema_keys: HashSet<String>,
}

impl StandIns {
    // Builds the table from every source, in a fixed order, each numbered by
    // its own id where it has one, so a spelling two names share goes to the
    // same one every time.
    fn build(
        components: &[Component],
        local: &LocalNames,
        stored: &StoredNames,
    ) -> Result<Self, String> {
        let mut table = Table::default();

        // Providers, by server id, also known by `host:port` and the domain
        // their host is registered under.
        let mut servers: Vec<&ServerConfig> = stored.servers.iter().collect();
        servers.sort_by_key(|server| server.id);
        for server in servers {
            let stand_in = format!("provider-{}", server.id);
            let host = server.host.as_str();
            table.name(&format!("{host}:{}", server.port), &stand_in);
            table.name(host, &stand_in);
            if let Some(domain) = registrable_domain(host) {
                table.name(&domain, &stand_in);
            }
            let certificate = format!("{stand_in}/certificate");
            if let Some(der) = &server.tls_name_mismatch_certificate_der {
                table.name(&certificate_fingerprint(der), &certificate);
            }
            if let Some(path) = &server.tls_ca_cert {
                table.dir(&path.to_string_lossy(), &certificate);
            }
            if let Some(username) = &server.username {
                table.name(username, &format!("{stand_in}/account"));
            }
        }

        // The host's directories, then each category's.
        for (dir, stand_in) in local.dirs() {
            for spelling in dir_spellings(dir) {
                table.dir(&spelling, stand_in);
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

        // Jobs, by id.
        let ids: BTreeSet<u64> = stored
            .jobs
            .keys()
            .chain(stored.posted.keys())
            .copied()
            .collect();
        let no_record = JobNameRecord::default();
        let no_nzb = NzbNames::default();
        for id in ids {
            let record = stored.jobs.get(&id).unwrap_or(&no_record);
            let nzb = stored.posted.get(&id).unwrap_or(&no_nzb);
            let job = format!("job-{id}");

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
            // NZB; a file is named both with and without the folder the NZB
            // or the archive put it in.
            let mut seen = HashSet::new();
            for file in nzb.file_names.iter().chain(&record.files) {
                if !seen.insert(file.as_str()) {
                    continue;
                }
                let stand_in = format!("{job}/file-{}{}", seen.len(), file_suffix(file));
                table.name(file, &stand_in);
                if let Some(base) = file.rsplit(['/', '\\']).next() {
                    table.name(base, &stand_in);
                }
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
            table.name(&proxy.host, &stand_in);
            if let Some(username) = &proxy.secrets.username {
                table.name(username, &format!("{stand_in}/account"));
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
                table.dir(&spelling, "<scripts>");
            }
        }
        for feed in &stored.feeds {
            let stand_in = format!("feed-{}", feed.id);
            table.name(&feed.name, &stand_in);
            if let Some(username) = &feed.username {
                table.name(username, &format!("{stand_in}/account"));
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
            table.name(login, "<login>");
        }
        if let Some(user) = &local.user {
            table.name(user, "<user>");
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
            table.name(hostname, "<hostname>");
            if let Some((short, _)) = hostname.split_once('.') {
                table.name(short, "<hostname>");
            }
        }

        // Every other URL host the package names.
        let mut hosts = 0;
        for data in components
            .iter()
            .filter_map(|component| component.outcome.as_ref().ok())
        {
            for host in url_hosts(data) {
                hosts += usize::from(table.name(&host, &format!("host-{}", hosts + 1)));
            }
        }

        let automaton = if table.keys.is_empty() {
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
        Ok(Self {
            table,
            automaton,
            log_names,
            schema_keys: HashSet::new(),
        })
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
            Ok(data) => match component.form {
                Form::Text => Ok(self.sanitize_bytes(&data)),
                Form::Json => self.sanitize_json(&data),
                Form::Toml => self.sanitize_toml(&data),
                Form::GzipText => self.sanitize_gzip(&data),
                // Only text is sanitized; a stream not opened is not sent.
                Form::Gzip => Err("left out because it was not decompressed for sanitizing".into()),
            },
        };
    }

    fn sanitize_json(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut value: Value = serde_json::from_slice(data).map_err(|error| {
            self.sanitize_str(&format!(
                "left out because it could not be read for sanitizing: {error}"
            ))
        })?;
        self.sanitize_json_value(&mut value);
        serde_json::to_vec_pretty(&value).map_err(|error| self.sanitize_str(&error.to_string()))
    }

    fn sanitize_json_value(&self, value: &mut Value) {
        match value {
            Value::String(text) => *text = self.sanitize_string(text),
            Value::Array(items) => items
                .iter_mut()
                .for_each(|item| self.sanitize_json_value(item)),
            Value::Object(map) => {
                let fields = std::mem::take(map);
                for (key, mut child) in fields {
                    self.sanitize_json_value(&mut child);
                    let key = self.sanitize_key(key, |key| map.contains_key(key));
                    map.insert(key, child);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    // A whole JSON or TOML string: a whole-string name it is, or the text
    // pass.
    fn sanitize_string(&self, text: &str) -> String {
        match self.table.whole.get(&text.to_ascii_lowercase()) {
            Some(stand_in) => stand_in.clone(),
            None => self.sanitize_str(text),
        }
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
        self.sanitize_toml_value(&mut value);
        toml::to_string_pretty(&value)
            .map(String::into_bytes)
            .map_err(|error| {
                self.sanitize_str(&format!(
                    "left out because it could not be re-serialized: {error}"
                ))
            })
    }

    fn sanitize_toml_value(&self, value: &mut toml::Value) {
        match value {
            toml::Value::String(text) => *text = self.sanitize_string(text),
            toml::Value::Array(items) => items
                .iter_mut()
                .for_each(|item| self.sanitize_toml_value(item)),
            toml::Value::Table(table) => {
                let fields = std::mem::take(table);
                for (key, mut child) in fields {
                    self.sanitize_toml_value(&mut child);
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
        let sanitized = self.sanitize_bytes(text);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(&sanitized)
            .and_then(|()| encoder.finish())
            .map_err(|error| format!("left out because it could not be compressed again: {error}"))
    }

    fn sanitize_str(&self, text: &str) -> String {
        // Every substitution replaces whole names with ASCII, and every shape
        // rule matches and replaces ASCII only, so valid UTF-8 stays valid.
        String::from_utf8(self.sanitize_bytes(text.as_bytes()))
            .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
    }

    // The text pass: the table, then the shapes.
    fn sanitize_bytes(&self, text: &[u8]) -> Vec<u8> {
        redact_ips(&redact_macs(&redact_emails(
            &self.redact_url_parts(&self.substitute(text)),
        )))
    }

    // Replaces every name in `text`: the longest at the leftmost place that
    // the rule lets stand.
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
            out.extend_from_slice(self.table.names[name].0.as_bytes());
            copied = end;
            at = end;
        }
        out.extend_from_slice(&text[copied..]);
        out
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
            let stands = if self.table.names[name].1 {
                !path_glued_before(text, start) && !path_glued_after(text, end)
            } else {
                !(word_after(text, start) && word_before(text, start))
                    && !(word_before(text, end) && word_after(text, end))
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
        self.table.stand_ins.contains(host) || host.parse::<IpAddr>().is_ok()
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
        let value = json(&table, r#"{"progress": "progress", "progress of x": 1}"#);
        assert_eq!(value["progress"], "job-1");
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
