// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The text form of a report: ASCII, at most 80 columns, short enough to
// paste into a fenced block.

use std::fmt::Write as _;

use super::{Endpoint, FileSummary, RedFlag, TorrentReport, anomalous_files};

// No line of the text form is wider than this.
pub const MAX_COLUMNS: usize = 80;
// Trackers and web seeds shown before the rest are left to the JSON.
const MAX_ENDPOINTS: usize = 8;

pub(super) fn render(report: &TorrentReport) -> String {
    let mut out = Text::default();
    header(&mut out, report);
    red_flags(&mut out, report);
    shape(&mut out, report);
    layout(&mut out, report);
    about(&mut out, report);
    trackers(&mut out, report);
    files(&mut out, report);
    out.finish()
}

#[derive(Default)]
struct Text {
    buffer: String,
}

impl Text {
    fn line(&mut self, line: impl AsRef<str>) {
        let line = line.as_ref();
        // Every value is ASCII: a constant, a number, a token or a host. A
        // line can only run long through a long host; cut it rather than
        // wrap.
        if line.len() > MAX_COLUMNS {
            let mut cut = MAX_COLUMNS - 3;
            while !line.is_char_boundary(cut) {
                cut -= 1;
            }
            self.buffer.push_str(&line[..cut]);
            self.buffer.push_str("...");
        } else {
            self.buffer.push_str(line);
        }
        self.buffer.push('\n');
    }

    fn finish(self) -> String {
        self.buffer
    }
}

fn header(out: &mut Text, report: &TorrentReport) {
    let identity = &report.identity;
    out.line(format!(
        "weaver torrent report  schema {}  weaver {}",
        identity.schema_version,
        identity.weaver_version.unwrap_or("-"),
    ));
    out.line(format!("fingerprint {}", identity.fingerprint.to_hex()));
    let hashes = match (identity.has_v1_hash, identity.has_v2_hash) {
        (true, true) => "v1 v2",
        (true, false) => "v1",
        (false, true) => "v2",
        (false, false) => "none",
    };
    out.line(format!(
        "source {}  version {}  hashes {}",
        identity.source.as_str(),
        identity
            .version
            .map_or("unknown", |version| version.as_str()),
        hashes,
    ));
}

fn red_flags(out: &mut Text, report: &TorrentReport) {
    if report.red_flags.is_empty() {
        out.line("red flags: none");
        return;
    }
    out.line(format!("red flags ({})", report.red_flags.len()));
    for flag in &report.red_flags {
        out.line(format!("  ! {}", describe(flag)));
    }
}

pub(super) fn describe(flag: &RedFlag) -> String {
    match flag {
        RedFlag::DoesNotLoad { reason } => format!("weaver does not load it: {reason}"),
        RedFlag::PrivateWithoutTrackers => "private, and no tracker weaver announces to".into(),
        RedFlag::NoTrackers => "no tracker weaver announces to".into(),
        RedFlag::UnusableTrackers { trackers } => {
            format!("{trackers} tracker(s) of a scheme weaver does not announce to")
        }
        RedFlag::LocalTrackers { trackers } => {
            format!("{trackers} tracker(s) at a loopback or lan address")
        }
        RedFlag::PieceHashMismatch { expected, listed } => {
            format!("{listed} B of piece hashes for {expected} B the pieces need")
        }
        RedFlag::OddPieceLength { piece_length } => {
            format!("piece length {piece_length} B: not a power of two of 16 KiB or more")
        }
        RedFlag::PieceLayersMissing { files } => {
            format!("{files} file(s) without their piece layer")
        }
        RedFlag::EmptyFiles { files } => format!("{files} empty file(s)"),
        RedFlag::PathTraversal { files } => format!("{files} path(s) climb out with . or .."),
        RedFlag::AbsolutePaths { files } => format!("{files} absolute path(s)"),
        RedFlag::SeparatorInNames { files } => {
            format!("{files} name(s) hold a path separator")
        }
        RedFlag::DuplicatePaths { files } => format!("{files} file(s) share a path"),
        RedFlag::CaseDuplicatePaths { files } => {
            format!("{files} file(s) share a path but for letter case")
        }
        RedFlag::OverlongNames { files } => {
            format!("{files} name(s) longer than a filesystem takes")
        }
        RedFlag::NonUtf8Names { files } => format!("{files} name(s) are not utf-8"),
        RedFlag::ExecutableFiles { files } => {
            format!("{files} file(s) named as programs that run")
        }
        RedFlag::SingleFileArchive { archive } => {
            format!("the one file is a {} archive", archive.as_str())
        }
        RedFlag::CreatedInFuture { days } => format!("created {days} days in the future"),
    }
}

fn shape(out: &mut Text, report: &TorrentReport) {
    let Some(shape) = &report.shape else {
        out.line("shape: unknown until the metadata arrives");
        return;
    };
    out.line("shape");
    out.line(format!(
        "  pieces {} x {}  last {}  total {}",
        shape.piece_count,
        human_bytes(u64::from(shape.piece_length)),
        human_bytes(shape.last_piece_bytes),
        human_bytes(shape.total_bytes),
    ));
    let mut files = format!("  files {}", shape.file_count);
    if shape.padding_files > 0 {
        let _ = write!(
            files,
            "  padding {} ({})",
            shape.padding_files,
            human_bytes(shape.padding_bytes)
        );
    }
    let _ = write!(files, "  empty {}", shape.empty_files);
    out.line(files);
    if let (Some(largest), Some(smallest)) = (shape.largest, shape.smallest) {
        out.line(format!(
            "  largest {} {}  smallest {} {}",
            largest.file,
            human_bytes(largest.bytes),
            smallest.file,
            human_bytes(smallest.bytes),
        ));
    }
    let mut checks = String::from(" ");
    if let Some(hashes) = shape.piece_hashes {
        let _ = write!(
            checks,
            " piece hashes {} of {} B",
            hashes.listed, hashes.expected
        );
    }
    if let Some(layers) = shape.piece_layers {
        let _ = write!(checks, "  layers {} of {}", layers.carried, layers.needed);
    }
    let _ = write!(checks, "  loads {}", shape.load_error.unwrap_or("yes"));
    out.line(checks);
}

fn layout(out: &mut Text, report: &TorrentReport) {
    let Some(layout) = &report.layout else {
        return;
    };
    out.line("layout");
    out.line(format!(
        "  single file {}  payload {}  depth {}",
        yes_no(layout.single_file),
        layout.payload.as_str(),
        layout.max_depth,
    ));
    out.line(format!(
        "  archives {}  recovery {}  programs {}  hidden {}  symlinks {}",
        layout.archive_files,
        layout.recovery_files,
        layout.executable_files,
        layout.hidden_files,
        layout.symlinks,
    ));
    let names = &layout.names;
    let odd = [
        ("traversal", names.traversal),
        ("absolute", names.absolute),
        ("separator", names.separator),
        ("dup", names.duplicate),
        ("case dup", names.case_duplicate),
        ("overlong", names.overlong),
        ("non-utf8", names.non_utf8),
        ("empty part", names.empty_component),
    ];
    if odd.iter().all(|(_, count)| *count == 0) {
        out.line("  names: nothing odd");
    } else {
        let mut line = String::from("  names");
        for (label, count) in odd.iter().filter(|(_, count)| *count > 0) {
            let _ = write!(line, " {label} {count}");
        }
        out.line(line);
    }
}

fn about(out: &mut Text, report: &TorrentReport) {
    let about = &report.about;
    out.line("about");
    if report.identity.source == super::Source::Metainfo {
        let created = match about.created_age_days {
            Some(_) if about.created_in_future => "in the future".to_owned(),
            Some(days) => format!("{days} days ago"),
            None => "-".to_owned(),
        };
        out.line(format!(
            "  created {}  by {}",
            created,
            about.created_by.as_deref().unwrap_or("-"),
        ));
    }
    let mut text = String::from(" ");
    for (label, shape) in [("name", about.name), ("comment", about.comment)] {
        match shape {
            Some(shape) => {
                let _ = write!(text, " {label} {} B", shape.bytes);
                if !shape.utf8 {
                    text.push_str(" non-utf8");
                }
                if shape.has_url {
                    text.push_str(" with link");
                }
            }
            None => {
                let _ = write!(text, " {label} -");
            }
        }
        text.push(' ');
    }
    let _ = write!(text, " ssl {}", yes_no(about.ssl));
    out.line(text);
    if let Some(magnet) = &about.magnet {
        out.line(format!(
            "  length {}  peers {}  selects files {}",
            magnet
                .exact_length
                .map_or("unknown".to_owned(), human_bytes),
            magnet.peers,
            yes_no(magnet.selects_files),
        ));
    }
}

fn trackers(out: &mut Text, report: &TorrentReport) {
    let trackers = &report.trackers;
    out.line("trackers");
    let on_off = |value: Option<bool>| match value {
        Some(true) => "on",
        Some(false) => "off",
        None => "unknown",
    };
    out.line(format!(
        "  private {}  dht {}  pex {}  lsd {}  tiers {}",
        match trackers.private {
            Some(private) => yes_no(private),
            None => "unknown",
        },
        on_off(trackers.discovery.dht),
        on_off(trackers.discovery.pex),
        on_off(trackers.discovery.lsd),
        trackers.tiers,
    ));
    endpoints(out, "tier", &trackers.trackers, true);
    if trackers.web_seeds.is_empty() {
        out.line("  web seeds: none");
    } else {
        out.line(format!("  web seeds ({})", trackers.web_seeds.len()));
        endpoints(out, "seed", &trackers.web_seeds, false);
    }
}

fn endpoints(out: &mut Text, label: &str, endpoints: &[Endpoint], tiered: bool) {
    for (index, endpoint) in endpoints.iter().take(MAX_ENDPOINTS).enumerate() {
        let mut line = if tiered {
            format!("  {label} {} {}", endpoint.tier, endpoint.scheme.as_str())
        } else {
            format!("  {label} {} {}", index + 1, endpoint.scheme.as_str())
        };
        if let Some(host) = &endpoint.host {
            let _ = write!(line, " {host}");
        }
        if let Some(port) = endpoint.port {
            let _ = write!(line, ":{port}");
        }
        let _ = write!(line, "  {}", endpoint.host_kind.as_str());
        if tiered && !endpoint.scheme.announces() {
            line.push_str("  not announced to");
        }
        out.line(line);
    }
    if endpoints.len() > MAX_ENDPOINTS {
        out.line(format!(
            "  ... {} more in the json",
            endpoints.len() - MAX_ENDPOINTS
        ));
    }
}

fn files(out: &mut Text, report: &TorrentReport) {
    if report.shape.is_none() {
        return;
    }
    let listed: Vec<&FileSummary> = anomalous_files(report).collect();
    let total = report.files.iter().filter(|f| f.is_anomalous()).count();
    if total == 0 {
        out.line("files: nothing unusual");
        return;
    }
    out.line(format!(
        "files with findings ({} of {})",
        total,
        report.files.len()
    ));
    for file in listed {
        let mut line = format!(
            "  {} {:<6} {:>10}  depth {}",
            file.token,
            file.ext.as_str(),
            human_bytes(file.bytes),
            file.depth,
        );
        for (set, label) in [
            (file.bytes == 0, "empty"),
            (file.executable_name, "program"),
            (file.traversal, "traversal"),
            (file.absolute, "absolute"),
            (file.separator, "separator"),
            (file.duplicate, "dup"),
            (file.case_duplicate, "case-dup"),
            (file.overlong, "overlong"),
            (file.non_utf8, "non-utf8"),
            (file.empty_component, "empty-part"),
        ] {
            if set {
                let _ = write!(line, " {label}");
            }
        }
        out.line(line);
    }
    if total > super::MAX_LISTED_FILES {
        out.line(format!(
            "  ... {} more in the json",
            total - super::MAX_LISTED_FILES
        ));
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

pub(super) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}
