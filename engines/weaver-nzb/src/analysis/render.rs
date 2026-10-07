//! The text form of a report: ASCII, at most 80 columns, short enough to
//! paste into a fenced block.

use std::fmt::Write as _;

use super::{FileSummary, NzbReport, RedFlag, anomalous_files};

/// No line of the text form is wider than this.
pub const MAX_COLUMNS: usize = 80;
/// Repeats of one red flag kind shown before the rest are summarized.
const MAX_FLAGS_PER_KIND: usize = 3;
/// Archive sets, and recovery sets of each kind, shown before the rest are
/// left to the JSON.
const MAX_SETS: usize = 4;

pub(super) fn render(report: &NzbReport) -> String {
    let mut out = Text::default();
    header(&mut out, report);
    red_flags(&mut out, report);
    shape(&mut out, report);
    layout(&mut out, report);
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
        // Every value is either a constant or a number, so a line can only
        // run long through a long list; cut it rather than wrap.
        if line.len() > MAX_COLUMNS {
            self.buffer.push_str(&line[..MAX_COLUMNS - 3]);
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

fn header(out: &mut Text, report: &NzbReport) {
    let identity = &report.identity;
    out.line(format!(
        "weaver nzb report  schema {}  weaver {}",
        identity.schema_version,
        identity.weaver_version.unwrap_or("-"),
    ));
    out.line(format!("fingerprint {}", identity.fingerprint.to_hex()));
}

fn red_flags(out: &mut Text, report: &NzbReport) {
    if report.red_flags.is_empty() {
        out.line("red flags: none");
        return;
    }
    out.line(format!("red flags ({})", report.red_flags.len()));
    // Per-set flags repeat once per set; a post with dozens of sets would
    // push every other finding out of view, so each kind shows a few.
    let mut index = 0;
    while index < report.red_flags.len() {
        let kind = std::mem::discriminant(&report.red_flags[index]);
        let run = report.red_flags[index..]
            .iter()
            .take_while(|flag| std::mem::discriminant(*flag) == kind)
            .count();
        for flag in &report.red_flags[index..index + run.min(MAX_FLAGS_PER_KIND)] {
            out.line(format!("  ! {}", describe(flag)));
        }
        if run > MAX_FLAGS_PER_KIND {
            out.line(format!(
                "  ! ... {} more like the above",
                run - MAX_FLAGS_PER_KIND
            ));
        }
        index += run;
    }
}

fn describe(flag: &RedFlag) -> String {
    match flag {
        RedFlag::MissingVolumes { set, missing } => {
            format!("archive {set} is missing {missing} volume(s)")
        }
        RedFlag::MissingFiles { declared, listed } => {
            format!("subjects declare {declared} files, the nzb lists {listed}")
        }
        RedFlag::MissingSegments { files, segments } => {
            format!("{segments} segment(s) missing across {files} file(s)")
        }
        RedFlag::DroppedFiles { files } => format!("{files} file(s) have no usable segment"),
        RedFlag::NoRecoveryData => "no par2 or par3 recovery data".to_string(),
        RedFlag::ThinRecovery {
            set,
            permille,
            oldest_age_days,
        } => match oldest_age_days {
            Some(age) => format!(
                "thin recovery: {set} ~{} on a post {age} days old",
                percent(*permille)
            ),
            None => format!("thin recovery: {set} ~{}", percent(*permille)),
        },
        RedFlag::HashedWithoutRecovery => "names are hidden and there is no recovery".to_string(),
        RedFlag::ShortDeclaredSize { files } => {
            format!("{files} file(s) sum to less than their subject declares")
        }
        RedFlag::SegmentCountMismatch { files } => {
            format!("{files} file(s) list a segment count unlike their subject")
        }
        RedFlag::DuplicateFiles { files } => format!("{files} file(s) repeat another file"),
        RedFlag::SharedMessageIds { count } => {
            format!("{count} message-id(s) listed by more than one file")
        }
        RedFlag::MalformedSegments { count } => format!("{count} malformed segment(s) skipped"),
        RedFlag::NearRetentionEdge { oldest_age_days } => {
            format!("oldest article is {oldest_age_days} days old")
        }
        RedFlag::PasswordDeclared => "a password is declared".to_string(),
    }
}

fn shape(out: &mut Text, report: &NzbReport) {
    let shape = &report.shape;
    out.line("shape");
    let mut files = format!("  files {}", shape.file_count);
    if let Some(declared) = shape.declared_file_count {
        let _ = write!(files, " (subjects say {declared})");
    }
    if shape.files_without_segments > 0 {
        let _ = write!(files, " +{} empty dropped", shape.files_without_segments);
    }
    let _ = write!(
        files,
        "  segments {}  groups {}  posters {}",
        shape.segment_count, shape.group_count, shape.poster_count
    );
    out.line(files);

    let mut bytes_line = format!("  bytes {}", human_bytes(shape.summed_bytes));
    if let Some(declared) = &shape.declared_bytes {
        let _ = write!(
            bytes_line,
            "  declared {} on {} file(s), {} short",
            human_bytes(declared.declared),
            declared.files,
            declared.short_files
        );
    }
    out.line(bytes_line);

    if let Some(sizes) = &shape.segment_sizes {
        out.line(format!(
            "  segment min {}  median {}  max {}",
            human_bytes(u64::from(sizes.min)),
            human_bytes(u64::from(sizes.median)),
            human_bytes(u64::from(sizes.max)),
        ));
        out.line(format!(
            "  segment most common {} ({})",
            human_bytes(u64::from(sizes.common)),
            percent(sizes.common_permille),
        ));
    }

    match &shape.posted {
        Some(posted) => {
            let age = if posted.newest_age_days == posted.oldest_age_days {
                format!("{} days ago", posted.oldest_age_days)
            } else {
                format!(
                    "{}-{} days ago",
                    posted.newest_age_days, posted.oldest_age_days
                )
            };
            let span = if posted.span_secs == 0 {
                String::new()
            } else {
                format!(" over {}", human_span(posted.span_secs))
            };
            let bad = if shape.invalid_dates > 0 {
                format!(", {} bad date(s)", shape.invalid_dates)
            } else {
                String::new()
            };
            out.line(format!("  posted {age}{span}{bad}"));
        }
        None => out.line("  posted: no dates"),
    }

    out.line(format!(
        "  missing segs {} in {} file(s)  count mismatch {}  malformed {}",
        shape.missing_segments,
        shape.files_missing_segments,
        shape.segment_count_mismatch_files,
        shape.malformed_segments,
    ));
    out.line(format!(
        "  dup segs {}  shared ids {}  dup files {}  out of order {}",
        shape.duplicate_segments,
        shape.shared_message_ids,
        shape.duplicate_files,
        shape.out_of_order_files,
    ));
}

fn layout(out: &mut Text, report: &NzbReport) {
    let layout = &report.layout;
    let obfuscation = &layout.obfuscation;
    out.line("layout");
    out.line(format!(
        "  payload {}  names {} ({} readable, {} hashed, {} unnamed)",
        layout.payload.as_str(),
        obfuscation.level.as_str(),
        obfuscation.readable_files,
        obfuscation.hashed_files,
        obfuscation.unnamed_files,
    ));
    for set in layout.archive_sets.iter().take(MAX_SETS) {
        let mut line = format!(
            "  {} {} {}  {} vol  {}..{}",
            set.token,
            set.kind.as_str(),
            set.scheme.as_str(),
            set.volumes,
            set.first_volume,
            set.last_volume,
        );
        if set.missing_volumes > 0 {
            let _ = write!(line, " missing {}", set.missing_volumes);
        }
        if set.repeated_volumes > 0 {
            let _ = write!(line, " repeated {}", set.repeated_volumes);
        }
        let _ = write!(
            line,
            "  {}  stem {}",
            human_bytes(set.bytes),
            set.stem.as_str()
        );
        out.line(line);
    }
    if layout.archive_sets.len() > MAX_SETS {
        out.line(format!(
            "  ... {} more archive sets in the json",
            layout.archive_sets.len() - MAX_SETS
        ));
    }
    for (label, sets) in [("par2", &layout.par2_sets), ("par3", &layout.par3_sets)] {
        for set in sets.iter().take(MAX_SETS) {
            let mut line = format!(
                "  {} {} index {}  {} vol",
                set.token,
                label,
                if set.has_index { "yes" } else { "no" },
                set.volumes,
            );
            if set.declared_blocks > 0 {
                let _ = write!(line, "  {} blocks", set.declared_blocks);
            }
            let _ = write!(line, "  {}", human_bytes(set.recovery_bytes));
            match set.estimated_recovery_permille {
                Some(permille) => {
                    let _ = write!(
                        line,
                        "  ~{} of {}",
                        percent(permille),
                        human_bytes(set.protected_bytes)
                    );
                }
                None => line.push_str("  covers nothing named"),
            }
            out.line(line);
        }
        if sets.len() > MAX_SETS {
            out.line(format!(
                "  ... {} more {label} sets in the json",
                sets.len() - MAX_SETS
            ));
        }
    }
    let extras = &layout.extras;
    out.line(format!(
        "  extras nfo {} sfv {} srr {} srs {} sample {} nzb {} img {} txt {}",
        extras.nfo,
        extras.sfv,
        extras.srr,
        extras.srs,
        extras.sample,
        extras.nzb,
        extras.image,
        extras.text,
    ));
    out.line(format!(
        "  nested archives {}  unclassified {}  password meta {} name {}",
        layout.nested_archive_sets,
        layout.unclassified_files,
        yes_no(layout.password.in_meta),
        yes_no(layout.password.in_name),
    ));
    out.line(format!(
        "  poster tool {}  message-id domains {}",
        report.poster.tool.as_str(),
        report.poster.message_id_domains.as_str(),
    ));
}

fn files(out: &mut Text, report: &NzbReport) {
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
            "  {} {:<6} {:<4}",
            file.token,
            file.ext.as_str(),
            file.set
                .map_or_else(|| "-".to_string(), |set| set.to_string()),
        );
        match file.declared_segments {
            Some(declared) => {
                let _ = write!(line, " {}/{} segs", file.segments, declared);
            }
            None => {
                let _ = write!(line, " {} segs", file.segments);
            }
        }
        if file.missing_segments > 0 {
            let _ = write!(line, " missing {}", file.missing_segments);
        }
        if file.duplicate_segments > 0 {
            let _ = write!(line, " dup {}", file.duplicate_segments);
        }
        if file.malformed_segments > 0 {
            let _ = write!(line, " malformed {}", file.malformed_segments);
        }
        if let Some(declared) = file.declared_bytes
            && file.bytes < declared
        {
            let _ = write!(line, " short {}", human_bytes(declared - file.bytes));
        }
        if file.out_of_order {
            line.push_str(" out-of-order");
        }
        if let Some(original) = file.repeats {
            let _ = write!(line, " repeats {original}");
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

fn percent(permille: u32) -> String {
    format!("{}.{}%", permille / 10, permille % 10)
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

fn human_span(secs: u64) -> String {
    match secs {
        0..60 => format!("{secs}s"),
        60..3_600 => format!("{}m", secs / 60),
        3_600..86_400 => format!("{}h{:02}m", secs / 3_600, secs % 3_600 / 60),
        _ => format!("{}d{:02}h", secs / 86_400, secs % 86_400 / 3_600),
    }
}
