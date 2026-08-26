//! Naming policy for the files a finished job delivers.
//!
//! [`crate::deobfuscate`] answers "is this *posted* file's name meaningless",
//! which is what the PAR2 rename pass needs. This module answers a different
//! question at the other end of the pipeline: once the archives are open and
//! their members sit in the delivery directory, is the payload still wearing a
//! randomized name that no downstream tool can match?
//!
//! The two verdicts are tuned in opposite directions on purpose. The posted-file
//! verdict is precision-tuned — a wrong `true` there sends the pipeline hunting
//! for a rename source that does not exist. The delivery verdict is
//! recall-tuned and **defaults to obfuscated**: the only action it can take is
//! renaming a file to the job's own name, and a job that reached delivery under
//! a meaningful name is a better label than a name we could not vouch for. The
//! refusal gate the caller applies — never rename when the target name is
//! itself unreadable — is what keeps that default from doing harm.
//!
//! Everything here is pure. The caller owns the filesystem, the target-name
//! decision, and the ordering of the renames it is handed.

use crate::deobfuscate::{contains_protected_media_structure, is_obfuscated};

/// A file that the finished job is about to hand to the user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveredFile<'a> {
    /// Path relative to the delivery root, `/`-separated. Top-level files are
    /// a bare filename.
    pub relative_path: &'a str,
    pub bytes: u64,
}

/// One rename to apply, both paths relative to the delivery root.
///
/// `from` and `to` always share a parent directory: the pass renames payload
/// in place and never reshapes the delivery's layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRename {
    pub from: String,
    pub to: String,
}

/// A delivered file below this size is never worth naming a release after, no
/// matter how randomized its name looks.
pub const MIN_CANDIDATE_BYTES: u64 = 10 * 1024 * 1024;

/// How far the biggest file must outweigh the runner-up before it can be called
/// *the* payload. A delivery whose two largest files are comparable is a set
/// (episodes, discs, parts), and renaming one of them after the job would lie
/// about the other.
const CANDIDATE_DOMINANCE_FACTOR: u64 = 3;

/// Extensions that never carry a release name even when they are the biggest
/// file present: disc-structure parts, whose names are load-bearing for the
/// players that read them, and archive parts, which the PAR2 rename pass owns.
const EXCLUDED_CANDIDATE_EXTENSIONS: &[&str] = &[
    "vob", "m2ts", "mts", "cpi", "clpi", "mpl", "mpls", "bdm", "bdmv", "rar", "par2",
];

/// Separators that make a name readable to a human. `-` is deliberately absent:
/// it appears inside hashes often enough that counting it as a readability
/// signal would clear names that are not readable at all.
const READABILITY_SEPARATORS: [char; 3] = [' ', '.', '_'];

/// Picks the delivered file whose name the job should be renamed after, or
/// `None` when the delivery should be left alone.
///
/// Returns an index into `files`. A `Some` answer means every gate passed *and*
/// the name was judged obfuscated — the caller still owes the target-name
/// refusal gate before it touches the disk.
pub fn select_rename_candidate(files: &[DeliveredFile<'_>]) -> Option<usize> {
    // A disc rip is a structure, not a payload with helpers. Its biggest file
    // would pass every size gate and renaming it would break the disc.
    if files
        .iter()
        .any(|file| contains_protected_media_structure(file.relative_path))
    {
        return None;
    }

    let (biggest, candidate) = files
        .iter()
        .enumerate()
        .max_by_key(|(index, file)| (file.bytes, std::cmp::Reverse(*index)))
        .map(|(index, file)| (index, *file))?;

    if candidate.bytes < MIN_CANDIDATE_BYTES {
        return None;
    }

    let runner_up = files
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != biggest)
        .map(|(_, file)| file.bytes)
        .max();
    if let Some(runner_up) = runner_up
        && candidate.bytes < runner_up.saturating_mul(CANDIDATE_DOMINANCE_FACTOR)
    {
        return None;
    }

    let name = file_name_of(candidate.relative_path);
    if extension_of(name).is_some_and(|ext| {
        EXCLUDED_CANDIDATE_EXTENSIONS
            .iter()
            .any(|excluded| ext.eq_ignore_ascii_case(excluded))
    }) {
        return None;
    }

    if !looks_obfuscated_for_delivery(name) {
        return None;
    }

    Some(biggest)
}

/// Returns `true` when a delivered file's name should be replaced.
///
/// Structured as two allow-lists around a default. The obfuscated patterns are
/// checked first because a hash wrapped in real-looking tags would otherwise
/// read as clean; the clean patterns then rescue anything that carries the
/// shape of a human-written name; everything left over is treated as
/// obfuscated.
///
/// `name` is a bare filename, not a path. Its final extension is judged
/// separately from the stem so that `.mkv` does not contribute lowercase
/// letters to the readability counts.
pub fn looks_obfuscated_for_delivery(name: &str) -> bool {
    let stem = strip_final_extension(name);
    if stem.is_empty() {
        return false;
    }

    // ── definitely obfuscated ───────────────────────────────────────────
    if stem.len() == 32
        && stem
            .chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase() && c.is_ascii_hexdigit())
    {
        return true;
    }
    if stem.len() >= 40 && stem.chars().all(|c| c.is_ascii_hexdigit() || c == '.') {
        return true;
    }
    // A hash that someone dressed in bracketed tags. The tags are what would
    // otherwise satisfy the readability counts below.
    if bracket_group_count(stem) >= 2 && longest_hex_run(stem) >= 20 {
        return true;
    }
    // The posted-name verdict recognizes obfuscator families this one has no
    // rule for. It never fires on a readable name, so folding it in only adds
    // recall.
    if is_obfuscated(name) {
        return true;
    }

    // ── definitely clean ────────────────────────────────────────────────
    let separators = stem
        .chars()
        .filter(|c| READABILITY_SEPARATORS.contains(c))
        .count();
    let uppercase = stem.chars().filter(char::is_ascii_uppercase).count();
    let lowercase = stem.chars().filter(char::is_ascii_lowercase).count();
    let digits = stem.chars().filter(char::is_ascii_digit).count();
    let letters = uppercase + lowercase;

    // Mixed case around a separator: "Silver Horizon", "Silver.Horizon".
    if uppercase >= 2 && lowercase >= 2 && separators >= 1 {
        return false;
    }
    // Enough separators that the name is a phrase, whatever the words are.
    if separators >= 3 {
        return false;
    }
    // Words plus a number: a title with a year, a season, a resolution.
    if letters >= 4 && digits >= 4 && separators >= 1 {
        return false;
    }
    // A single Title-cased word. Hashes that begin with a capital keep
    // producing uppercase further in, so the run of lowercase has to dominate.
    if stem.starts_with(|c: char| c.is_ascii_uppercase())
        && letters >= 4
        && lowercase * 4 >= letters * 3
    {
        return false;
    }

    true
}

/// Plans the renames that give `candidate` the name `target_stem`, plus the
/// same-stem helpers that must follow it.
///
/// `target_stem` is used verbatim: the caller is responsible for making it a
/// legal path component. `files[candidate]` keeps its own extension, and every
/// other file whose name begins with the candidate's stem keeps everything
/// after it — that is what carries `-sample` and `.eng.srt` across.
///
/// Returns an empty plan when the candidate would not actually change name.
pub fn plan_renames(
    files: &[DeliveredFile<'_>],
    candidate: usize,
    target_stem: &str,
) -> Vec<PlannedRename> {
    let Some(candidate_file) = files.get(candidate) else {
        return Vec::new();
    };
    if target_stem.is_empty() {
        return Vec::new();
    }

    let candidate_name = file_name_of(candidate_file.relative_path);
    let old_stem = strip_final_extension(candidate_name);
    if old_stem.is_empty() || old_stem == target_stem {
        return Vec::new();
    }
    let candidate_suffix = &candidate_name[old_stem.len()..];

    // Every name the delivery already occupies, plus every name this plan is
    // about to create. Collisions are resolved by numbering the stem rather
    // than by giving up: a delivery that already holds the job's name is a
    // rerun, not a reason to keep the hash.
    let mut occupied: std::collections::HashSet<String> = files
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != candidate)
        .map(|(_, file)| collision_key(file.relative_path))
        .collect();

    let parent = parent_of(candidate_file.relative_path);
    let final_stem = allocate_free_stem(target_stem, candidate_suffix, parent, &occupied);
    let candidate_to = join_relative(parent, &format!("{final_stem}{candidate_suffix}"));
    occupied.insert(collision_key(&candidate_to));

    let mut plan = vec![PlannedRename {
        from: candidate_file.relative_path.to_string(),
        to: candidate_to,
    }];

    for (index, file) in files.iter().enumerate() {
        if index == candidate {
            continue;
        }
        let name = file_name_of(file.relative_path);
        let Some(suffix) = name.strip_prefix(old_stem) else {
            continue;
        };
        // Only a suffix that starts a new token belongs to this stem.
        // Without this, `Show.mkv` would claim `Showreel.mkv`.
        if !suffix.is_empty() && !suffix.starts_with(['.', '-', '_', ' ']) {
            continue;
        }
        let sibling_parent = parent_of(file.relative_path);
        let to = join_relative(sibling_parent, &format!("{final_stem}{suffix}"));
        if !occupied.insert(collision_key(&to)) {
            // Renaming this helper would land on a name the delivery already
            // holds. Leaving it under its old name loses nothing.
            continue;
        }
        plan.push(PlannedRename {
            from: file.relative_path.to_string(),
            to,
        });
    }

    plan
}

fn allocate_free_stem(
    target_stem: &str,
    suffix: &str,
    parent: &str,
    occupied: &std::collections::HashSet<String>,
) -> String {
    let first = join_relative(parent, &format!("{target_stem}{suffix}"));
    if !occupied.contains(&collision_key(&first)) {
        return target_stem.to_string();
    }
    for attempt in 1u32.. {
        let stem = format!("{target_stem}.{attempt}");
        let path = join_relative(parent, &format!("{stem}{suffix}"));
        if !occupied.contains(&collision_key(&path)) {
            return stem;
        }
    }
    unreachable!("the attempt counter is unbounded")
}

fn collision_key(relative_path: &str) -> String {
    relative_path.to_ascii_lowercase()
}

fn join_relative(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn parent_of(relative_path: &str) -> &str {
    relative_path
        .rfind('/')
        .map_or("", |slash| &relative_path[..slash])
}

fn file_name_of(relative_path: &str) -> &str {
    relative_path
        .rfind('/')
        .map_or(relative_path, |slash| &relative_path[slash + 1..])
}

/// Splits off a trailing `.ext` only when it looks like one: short, and made of
/// alphanumerics. `archive.part01` keeps its whole name; `payload.mkv` does not.
fn strip_final_extension(name: &str) -> &str {
    match extension_of(name) {
        Some(ext) => &name[..name.len() - ext.len() - 1],
        None => name,
    }
}

fn extension_of(name: &str) -> Option<&str> {
    let dot = name.rfind('.')?;
    if dot == 0 {
        return None;
    }
    let ext = &name[dot + 1..];
    if ext.is_empty() || ext.len() > 5 || !ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(ext)
}

fn bracket_group_count(stem: &str) -> usize {
    let mut groups = 0usize;
    let mut open = false;
    for c in stem.chars() {
        match c {
            '[' => open = true,
            ']' if open => {
                groups += 1;
                open = false;
            }
            _ => {}
        }
    }
    groups
}

fn longest_hex_run(stem: &str) -> usize {
    let mut run = 0usize;
    let mut longest = 0usize;
    for c in stem.chars() {
        if c.is_ascii_hexdigit() || c == '.' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    longest
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn file(relative_path: &str, bytes: u64) -> DeliveredFile<'_> {
        DeliveredFile {
            relative_path,
            bytes,
        }
    }

    // ── looks_obfuscated_for_delivery ───────────────────────────────────

    #[test]
    fn thirty_two_lowercase_hex_is_obfuscated() {
        assert!(looks_obfuscated_for_delivery(
            "2c0837e5fa42c8cfb5d5e583168a2af4.mkv"
        ));
    }

    #[test]
    fn long_hex_and_dot_stem_is_obfuscated() {
        assert!(looks_obfuscated_for_delivery(
            "a1b2c3d4.e5f60718.90abcdef.12345678.9abcdef0.mkv"
        ));
    }

    #[test]
    fn hex_run_wrapped_in_bracket_tags_is_obfuscated() {
        assert!(looks_obfuscated_for_delivery(
            "[Tagged] some [More] b2bef89a622e4a23f07b0d3757ad5e8aa0.mkv"
        ));
    }

    #[test]
    fn random_mixed_case_member_name_is_obfuscated_by_default() {
        assert!(looks_obfuscated_for_delivery("Yb5drZSkNi20UCMkb.mkv"));
        assert!(looks_obfuscated_for_delivery("qpzmwoxneidb.mkv"));
    }

    #[test]
    fn readable_release_names_are_clean() {
        for name in [
            "Silver.Horizon.S01E04.1080p.WEB-DL.x264-CREW.mkv",
            "Silver Horizon 2024.mkv",
            "Silverhorizon.mkv",
            "Quiet_Harbour_2021.mkv",
        ] {
            assert!(
                !looks_obfuscated_for_delivery(name),
                "{name} was judged obfuscated"
            );
        }
    }

    #[test]
    fn extension_does_not_rescue_a_hash() {
        // The stem is judged alone, so `.mkv`'s three lowercase letters cannot
        // push a hash over the Title-cased-word rule.
        assert!(looks_obfuscated_for_delivery("A1B2C3D4E5F60718.mkv"));
    }

    // ── select_rename_candidate ─────────────────────────────────────────

    #[test]
    fn dominant_obfuscated_file_is_selected() {
        let files = [
            file("Yb5drZSkNi20UCMkb.mkv", 900 * MIB),
            file("Yb5drZSkNi20UCMkb.nfo", 4096),
        ];
        assert_eq!(select_rename_candidate(&files), Some(0));
    }

    #[test]
    fn a_lone_obfuscated_file_is_selected() {
        let files = [file("Yb5drZSkNi20UCMkb.mkv", 900 * MIB)];
        assert_eq!(select_rename_candidate(&files), Some(0));
    }

    #[test]
    fn comparable_second_file_blocks_the_pass() {
        // Two episodes, not a payload plus helpers.
        let files = [
            file("Yb5drZSkNi20UCMkb.mkv", 900 * MIB),
            file("Kf2ptQWmXe81ZBnrd.mkv", 800 * MIB),
        ];
        assert_eq!(select_rename_candidate(&files), None);
    }

    #[test]
    fn small_files_are_never_candidates() {
        let files = [file("Yb5drZSkNi20UCMkb.mkv", 9 * MIB)];
        assert_eq!(select_rename_candidate(&files), None);
    }

    #[test]
    fn readable_payload_is_left_alone() {
        let files = [file("Silver.Horizon.2024.1080p.mkv", 900 * MIB)];
        assert_eq!(select_rename_candidate(&files), None);
    }

    #[test]
    fn excluded_extensions_are_never_candidates() {
        for name in ["Yb5drZSkNi20UCMkb.vob", "Yb5drZSkNi20UCMkb.rar"] {
            let files = [file(name, 900 * MIB)];
            assert_eq!(select_rename_candidate(&files), None, "{name}");
        }
    }

    #[test]
    fn disc_structure_delivery_skips_the_whole_pass() {
        let files = [
            file("VIDEO_TS/VTS_01_1.VOB", 900 * MIB),
            file("Yb5drZSkNi20UCMkb.mkv", 400 * MIB),
        ];
        assert_eq!(select_rename_candidate(&files), None);

        let bluray = [
            file("BDMV/STREAM/00000.m2ts", 900 * MIB),
            file("Yb5drZSkNi20UCMkb.mkv", 400 * MIB),
        ];
        assert_eq!(select_rename_candidate(&bluray), None);
    }

    // ── plan_renames ────────────────────────────────────────────────────

    #[test]
    fn candidate_and_same_stem_helpers_are_renamed() {
        let files = [
            file("Yb5drZSkNi20UCMkb.mkv", 900 * MIB),
            file("Yb5drZSkNi20UCMkb-sample.mkv", 20 * MIB),
            file("Yb5drZSkNi20UCMkb.dut.srt", 40 * 1024),
            file("unrelated.nfo", 1024),
        ];
        let plan = plan_renames(&files, 0, "Silver Horizon 2024");
        assert_eq!(
            plan,
            vec![
                PlannedRename {
                    from: "Yb5drZSkNi20UCMkb.mkv".into(),
                    to: "Silver Horizon 2024.mkv".into(),
                },
                PlannedRename {
                    from: "Yb5drZSkNi20UCMkb-sample.mkv".into(),
                    to: "Silver Horizon 2024-sample.mkv".into(),
                },
                PlannedRename {
                    from: "Yb5drZSkNi20UCMkb.dut.srt".into(),
                    to: "Silver Horizon 2024.dut.srt".into(),
                },
            ]
        );
    }

    #[test]
    fn a_longer_name_sharing_the_stem_is_not_a_helper() {
        let files = [
            file("Ab5drZSkNi.mkv", 900 * MIB),
            file("Ab5drZSkNiXtra.mkv", 1024),
        ];
        let plan = plan_renames(&files, 0, "Silver Horizon");
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].to, "Silver Horizon.mkv");
    }

    #[test]
    fn a_colliding_target_is_numbered_rather_than_abandoned() {
        let files = [
            file("Yb5drZSkNi20UCMkb.mkv", 900 * MIB),
            file("Silver Horizon.mkv", 4096),
        ];
        let plan = plan_renames(&files, 0, "Silver Horizon");
        assert_eq!(plan[0].to, "Silver Horizon.1.mkv");
    }

    #[test]
    fn renames_stay_in_the_files_own_directory() {
        let files = [
            file("payload/Yb5drZSkNi20UCMkb.mkv", 900 * MIB),
            file("subs/Yb5drZSkNi20UCMkb.eng.srt", 40 * 1024),
        ];
        let plan = plan_renames(&files, 0, "Silver Horizon");
        assert_eq!(plan[0].to, "payload/Silver Horizon.mkv");
        assert_eq!(plan[1].to, "subs/Silver Horizon.eng.srt");
    }

    #[test]
    fn a_candidate_already_named_after_the_target_plans_nothing() {
        let files = [file("Silver Horizon.mkv", 900 * MIB)];
        assert!(plan_renames(&files, 0, "Silver Horizon").is_empty());
    }
}
