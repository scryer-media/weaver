use super::*;

/// Distinct segments that must refuse with one agreed foreign geometry before
/// the file is abandoned.
///
/// Twelve is a run no ordinary corruption produces: a damaged article
/// disagrees with the declared layout in whatever way its damage happened to
/// land, so twelve of them agreeing on the *same* other layout means the
/// servers really are holding one other coherent file under these message ids.
/// It is also small enough that the wire cost of finding out is a rounding
/// error against a file that would otherwise be fetched to the last article —
/// job 10220 spent roughly 1500 article fetches and a measurable throughput
/// dip proving what twelve had already said.
const FOREIGN_LAYOUT_TRIP_SEGMENTS: usize = 12;

const FOREIGN_LAYOUT_BREAKER_ENV: &str = "WEAVER_FOREIGN_LAYOUT_BREAKER";

/// Read the escape hatch once. Set to `0`/`false`/`off` to keep fetching a file
/// whatever its articles say they belong to.
fn foreign_layout_breaker_enabled_from_env() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        foreign_layout_breaker_enabled_from(
            std::env::var(FOREIGN_LAYOUT_BREAKER_ENV).ok().as_deref(),
        )
    })
}

fn foreign_layout_breaker_enabled_from(raw: Option<&str>) -> bool {
    let Some(value) = raw else {
        return true;
    };
    let normalized = value.trim().to_ascii_lowercase();
    !(normalized == "0" || normalized == "false" || normalized == "off")
}

/// Whether a refusal is evidence about *which file* the article belongs to, as
/// opposed to evidence that the article is broken.
///
/// Only disagreements with the NZB's declared envelope qualify. An article
/// whose own `=ypart` range contradicts its own decoded length, or that decoded
/// more bytes than it declared, is malformed on its own terms and says nothing
/// about anyone's layout — that shape is corruption, and corruption is what the
/// ordinary retry path is for.
fn refusal_is_declared_envelope_disagreement(mismatch: YencLayoutMismatch) -> bool {
    matches!(
        mismatch,
        YencLayoutMismatch::Total
            | YencLayoutMismatch::Part
            | YencLayoutMismatch::BeginAboveDeclaredPrefix
            | YencLayoutMismatch::EndAboveDeclaredFileSize
            | YencLayoutMismatch::FileSizeAboveDeclared
    )
}

/// Whether the disagreement is about part geometry rather than about the
/// `=ybegin size=` header alone.
fn refusal_disagrees_on_part_geometry(mismatch: YencLayoutMismatch) -> bool {
    matches!(
        mismatch,
        YencLayoutMismatch::Total
            | YencLayoutMismatch::Part
            | YencLayoutMismatch::BeginAboveDeclaredPrefix
            | YencLayoutMismatch::EndAboveDeclaredFileSize
    )
}

impl Pipeline {
    fn foreign_layout_breaker_enabled(&self) -> bool {
        #[cfg(test)]
        if let Some(enabled) = self.foreign_layout_breaker_override {
            return enabled;
        }
        foreign_layout_breaker_enabled_from_env()
    }

    /// A segment of this file decoded into the declared layout, so the declared
    /// file demonstrably exists on the wire.
    ///
    /// Permanent, and deliberately so: one arriving article outranks any amount
    /// of later foreign evidence, because a file that has delivered even one of
    /// its own segments is not a file nobody has.
    pub(in crate::pipeline) fn disarm_foreign_layout_watch(&mut self, file_id: NzbFileId) {
        if let Some(watch) = self.foreign_layout_watches.get_mut(&file_id) {
            watch.disarmed = true;
            watch.segments.clear();
        }
    }

    /// Record one yEnc layout refusal, and retire the file when the refusals
    /// have agreed often enough about what the servers are actually serving.
    ///
    /// # What the run of refusals means
    ///
    /// A message-id collision with a repost is not damage and cannot be
    /// retried out of: every server answers with an article that really does
    /// belong to some other, coherent file, so every attempt refuses in exactly
    /// the same way and the file the NZB declared never arrives. Job 10220's
    /// duplicate declared 1486 parts and was served 1525, article after
    /// article, for its entire fetch.
    ///
    /// The signature is therefore the served `(file_size, total)` pair, which
    /// is constant across a foreign file's articles — `begin`/`end` differ per
    /// segment by construction and cannot be part of it. A run that disagrees
    /// with itself is ordinary corruption of a real file and keeps fetching;
    /// only a run that agrees, and that disagrees on part geometry rather than
    /// on a size header real posters misstate, is allowed to retire anything.
    ///
    /// # What retiring costs
    ///
    /// Nothing that could have arrived. The remaining *undispatched* segments
    /// are moved to a terminal state through the same transition every wire
    /// outcome uses, so the ledger counts them once and settlement prices the
    /// outcome; articles already on the wire are left alone to resolve
    /// normally. There is no rollback machinery because there is nothing to
    /// roll back to: the consistent-foreign evidence says the declared file is
    /// on no configured server.
    pub(in crate::pipeline) fn note_yenc_layout_refusal(
        &mut self,
        segment_id: SegmentId,
        mismatch: YencLayoutMismatch,
        served: YencLayoutAssertions,
    ) {
        if !self.foreign_layout_breaker_enabled()
            || !refusal_is_declared_envelope_disagreement(mismatch)
        {
            return;
        }
        let file_id = segment_id.file_id;
        let geometry = ForeignYencGeometry {
            served_total: served.total,
            served_file_size: served.file_size,
        };

        let watch = self
            .foreign_layout_watches
            .entry(file_id)
            .or_insert_with(|| ForeignLayoutWatch {
                geometry,
                segments: HashSet::new(),
                disarmed: false,
                tripped: false,
                geometry_disagreed: false,
            });
        if watch.disarmed || watch.tripped {
            return;
        }
        if watch.geometry != geometry {
            // This refusal belongs to a different story than the run so far,
            // which is what corruption looks like. Start counting from it.
            watch.geometry = geometry;
            watch.segments.clear();
            watch.geometry_disagreed = false;
        }
        watch.segments.insert(segment_id.segment_number);
        watch.geometry_disagreed |= refusal_disagrees_on_part_geometry(mismatch);
        if !watch.geometry_disagreed || watch.segments.len() < FOREIGN_LAYOUT_TRIP_SEGMENTS {
            return;
        }
        let segment_count = watch.segments.len();

        // A file that has delivered even one of its own segments is a file the
        // servers demonstrably have. Checked against the assembly rather than
        // against `disarmed` alone so a segment that landed before the watch
        // existed counts too.
        let Some(file) = self
            .jobs
            .get(&file_id.job_id)
            .and_then(|state| state.assembly.file(file_id))
        else {
            return;
        };
        if file.missing_count() != file.total_segments() {
            self.disarm_foreign_layout_watch(file_id);
            return;
        }
        let declared_total = file.total_segments();
        let declared_file_size = file.total_bytes();
        let filename = file.filename().to_string();

        if let Some(watch) = self.foreign_layout_watches.get_mut(&file_id) {
            watch.tripped = true;
        }
        let retired = self.retire_undispatched_segments(file_id);
        info!(
            job_id = file_id.job_id.0,
            file_index = file_id.file_index,
            filename = %filename,
            declared_total,
            declared_file_size,
            served_total = ?geometry.served_total,
            served_file_size = geometry.served_file_size,
            refused_segments = segment_count,
            retired_segments = retired,
            "articles for this file consistently belong to a different post; abandoning its remaining segments"
        );
    }

    /// Move every segment of this file that is still waiting in a queue into the
    /// foreign-layout terminal state, and return how many moved.
    ///
    /// Queued work only. Articles in flight, and retries already scheduled,
    /// resolve through their own paths; the transition is idempotent, so when
    /// they land as failures they book nothing twice.
    fn retire_undispatched_segments(&mut self, file_id: NzbFileId) -> usize {
        let undispatched: Vec<SegmentId> = {
            let Some(state) = self.jobs.get_mut(&file_id.job_id) else {
                return 0;
            };
            let mut works = state
                .download_queue
                .extract_matching(|work| work.segment_id.file_id == file_id);
            works.extend(
                state
                    .recovery_queue
                    .extract_matching(|work| work.segment_id.file_id == file_id),
            );
            let held = std::mem::take(&mut state.held_segments);
            let (mine, others): (Vec<_>, Vec<_>) = held
                .into_iter()
                .partition(|work| work.segment_id.file_id == file_id);
            state.held_segments = others;
            works.extend(mine);
            works.into_iter().map(|work| work.segment_id).collect()
        };
        let mut retired = 0;
        for segment_id in undispatched {
            if self.book_terminal_segment(segment_id, SegmentTerminalState::ForeignLayout) {
                retired += 1;
            }
        }
        self.update_queue_metrics();
        retired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_breaker_is_armed_unless_the_escape_hatch_says_otherwise() {
        assert!(foreign_layout_breaker_enabled_from(None));
        assert!(foreign_layout_breaker_enabled_from(Some("1")));
        assert!(foreign_layout_breaker_enabled_from(Some("")));
        assert!(!foreign_layout_breaker_enabled_from(Some("0")));
        assert!(!foreign_layout_breaker_enabled_from(Some(" OFF ")));
        assert!(!foreign_layout_breaker_enabled_from(Some("false")));
    }

    #[test]
    fn only_declared_envelope_disagreements_are_evidence_about_identity() {
        for mismatch in [
            YencLayoutMismatch::Total,
            YencLayoutMismatch::Part,
            YencLayoutMismatch::BeginAboveDeclaredPrefix,
            YencLayoutMismatch::EndAboveDeclaredFileSize,
            YencLayoutMismatch::FileSizeAboveDeclared,
        ] {
            assert!(refusal_is_declared_envelope_disagreement(mismatch));
        }
        for mismatch in [
            YencLayoutMismatch::RangeContradictsDecode,
            YencLayoutMismatch::DecodedSizeAboveDeclared,
            YencLayoutMismatch::PartialRange,
        ] {
            assert!(!refusal_is_declared_envelope_disagreement(mismatch));
        }
        assert!(
            !refusal_disagrees_on_part_geometry(YencLayoutMismatch::FileSizeAboveDeclared),
            "a misstated size header corroborates a geometry disagreement, never triggers one"
        );
    }
}
