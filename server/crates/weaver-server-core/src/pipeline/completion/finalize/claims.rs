use super::*;
use crate::jobs::model::{TerminalDiscard, TerminalDiscardKind};
use crate::pipeline::TerminalReconciliation;
use crate::pipeline::completion::finalize::check::par2_damage_ignorable;

/// Below this share of its declared segments, a file has produced no delivery
/// evidence at all: what is on disk for it is a rounding error, not a short
/// delivery.
///
/// The distinction matters because the two are settled differently. A file the
/// job substantially delivered but that ended a few articles short is ordinary
/// Usenet damage and ships as it stands; a file that produced nothing and that
/// no settlement fact vouches for is a hole being handed over as a delivery,
/// and that is a failed job whatever the counters say.
const DELIVERY_EVIDENCE_PERCENT: u32 = 10;

/// Which settlement fact — if any — accounts for a payload file at the moment
/// the job would be delivered.
///
/// The census asks this of every file that counts toward health, and the whole
/// terminal record follows from the answers. Nothing here consults a filename
/// to decide *whether* a file is claimed: bindings resolve through the PAR2
/// description identity, direct sets through the plan's own file index. Names
/// appear only in what the operator is told afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum TerminalFileClaim {
    /// A settled recovery-set verdict verified or repaired this file.
    Par2Verdict,
    /// A finalized direct set routed this file's bytes into its output; the
    /// file itself was never written and never needed to be.
    InStreamProof,
    /// No parsed recovery set describes it. Whatever arrived is what ships,
    /// short articles and all.
    Unprotected,
    /// The settlement dropped it. Neither delivered nor missing: not part of
    /// the delivery at all.
    Discarded(TerminalDiscardKind),
    /// Nothing accounts for it. Either bytes that read complete without a
    /// verdict behind them, or a file that produced nothing and that no
    /// verdict, proof or discard ever spoke for.
    Unclaimed,
}

impl TerminalFileClaim {
    /// Whether this claim says the job handed over content for the file.
    ///
    /// A repair leftover counts: a join consumed it into an output the job
    /// delivered. An unfetchable-duplicate discard does not — that claim only
    /// says the bytes could never arrive, which is a statement about the wire,
    /// not about anything reaching the destination.
    fn delivers_content(self) -> bool {
        matches!(
            self,
            TerminalFileClaim::Par2Verdict
                | TerminalFileClaim::InStreamProof
                | TerminalFileClaim::Unprotected
                | TerminalFileClaim::Discarded(TerminalDiscardKind::RepairLeftover)
        )
    }
}

/// One file's row in the census.
struct ClaimedFile {
    file_id: NzbFileId,
    filename: String,
    /// The NZB's declaration, which is what health is measured against.
    declared_bytes: u64,
    claim: TerminalFileClaim,
    /// Declared bytes of this file's segments that reached a terminal state
    /// without arriving.
    terminal_failed_bytes: u64,
    /// Nothing accounts for this file and it produced no delivery evidence, so
    /// delivering the job would hand over a hole as though it were content.
    blocks_delivery: bool,
    /// Furniture by the settlement's own ignore list. Never blocks a delivery,
    /// and never counts as one either.
    is_furniture: bool,
}

impl Pipeline {
    /// Re-derive the terminal record from what claimed the job's files, rather
    /// than from the wire counters the download layer left behind.
    ///
    /// # The two records disagree, and the counters are the wrong one
    ///
    /// `failed_bytes` and health are live download telemetry: they say what the
    /// article layer could not fetch, and they are the truth while a job is
    /// downloading. They are not the truth about a *delivered* job, because
    /// everything that happens after the download — a PAR2 repair, an in-stream
    /// verification, a discard — answers those misses without touching them.
    /// Job 10206 delivered a clean repaired payload and archived health 91/1000
    /// with 1.17 GB failed, which was the exact size of a dead duplicate the
    /// settlement had already thrown away. Automation reads those fields to
    /// decide whether a download failed, so the record misreported a good
    /// delivery as a broken one.
    ///
    /// So at the last gate before the payload leaves, every file that counts
    /// toward health is matched to the settlement fact that claims it, and the
    /// record is rebuilt from the census:
    ///
    /// * claimed-delivered files contribute no failure — their wire misses were
    ///   answered;
    /// * discarded files leave both sides of the fraction, and say so in a
    ///   typed detail;
    /// * an unprotected file delivered short keeps its damage, so a job that
    ///   really is imperfect still reports as imperfect;
    /// * a file nothing claims is *not* forgiven — see below.
    ///
    /// # Both directions, because only one of them is safe to guess
    ///
    /// Forgiving a file no fact vouches for would turn every reconciliation
    /// defect of ours into a green job. Job 10220 is what that looks like: its
    /// PAR2 index was itself unfetchable, so no set described anything, the
    /// absent-set arm settled the job as verified, the protected-file count was
    /// zero because nothing was ever described — and a post that delivered
    /// 10 KB of a 1.2 GB payload archived as a success. So when no settlement
    /// fact delivered any content at all, a file that produced no delivery
    /// evidence and that nothing claims blocks the delivery outright — and a
    /// breaker discard stops counting as a discard, because with nothing else
    /// delivered it is the payload, not a surplus copy. When the job did
    /// deliver, the unclaimed and the undelivered keep their failure
    /// contributions in an honest record and are named in warnings; a delivered
    /// job with a hole beside it is a warning, not a failure.
    ///
    /// Pure fold over state the pipeline actor already holds: no I/O, no file
    /// reads, nothing that can block the single task this runs on.
    ///
    /// Returns the operator-facing failure message when the census refuses the
    /// delivery.
    pub(in crate::pipeline) fn reconcile_terminal_delivery(
        &mut self,
        job_id: JobId,
    ) -> Result<(), String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(());
        };
        let total_bytes = state.spec.total_bytes;
        let payload: Vec<NzbFileId> = state
            .assembly
            .files()
            .filter(|file| file.role().counts_toward_health())
            .map(|file| file.file_id())
            .collect();

        let mut census: Vec<ClaimedFile> = payload
            .into_iter()
            .filter_map(|file_id| self.census_row(job_id, file_id))
            .collect();

        // Whether any settlement fact handed content to the destination. The
        // question decides which way the ambiguous rows fall, in both
        // directions, so it is answered once over the whole census — and
        // furniture does not answer it: an `.nfo` that arrived is not a
        // delivery a payload can hide behind.
        //
        // With a delivery behind them, an unfetchable-duplicate discard is what
        // it says (job 10206: the repaired canonical ships, the collided
        // `.mkv.1` leaves the accounting), and a file nothing claims keeps its
        // failure in an honest sub-1000 record rather than refusing a job that
        // delivered — a whole payload file lost beside a delivered one is a
        // warning in both reference downloaders, not a failure.
        //
        // With no delivery anywhere, the same discard claim would launder the
        // entire payload out of the record: every file of a job whose
        // message-ids collided with a repost trips the breaker, and a census
        // that honored those discards would archive a green job that moved
        // nothing — job 10220's false success rebuilt through a newer door. A
        // breaker verdict proves the declared bytes can never arrive; when
        // nothing else delivered, that is not a surplus duplicate, it is the
        // payload, and the job has nothing to hand over.
        let delivered_any = census
            .iter()
            .any(|row| !row.is_furniture && row.claim.delivers_content());
        if delivered_any {
            for row in &mut census {
                row.blocks_delivery = false;
            }
        } else {
            for row in &mut census {
                if matches!(
                    row.claim,
                    TerminalFileClaim::Discarded(TerminalDiscardKind::UnfetchableDuplicate)
                ) {
                    row.claim = TerminalFileClaim::Unclaimed;
                    row.blocks_delivery = !row.is_furniture;
                }
            }
        }

        let blocking: Vec<&ClaimedFile> = census.iter().filter(|row| row.blocks_delivery).collect();
        if !blocking.is_empty() {
            let missing_bytes: u64 = blocking.iter().map(|row| row.declared_bytes).sum();
            let names = blocking
                .iter()
                .map(|row| row.filename.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            self.semantic_terminal_causes.insert(
                job_id,
                crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth,
            );
            return Err(format!(
                "{} payload file(s) were never delivered and no verification, repair or discard \
                 accounts for them ({missing_bytes} declared bytes): {names}",
                blocking.len()
            ));
        }

        let mut discarded_bytes = 0u64;
        let mut failed_bytes = 0u64;
        let mut discards = Vec::new();
        for row in &census {
            match row.claim {
                TerminalFileClaim::Discarded(kind) => {
                    discarded_bytes = discarded_bytes.saturating_add(row.declared_bytes);
                    discards.push(TerminalDiscard {
                        file_index: row.file_id.file_index,
                        filename: row.filename.clone(),
                        kind,
                        bytes: row.declared_bytes,
                    });
                }
                TerminalFileClaim::Par2Verdict | TerminalFileClaim::InStreamProof => {}
                TerminalFileClaim::Unprotected => {
                    failed_bytes = failed_bytes.saturating_add(row.terminal_failed_bytes);
                }
                TerminalFileClaim::Unclaimed => {
                    warn!(
                        job_id = job_id.0,
                        file_index = row.file_id.file_index,
                        filename = %row.filename,
                        terminal_failed_bytes = row.terminal_failed_bytes,
                        "no settlement fact claims this file; keeping its failure \
                         contribution in the terminal record"
                    );
                    failed_bytes = failed_bytes.saturating_add(row.terminal_failed_bytes);
                }
            }
        }

        // The denominator is the job as the NZB declared it, less what left the
        // delivery. Recovery volumes stay in it exactly as they always have —
        // health has never been measured against payload alone.
        let delivered_total = total_bytes.saturating_sub(discarded_bytes);
        let failed_bytes = failed_bytes.min(delivered_total);
        let health = if delivered_total == 0 {
            1000
        } else {
            health_milli(delivered_total, failed_bytes)
        };

        for discard in &discards {
            info!(
                job_id = job_id.0,
                file_index = discard.file_index,
                filename = %discard.filename,
                kind = discard.kind.as_str(),
                bytes = discard.bytes,
                "settlement discarded a file; its bytes leave the delivery accounting"
            );
        }

        self.terminal_reconciliations.insert(
            job_id,
            TerminalReconciliation {
                failed_bytes,
                health,
                discards,
            },
        );
        Ok(())
    }

    fn census_row(&self, job_id: JobId, file_id: NzbFileId) -> Option<ClaimedFile> {
        let state = self.jobs.get(&job_id)?;
        let file = state.assembly.file(file_id)?;
        let declared_bytes = file.total_bytes();
        let is_complete = file.is_complete();
        let total_segments = file.total_segments();
        let delivered_segments = total_segments.saturating_sub(file.missing_count());
        let filename = self
            .current_filename_for_file_id(job_id, file_id)
            .unwrap_or_else(|| file.filename().to_string());

        let has_delivery_evidence = total_segments == 0
            || delivered_segments.saturating_mul(100) >= total_segments * DELIVERY_EVIDENCE_PERCENT;
        let claim =
            self.classify_terminal_file_claim(job_id, file_id, is_complete, has_delivery_evidence);
        // Furniture never fails a job in either reference downloader, and it
        // must not fail one here either: a wholly missing `.nfo` is not a
        // reason to refuse the payload it describes.
        let is_furniture = par2_damage_ignorable(&filename, &self.par2_ignore_extensions());
        let blocks_delivery = matches!(claim, TerminalFileClaim::Unclaimed)
            && !is_complete
            && !has_delivery_evidence
            && !is_furniture;

        Some(ClaimedFile {
            file_id,
            filename,
            declared_bytes,
            claim,
            terminal_failed_bytes: self.file_terminal_failed_bytes(file_id),
            blocks_delivery,
            is_furniture,
        })
    }

    /// The settlement fact that accounts for one payload file.
    ///
    /// Discards are asked first: a file that left the delivery has no delivery
    /// question left to answer about it.
    pub(in crate::pipeline) fn classify_terminal_file_claim(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        is_complete: bool,
        has_delivery_evidence: bool,
    ) -> TerminalFileClaim {
        if self.par2_join_consumed_split_part(job_id, file_id) {
            return TerminalFileClaim::Discarded(TerminalDiscardKind::RepairLeftover);
        }
        if self.file_content_could_never_arrive(file_id) {
            return TerminalFileClaim::Discarded(TerminalDiscardKind::UnfetchableDuplicate);
        }
        if self.direct_set_delivered_file(file_id) {
            return TerminalFileClaim::InStreamProof;
        }

        let servable = self.par2_servable_set_ids(job_id);
        let binding = self
            .resolve_par2_file_binding(file_id)
            .filter(|binding| servable.contains(&binding.recovery_set_id));
        let Some(binding) = binding else {
            // Described by no parsed recovery set. That is ordinarily the
            // benign case — an unprotected file ships as it stands — but it is
            // also the shape a job takes when its recovery index was itself
            // unfetchable, and then "nothing describes it" is a statement about
            // our knowledge, not about the file. So the question asked here is
            // about delivery evidence rather than about description.
            //
            // Bytes that read complete while segments of theirs are terminally
            // lost were completed by *something*; if no verdict, proof or
            // discard says what, the reconciliation has a gap. And a file that
            // produced nothing at all has no delivery to reconcile.
            if !has_delivery_evidence && !is_complete {
                return TerminalFileClaim::Unclaimed;
            }
            if is_complete && self.file_terminal_failed_bytes(file_id) > 0 {
                return TerminalFileClaim::Unclaimed;
            }
            return TerminalFileClaim::Unprotected;
        };

        // A settled clean verdict *is* the claim, whether or not the article
        // bitmap agrees. That is the invariant the completion gate already
        // states outright: once a recovery set has verified or repaired an
        // output, missing-article state is diagnostic history and cannot
        // independently condemn the file. The one shape where a verdict vouches
        // for bytes that are nowhere on disk is refused before the delivery gate
        // is ever reached, so it cannot arrive here to be forgiven.
        let settled_clean = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(binding.recovery_set_id))
            .is_some_and(|set_runtime| set_runtime.settled && set_runtime.failure.is_none());
        if settled_clean {
            return TerminalFileClaim::Par2Verdict;
        }
        TerminalFileClaim::Unclaimed
    }

    /// Whether the breaker proved this file's declared bytes are not on any
    /// configured server.
    ///
    /// The only positive evidence for "could never arrive" the pipeline has:
    /// a run of refusals that agreed, article after article, that the servers
    /// hold one other coherent file under these message ids. Anything weaker —
    /// a file that merely failed a lot — is ordinary damage and stays counted.
    fn file_content_could_never_arrive(&self, file_id: NzbFileId) -> bool {
        self.foreign_layout_watches
            .get(&file_id)
            .is_some_and(|watch| watch.tripped)
    }

    /// Whether a finalized direct set already routed this file's bytes into its
    /// own output.
    fn direct_set_delivered_file(&self, file_id: NzbFileId) -> bool {
        self.direct_store
            .sets_for(file_id.job_id)
            .iter()
            .any(|set| {
                set.is_finalized()
                    && !set.is_demoted()
                    && set.plan().volume_for_file(file_id.file_index).is_some()
            })
    }

    /// The failed bytes and health the terminal record should carry, and the
    /// discards that explain them.
    ///
    /// Falls back to the live ledger for a job that never reached the census —
    /// a failed job, where the raw counters are the explanation and must be
    /// preserved exactly as the download layer left them.
    pub(in crate::pipeline) fn terminal_record_figures(
        &self,
        job_id: JobId,
        total_bytes: u64,
    ) -> (u64, u32, Vec<TerminalDiscard>) {
        if let Some(reconciliation) = self.terminal_reconciliations.get(&job_id) {
            return (
                reconciliation.failed_bytes,
                reconciliation.health,
                reconciliation.discards.clone(),
            );
        }
        let failed_bytes = self.settled_failed_bytes(job_id, total_bytes);
        (
            failed_bytes,
            health_milli(total_bytes, failed_bytes),
            Vec::new(),
        )
    }
}
