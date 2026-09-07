//! Continuation of the `impl DirectSetRouter` block from `direct_store/router.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl DirectSetRouter {
    /// Routes one decoded source span.
    ///
    /// The returned spans must **all** be written before the caller records the
    /// article as placed; a span that is not written is a coverage hole, not a
    /// silent loss, because the barrier is only told about writes that returned.
    pub(crate) fn route(
        &mut self,
        volume_index: u32,
        source_offset: u64,
        data: &[u8],
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if let Some(reason) = self.demoted {
            return Err(reason);
        }
        if !self.plan.volumes.contains_key(&volume_index) {
            return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
        }

        let staging = self.staging.entry(volume_index).or_default();
        staging.stage(source_offset, data);

        self.try_parse_volume(volume_index)?;
        // Every volume, not just this one: a header landing here is exactly what
        // resolves a *later* volume's split-continuation offset, so its holds
        // become routable in the same call.
        let volumes: Vec<u32> = self.staging.keys().copied().collect();
        let mut spans = self.take_migrated_spans();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }

        // A breach — of this set's budget, or of the process-wide limit every
        // set shares — pages rather than demoting. Demotion is what is left
        // when paging itself fails — a scratch I/O error, or a ceiling.
        if self.holds_over_budget()
            && let Err(reason) = self.page_holds_to_scratch()
        {
            return Err(self.fail(reason));
        }
        Ok(spans)
    }

    /// Re-enters the router with a span a PAR2 repair rebuilt.
    ///
    /// A repaired span is late-arriving article data with one difference that
    /// changes everything downstream: the bytes it carries are **not** the bytes
    /// already on disk for that range. So it takes the same path as an article —
    /// stage, parse, drain, one span per intersecting destination — through
    /// [`VolumeStaging::stage_repaired`], which force-stages the range and marks
    /// it so the drain overwrites the composition instead of clipping it as a
    /// duplicate.
    ///
    /// Two jobs, and the second is easy to overlook. The obvious one is the
    /// bytes: destination writes must land at the mapped offsets, or the member
    /// on disk stays damaged. The other is the **parse**: the lost articles that
    /// made the volume damaged may also have carried the header the walk stopped
    /// at, so feeding the repaired bytes back is what lets the walk resume — a
    /// repaired tail holding the end-of-archive record confirms a volume that
    /// could not otherwise be confirmed, and the set finishes instead of
    /// demoting.
    ///
    /// The returned spans must all be written before the caller records them,
    /// exactly as for [`Self::route`].
    /// Takes **all** of one volume's repaired spans at once, deliberately.
    /// Staging them one at a time would let the classification frontier hold an
    /// early span — its bytes sit at or past the header walk's tail, so they
    /// could still be an undiscovered member's payload — until a *later* span
    /// carrying the end record confirmed the volume. That is a real ordering,
    /// not a hypothetical: the article a set loses is often the last one, and it
    /// carries both a member's tail and the record that closes the archive.
    ///
    /// # `whole_volume`
    ///
    /// The caller states that these chunks are the volume's **entire** posted
    /// image — the shape a repair takes when the router has no facts for the
    /// volume at all, because every one of its articles failed and PAR2 rebuilt
    /// it from recovery. The staged image is then byte-complete, so the parse
    /// over it is exactly as authoritative as one over a fully downloaded
    /// volume and the volume may be confirmed from it.
    ///
    /// It has to be said rather than inferred, and it must never be said
    /// loosely. Confirmation is what lets the drain file the trailing region
    /// into the envelope, and doing that over a *truncated* image files an
    /// undiscovered member's header and payload as scratch that finalization
    /// deletes. Without it a wholly absent **last** volume — the one volume
    /// whose end record carries no `more_volumes` flag to confirm it — would
    /// hold its trailing region, leave repaired bytes with nowhere to go, and
    /// demote the set under [`DemotionReason::RepairRerouteFailed`] after the
    /// recovery had already been downloaded.
    pub(crate) fn route_repaired(
        &mut self,
        volume_index: u32,
        chunks: &[RepairedChunk],
        lead_in: &[(u32, u64, std::sync::Arc<[u8]>)],
        whole_volume: bool,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if let Some(reason) = self.demoted {
            return Err(reason);
        }
        if !self.plan.volumes.contains_key(&volume_index) {
            return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
        }
        let mut staged = false;
        {
            let staging = self.staging.entry(volume_index).or_default();
            for (source_offset, data) in chunks {
                if data.is_empty() {
                    continue;
                }
                staging.stage_repaired(*source_offset, std::sync::Arc::clone(data));
                staged = true;
            }
        }
        // Posted bytes an encrypted member's drain needs beside the
        // repaired span: the ones just below it, so the block its first byte
        // lands in and that block's CBC predecessor can be assembled, and the
        // ≤46 in a *neighbouring volume* that complete an edge block of a member
        // extent, and its predecessor ([`Self::cipher_edge_reads`]). Marked
        // **unrepaired** on purpose:
        // they did not change, so they must not overwrite a composition, and the
        // "every repaired byte finds a destination" rule below must not answer
        // for them.
        for (volume, source_offset, data) in lead_in {
            if data.is_empty() {
                continue;
            }
            self.staging
                .entry(*volume)
                .or_default()
                .stage_lead_in(*source_offset, std::sync::Arc::clone(data));
        }
        if !staged {
            return Ok(Vec::new());
        }

        // Set **before** the parse, exactly as [`Self::note_volume_complete`]
        // sets it before its own: it is what licenses the parse about to run to
        // be the confirming one. Only ever true for a rewrite that carries the
        // whole volume, so the image the parse walks holds every byte the
        // volume will ever have.
        if whole_volume && let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.source_complete = true;
        }
        // The repair has had its say about this volume, so the question the
        // damaged fact stood for is closed. Cleared *before* the drain, because
        // the drain re-runs the part gates over the repaired bytes: they either
        // pass — and the member's whole-member gate, no longer stalled,
        // verifies — or they fail again, and `repair_rerouted` below makes that
        // second failure the demotion it now is.
        self.repair_rerouted = true;
        self.damaged_volumes.remove(&volume_index);
        self.try_parse_volume(volume_index)?;
        // The repaired volume drains **first**, and only then does every staged
        // volume drain in the usual ascending order.
        //
        // A low-edge lead-in is staged in the neighbour *below* the repaired
        // volume, which in ascending order drains before it — and that drain
        // routes the straddling block's bytes away as an ordinary duplicate, so
        // the volume they were read for finds them gone and holds its first
        // block. Priming with the repaired volume costs one extra drain of a
        // volume that is about to be drained anyway and takes nothing away from
        // the ascending pass, which is still what lets a header in one volume
        // release another's holds.
        let volumes: Vec<u32> = std::iter::once(volume_index)
            .chain(self.staging.keys().copied())
            .collect();
        let mut spans = self.take_migrated_spans();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }

        // Every repaired byte must have found a destination. Unlike an ordinary
        // article — whose bytes may legitimately be held above the
        // classification frontier until a later header proves what they are —
        // a repair runs after the volume has finished downloading and been
        // parsed, so a byte with nowhere to go means the layout in front of us
        // cannot place bytes it previously placed. That is a demotion, not a
        // hold: leaving it staged would sit on a repaired byte the member is
        // waiting for, forever.
        if self
            .staging
            .get(&volume_index)
            .is_some_and(|staging| !staging.repaired.is_empty())
        {
            return Err(self.fail(DemotionReason::RepairRerouteFailed));
        }

        if self.holds_over_budget()
            && let Err(reason) = self.page_holds_to_scratch()
        {
            return Err(self.fail(reason));
        }
        Ok(spans)
    }

    /// The volume's source bytes are all accounted for. Runs the confirming
    /// header parse, re-checks the chain-close eligibility rule, and drains
    /// whatever confirmation just made routable.
    ///
    /// The drain is not incidental: a volume's trailing region — trailing
    /// headers, the end-of-archive record, a recovery record — is held until the
    /// volume is confirmed, because until then an undiscovered member could live
    /// there. For the *last* volume of a set confirmation only ever arrives
    /// here, so without this call those bytes would be held for the life of the
    /// set and never reach the envelope.
    pub(crate) fn note_volume_complete(
        &mut self,
        volume_index: u32,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        if let Some(reason) = self.demoted {
            return Err(reason);
        }
        // Set before the parse, not after: this is what licenses the parse
        // about to run to be the *confirming* one.
        self.staging
            .entry(volume_index)
            .or_default()
            .source_complete = true;
        self.try_parse_volume(volume_index)?;
        // A restored volume the parse above could not confirm will never be
        // confirmed by a parse of the **staged** image: its pre-restart bytes
        // are on disk rather than in `chunks`, so that image has a hole from
        // offset zero and every later attempt fails the same silent way.
        //
        // Leaving it unconfirmed is the failure that hides. The trailing region
        // — the end-of-archive record, and a `-rr` set's whole recovery record —
        // stays held for the life of the set, so the envelope never receives it,
        // so the virtual volume reads short, so PAR2 calls a byte-perfect set
        // damaged and the job pays a full redownload. So: the cheap structural
        // proof first, then the expensive arm that rebuilds the image the parse
        // needs out of the envelope, and only then the demotion — named,
        // counted, and while its routed bytes can still materialize the volumes.
        if self
            .staging
            .get(&volume_index)
            .is_some_and(|staging| staging.restored && !staging.confirmed)
        {
            if !self.restored_volume_completes_confirmed(volume_index)
                && !self.reconfirm_restored_volume(volume_index)?
            {
                return Err(self.fail(DemotionReason::UnconfirmedRestoredVolume));
            }
            if let Some(staging) = self.staging.get_mut(&volume_index) {
                staging.confirmed = true;
            }
        }
        self.check_eligibility()?;
        let volumes: Vec<u32> = self.staging.keys().copied().collect();
        let mut spans = self.take_migrated_spans();
        for volume in volumes {
            spans.extend(self.drain_volume(volume)?);
        }
        Ok(spans)
    }

    /// The expensive arm of the confirming parse: re-parse a restored volume's
    /// headers out of its **envelope file**, so the confirming walk can
    /// actually run.
    ///
    /// [`restored_volume_completes_confirmed`](Self::restored_volume_completes_confirmed)
    /// covers the volume whose last member splits into the next one — the middle
    /// of a set — by a structural argument. It deliberately cannot cover the
    /// volume that *closes* a chain, which is every set's **last** volume: a
    /// second member's header can sit past the first member's data area, and no
    /// argument short of a walk rules that out. Those demoted.
    ///
    /// They no longer have to. The envelope is a sparse image of the volume
    /// holding every non-member byte at its true physical offset — which is
    /// exactly the header region, because a header is a non-member byte by
    /// definition. Overlaying this run's staged bytes on it reconstitutes the
    /// reader the live path parses through, and the ordinary confirming parse
    /// runs over that.
    ///
    /// # It produces the confirmation proof, it does not bypass it
    ///
    /// The rule is that a volume is `confirmed` only on a parsed end block or a
    /// byte-complete image. `source_complete` alone cannot be that proof here:
    /// the envelope-backed image has holes exactly where member data was routed
    /// away, and a hole the walk needs to *read* stops it silently — at which
    /// point "every article arrived" would confirm a volume whose walk ended in
    /// the middle. So two things are checked instead:
    ///
    /// 1. **Every byte of the volume is accounted for** (condition 1 of the
    ///    structural proof, reused): routed plus staged is one run from zero, so
    ///    the volume's extent is known and nothing is outstanding.
    /// 2. **The walk could reach that extent.** The image's own runs, plus the
    ///    data areas the parsed headers declare — the regions a header walk
    ///    *seeks over* rather than reads — must tile the volume contiguously
    ///    from zero. If they do, a walk that stopped early is impossible: every
    ///    byte above the stopping point was either readable (so it would have
    ///    continued) or inside a declared data area (so it would have skipped
    ///    it). If they do not, the image had a hole the walk needed, and this
    ///    returns `false` rather than confirming on a truncated answer.
    ///
    /// Every other failure — no envelope file, a short or unreadable one, a
    /// parse that fails, facts that disagree with the cached ones — is `false`
    /// or a demotion in its own right, which leaves the caller's behaviour
    /// exactly as it was.
    pub(super) fn reconfirm_restored_volume(
        &mut self,
        volume_index: u32,
    ) -> Result<bool, DemotionReason> {
        let Some(staging) = self.staging.get(&volume_index) else {
            return Ok(false);
        };
        let mut accounted = ByteRanges::new();
        for &(start, end) in staging.routed.ranges() {
            accounted.insert(start, end - start);
        }
        for &(start, end) in staging.pending.ranges() {
            accounted.insert(start, end - start);
        }
        let [(0, volume_end)] = accounted.ranges() else {
            return Ok(false);
        };
        let volume_end = *volume_end;

        let Some(image) = self.volume_image(volume_index, VolumeImage::Envelope) else {
            return Ok(false);
        };
        let facts = match unrar_rs::RarArchive::parse_volume_facts_with_shared_kdf_cache(
            image,
            self.header_password(),
            std::sync::Arc::clone(&self.kdf_cache),
        ) {
            Ok(facts) => facts,
            // A restored `-hp` set. The archive key is never
            // persisted, so this run has to prove one of the job's candidates
            // again before it can read a header — and this is the *first* place
            // it can, because a restored volume's staged image has a hole from
            // offset zero and `try_parse_volume` never reaches the record
            // through it. The envelope holds the headers, so it does.
            Err(unrar_rs::RarError::EncryptedArchive)
                if self.key_header_encrypted_volume(volume_index, VolumeImage::Envelope)? =>
            {
                let Some(image) = self.volume_image(volume_index, VolumeImage::Envelope) else {
                    return Ok(false);
                };
                match unrar_rs::RarArchive::parse_volume_facts_with_shared_kdf_cache(
                    image,
                    self.header_password(),
                    std::sync::Arc::clone(&self.kdf_cache),
                ) {
                    Ok(facts) => facts,
                    Err(_) => return Ok(false),
                }
            }
            Err(_) => return Ok(false),
        };
        if facts.members.is_empty() || !self.walk_covered_volume(volume_index, &facts, volume_end) {
            return Ok(false);
        }

        // `reached_end` is `true` on the strength of the tiling check above:
        // this walk saw every byte it could have needed, which is the
        // byte-complete image. A disagreement with the cached facts is still a
        // demotion, and a member the walk found in the tail is still adopted —
        // both through the one seam the live path uses.
        self.accept_volume_facts(volume_index, facts, true, VolumeImage::Envelope)?;
        Ok(true)
    }

    /// Whether the header walk could have reached `volume_end`: every byte below
    /// it is either one the image can serve or one inside a data area the parsed
    /// headers declare, and the two together leave no gap.
    pub(super) fn walk_covered_volume(
        &self,
        volume_index: u32,
        facts: &RarVolumeFacts,
        volume_end: u64,
    ) -> bool {
        let mut reachable = self.envelope_backed_ranges(volume_index);
        if let Some(staging) = self.staging.get(&volume_index) {
            for (offset, chunk) in &staging.chunks {
                reachable.insert(*offset, chunk.len());
            }
        }
        for member in &facts.members {
            reachable.insert(member.data_offset, member.data_size);
        }
        for service in &facts.services {
            reachable.insert(service.data_offset, service.data_size);
        }
        reachable.contiguous_from_zero() >= volume_end
    }

    /// Whether a **restored** volume that has just finished downloading may be
    /// confirmed without the parse it can no longer run.
    ///
    /// Two conditions, both necessary:
    ///
    /// 1. **Every byte of the volume is accounted for, with no gap** — the
    ///    checkpoint's restored ranges and this run's staged holds together form
    ///    one run from offset zero. The caller has already established that no
    ///    further article is coming, so a single run from zero *is* coverage to
    ///    the volume's decoded length; it is expressed as contiguity rather than
    ///    compared against a number because the assembly's `received_bytes` for a
    ///    restored volume is the spec's yEnc-**encoded** total, ~3% too large.
    ///    This is the same fact `source_complete` states in the live path:
    ///    nothing of this volume is still outstanding.
    /// 2. **The volume's last known member continues into the next volume**
    ///    (`split_after`). A split member is by construction the last *file* in
    ///    its volume — that is what splitting means: the volume filled up — so the
    ///    unproven region above `tail_base` can only hold service data (a `-rr`
    ///    recovery record) and the end-of-archive record, which are envelope
    ///    content by definition. No undiscovered member can live there, which is
    ///    the one thing the confirming parse was there to rule out.
    ///
    /// Condition 2 is what keeps this honest, and why the volume that *closes* a
    /// chain — the last of a set, or one whose member ends inside it — is not
    /// confirmed this way: a second member's header can sit past the first's data
    /// area, which is exactly the shape `payload_past_the_last_known_header…`
    /// pins. Those go to [`Self::reconfirm_restored_volume`], which pays for a
    /// real walk instead of arguing from the format, and demote only if that
    /// walk cannot be run or cannot be trusted.
    pub(super) fn restored_volume_completes_confirmed(&self, volume_index: u32) -> bool {
        let Some(staging) = self.staging.get(&volume_index) else {
            return false;
        };
        let mut held = ByteRanges::new();
        for &(start, end) in staging.routed.ranges() {
            held.insert(start, end - start);
        }
        for &(start, end) in staging.pending.ranges() {
            held.insert(start, end - start);
        }
        if !matches!(held.ranges(), [(0, _)]) {
            return false;
        }
        self.volume_facts
            .get(&volume_index)
            .and_then(|facts| facts.members.last())
            .is_some_and(|member| member.split_after)
    }

    pub(super) fn fail(&mut self, reason: DemotionReason) -> DemotionReason {
        self.demoted.get_or_insert(reason);
        reason
    }

    /// Parses the volume's headers out of its staged image, provisionally the
    /// first time and confirmingly once the walk reaches the archive end.
    ///
    /// # Quick Open is dropped, not implemented
    ///
    /// An earlier revision allowed RAR5 Quick Open records to *prime* the
    /// layout, under one hard condition: no byte routes on QO evidence alone,
    /// so the corresponding physical header must be parsed and confirmed
    /// identical first. It also said, in the same breath, that if the
    /// confirmation erases the benefit the feature should be deleted rather
    /// than weakened. It does, and it is:
    ///
    /// - **The fetch saving QO exists for is already banked.** QO's purpose is
    ///   avoiding a seek-and-read walk across a large archive. This router never
    ///   walks an archive: each volume's mapping comes from *that volume's own*
    ///   headers, parsed out of the prefix its first article delivers during
    ///   ordinary download. There is no extra fetch for QO to save, because
    ///   there is no extra fetch.
    /// - **Confirmation would cost strictly more than it saves.** The physical
    ///   headers must be parsed anyway to admit the volume; priming from QO
    ///   first would add a second parse and a field-by-field comparison to reach
    ///   the same mapping.
    /// - **QO records live at the end of the archive**, past every member's
    ///   payload, so on a set that is still downloading they are the *last*
    ///   thing to arrive. Priming from them would resolve mappings after the
    ///   bytes they describe, which is the wrong end of the job.
    ///
    /// So there is no QO code here and none is wanted. What there *was* — and
    /// this is the part a future reader must not mistake for QO being absent —
    /// is the library's own preference: `parse_volume_facts` calls
    /// `parse_all_headers`, which on seeing a main header carrying a locator
    /// Quick Open offset tries the QO records first and returns **those**
    /// headers when they parse cleanly through an end-of-archive record. On a
    /// truncated prefix that read hits a hole and falls back to the physical
    /// walk, so a provisional parse is always physical; a *confirming* parse of
    /// a fully staged `-qo` volume could be QO-derived.
    ///
    /// [`Self::refuse_quick_open_derived_facts`] closes that: every parse whose
    /// facts could have come from the cache is now checked against the physical
    /// walk before a single member reaches the layout.
    pub(super) fn try_parse_volume(&mut self, volume_index: u32) -> Result<(), DemotionReason> {
        let Some(staging) = self.staging.get(&volume_index) else {
            return Ok(());
        };
        if staging.confirmed {
            return Ok(());
        }
        // Nothing can be parsed until the volume's own prefix is staged from
        // zero: the signature lives there.
        if staging.pending.contiguous_from_zero() == 0 && staging.routed.contiguous_from_zero() == 0
        {
            return Ok(());
        }
        // The previous walk said which offset it ran out at. Until the image
        // reaches that offset this walk would stop in the same place, having
        // read the same headers and — on a `-hp` volume — derived the same
        // archive key again. See [`VolumeStaging::parse_short_at`].
        //
        // Never on the volume's **last** article, whatever the gate says: a
        // complete source image is one of the two proofs that confirm a volume
        // (`reached_end` below), so the walk over it has to run even when it
        // will stop exactly where the previous one did. Holding it back would
        // leave the volume unconfirmed with no further article to reopen it,
        // and its trailing region held for the life of the set.
        if let Some(short_at) = staging.parse_short_at
            && !staging.source_complete
            && !staging.parse_can_reach(short_at)
        {
            return Ok(());
        }
        let image = SparseImage::from_staged(&staging.chunks, self.scratch.handle());
        #[cfg(test)]
        {
            self.parse_walks = self.parse_walks.saturating_add(1);
        }
        let walk = match unrar_rs::RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
            image,
            self.header_password(),
            std::sync::Arc::clone(&self.kdf_cache),
        ) {
            Ok(walk) => walk,
            // The one parse failure that is a *fact* rather than a shortage of
            // bytes: the volume's headers are encrypted, so the
            // layout is withheld until a key exists. Every other failure is a
            // prefix too short to hold a whole header, which is normal early on
            // and which the next article retries; a genuinely unparsable volume
            // is caught by the prefix ceiling above.
            Err(unrar_rs::RarError::EncryptedArchive) => {
                return if self.key_header_encrypted_volume(volume_index, VolumeImage::Staged)? {
                    self.try_parse_volume(volume_index)
                } else {
                    Ok(())
                };
            }
            // The archive's own type-4 record states key material this build
            // will not derive from. Named here for the same reason: it is a
            // property of the archive that no amount of further staging changes.
            Err(
                unrar_rs::RarError::UnsupportedEncryption { .. }
                | unrar_rs::RarError::UnsupportedEncryptionKdf { .. },
            ) => {
                return Err(self.refuse_header_encrypted(HeaderCryptRefusal::Unkeyable));
            }
            Err(_) => return self.judge_unparsed_prefix(volume_index),
        };
        let facts = walk.facts;
        // Recorded from the walk that just ran, over the image it just read:
        // the gate above is only ever as old as the last answer.
        if let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.parse_short_at = walk.short_at;
        }
        if facts.members.is_empty() {
            return self.judge_unparsed_prefix(volume_index);
        }
        // Two proofs, and only two. `more_volumes` can only be true when the
        // library parsed an end-of-archive record, which is the last header a
        // volume can carry; a complete source image is the parse having seen
        // every byte there will ever be. Chain closure (`!split_after`) is
        // **not** a third: a truncated prefix closes the chain the moment the
        // first member's final part is read, while a second member's header
        // sits unread past that member's data area.
        let source_complete = self
            .staging
            .get(&volume_index)
            .is_some_and(|staging| staging.source_complete);
        let reached_end = facts.more_volumes || source_complete;
        self.accept_volume_facts(volume_index, facts, reached_end, VolumeImage::Staged)
    }

    /// A parse attempt over the volume's current image produced no member —
    /// decide whether that is still patience or already a verdict.
    ///
    /// Producing nothing is normal early on: the staged prefix may simply not
    /// reach the first file header yet, and the next article retries. It stops
    /// being normal once the walk has been *shown*
    /// [`MAX_HEADER_PREFIX_BYTES`] of genuine prefix — contiguous coverage
    /// from offset zero, the only bytes the walk can consume — and still found
    /// nothing; real RAR headers live in the first few hundred bytes, so such
    /// a volume never will.
    ///
    /// Two properties of that sentence carry the correctness, and both were
    /// learned from a live failure:
    ///
    /// - **Judged on the zero-prefix, never on total staged bytes.** A
    ///   12-connection download delivers articles in completion order, so
    ///   several mid-file articles routinely stage before the one carrying
    ///   offset zero. Their sum says nothing about whether headers parse:
    ///   judging it demoted store-method sets `unparsable_volume` — 23 of 44
    ///   demotions in one functional-direct run — with their headers unread.
    /// - **Judged only after a parse attempt over that prefix, never before
    ///   one.** The moment the zero article completes a previously tail-only
    ///   volume, the whole volume is contiguous and the prefix legitimately
    ///   dwarfs the ceiling; a pre-parse check reads that as unparsable one
    ///   line before the parse that would have adopted the set.
    pub(super) fn judge_unparsed_prefix(
        &mut self,
        volume_index: u32,
    ) -> Result<(), DemotionReason> {
        let over_ceiling = self.staging.get(&volume_index).is_some_and(|staging| {
            !staging.provisional && staging.parse_prefix_len() > MAX_HEADER_PREFIX_BYTES
        });
        if over_ceiling {
            return Err(self.fail(DemotionReason::UnparsableVolume));
        }
        Ok(())
    }

    /// Keys a header-encrypted (`-hp`) set, or refuses it by name.
    ///
    /// Reached from the two places a volume's headers are parsed, on the parse
    /// coming back `EncryptedArchive`. `Ok(true)` means a password is now
    /// available and the caller should re-run its parse; `Ok(false)` means the
    /// image has not reached the record yet and the next article will retry.
    /// Three steps, and the middle one is the whole phase:
    ///
    /// 1. **Read the keying facts, which `-hp` does not hide.** RAR5's type-4
    ///    record is plaintext and sits at the front of the volume, exactly where
    ///    the header walk already reads — `parse_volume_header_encryption`
    ///    returns it with no password. RAR4 has no such record and answers
    ///    `Rar4`.
    /// 2. **Prove a candidate against the archive's own check, or refuse.** Not
    ///    "try one and see": see [`HeaderCryptRefusal::Unverifiable`] for why
    ///    `-hp` demands `Verified` where `-p` accepts `Unverifiable`.
    /// 3. **Adopt it for the whole set.** RAR uses **one** password for header
    ///    and file data alike, so the proved candidate is also the file key, and
    ///    binding it into [`Self::crypt`] here is what stops a set whose spec
    ///    carries a *different* candidate from opening its headers and then
    ///    refusing its members.
    ///
    /// From there the set is an ordinary encrypted set: nothing downstream is
    /// `-hp`-shaped, and [`Self::header_password`] keeps every later parse of
    /// every volume keyed.
    ///
    /// # Why the bytes are still here to re-parse
    ///
    /// Nothing is discarded while unkeyed. The volume's articles stage exactly
    /// as they do for any volume whose prefix has not yet yielded a header —
    /// holds in RAM, paged to the holds scratch under budget pressure — and the
    /// retry reads the same staged image. The only thing the
    /// named refusal changes is *when* the set stops waiting: a named refusal
    /// at the first parse instead of [`DemotionReason::UnparsableVolume`] after
    /// [`MAX_HEADER_PREFIX_BYTES`] of staging per volume.
    pub(super) fn key_header_encrypted_volume(
        &mut self,
        volume_index: u32,
        source: VolumeImage,
    ) -> Result<bool, DemotionReason> {
        if let Some(refusal) = self.header_crypt.refusal() {
            return Err(self.refuse_header_encrypted(refusal));
        }
        // A password already proved that still will not open this volume: the
        // set's volumes disagree about their archive key, which is not a shape a
        // single `rar -hp` run produces. Also the termination proof for the
        // caller's retry — a second pass can only reach this arm.
        if self.header_crypt.password().is_some() {
            return Err(self.refuse_header_encrypted(HeaderCryptRefusal::NoVerifiedCandidate));
        }
        let Some(image) = self.volume_image(volume_index, source) else {
            return Ok(false);
        };
        let encryption = match unrar_rs::RarArchive::parse_volume_header_encryption(image) {
            Ok(encryption) => encryption,
            Err(
                unrar_rs::RarError::UnsupportedEncryption { .. }
                | unrar_rs::RarError::UnsupportedEncryptionKdf { .. },
            ) => return Err(self.refuse_header_encrypted(HeaderCryptRefusal::Unkeyable)),
            // The record is at the front of the volume, so this is an image that
            // has not reached it yet. The next article retries, and the prefix
            // ceiling still backstops a volume that never yields one.
            Err(_) => return Ok(false),
        };
        let verified = match self.header_crypt.resolve(&encryption) {
            Ok(password) => password.to_string(),
            Err(refusal) => return Err(self.refuse_header_encrypted(refusal)),
        };
        self.crypt.set_password(Some(&verified));
        crate::runtime::perf_probe::record(
            "direct_store.header_encrypted.keyed",
            std::time::Duration::from_nanos(1),
        );
        Ok(true)
    }

    /// Records a `-hp` refusal and demotes under it.
    ///
    /// Logs which password *sources* were offered and never a value: a password
    /// in a log is a password on disk.
    pub(super) fn refuse_header_encrypted(
        &mut self,
        refusal: HeaderCryptRefusal,
    ) -> DemotionReason {
        tracing::debug!(
            set_name = %self.plan.set_name,
            reason = refusal.metric(),
            sources = ?self.header_crypt.offered_sources(),
            "direct-store refused a header-encrypted set"
        );
        self.fail(DemotionReason::HeaderEncryptedRefused(refusal))
    }

    /// The archive-header password, once one has been proved.
    ///
    /// `None` for every set whose headers are readable, which is what makes
    /// every parse on those sets a no-password parse exactly as before.
    pub(super) fn header_password(&self) -> Option<&str> {
        self.header_crypt.password()
    }

    /// The password an extraction of this set needs, if any.
    ///
    /// RAR keys headers and file data from the same password, so a proved `-hp`
    /// password is also the file password; a `-p` set has no header password and
    /// keeps its own in the file ring.
    ///
    /// **`None` for a plaintext set**, and the `admitted()` gate is what makes
    /// that true rather than merely intended. `set_password` runs for *every*
    /// admitted set in a job that carries one — from the NZB meta, the filename
    /// convention or the operator — and [`KeyRing::password`] holds that string
    /// whether or not any encrypted member ever admitted against it. Reading it
    /// unconditionally handed a password to plaintext sets, which `unrar-rs`
    /// ignores for want of an encryption record, so nothing broke and nothing
    /// would have: the reason to gate it is that a doc comment claiming a
    /// property the code does not have is how the next person gets caught.
    ///
    /// This is the one place a password leaves the router as a string. The
    /// tolerated-member extraction hands it to `unrar-rs` rather than
    /// decrypting anything itself, so it needs the secret and not a key.
    pub(crate) fn archive_password(&self) -> Option<&str> {
        self.header_password()
            .or_else(|| self.crypt.admitted().then(|| self.crypt.password())?)
    }

    /// Files one parse's facts against the layout and drains what they unlock.
    ///
    /// Split out of [`Self::try_parse_volume`] because there are two readers a
    /// volume's headers can come from — the staged image, and a restored
    /// volume's envelope-backed one ([`Self::reconfirm_restored_volume`]) — and
    /// everything downstream of "these are the volume's members" must be
    /// identical for both. `source` names the reader the facts came from, so the
    /// Quick Open cross-check below re-reads the same image the claim came from.
    pub(super) fn accept_volume_facts(
        &mut self,
        volume_index: u32,
        facts: RarVolumeFacts,
        reached_end: bool,
        source: VolumeImage,
    ) -> Result<(), DemotionReason> {
        // Before anything is adopted: the library may have answered this parse
        // out of the archive's Quick Open cache, which is not authoritative and
        // is craftable. Nothing may enter the layout on that evidence.
        if self.refuse_quick_open_derived_facts(volume_index, &facts, source)?
            == QuickOpenCrossCheck::Inconclusive
        {
            return Ok(());
        }

        // For an identity-admitted set only: the volume's own headers get a
        // vote on the binding. A fingerprint match placed this file at
        // `volume_index`; a RAR5 volume states its number in its main header
        // and a numbered RAR4 set states it in its end record, and a *declared*
        // number that disagrees means the identity evidence and the archive
        // disagree about what this file is. Nothing can reconcile that — one
        // of them is describing a different file — so the set demotes before
        // the layout adopts a member from the wrong position. An absent number
        // stays silent (every unnumbered RAR4 volume would otherwise demote),
        // with one exception the format guarantees: a RAR5 *set member* always
        // declares its number past the first volume, so an absent number under
        // a nonzero binding is itself a disagreement — and a stated number
        // that disagrees demotes even when what it states is zero.
        if self.plan.identity.is_some() {
            let declared_disagrees = facts
                .volume_number
                .is_some_and(|stated| stated != volume_index);
            let rar5_missing_number = facts.archive_format() == ArchiveFormat::Rar5
                && facts.is_volume
                && facts.volume_number.is_none()
                && volume_index != 0;
            if declared_disagrees || rar5_missing_number {
                return Err(self.fail(DemotionReason::IdentityVolumeMismatch));
            }
            // A RAR5 parse that reached the end-of-archive record and found
            // no "more volumes follow" has read the set's own statement of
            // its size: this volume is the last, so the set has
            // `volume_index + 1` volumes. For a header-admitted set that is
            // the one place the count exists and what closes the open plan;
            // for a PAR2-described set it is a free cross-check against the
            // roster's count. A close that contradicts the bindings — or a
            // roster that counted differently — is the same evidence
            // disagreement the number check above refuses. RAR5 only: the
            // record is mandatory there, while a RAR4 volume may simply lack
            // one, and its absence parses exactly like "last volume".
            if facts.archive_format() == ArchiveFormat::Rar5
                && reached_end
                && !facts.more_volumes
                && !self.plan.close_identity_roster(volume_index + 1)
            {
                return Err(self.fail(DemotionReason::IdentityVolumeMismatch));
            }
        }

        if self.layout.is_none() {
            let format = facts.archive_format();
            if !matches!(format, ArchiveFormat::Rar4 | ArchiveFormat::Rar5) {
                return Err(self.fail(DemotionReason::UnsupportedFormat));
            }
            self.layout = Some(StoredLayoutBuilder::new(format));
        }

        // What this parse says about the volume, against what the last accepted
        // one said. A longer staged prefix can only *append* headers — the walk
        // is sequential from offset 0 — so an extension is the confirming parse
        // doing its job, and anything else is a real disagreement.
        match self.volume_facts.get(&volume_index) {
            // The layout consumes the member list and nothing else, so a parse
            // that only learned more *about the volume* (it finally reached the
            // end-of-archive record, say) needs no layout work at all.
            Some(previous) if previous.members == facts.members => {
                let changed = *previous != facts;
                self.volume_facts.insert(volume_index, facts.clone());
                if changed {
                    self.dirty_facts.insert(volume_index);
                }
            }
            Some(previous) if members_extend(previous, &facts) => {
                self.volume_facts.insert(volume_index, facts.clone());
                self.dirty_facts.insert(volume_index);
                self.rebuild_layout()?;
            }
            Some(_) => return Err(self.fail(DemotionReason::ConflictingVolumeFacts)),
            None => {
                self.volume_facts.insert(volume_index, facts.clone());
                self.dirty_facts.insert(volume_index);
                let added = self
                    .layout
                    .as_mut()
                    .expect("the layout was bound above")
                    .add_volume(volume_index, &facts);
                match added {
                    Ok(()) => {}
                    Err(StoredLayoutError::ConflictingVolume { .. }) => {
                        return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
                    }
                    Err(StoredLayoutError::FormatMismatch { .. }) => {
                        return Err(self.fail(DemotionReason::FormatMismatch));
                    }
                }
                // A volume arriving out of order can put a member's *first* part
                // in a volume the layout only learned about now, which moves the
                // archive order the commit loop walks.
                self.member_order_stale = true;
            }
        }

        let tail_base = facts
            .members
            .iter()
            .map(|member| member.data_offset.saturating_add(member.data_size))
            .max()
            .unwrap_or(0);
        if let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.provisional = true;
            staging.confirmed = reached_end;
            staging.tail_base = staging.tail_base.max(tail_base);
        }

        self.sync_members()?;
        // A member can become verifiable from a *parse* rather than from a
        // routed byte. A zero-length stored member has no byte to route at all,
        // and a chain whose closing header arrives after its last byte was
        // already placed has nothing left to trigger the gate. Either one would
        // otherwise stay unverified for the life of the job: the set never
        // finalizes, never demotes, and its suppressions stay armed over files
        // that will never exist.
        let member_ids: Vec<u32> = self.members.keys().copied().collect();
        for member_id in member_ids {
            self.try_verify_member(member_id)?;
        }
        self.check_eligibility()?;
        Ok(())
    }

    /// Rebuilds the reader one volume's headers were parsed out of.
    ///
    /// `None` means the image cannot be built at all — no staging entry, or a
    /// restored volume whose envelope file will not open — which every caller
    /// treats as "no parse", never as "an empty parse".
    pub(super) fn volume_image(
        &self,
        volume_index: u32,
        source: VolumeImage,
    ) -> Option<SparseImage> {
        let staging = self.staging.get(&volume_index)?;
        match source {
            VolumeImage::Staged => Some(SparseImage::from_staged(
                &staging.chunks,
                self.scratch.handle(),
            )),
            VolumeImage::Envelope => {
                let file = std::fs::File::open(self.plan.envelope_path(volume_index)).ok()?;
                Some(SparseImage::over_envelope(
                    &staging.chunks,
                    self.scratch.handle(),
                    std::sync::Arc::new(file),
                    &self.envelope_backed_ranges(volume_index),
                ))
            }
        }
    }

    /// The physical ranges of one volume its **envelope file** holds:
    /// everything the router routed, minus every member extent the routing
    /// history claims.
    ///
    /// Derived from the history rather than from the layout's current answer for
    /// the same reason [`Self::volume_member_extents`] is: a member that turned
    /// ineligible after routing maps to the envelope *now*, and reading its
    /// offsets out of the envelope file would read the sparse hole standing in
    /// for bytes that went to a `.direct.partial`.
    ///
    /// Deliberately **not** named `envelope_coverage`, which `DirectSet` already
    /// uses for a different fact: that one is what *reached disk*, and is the
    /// truth the provider serves a virtual volume from. This one is what routing
    /// *emitted*, which is weaker — and the difference matters, because a hole
    /// inside a file's length reads as zeros rather than as an error, so a range
    /// claimed here that the envelope never received would feed the header walk
    /// fabricated bytes.
    ///
    /// It is sound at the one place it is read from. A restored volume's
    /// `routed` comes from the checkpoint, which records only writes that
    /// returned; a live volume's comes from drains whose writes either returned
    /// or demoted the set on the spot; and the only caller runs from
    /// [`Self::note_volume_complete`], **before** that call's own drain, so no
    /// span of the article in hand is claimed yet.
    pub(super) fn envelope_backed_ranges(&self, volume_index: u32) -> ByteRanges {
        let mut coverage = ByteRanges::new();
        let Some(staging) = self.staging.get(&volume_index) else {
            return coverage;
        };
        for &(start, end) in staging.routed.ranges() {
            coverage.insert(start, end - start);
        }
        for extent in self.routed_extents.get(&volume_index).into_iter().flatten() {
            coverage = subtract(&coverage, extent.physical_offset, extent.len);
        }
        coverage
    }

    /// Refuses a parse whose headers may have come from the archive's Quick Open
    /// cache rather than from the physical header walk.
    ///
    /// # Why this is a direct-store decision and not a library default
    ///
    /// A `QO` service block caches every header of the archive, and the main
    /// header's locator record points at it. The format binds nothing: the RAR
    /// spec itself warns that "it would be possible to see one file name and
    /// extract another in case the quick open data and real archive data are
    /// intentionally created different". `parse_all_headers` consults the cache
    /// by default — the right default for *listing* an archive, and the wrong
    /// one for a component that decides where posted bytes get written.
    ///
    /// Two of the three disagreement shapes were already safe here: QO agreeing
    /// with the physical walk changes nothing, and QO contradicting a previous
    /// parse of the same volume is [`DemotionReason::ConflictingVolumeFacts`].
    /// The third is not: a forged record *appending* a member the physical walk
    /// never saw is a strict extension of the previous facts, so `members_extend`
    /// adopts it, `sync_members` gives it a destination and the drain routes
    /// payload into it — all on cache evidence alone.
    ///
    /// So the cache is cross-examined. Only a parse that actually **used** it pays
    /// for this: `headers_from_quick_open` is the library's own account that every
    /// header in `facts` came out of the `QO` cache rather than a physical walk.
    /// A locator alone is not enough — real archivers write a cache the reader
    /// rejects (no cached end record), and the members then already come from
    /// the walk this method would repeat. The walk that answers is the same one the
    /// library would have fallen back to — `allow_quick_open: false`, over the
    /// very image the claim came from — and its file headers must match the
    /// claim's members one for one on identity, extent and split flags.
    ///
    /// Weaver's **conventional** extraction paths are deliberately untouched:
    /// they open a real volume file that PAR2 and the whole-file hashes have
    /// already vouched for, and they are not choosing destinations for bytes off
    /// the wire.
    ///
    /// Three answers, not two: a walk over an image that is still arriving can
    /// stop at a hole before it reaches a header the cache already described,
    /// and that is [`QuickOpenCrossCheck::Inconclusive`] — wait, adopt nothing —
    /// rather than a refusal. See the body for why.
    pub(super) fn refuse_quick_open_derived_facts(
        &mut self,
        volume_index: u32,
        facts: &RarVolumeFacts,
        source: VolumeImage,
    ) -> Result<QuickOpenCrossCheck, DemotionReason> {
        if !facts.headers_from_quick_open {
            return Ok(QuickOpenCrossCheck::Physical);
        }
        #[cfg(test)]
        {
            self.quick_open_walks = self.quick_open_walks.saturating_add(1);
        }
        let claimed: Vec<MemberIdentity> = facts.members.iter().map(MemberIdentity::of).collect();
        let physical = self.physical_member_identities(volume_index, source);
        if physical.as_deref() == Some(claimed.as_slice()) {
            return Ok(QuickOpenCrossCheck::Agreed);
        }
        // The cache sits at the tail of the volume, so a staged image can hold
        // it — and the end record that makes the library adopt it — while a
        // file header in the middle of the volume is still in flight. The walk
        // then stops at that hole (the sparse image answers a hole as a clean
        // end of file) with the headers before it and nothing after, and the
        // cache, which described the whole volume, looks like it claimed more.
        // That is a walk that could not see enough yet, not a disagreement:
        // nothing is adopted, and the next article re-parses exactly as it does
        // for a prefix too short to hold a header. Only a walk over a
        // **complete** image can refuse — and there, a walk that will not run
        // at all is a refusal, not a pass: the whole point is that nothing
        // enters the layout without it.
        let image_complete = match source {
            VolumeImage::Envelope => true,
            VolumeImage::Staged => self
                .staging
                .get(&volume_index)
                .is_some_and(|staging| staging.source_complete),
        };
        let walk_stopped_short = physical
            .as_deref()
            .is_none_or(|walked| walked.len() < claimed.len() && claimed.starts_with(walked));
        if !image_complete && walk_stopped_short {
            return Ok(QuickOpenCrossCheck::Inconclusive);
        }
        Err(self.fail(DemotionReason::QuickOpenMismatch))
    }

    /// The file headers a **physical** walk of one volume's image finds, with
    /// the archive's Quick Open cache suppressed.
    ///
    /// `None` when the walk cannot be run or does not complete cleanly, which
    /// the caller treats as a refusal.
    pub(super) fn physical_member_identities(
        &self,
        volume_index: u32,
        source: VolumeImage,
    ) -> Option<Vec<MemberIdentity>> {
        let mut image = self.volume_image(volume_index, source)?;
        // The library's own entry point expects a reader positioned just past
        // the signature, and reading it here is what proves this is the RAR5
        // stream the locator belongs to rather than an assumption about it.
        if unrar_rs::signature::read_signature(&mut image).ok()? != ArchiveFormat::Rar5 {
            return None;
        }
        let parsed = unrar_rs::header::parse_all_headers_with_kdf_cache_and_options(
            &mut image,
            // The archive-header password for a `-hp` set, `None` for every
            // other. Without it this walk cannot read a `-hp` volume's headers
            // at all, so a `-hp -qo` set would refuse every parse as
            // `QuickOpenMismatch` — fail-closed, but for the wrong reason.
            self.header_password(),
            // The set's cache, so this second walk of the same volume — one
            // per cross-checked parse — costs a lookup rather than the whole
            // key derivation the first walk already paid for.
            &self.kdf_cache,
            unrar_rs::header::HeaderParseOptions {
                allow_quick_open: false,
            },
        )
        .ok()?;
        Some(
            parsed
                .files
                .iter()
                .map(|file| MemberIdentity {
                    name: file.header.name.clone(),
                    data_offset: file.header.data_offset,
                    data_size: file.header.data_size,
                    split_before: file.header.split_before,
                    split_after: file.header.split_after,
                })
                .collect(),
        )
    }

    /// Rebuilds the layout from every volume's newest facts.
    ///
    /// Volumes are re-added in ascending order so the rebuild is deterministic,
    /// and members keep their weaver-side identity because that identity is the
    /// header name, not the layout's index — which the rebuild is free to move.
    pub(super) fn rebuild_layout(&mut self) -> Result<(), DemotionReason> {
        let Some(format) = self.layout.as_ref().map(StoredLayoutBuilder::format) else {
            return Ok(());
        };
        let mut rebuilt = StoredLayoutBuilder::new(format);
        for (volume_index, facts) in &self.volume_facts {
            match rebuilt.add_volume(*volume_index, facts) {
                Ok(()) => {}
                Err(StoredLayoutError::ConflictingVolume { .. }) => {
                    return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
                }
                Err(StoredLayoutError::FormatMismatch { .. }) => {
                    return Err(self.fail(DemotionReason::FormatMismatch));
                }
            }
        }
        self.layout = Some(rebuilt);
        // A rebuild is free to renumber and reposition every member.
        self.member_order_stale = true;
        Ok(())
    }

    /// Adopts every direct-routable member the layout now knows about.
    ///
    /// The first shape demoted a set the moment a second routable member
    /// appeared. There is nothing in the router that needs one member — the
    /// layout already maps several members' extents inside one volume,
    /// per-member state is a map, and every gate is per member. What the
    /// restriction bought was the finalization and demotion bookkeeping being
    /// trivially per-set; the router pays for those properly instead.
    pub(super) fn sync_members(&mut self) -> Result<(), DemotionReason> {
        // Collisions are decided over **every member the layout has started**,
        // not just the routable ones, and not pairwise as members are adopted:
        // the second member of a colliding pair may be the one that arrives
        // first, and the member tolerance will keep an ineligible
        // member's bytes inside the set rather than demoting on sight — at
        // which point an ineligible member colliding with a routed one is a
        // member silently overwriting another, exactly what the extractor
        // refuses.
        let started: Vec<String> = self
            .layout_members()
            .iter()
            .map(|member| member.name.clone())
            .collect();
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(started.len());
        // Second key, same sweep: two names that differ only past the filename
        // clamp resolve to distinct destinations but to *one* `.direct.partial`,
        // and the extractor-parity key above cannot see that because it folds
        // the unclamped path.
        let mut partials: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(started.len());
        for name in &started {
            let Ok(key) = DirectSetPlan::member_collision_key(name) else {
                return Err(self.fail(DemotionReason::UnsafeDestination));
            };
            let Ok(partial) = self.plan.member_partial_path(name) else {
                return Err(self.fail(DemotionReason::UnsafeDestination));
            };
            if !seen.insert(key) || !partials.insert(partial.to_ascii_lowercase()) {
                return Err(self.fail(DemotionReason::CollidingDestinations));
            }
        }

        // `routes_direct()` is now true for an encrypted store member, so this
        // filter admits ciphertext — and would create a `.direct.partial` for
        // it, size every extent off the *plaintext* `unpacked_size` while the
        // cipher stream runs to `align16(unpacked_size)`, and route the bytes
        // unchanged. Admission is therefore decided **first**, before a single
        // destination exists: no password, a refuted one, or key material this
        // build cannot use, and the set demotes here rather than writing
        // anything.
        let keys = self.admit_encrypted()?;

        let routable: Vec<(String, u64, Option<unrar_rs::EncryptedStore>)> = self
            .layout_members()
            .iter()
            .filter(|member| member.eligibility.routes_direct())
            .map(|member| {
                (
                    member.name.clone(),
                    member.unpacked_size.unwrap_or(0),
                    member.eligibility.encrypted_store(),
                )
            })
            .collect();
        for (name, unpacked_size, encrypted) in routable {
            // Unreachable while `admit_encrypted` demotes on every refusal, and
            // stated anyway: an encrypted member with no key routes nothing, and
            // the alternative to skipping it is a destination full of cipher.
            if encrypted.is_some() && !keys.contains_key(&name) {
                continue;
            }
            if let Some(member_id) = self.member_ids.get(&name).copied() {
                // Recorded and applied after the member borrow ends: the member
                // accessor borrows the whole router, because dropping the crypt
                // snapshot is part of what it does.
                let mut size_moved = false;
                if let Some(existing) = self.member_mut(member_id) {
                    // A size that actually moved is a digest fact that moved:
                    // the first header of a member can declare none at all
                    // (`unwrap_or(0)`) and a later one fill it in, and the
                    // checkpoint's digest binds the size it was written under.
                    if existing.unpacked_size != unpacked_size {
                        existing.unpacked_size = unpacked_size;
                        size_moved = true;
                    }
                    if let (Some(facts), Some(crypt)) = (encrypted, existing.crypt.as_mut()) {
                        // The cipher extent resolves — from unknown to known —
                        // as the headers that declare a size arrive.
                        crypt.observe(&facts);
                    }
                }
                if size_moved {
                    self.member_facts_revision = self.member_facts_revision.saturating_add(1);
                }
                continue;
            }
            let relative_partial = match self.plan.member_partial_path(&name) {
                Ok(path) => path,
                Err(()) => return Err(self.fail(DemotionReason::UnsafeDestination)),
            };
            let crypt = encrypted.and_then(|facts| {
                let (member_keys, keying) = keys.get(&name)?;
                let mut crypt = MemberCrypt::new(*member_keys, keying);
                crypt.observe(&facts);
                Some(crypt)
            });
            let member_id = self.next_member_id;
            self.next_member_id = self.next_member_id.saturating_add(1);
            self.member_ids.insert(name.clone(), member_id);
            self.invalidate_member_ciphers();
            self.members.insert(
                member_id,
                MemberRouting {
                    name,
                    relative_partial,
                    unpacked_size,
                    covered: ByteRanges::new(),
                    parts: BTreeMap::new(),
                    checked_parts: BTreeMap::new(),
                    restart_seeded: ByteRanges::new(),
                    stale_gaps: ByteRanges::new(),
                    verified: false,
                    crypt,
                },
            );
            self.member_order_stale = true;
            self.member_facts_revision = self.member_facts_revision.saturating_add(1);
        }
        if self.member_order_stale {
            self.rebuild_member_order();
            self.member_order_stale = false;
        }
        Ok(())
    }

    /// The encrypted-store admission decision.
    ///
    /// Runs at every parse, before [`Self::sync_members`] creates anything, and
    /// answers one question per encrypted member: is there a password that may
    /// key it? Key derivation happens once per KDF tuple — a set whose members
    /// share one pays a single PBKDF2 — and the RAR5 password check is verified
    /// **before any byte routes**.
    ///
    /// Four refusals, all of them demotions:
    ///
    /// - the job's spec declares a PAR2 file. An encrypted set's destinations
    ///   hold plaintext where PAR2 describes the posted cipher, and the guard
    ///   that catches this behind the authoritative pass cannot run until the
    ///   whole set has downloaded — at which point demoting costs a full
    ///   refetch, because plaintext partials cannot reconstruct posted bytes.
    ///   Refusing here is the pre-plan-136 behaviour exactly: one hard demotion
    ///   on the first header parse, one article back on the wire;
    /// - no password: an encrypted set routes only with one;
    /// - a check present that this password does not reproduce: nothing is
    ///   written on the strength of a refuted password;
    /// - key material this build cannot derive from: a RAR5 KDF count over the
    ///   crate's ceiling.
    ///
    /// A check the header **omits** admits provisionally: nothing can be
    /// concluded before the bytes, and the member's keyed checksum gate is then
    /// the earliest detector — the same position layer 1 is in for a plaintext
    /// member.
    ///
    /// # RAR4
    ///
    /// RAR4 file encryption keys here too, off the header's 8-byte file salt
    /// rather than a `FHEXTRA_CRYPT` record —
    /// [`unrar_rs::MemberKeying`] is the discriminant and it is total, so
    /// there is no "no record" arm to refuse on any more. A RAR4 member the
    /// library cannot key (one of the pre-AES ciphers) never becomes an
    /// `EncryptedStore` at all, so it demotes as an ineligible member and never
    /// reaches this function. RAR4 carries no password-check value, so every
    /// RAR4 member takes the provisional path above by construction.
    pub(super) fn admit_encrypted(
        &mut self,
    ) -> Result<HashMap<String, (crypt::MemberKeys, unrar_rs::MemberKeying)>, DemotionReason> {
        let encrypted: Vec<(String, unrar_rs::MemberKeying)> = self
            .layout_members()
            .iter()
            .filter_map(|member| {
                member
                    .eligibility
                    .encrypted_store()
                    .map(|facts| (member.name.clone(), facts.keying()))
            })
            .collect();
        let mut keys = HashMap::with_capacity(encrypted.len());
        for (name, keying) in encrypted {
            match self.crypt.admit(&keying) {
                Ok(member_keys) => {
                    keys.insert(name, (member_keys, keying));
                }
                Err(refusal) => {
                    return Err(self.fail(DemotionReason::EncryptedMemberRefused(refusal)));
                }
            }
        }
        Ok(keys)
    }

    /// A provisional member that resolves `Ineligible` at chain close demotes
    /// the group at that transition — **unless** its shape is one the member
    /// tolerance carries.
    ///
    /// The tolerance is a deliberate weaver extension over the oracle, and it is
    /// bounded two ways, each of which demotes on breach:
    ///
    /// 1. **By kind**, which is [`member_shape_is_tolerable`]'s whole subject.
    /// 2. **By the set still being a store set.** A set whose members are *all*
    ///    ineligible has nothing to route and no benefit to gain; it demotes and
    ///    the ordinary extractor produces every member.
    ///
    /// # There is deliberately no size ceiling
    ///
    /// The first shape also bounded the tolerance by size —
    /// `min(64 MiB, 1% of the archive's packed bytes)` packed, 256 MiB
    /// unpacked — and demoted the whole set on a breach. That rule made a
    /// *member's* shape a *set's* verdict, and it cost the whole set the direct
    /// route for one member it could not route: a store video beside a
    /// compressed subtitle pack, a season pack with one compressed episode, or
    /// a 6 GiB folder tree whose closing volume carries a directory header, all
    /// threw away a complete direct route at the very end of the download and
    /// paid for a full materialization plus a full conventional extraction.
    ///
    /// The ceiling is gone, and the two costs it was standing in for are
    /// answered where they actually live:
    ///
    /// - **Disk.** A tolerated member's packed bytes are routed to the
    ///   volume's *envelope*, which is a sparse file in the job's working
    ///   directory. The envelope therefore holds exactly the ineligible
    ///   members' packed bytes and nothing else, so the set's own working set
    ///   is `routed member bytes + ineligible packed bytes` — one copy of the
    ///   posted payload. The conventional path this would demote to writes one
    ///   copy of the *whole* volume set and then extracts every member out of
    ///   it, so the tolerated shape is strictly cheaper at any ratio of
    ///   ineligible to stored bytes. Nothing here touches the holds scratch:
    ///   [`DemotionReason::HoldsScratchCeiling`] bounds *staged, unrouted*
    ///   bytes, and a tolerated member's bytes are routed the moment the header
    ///   walk reaches them.
    /// - **Output size.** The unpacked ceiling was the only thing bounding what
    ///   a tolerated decode writes. That bound now comes from the same place
    ///   the conventional extractor's does — the job's `JobExtractionBudget`,
    ///   which `extract_tolerated_members` writes through — so a hostile
    ///   expansion is refused by the budget rather than by a second, weaker
    ///   ceiling that also happened to demote healthy sets.
    ///
    /// One bound on *bytes moved* does remain, and it is not a tolerance rule:
    /// an adopted member that turns ineligible at chain close is **migrated**
    /// out of its partial and into the envelopes, and that move reads the
    /// member's routed bytes into memory on the parsing task. A member with
    /// more than [`MIGRATION_CEILING_BYTES`] routed demotes on its own reason
    /// instead, which is the answer it had before migration existed.
    ///
    /// What is *not* answered here is tail latency: the tolerated decode still
    /// runs once, at finalization, rather than incrementally as chains close.
    /// See [`Self::tolerated_members`].
    pub(super) fn check_eligibility(&mut self) -> Result<(), DemotionReason> {
        let mut tolerated = 0usize;
        let mut routable = 0usize;
        let mut first_tolerated: Option<IneligibilityReason> = None;
        // Adopted members that must have their routed bytes moved into the
        // envelope before they can ride the tolerance. Collected rather than
        // migrated in place: the loop holds a borrow of the layout, and the
        // `routable == 0` verdict below is not reached until every member has
        // been classified.
        let mut migrating: Vec<(u32, IneligibilityReason)> = Vec::new();

        for member in self.layout_members() {
            let reason = match member.eligibility {
                MemberEligibility::DirectEligible | MemberEligibility::ProvisionallyDirect => {
                    routable += 1;
                    continue;
                }
                // The `let ... else` this replaces counted **every**
                // non-`Ineligible` member routable, and `EncryptedStore` is not
                // `Ineligible` — so an all-encrypted set with no password would
                // have sailed past the `routable == 0` demotion below with
                // nothing to route and no reason to stop, silently deleting the
                // hard demotion this path has always guaranteed. Routable means
                // *decryptable*: a member the key ring admitted counts, and one
                // it did not is the set's own reason to leave direct mode.
                MemberEligibility::EncryptedStore(_) => {
                    if self.crypt.admitted() {
                        routable += 1;
                        continue;
                    }
                    let refusal = self.crypt.refusal().unwrap_or(CryptRefusal::NoPassword);
                    return Err(self.fail(DemotionReason::EncryptedMemberRefused(refusal)));
                }
                MemberEligibility::Ineligible(reason) => reason,
            };
            if !member_shape_is_tolerable(reason) {
                return Err(self.fail(DemotionReason::MemberIneligible(reason.into())));
            }
            // A member the router **adopted** routed bytes into its own
            // `.direct.partial` while it was still `ProvisionallyDirect` — a
            // split BLAKE2sp-only member is the reachable case, since the digest
            // only disqualifies it once its chain closes. Those bytes are not in
            // the envelope, so the virtual volume the tolerated extraction reads
            // is not the volume the archive describes, and finalization would
            // additionally commit the partial as if it were a stored member's.
            //
            // An earlier shape demoted the whole set on sight for that. It no
            // longer has to: the bytes are moved back into the envelope at
            // their true physical offsets, the member is un-adopted, and it is
            // extracted conventionally at finalization exactly like a member
            // that was never adopted at all. The migration is still deferred to
            // the end of this function, because the `routable == 0` verdict
            // below is a demotion and a member that cannot ride the tolerance
            // must not have been half-moved to find that out.
            if let Some(member_id) = self.member_ids.get(&member.name).copied() {
                migrating.push((member_id, reason));
            }
            tolerated += 1;
            // A directory is the *last* reason worth reporting: it is dataless
            // and free, so a set that has nothing to route is never a set the
            // directories made unroutable. Any other tolerated reason takes the
            // slot from it, whichever arrived first — an archiver that writes
            // its directory headers before the files would otherwise have every
            // such demotion labelled `member_directory`.
            match first_tolerated {
                None => first_tolerated = Some(reason),
                Some(IneligibilityReason::Directory)
                    if !matches!(reason, IneligibilityReason::Directory) =>
                {
                    first_tolerated = Some(reason);
                }
                Some(_) => {}
            }
        }

        let Some(first_tolerated) = first_tolerated else {
            debug_assert_eq!(tolerated, 0);
            debug_assert!(migrating.is_empty());
            return Ok(());
        };
        if routable == 0 {
            // Nothing to route: every byte would land in the envelope and every
            // member would be extracted conventionally at the end, which is the
            // conventional path with an extra copy of the volumes in it.
            // Reported under the member's *own* reason — the set is simply not
            // a store set.
            return Err(self.fail(DemotionReason::MemberIneligible(first_tolerated.into())));
        }
        // Checked over every migrating member before any is moved: the ceiling
        // is a demotion, and a member that cannot ride the tolerance must not
        // have been half-moved to find that out.
        for (member_id, reason) in &migrating {
            if self.routed_bytes_of(*member_id) > self.migration_ceiling_bytes {
                return Err(self.fail(DemotionReason::MemberIneligible((*reason).into())));
            }
        }
        for (member_id, reason) in migrating {
            self.migrate_member_to_envelope(member_id, reason)?;
        }
        Ok(())
    }

    /// Bytes the router has routed into one member's partial so far.
    pub(super) fn routed_bytes_of(&self, member_id: u32) -> u64 {
        self.routed_extents
            .values()
            .flatten()
            .filter(|extent| extent.member_id == member_id)
            .fold(0u64, |total, extent| total.saturating_add(extent.len))
    }

    #[cfg(test)]
    pub(crate) fn set_migration_ceiling_bytes(&mut self, bytes: u64) {
        self.migration_ceiling_bytes = bytes;
    }

    /// Moves an already-adopted member's routed bytes out of its
    /// `.direct.partial` and into the envelopes, then un-adopts it, so it can
    /// ride the member tolerance instead of demoting the set.
    ///
    /// # What has to move, and what has to stop claiming
    ///
    /// The bytes are read back from the partial at the logical offsets the
    /// **routing history** records for them and re-emitted as envelope spans at
    /// their physical offsets. The history is the right source and the layout
    /// is not: by the time this runs the member is `Ineligible`, so
    /// `map_physical_range` already calls its packed range envelope, and the
    /// history is the only record of where the bytes actually went. It is then
    /// dropped for this member — the whole point, since a migrated member's
    /// extents must stop claiming member space or the hybrid provider would
    /// keep answering those offsets out of a partial that is about to
    /// disappear.
    ///
    /// The member's routing state goes with it: coverage, per-part `CrcRuns`,
    /// checked parts, restart seeds and stale gaps. None of it survives, and
    /// none of it should — the composition existed to gate a member weaver was
    /// writing itself, and `extract_member_streaming` verifies this one natively
    /// (BLAKE2sp included) when finalization extracts it.
    ///
    /// # Two things this deliberately does not do
    ///
    /// It does not run for an **encrypted** member. Its destination holds
    /// plaintext where the volume held cipher, so moving those bytes into the
    /// envelope would file plaintext at offsets the posted volume has ciphertext
    /// at — visible to PAR2, to reconstruction and to any later reader. Such a
    /// member demotes on its own reason, as before.
    ///
    /// It does not fsync anything, and it does not need to: the spans go back
    /// through the ordinary write path, so the coverage barrier records them,
    /// syncs the envelope and only then publishes a floor. A crash before that
    /// barrier leaves the envelope without the bytes — and the provider refuses
    /// an envelope range it has no positive coverage for rather than serving the
    /// hole, so the set refetches instead of reading zeros.
    ///
    /// # The one thing it cannot clean up
    ///
    /// The coverage barrier keeps claiming the deleted `.direct.partial` in every
    /// later checkpoint: destinations are registered once and there is no
    /// unregister, and that is `CoverageBarrier`'s to add, not the router's. A
    /// restart in the window between the migration and the set finishing
    /// therefore refuses the row on a missing destination and redownloads the
    /// set — safe, and no worse than the demotion this replaced, which threw the
    /// checkpoint away outright. Retiring the destination would make the window
    /// free instead of merely safe.
    pub(super) fn migrate_member_to_envelope(
        &mut self,
        member_id: u32,
        reason: IneligibilityReason,
    ) -> Result<(), DemotionReason> {
        let refuse =
            |router: &mut Self| router.fail(DemotionReason::MemberIneligible(reason.into()));
        let Some(member) = self.members.get(&member_id) else {
            return Ok(());
        };
        if member.crypt.is_some() {
            return Err(refuse(self));
        }
        let partial = self.plan.destination_path(&member.relative_partial);

        // Every extent the member ever had bytes written for, by volume, in
        // physical order within each — so the envelope is written the way it is
        // laid out. The whole move is bounded by `MIGRATION_CEILING_BYTES`,
        // which the caller checked before moving anything; `MIGRATION_SPAN_BYTES`
        // bounds each individual allocation inside it.
        let extents: Vec<(u32, MemberExtent)> = self
            .routed_extents
            .iter()
            .flat_map(|(volume_index, extents)| {
                extents
                    .iter()
                    .filter(|extent| extent.member_id == member_id)
                    .map(|extent| (*volume_index, *extent))
            })
            .collect();

        let mut spans = Vec::new();
        if !extents.is_empty() {
            let Ok(file) = std::fs::File::open(&partial) else {
                return Err(refuse(self));
            };
            for (volume_index, extent) in &extents {
                let mut moved = 0u64;
                while moved < extent.len {
                    let take = (extent.len - moved).min(MIGRATION_SPAN_BYTES);
                    let mut bytes = vec![0u8; take as usize];
                    if read_at(
                        &file,
                        extent.logical_offset.saturating_add(moved),
                        &mut bytes,
                    )
                    .is_err()
                    {
                        return Err(refuse(self));
                    }
                    let offset = extent.physical_offset.saturating_add(moved);
                    spans.push(RoutedSpan {
                        destination: DirectDestination::Envelope {
                            volume_index: *volume_index,
                        },
                        destination_offset: offset,
                        volume_index: *volume_index,
                        source_offset: offset,
                        bytes,
                    });
                    moved += take;
                }
            }
        }

        // Nothing above this line has changed any state, so every refusal so
        // far left the set exactly as an earlier shape would have.
        for (volume_index, extent) in &extents {
            if let Some(held) = self.routed_extents.get_mut(volume_index) {
                held.retain(|candidate| candidate.member_id != extent.member_id);
            }
        }
        self.routed_extents.retain(|_, held| !held.is_empty());
        self.invalidate_member_ciphers();
        if let Some(member) = self.members.remove(&member_id) {
            self.member_ids.remove(&member.name);
            // The barrier's claim on the partial goes with the partial. Parked
            // rather than applied, because the barrier lives a layer up; the set
            // drains it before it records the migrated spans, so no snapshot is
            // ever built between the unlink and the retirement.
            self.retired_destinations
                .push((member_id, member.relative_partial));
        }
        self.member_facts_revision = self.member_facts_revision.saturating_add(1);
        self.rebuild_member_order();
        self.migrated.extend(spans);
        // The partial is deleted rather than left behind: everything still in
        // the staging root when the job completes is moved into its output, so
        // a stray `.direct.partial` beside the extracted member would ship.
        // Its bytes are in `migrated` already, so this cannot lose them, and a
        // failure to unlink is not worth demoting a set over — the restart sweep
        // knows the suffix.
        let _ = std::fs::remove_file(&partial);
        crate::runtime::perf_probe::record(
            "direct_store.member.migrated_to_envelope",
            std::time::Duration::from_nanos(1),
        );
        Ok(())
    }

    /// Hands the caller whatever a migration parked (the small-member
    /// tolerance).
    pub(super) fn take_migrated_spans(&mut self) -> Vec<RoutedSpan> {
        std::mem::take(&mut self.migrated)
    }

    /// Hands the caller the destinations a migration deleted, so their coverage
    /// claims can be retired. Drained, so each is reported exactly once.
    pub(crate) fn take_retired_destinations(&mut self) -> Vec<(u32, String)> {
        std::mem::take(&mut self.retired_destinations)
    }

    /// Revision of the facts the checkpoint's plan digest binds. Changes only
    /// when the digest would.
    pub(crate) fn member_facts_revision(&self) -> u64 {
        self.member_facts_revision
    }

    /// Members riding the member tolerance, by raw header name, in archive
    /// order.
    ///
    /// # Tail latency, stated where it is paid
    ///
    /// Finalization extracts this whole list in one blocking task, after the
    /// last article of the set has arrived. While the list was bounded to a few
    /// small extras that was invisible; with the size ceiling gone it is the
    /// tolerance's one remaining cost, and it is a *serial tail* rather than an
    /// I/O amplification — the bytes are read once, out of the envelope they
    /// were routed to. The conventional incremental scheduler
    /// (`pipeline::extraction::rar::scheduler`) already does the same decode
    /// incrementally against real volumes as each one completes; feeding it the
    /// hybrid provider instead of files is the seam that would retire this
    /// tail, and it is not opened here.
    ///
    /// A **directory** member rides it too, and is flagged rather than filtered
    /// out: it is dataless, so finalization creates the directory and applies
    /// the archive's metadata to it instead of decoding anything. Filtering it
    /// out here is what would lose the entry — the routed list never contained
    /// it either.
    ///
    /// Finalization extracts exactly these and nothing else: the direct-routed
    /// members are already at their destinations, and re-extracting one would
    /// overwrite verified output with a second decode of the same bytes.
    ///
    /// # Why `Ineligible(_)` is not the whole predicate
    ///
    /// `Ineligible(_)` stopped spanning "every member finalization must extract"
    /// the moment `EncryptedStore` existed: it is not `Ineligible`, so the old
    /// predicate dropped an encrypted member from this list while nothing put it
    /// on the routed one — a member in neither list is a member silently missing
    /// from the output. The decision is stated rather than implied. An
    /// **admitted** encrypted member is direct-routed and must not be
    /// re-extracted over its own verified bytes. One the set could **not** key
    /// belongs here, because the conventional extractor — which asks the job's
    /// whole password-candidate list, a superset of the single password
    /// direct-store is handed — is the only thing that can still produce it.
    /// (While the set lives that case is unreachable, since admission demotes
    /// the whole set rather than routing around one member; it is written down
    /// because the alternative to writing it down is losing a file.)
    pub(crate) fn tolerated_members(&self) -> Vec<ToleratedMember> {
        let admitted = self.crypt.admitted();
        let mut names: Vec<(u32, u64, ToleratedMember)> = self
            .layout_members()
            .iter()
            .filter(|member| match member.eligibility {
                MemberEligibility::Ineligible(_) => true,
                MemberEligibility::EncryptedStore(_) => !admitted,
                MemberEligibility::DirectEligible | MemberEligibility::ProvisionallyDirect => false,
            })
            .map(|member| {
                let position = member
                    .parts
                    .first()
                    .map(|part| (part.volume, part.data_offset))
                    .unwrap_or((u32::MAX, u64::MAX));
                (
                    position.0,
                    position.1,
                    ToleratedMember {
                        name: member.name.clone(),
                        is_directory: matches!(
                            member.eligibility,
                            MemberEligibility::Ineligible(IneligibilityReason::Directory)
                        ),
                    },
                )
            })
            .collect();
        names.sort_unstable();
        names.into_iter().map(|(_, _, member)| member).collect()
    }

    /// Maps and emits every pending byte of one volume whose destination the
    /// layout can now name.
    pub(super) fn drain_volume(
        &mut self,
        volume_index: u32,
    ) -> Result<Vec<RoutedSpan>, DemotionReason> {
        let Some(staging) = self.staging.get(&volume_index) else {
            return Ok(Vec::new());
        };
        if staging.pending.is_empty() {
            return Ok(Vec::new());
        }
        // Split at the repair boundaries **before** anything is mapped: the
        // emitted run's `replace` flag is all-or-nothing, and neither the layout
        // nor `pending`'s coalescing knows where a repair starts and stops
        // ([`VolumeStaging::repair_partition`]).
        let pending: Vec<(u64, u64)> = staging
            .pending
            .ranges()
            .iter()
            .flat_map(|(start, end)| staging.repair_partition(*start, *end))
            .collect();
        // Beyond this the volume's classification is unproven; see
        // [`VolumeStaging::tail_base`]. A confirmed volume has no such region.
        let unproven_from = if staging.confirmed {
            u64::MAX
        } else {
            staging.tail_base
        };
        let mut spans = Vec::new();
        let mut routed = Vec::new();

        for (start, end) in pending {
            let mut cursor = start;
            for slice in self.map_physical_range(volume_index, start, end - start) {
                match slice {
                    MappedSlice::Unroutable { len } => {
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Envelope { len } if cursor >= unproven_from => {
                        // Held, not routed: a member whose header the walk has
                        // not reached yet would have its payload written into
                        // the envelope and deleted with it.
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Envelope { len } => {
                        // Envelope v2: the destination offset *is* the physical
                        // offset, so there is no slot arithmetic left to
                        // overflow and no ceiling left to demote against. A
                        // recovery record, a quick-open block or an ineligible
                        // member's packed range fits by definition — the file is
                        // a sparse image of the volume it came from.
                        let bytes = self
                            .staging
                            .get(&volume_index)
                            .and_then(|staging| staging.slice(cursor, len, &self.scratch));
                        if let Some(bytes) = bytes {
                            spans.push(RoutedSpan {
                                destination: DirectDestination::Envelope { volume_index },
                                destination_offset: cursor,
                                volume_index,
                                source_offset: cursor,
                                bytes,
                            });
                            routed.push((cursor, len));
                        }
                        cursor = cursor.saturating_add(len);
                    }
                    MappedSlice::Member {
                        member_index,
                        logical_offset,
                        len,
                    } => {
                        // Unreachable by construction: the layout only maps a
                        // member the router has adopted, and adoption assigns
                        // the id. Leaving the run pending rather than asserting
                        // keeps a would-be panic as a holds-budget demotion.
                        let Some(member_id) = self.member_id_for_layout(member_index) else {
                            debug_assert!(
                                false,
                                "the layout mapped member {member_index} of {}, which the router \
                                 never adopted",
                                self.plan.set_name
                            );
                            cursor = cursor.saturating_add(len);
                            continue;
                        };
                        let staging = self.staging.get(&volume_index);
                        // The repair marker: read before the slice, from the
                        // same staging entry, so the decision is made on the
                        // range that is about to drain rather than on a
                        // router-wide mode a concurrent duplicate could ride.
                        let replace =
                            staging.is_some_and(|staging| staging.is_repaired(cursor, len));
                        let bytes =
                            staging.and_then(|staging| staging.slice(cursor, len, &self.scratch));
                        if let Some(bytes) = bytes {
                            self.note_member_bytes(
                                member_id,
                                volume_index,
                                logical_offset,
                                &bytes,
                                replace,
                            )?;
                            // Recorded here, at the moment a member destination
                            // is chosen, and never revisited: this is the only
                            // account of where the bytes went that survives the
                            // member turning ineligible.
                            self.record_routed_extent(
                                volume_index,
                                MemberExtent {
                                    member_id,
                                    physical_offset: cursor,
                                    logical_offset,
                                    len,
                                },
                            );
                            spans.push(RoutedSpan {
                                destination: DirectDestination::Member { member_id },
                                destination_offset: logical_offset,
                                volume_index,
                                source_offset: cursor,
                                bytes,
                            });
                            routed.push((cursor, len));
                        }
                        cursor = cursor.saturating_add(len);
                    }
                    // Cipher bytes: a routing decision of their
                    // own, never a copy of the `Member` arm above — writing them
                    // where that arm writes would put ciphertext in the
                    // destination, which is the whole reason the layout gives
                    // them their own variant. What they share is the
                    // *coordinates*: cipher offset and member-logical offset are
                    // the same number for a stored member, so every range answer
                    // the router computes is unchanged and only the bytes differ.
                    MappedSlice::EncryptedMember {
                        member_index,
                        logical_offset,
                        len,
                    } => {
                        let staging = self.staging.get(&volume_index);
                        let replace =
                            staging.is_some_and(|staging| staging.is_repaired(cursor, len));
                        self.route_encrypted_slice(
                            volume_index,
                            cursor,
                            member_index,
                            logical_offset,
                            len,
                            replace,
                            &mut spans,
                            &mut routed,
                        )?;
                        cursor = cursor.saturating_add(len);
                    }
                }
            }
        }

        if let Some(staging) = self.staging.get_mut(&volume_index) {
            for (start, len) in &routed {
                staging.routed.insert(*start, *len);
            }
            let mut still_pending = ByteRanges::new();
            for (start, end) in staging.pending.ranges() {
                still_pending.insert(*start, end - start);
            }
            for (start, len) in &routed {
                still_pending = subtract(&still_pending, *start, *len);
                // The repair marker lives exactly as long as the bytes it
                // describes: a routed range is composed, so a duplicate of it
                // arriving later is a duplicate again and must clip.
                staging.repaired = subtract(&staging.repaired, *start, *len);
            }
            staging.pending = still_pending;
        }
        self.trim_volume(volume_index);
        Ok(spans)
    }

    /// Keeps only what the header parser still needs: the envelope, and any
    /// bytes still waiting for a destination.
    ///
    /// Once a volume is **confirmed** the parser will never walk it again, so
    /// its envelope bytes are dropped from RAM entirely. That matters much more
    /// with envelope v2 than it did with the 64 KiB slots: a `-rr` volume's
    /// recovery record is envelope-classified and can be percent-of-volume
    /// sized, so keeping it staged for the life of the set would put an
    /// unbounded, volume-count-proportional term in RSS. Until confirmation it
    /// is retained, because the walk has to seek past the recovery service
    /// header to reach the end-of-archive record.
    pub(super) fn trim_volume(&mut self, volume_index: u32) {
        let mut keep = ByteRanges::new();
        if let Some(staging) = self.staging.get(&volume_index) {
            for (start, end) in staging.pending.ranges() {
                keep.insert(*start, end - start);
            }
            if staging.confirmed {
                let pending = keep;
                if let Some(staging) = self.staging.get_mut(&volume_index) {
                    staging.trim(&pending);
                }
                return;
            }
            let chunks: Vec<(u64, u64)> = staging
                .chunks
                .iter()
                .map(|(offset, chunk)| (*offset, chunk.len()))
                .collect();
            for (offset, len) in chunks {
                let mut cursor = offset;
                for slice in self.map_physical_range(volume_index, offset, len) {
                    match slice {
                        // An encrypted member's bytes are as droppable as a
                        // plaintext one's, and for the same reason: what is not
                        // droppable is still **pending**, which `keep` starts
                        // from. A sub-block remainder the drain could not decrypt
                        // was never routed, so it is pending here and pending in
                        // whichever neighbouring volume holds the rest of its
                        // block — and each side's own `pending` is what keeps
                        // both halves alive until the block closes. Keeping
                        // routed cipher on top of that would retain a second copy
                        // of the payload for the life of the volume, which is the
                        // RSS term envelope v2's trim exists to remove.
                        MappedSlice::Member { len, .. }
                        | MappedSlice::EncryptedMember { len, .. } => {
                            cursor = cursor.saturating_add(len)
                        }
                        MappedSlice::Envelope { len } | MappedSlice::Unroutable { len } => {
                            keep.insert(cursor, len);
                            cursor = cursor.saturating_add(len);
                        }
                    }
                }
            }
        }
        if let Some(staging) = self.staging.get_mut(&volume_index) {
            staging.trim(&keep);
        }
    }

    /// Feeds one routed member run into the integrity gates.
    ///
    /// `replace` is the repair marker. Without it a run whose bytes the
    /// coverage map already claims is a duplicate and contributes nothing; with
    /// it the run is a PAR2 repair of those very bytes, so the composition is
    /// **overwritten** and whatever the rewrite half-covered becomes a stale
    /// gap the caller must re-read.
    pub(super) fn note_member_bytes(
        &mut self,
        member_id: u32,
        volume_index: u32,
        logical_offset: u64,
        bytes: &[u8],
        replace: bool,
    ) -> Result<(), DemotionReason> {
        let len = bytes.len() as u64;
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some(part) = self.part_for(layout_index, volume_index) else {
            return Ok(());
        };
        let (part_position, part_logical_offset, part_len, packed_crc32) = part;
        let Some(member) = self.member_mut(member_id) else {
            return Ok(());
        };
        if member.covered.insert(logical_offset, len) == 0 && !replace {
            // Wholly duplicate: never advance a gate twice.
            return Ok(());
        }
        let crc = par2_rs::checksum::crc32(bytes);
        let part_relative = logical_offset.saturating_sub(part_logical_offset);
        if replace {
            let gaps =
                member
                    .parts
                    .entry(part_position)
                    .or_default()
                    .overwrite(part_relative, len, crc);
            // A repaired span can only *resolve* gaps that fall inside it, so
            // the rewritten range leaves the stale set before the new gaps join
            // it — and both are recorded in member-logical space, which is what
            // the re-read plan and the coverage map speak.
            member.stale_gaps = subtract(&member.stale_gaps, logical_offset, len);
            for (start, end) in gaps {
                member.stale_gaps.insert(
                    start.saturating_add(part_logical_offset),
                    end.saturating_sub(start),
                );
            }
            // Both of these described bytes that no longer exist. Dropping the
            // part's checked value is what keeps a stale one from surviving the
            // rewrite: while the gaps are open the composition below yields
            // nothing, so without the removal the member would go on verifying
            // against the value the damaged bytes produced.
            member.checked_parts.remove(&part_position);
            member.verified = false;
        } else {
            member
                .parts
                .entry(part_position)
                .or_default()
                .insert(part_relative, len, crc);
        }

        // Layer 1: the part's packed CRC32, as soon as the part is complete.
        //
        // Guarded by the coverage map rather than attempted every time: the
        // composition now walks the runs it was fed instead of reading one
        // merged value, so asking before the part is whole would be a scan per
        // span for an answer that cannot exist yet.
        let part_complete = member
            .covered
            .missing(part_logical_offset, part_len)
            .is_empty();
        let part_value = part_complete
            .then(|| {
                member
                    .parts
                    .get(&part_position)
                    .and_then(|runs| runs.compose(0, part_len))
            })
            .flatten();
        if let Some(value) = part_value {
            member.checked_parts.insert(part_position, value);
            if let Some(expected) = packed_crc32
                && expected != value
                && !self.record_part_checksum_damage(volume_index, member_id, part_position)
            {
                return Err(self.fail(DemotionReason::PartChecksumMismatch));
            }
        }

        self.try_verify_member(member_id)
    }
}
