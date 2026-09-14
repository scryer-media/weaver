//! Continuation of the `impl DirectSetRouter` block from `direct_store/router.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;
use crate::pipeline::direct_store::provider::HeldRun;

impl DirectSetRouter {
    // ---- Restart ----------------------------------------------------------
    //
    // A restarted set rebuilds its layout from the **cached volume facts**, not
    // from bytes: the header bytes sit below the published floors, so they are
    // not refetched and nothing would re-parse them. Everything the router
    // derives from a parse — members, ids, destinations, the plan digest — comes
    // back from the same `add_volume` calls the live path makes, in ascending
    // volume order so the rebuild is deterministic.
    //
    // What deliberately does **not** come back is any integrity state. Coverage
    // is re-derived (it says which bytes are on disk); `CrcRuns` is not (it would
    // say those bytes are *good*, on the authority of a process that is gone).

    /// Takes the volume facts this run has accepted and not yet cached.
    ///
    /// Drained by the caller, which owns the database, and cleared only by the
    /// take: a failed write leaves nothing dirty, so the cache would go stale
    /// silently. That is deliberate — the caller re-marks on failure, and losing
    /// a fact costs a redownload of that set on the next restart, never a wrong
    /// restore, because the checkpoint's plan digest is computed from the same
    /// facts and a missing one cannot reproduce it.
    pub(crate) fn take_dirty_facts(&mut self) -> Vec<(u32, RarVolumeFacts)> {
        let dirty = std::mem::take(&mut self.dirty_facts);
        dirty
            .into_iter()
            .filter_map(|volume_index| {
                self.volume_facts
                    .get(&volume_index)
                    .map(|facts| (volume_index, facts.clone()))
            })
            .collect()
    }

    /// Puts a volume back in the dirty set after a failed cache write.
    pub(crate) fn remark_dirty_fact(&mut self, volume_index: u32) {
        if self.volume_facts.contains_key(&volume_index) {
            self.dirty_facts.insert(volume_index);
        }
    }

    /// Rebuilds the layout from cached `RarVolumeFacts`.
    ///
    /// The facts are the ones this router itself accepted before the restart, so
    /// re-adding them exercises exactly the paths the live parse does — the
    /// format check, the layout's conflict detection, member adoption, the
    /// collision keys and the chain-close eligibility rule. A set whose cached
    /// facts no longer form a routable archive demotes here, at restore, rather
    /// than after its first refetched article.
    pub(crate) fn restore_layout(
        &mut self,
        facts: &BTreeMap<u32, RarVolumeFacts>,
    ) -> Result<(), DemotionReason> {
        for (volume_index, volume_facts) in facts {
            if !self.plan.volumes.contains_key(volume_index) {
                // The row names a volume this job no longer plans. Refusing is
                // the same stance the checkpoint reader takes on an unknown set.
                return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
            }
            if self.layout.is_none() {
                let format = volume_facts.archive_format();
                if !matches!(format, ArchiveFormat::Rar4 | ArchiveFormat::Rar5) {
                    return Err(self.fail(DemotionReason::UnsupportedFormat));
                }
                self.layout = Some(StoredLayoutBuilder::new(format));
            }
            let added = self
                .layout
                .as_mut()
                .expect("the layout was bound above")
                .add_volume(*volume_index, volume_facts);
            match added {
                Ok(()) => {}
                Err(StoredLayoutError::ConflictingVolume { .. }) => {
                    return Err(self.fail(DemotionReason::ConflictingVolumeFacts));
                }
                Err(StoredLayoutError::FormatMismatch { .. }) => {
                    return Err(self.fail(DemotionReason::FormatMismatch));
                }
            }
            self.volume_facts
                .insert(*volume_index, volume_facts.clone());
            self.member_order_stale = true;
        }
        if self.layout.is_none() {
            return Ok(());
        }
        self.sync_members()?;
        self.check_eligibility()?;
        Ok(())
    }

    /// Seeds one member's coverage from a checkpoint's destination claim.
    ///
    /// `extents` are half-open logical ranges of the member's `.direct.partial`.
    /// They enter [`MemberRouting::covered`], so those bytes are neither
    /// refetched nor re-routed, and [`MemberRouting::restart_seeded`], so the
    /// whole-member gate stays disarmed over them until they are re-read.
    ///
    /// Keyed by the destination's **relative path** rather than by the blob's
    /// member index: the index is an in-run counter, and a set whose volumes
    /// arrived in a different order last run numbered its members differently.
    /// The path is derived from the header name, which is the layout's own key.
    pub(crate) fn restore_member_coverage(
        &mut self,
        relative_partial: &str,
        extents: &[(u64, u64)],
    ) -> Option<u32> {
        let member_id = self.members.iter().find_map(|(member_id, member)| {
            (member.relative_partial == relative_partial).then_some(*member_id)
        })?;
        let member = self.member_mut(member_id)?;
        for (start, end) in extents {
            let len = end.saturating_sub(*start);
            if len == 0 {
                continue;
            }
            member.covered.insert(*start, len);
            member.restart_seeded.insert(*start, len);
            // An encrypted member keeps a second, cipher-space account, because
            // its part completeness and its duplicate filter both live there
            // (the tail padding is cipher the destination map cannot name).
            // Seeded, like `covered`, purely so nothing is re-emitted: the
            // verification claim stays with `restart_seeded` alone.
            if let Some(crypt) = member.crypt.as_mut() {
                crypt.seed_emitted(*start, len);
            }
        }
        Some(member_id)
    }

    /// Seeds one source volume's restored state: what is already on disk, and
    /// whether the volume's header walk is finished.
    ///
    /// Three things depend on this and none of them can be re-derived from
    /// bytes, because the bytes are not coming back:
    ///
    /// - `routed` stops a refetched article from re-staging a range the previous
    ///   run already placed;
    /// - `confirmed` is what lets the drain route the volume's trailing region
    ///   instead of holding it as unproven classification — an unconfirmed volume
    ///   holds every envelope byte at or past `tail_base`, which for a restored
    ///   volume is offset zero, so without this a restart would hold the whole
    ///   volume and demote on the holds budget;
    /// - the routed-extent history is what the hybrid provider reads a virtual
    ///   volume through, and it is a **history**, not a classification.
    ///
    /// The history is re-derived by mapping the covered physical ranges through
    /// the rebuilt layout and clipping each member slice to that member's own
    /// restored claim: a byte is claimed as a member's only when the volume floor
    /// and the destination claim agree it was written, which is the same
    /// both-sides rule the barrier records writes under.
    ///
    /// `decoded_len` is `Some(len)` exactly when the checkpoint calls the volume
    /// complete, and `len` is then the volume's whole decoded length — the same
    /// number as the row's floor, because the published `complete` bit is itself
    /// the conjunction of "the download finished" and "the floor covers all of
    /// it". It is passed as a length rather than a flag so the confirmation
    /// derivation can *check* the claim against the coverage in front of it
    /// instead of trusting a bit.
    pub(crate) fn restore_volume_coverage(
        &mut self,
        volume_index: u32,
        covered: &ByteRanges,
        decoded_len: Option<u64>,
    ) {
        let source_complete = decoded_len.is_some();
        let confirmed = restored_volume_is_confirmed(
            covered,
            decoded_len,
            self.volume_facts
                .get(&volume_index)
                .is_some_and(|facts| facts.more_volumes),
        );
        let tail_base = self
            .volume_facts
            .get(&volume_index)
            .map(|facts| {
                facts
                    .members
                    .iter()
                    .map(|member| member.data_offset.saturating_add(member.data_size))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let ranges: Vec<(u64, u64)> = covered.ranges().to_vec();
        {
            let staging = self.staging.entry(volume_index).or_default();
            for (start, end) in &ranges {
                staging.routed.insert(*start, end - start);
            }
            staging.provisional = true;
            staging.confirmed = confirmed;
            staging.source_complete = source_complete;
            staging.restored = true;
            staging.tail_base = staging.tail_base.max(tail_base);
        }

        for (start, end) in ranges {
            let mut cursor = start;
            for slice in self.map_physical_range(volume_index, start, end - start) {
                // This was a refutable `let ... else` on `MappedSlice::Member`,
                // which `EncryptedMember` slid straight past — silently, and
                // *permanently* silently once `slice_len` grew an
                // `EncryptedMember` arm, since that exhaustiveness was the only
                // thing keeping this site loud. The effect was not a lost byte
                // but a lost **record**: `record_routed_extent` never ran, so
                // the routed-extent history had no claim on those offsets and
                // the post-restart provider answered them out of the envelope —
                // where they are a sparse hole inside its length, which is to
                // say zeros, in a volume whose floor says the bytes are
                // durable.
                //
                // An encrypted member's bytes are at exactly the same
                // coordinates (cipher offset and member-logical offset coincide
                // for a stored member), so the two arms share a body. The one
                // difference needs no code: an encrypted member's final ≤15
                // cipher bytes are tail padding that rode the envelope rather
                // than the destination, and the clip against the member's own
                // restored claim below already refuses to hand them back.
                let (member_index, logical_offset, len) = match slice {
                    MappedSlice::Member {
                        member_index,
                        logical_offset,
                        len,
                    }
                    | MappedSlice::EncryptedMember {
                        member_index,
                        logical_offset,
                        len,
                    } => (member_index, logical_offset, len),
                    MappedSlice::Envelope { .. } | MappedSlice::Unroutable { .. } => {
                        cursor = cursor.saturating_add(slice_len(&slice));
                        continue;
                    }
                };
                let Some(member_id) = self.member_id_for_layout(member_index) else {
                    cursor = cursor.saturating_add(len);
                    continue;
                };
                // Clip to what the member's own claim backs. A gap here is not a
                // contradiction to resolve — it is a byte the previous run's
                // floor covered but whose destination write the checkpoint never
                // claimed, so nothing may read it back.
                let claimed = self
                    .members
                    .get(&member_id)
                    .map(|member| &member.covered)
                    .map(|covered| covered.missing(logical_offset, len))
                    .unwrap_or_else(|| vec![(logical_offset, logical_offset + len)]);
                let mut logical_cursor = logical_offset;
                let logical_end = logical_offset.saturating_add(len);
                let mut runs: Vec<(u64, u64)> = Vec::new();
                for (gap_start, gap_end) in claimed {
                    if gap_start > logical_cursor {
                        runs.push((logical_cursor, gap_start));
                    }
                    logical_cursor = gap_end;
                }
                if logical_cursor < logical_end {
                    runs.push((logical_cursor, logical_end));
                }
                for (run_start, run_end) in runs {
                    self.record_routed_extent(
                        volume_index,
                        MemberExtent {
                            member_id,
                            physical_offset: cursor.saturating_add(run_start - logical_offset),
                            logical_offset: run_start,
                            len: run_end - run_start,
                        },
                    );
                }
                cursor = cursor.saturating_add(len);
            }
        }
    }

    /// The crypt rows the next checkpoint must carry, by member id. Empty for
    /// a set with no encrypted member.
    pub(crate) fn member_crypt_snapshots(&self) -> BTreeMap<u32, crypt::MemberCryptSnapshot> {
        let mut rows = BTreeMap::new();
        for (member_id, member) in &self.members {
            let Some(state) = member.crypt.as_ref() else {
                continue;
            };
            let uses_mac = self
                .layout_index_for_member(*member_id)
                .and_then(|index| self.layout_members().get(index))
                .is_some_and(|layout| layout.data_hash_uses_mac);
            if let Some(row) = state.snapshot(uses_mac) {
                rows.insert(*member_id, row);
            }
        }
        rows
    }

    /// Seeds one restored member's crypt state from its checkpoint row.
    ///
    /// Both directions are a refusal, because both are the same mistake seen
    /// from opposite sides: a row with facts the rebuilt layout does not state
    /// would rebuild a key against the wrong IV or the wrong salt — and every
    /// gate would go on passing, over ciphertext — while a row *missing* for a
    /// member this run classified encrypted means the checkpoint was written by
    /// something that did not know the member was encrypted at all. Demoting
    /// costs a materialization from bytes already on disk. Trusting either one
    /// costs the file.
    pub(crate) fn restore_member_crypt(
        &mut self,
        relative_partial: &str,
        stored: Option<&crypt::MemberCryptSnapshot>,
    ) -> Result<(), DemotionReason> {
        let member_id = self.members.iter().find_map(|(member_id, member)| {
            (member.relative_partial == relative_partial).then_some(*member_id)
        });
        let Some(member_id) = member_id else {
            // Not a member of this run's layout: the claim is dropped by the
            // caller's re-keying, which is the established behaviour.
            return Ok(());
        };
        let uses_mac = self
            .layout_index_for_member(member_id)
            .and_then(|index| self.layout_members().get(index))
            .is_some_and(|layout| layout.data_hash_uses_mac);
        let member = self
            .member_mut(member_id)
            .expect("the member was just located");
        match (member.crypt.as_mut(), stored) {
            (None, None) => Ok(()),
            (Some(crypt), Some(stored)) => match crypt.restore(stored, uses_mac) {
                Ok(()) => Ok(()),
                Err(_) => Err(self.fail(DemotionReason::EncryptedFactsDisagree)),
            },
            _ => Err(self.fail(DemotionReason::EncryptedFactsDisagree)),
        }
    }

    /// The volumes the router holds parsed facts for — the volumes whose bytes
    /// it can classify. The restore seam validates a checkpoint's claims
    /// against this.
    pub(crate) fn fact_volumes(&self) -> std::collections::HashSet<u32> {
        self.volume_facts.keys().copied().collect()
    }

    /// Whether any member is still carrying restart-seeded, unverified coverage.
    pub(crate) fn has_restart_seeded_coverage(&self) -> bool {
        self.members
            .values()
            .any(|member| !member.restart_seeded.is_empty())
    }

    /// The runs of member partials that must be re-read from disk before the
    /// whole-member gates can compose (the "PAR2 absent" arm).
    ///
    /// Split at part boundaries, because the composition is per part, and
    /// returned in `(member, ascending offset)` order so the caller's read is one
    /// forward pass per file rather than a seek per run.
    pub(crate) fn restart_read_plan(&self) -> Vec<RestartReadRun> {
        self.reread_plan(|member| &member.restart_seeded)
    }

    /// Whether any member is carrying a repair's stale composition gaps.
    pub(crate) fn has_stale_gaps(&self) -> bool {
        self.members
            .values()
            .any(|member| !member.stale_gaps.is_empty())
    }

    /// The runs a repair left composed by nothing, in the same shape
    /// [`Self::restart_read_plan`] produces — the two are the same problem
    /// (covered bytes with no value in this process) reached from two
    /// directions, so they share a reader and a re-arm.
    pub(crate) fn stale_gap_read_plan(&self) -> Vec<RestartReadRun> {
        self.reread_plan(|member| &member.stale_gaps)
    }

    /// Select one stale run without allocating a plan or a part-boundary list.
    /// The caller supplies both the I/O stripe and retained path ceilings.
    pub(crate) fn next_stale_gap(
        &self,
        max_bytes: u64,
        max_path_bytes: usize,
    ) -> Result<Option<RestartReadRun>, DemotionReason> {
        if max_bytes == 0 {
            return Err(DemotionReason::RepairGapUnreadable);
        }
        for member_id in &self.member_order {
            let Some(member) = self.members.get(member_id) else {
                continue;
            };
            let Some(&(start, end)) = member.stale_gaps.ranges().first() else {
                continue;
            };
            if member.relative_partial.len() > max_path_bytes {
                return Err(DemotionReason::RepairGapUnreadable);
            }
            let layout = self
                .layout_index_for_member(*member_id)
                .and_then(|index| self.layout_members().get(index))
                .ok_or(DemotionReason::RepairGapUnreadable)?;
            let boundary = layout
                .parts
                .iter()
                .filter_map(|part| {
                    let low = part.logical_offset?;
                    let high = low.checked_add(part.data_size)?;
                    (start >= low && start < high).then_some(high)
                })
                .next()
                .ok_or(DemotionReason::RepairGapUnreadable)?;
            let stop = end.min(boundary).min(start.saturating_add(max_bytes));
            if stop <= start {
                return Err(DemotionReason::RepairGapUnreadable);
            }
            return Ok(Some(RestartReadRun {
                member_id: *member_id,
                relative_partial: member.relative_partial.clone(),
                logical_offset: start,
                len: stop - start,
            }));
        }
        if self.has_stale_gaps() {
            return Err(DemotionReason::RepairGapUnreadable);
        }
        Ok(None)
    }

    pub(super) fn reread_plan(
        &self,
        pick: impl Fn(&MemberRouting) -> &ByteRanges,
    ) -> Vec<RestartReadRun> {
        let mut plan = Vec::new();
        for member_id in &self.member_order {
            let Some(member) = self.members.get(member_id) else {
                continue;
            };
            let ranges = pick(member);
            if ranges.is_empty() {
                continue;
            }
            let boundaries = self.part_boundaries(*member_id);
            for &(start, end) in ranges.ranges() {
                let mut cursor = start;
                while cursor < end {
                    let stop = boundaries
                        .iter()
                        .copied()
                        .find(|boundary| *boundary > cursor)
                        .unwrap_or(end)
                        .min(end);
                    plan.push(RestartReadRun {
                        member_id: *member_id,
                        relative_partial: member.relative_partial.clone(),
                        logical_offset: cursor,
                        len: stop - cursor,
                    });
                    cursor = stop;
                }
            }
        }
        plan
    }

    /// Exclusive logical end offsets of every part of a member's chain.
    pub(super) fn part_boundaries(&self, member_id: u32) -> Vec<u64> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Vec::new();
        };
        let Some(member) = self.layout_members().get(layout_index) else {
            return Vec::new();
        };
        member
            .parts
            .iter()
            .map(|part| {
                part.logical_offset
                    .unwrap_or(0)
                    .saturating_add(part.data_size)
            })
            .collect()
    }

    /// Feeds one re-read run's CRC32 back into the member's composition and
    /// clears it from the restart-seeded set.
    ///
    /// This is the whole re-arm: the value comes from the bytes **on disk now**,
    /// so corruption introduced while the process was down fails the member gate
    /// exactly as a bad article would have.
    ///
    /// # Cannot-locate demotes
    ///
    /// A run whose part the layout cannot place — the member is gone from the
    /// layout, no part covers the offset, the member's routing state has been
    /// dropped — used to return `Ok(())` and leave the seeded range in place.
    /// That reads as success to the caller and as *never verifiable* to
    /// [`Self::try_verify_member`], so the set neither finalizes nor demotes: it
    /// sits there being re-read on every completion check for the life of the
    /// job. None of these are runtime conditions — each one means the layout the
    /// read plan was built from is not the layout in front of us — so each one
    /// demotes and lets the conventional path have the set.
    pub(crate) fn note_restored_member_crc(
        &mut self,
        member_id: u32,
        logical_offset: u64,
        len: u64,
        crc: u32,
    ) -> Result<(), DemotionReason> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Err(self.fail(DemotionReason::RestartRearmUnplaceable));
        };
        let part = self
            .layout_members()
            .get(layout_index)
            .and_then(|member| {
                member.parts.iter().enumerate().find(|(_, part)| {
                    let start = part.logical_offset.unwrap_or(0);
                    logical_offset >= start && logical_offset < start.saturating_add(part.data_size)
                })
            })
            .map(|(position, part)| {
                (
                    position as u32,
                    part.logical_offset.unwrap_or(0),
                    part.data_size,
                    part.packed_crc32,
                    part.volume,
                )
            });
        let Some((part_position, part_logical_offset, part_len, packed_crc32, part_volume)) = part
        else {
            return Err(self.fail(DemotionReason::RestartRearmUnplaceable));
        };
        let Some(member) = self.member_mut(member_id) else {
            return Err(self.fail(DemotionReason::RestartRearmUnplaceable));
        };
        // An encrypted member's re-read produces **plaintext** — that is what
        // is in the partial — so it feeds layer 2's member-wide composition and
        // nothing else. It must not touch `parts`, whose values are cipher
        // CRCs, and it must not be compared against the part's packed hash,
        // which describes cipher bytes this process no longer has. The keyed
        // member fold is what re-verifies the run, exactly as the re-arm
        // intends: the value comes from the bytes on disk now.
        if let Some(crypt) = member.crypt.as_mut() {
            crypt.plain_runs_mut().overwrite(logical_offset, len, crc);
            member.restart_seeded = subtract(&member.restart_seeded, logical_offset, len);
            member.stale_gaps = subtract(&member.stale_gaps, logical_offset, len);
            return self.try_verify_member(member_id);
        }
        // `overwrite`, not `insert`: a stale gap is by construction a *fragment*
        // of a run a repair discarded, and a plain insert would refuse it as
        // overlapping if any neighbour survived. Its own gaps are empty by
        // construction — the run it replaces was already removed — so this
        // cannot cascade.
        let gaps = member.parts.entry(part_position).or_default().overwrite(
            logical_offset.saturating_sub(part_logical_offset),
            len,
            crc,
        );
        debug_assert!(
            gaps.is_empty(),
            "re-reading member {member_id} at {logical_offset} left new stale gaps behind"
        );
        member.restart_seeded = subtract(&member.restart_seeded, logical_offset, len);
        member.stale_gaps = subtract(&member.stale_gaps, logical_offset, len);

        let part_value = member
            .parts
            .get(&part_position)
            .and_then(|runs| runs.compose(0, part_len));
        if let Some(value) = part_value {
            member.checked_parts.insert(part_position, value);
            if let Some(expected) = packed_crc32
                && expected != value
                && !self.record_part_checksum_damage(part_volume, member_id, part_position)
            {
                return Err(self.fail(DemotionReason::PartChecksumMismatch));
            }
        }
        self.try_verify_member(member_id)
    }

    /// Files one emitted member extent into the volume's routing history,
    /// coalescing it with the extent it continues.
    ///
    /// A physical byte is routed at most once *by ordinary routing* —
    /// [`VolumeStaging::stage`] never re-stages a routed range — but a PAR2
    /// repair re-routes bytes the history already holds
    /// ([`VolumeStaging::stage_repaired`]), so the parts already recorded are
    /// subtracted before anything is filed. The history stays disjoint, and a
    /// repair that also fills a range the set never routed (a slice lost to a
    /// missing article) still records that part.
    ///
    /// The subtraction is gated behind an **overlap pre-check**, because this
    /// runs once per emitted member run for the whole life of every set and the
    /// overlapping case is only ever a repair: without the gate, every ordinary
    /// article paid a `Vec` the length of the volume's extent history for a
    /// subtraction that removes nothing. The history is sorted by physical
    /// offset and disjoint, so its ends are monotonic too and one
    /// `partition_point` finds the first extent that could overlap.
    pub(super) fn record_routed_extent(&mut self, volume_index: u32, extent: MemberExtent) {
        if extent.len == 0 {
            return;
        }
        let end = extent.physical_offset.saturating_add(extent.len);
        let overlapping = self
            .routed_extents
            .get(&volume_index)
            .map(|extents| {
                let first = extents.partition_point(|held| {
                    held.physical_offset.saturating_add(held.len) <= extent.physical_offset
                });
                &extents[first..]
            })
            .filter(|extents| {
                extents
                    .first()
                    .is_some_and(|held| held.physical_offset < end)
            });
        let held: Vec<(u64, u64)> = overlapping
            .map(|extents| {
                extents
                    .iter()
                    .map(|held| (held.physical_offset, held.physical_offset + held.len))
                    .collect()
            })
            .unwrap_or_default();
        if !held.is_empty() {
            // Re-routing a physical byte to a *different* destination is not a
            // shape this history can express: the overlapping part is subtracted
            // rather than corrected, so the old destination silently survives.
            // Nothing can produce it — a repair rewrites bytes, never the layout
            // that placed them, and a layout rebuild that moved a member would
            // have demoted the set — so it is asserted rather than handled.
            debug_assert!(
                self.routed_extents
                    .get(&volume_index)
                    .into_iter()
                    .flatten()
                    .filter(|old| {
                        old.physical_offset < end
                            && old.physical_offset.saturating_add(old.len) > extent.physical_offset
                    })
                    .all(|old| {
                        // The offset delta as a *signed* quantity: a member's
                        // logical offset routinely sits below the physical one
                        // (the volume's header comes first), so an unsigned
                        // `checked_sub` would answer `None` on both sides and
                        // make the comparison vacuously true — which is the one
                        // thing an assertion must never be.
                        old.member_id == extent.member_id
                            && i128::from(old.logical_offset) - i128::from(old.physical_offset)
                                == i128::from(extent.logical_offset)
                                    - i128::from(extent.physical_offset)
                    }),
                "volume {volume_index} re-routed the bytes at {} to a different member \
                 destination than the history already holds for them",
                extent.physical_offset
            );
            let mut cursor = extent.physical_offset;
            let mut fresh = Vec::new();
            for (start, stop) in held {
                if stop <= cursor {
                    continue;
                }
                if start >= end {
                    break;
                }
                if start > cursor {
                    fresh.push((cursor, start.min(end)));
                }
                cursor = cursor.max(stop);
                if cursor >= end {
                    break;
                }
            }
            if cursor < end {
                fresh.push((cursor, end));
            }
            for (start, stop) in fresh {
                self.record_fresh_extent(
                    volume_index,
                    MemberExtent {
                        member_id: extent.member_id,
                        physical_offset: start,
                        logical_offset: extent
                            .logical_offset
                            .saturating_add(start - extent.physical_offset),
                        len: stop - start,
                    },
                );
            }
            return;
        }
        self.record_fresh_extent(volume_index, extent);
    }

    /// [`Self::record_routed_extent`] once the range is known to be new.
    pub(super) fn record_fresh_extent(&mut self, volume_index: u32, extent: MemberExtent) {
        if extent.len == 0 {
            return;
        }
        let extents = self.routed_extents.entry(volume_index).or_default();
        let position =
            extents.partition_point(|held| held.physical_offset < extent.physical_offset);
        extents.insert(position, extent);
        // Coalesce forwards, then backwards. A member's bytes arrive article by
        // article and span by span, so without this the history would carry one
        // extent per span and the provider's binary search would walk a list as
        // long as the download.
        if position + 1 < extents.len() && continues(extents[position], extents[position + 1]) {
            extents[position].len = extents[position]
                .len
                .saturating_add(extents[position + 1].len);
            extents.remove(position + 1);
        }
        if let Some(previous) = position.checked_sub(1)
            && continues(extents[previous], extents[position])
        {
            extents[previous].len = extents[previous].len.saturating_add(extents[position].len);
            extents.remove(position);
        }
    }

    /// The physical map of one volume, as the hybrid virtual-volume provider
    /// needs it: every member extent the router **has routed bytes for**, in
    /// physical order, with the logical offset the extent starts at inside its
    /// member's partial.
    ///
    /// Read off the routing history, deliberately **not** off
    /// [`StoredLayoutBuilder::map_physical_range`]'s current answer. The layout
    /// maps a member's packed range to the member only while `routes_direct()`
    /// holds, and that is a running verdict: a `ProvisionallyDirect` member
    /// whose chain closes with a BLAKE2sp digest and no CRC32 becomes
    /// `Ineligible` in the same call that demotes the set, and every byte
    /// already sitting in its `.direct.partial` would suddenly map to the
    /// envelope — where it is a hole inside the file's length, which a plain
    /// `read` answers with zeros. Demotion runs reconstruction, so those zeros
    /// would be written into the volume file under a published floor and never
    /// fetched again.
    ///
    /// The history is what the partials themselves are, so the two cannot
    /// disagree: an extent is here exactly when bytes were written for it.
    pub(crate) fn volume_member_extents(&self, volume_index: u32) -> Vec<MemberExtent> {
        self.routed_extents
            .get(&volume_index)
            .cloned()
            .unwrap_or_default()
    }

    /// The physical ranges of one volume that are staged but not yet routed:
    /// the holds, without their bytes.
    pub(crate) fn held_ranges(&self, volume_index: u32) -> Vec<(u64, u64)> {
        self.staging
            .get(&volume_index)
            .map(|staging| staging.pending.ranges().to_vec())
            .unwrap_or_default()
    }

    /// The holds of one volume as runs a provider reads on demand, ascending by
    /// physical offset.
    ///
    /// Posted bytes, verbatim: an article's yEnc-verified payload waiting for
    /// something before it can be routed — a header the walk has not reached,
    /// or for an encrypted member the other half of a cipher block. A reader
    /// that answers in posted space can serve them exactly as they are, which
    /// is what lets a set carry a hole through a repair: the cipher block on
    /// either side of a lost article is held precisely because its other half
    /// is in the article that never came, and without these the volume reads
    /// as if that block were missing too.
    ///
    /// Nothing is copied here. A run in RAM is shared by reference, and a run
    /// the budget paged out carries a pin on the scratch image and its offset
    /// in it, so the provider's RAM cost is what the holds budget already
    /// bounds and not the size of the holds. Copying instead was how a set
    /// with a gigabyte of holds on disk — a volume whose header article never
    /// came, or an encrypted member above a hole — put that gigabyte back in
    /// RAM the moment its PAR2 pass built a provider. One run per staged chunk
    /// rather than per pending range: the reader treats adjacent runs as one
    /// source anyway, and a chunk is the unit that has a single backing.
    pub(crate) fn held_runs(&self, volume_index: u32) -> Vec<HeldRun> {
        let Some(staging) = self.staging.get(&volume_index) else {
            return Vec::new();
        };
        let pin = self.scratch.pin();
        let mut runs = Vec::new();
        for &(pending_start, pending_end) in staging.pending.ranges() {
            // From the chunk containing the range's first byte, which may
            // start below it, to the last chunk starting inside the range.
            let first_chunk = staging
                .chunks
                .range(..=pending_start)
                .next_back()
                .map(|(start, _)| *start)
                .unwrap_or(pending_start);
            for (&chunk_start, chunk) in staging.chunks.range(first_chunk..pending_end) {
                let start = chunk_start.max(pending_start);
                let end = chunk_start.saturating_add(chunk.len()).min(pending_end);
                if start >= end {
                    continue;
                }
                let inside = start - chunk_start;
                let len = end - start;
                let run = match chunk {
                    StagedChunk::Memory(bytes) => {
                        HeldRun::memory(start, std::sync::Arc::clone(bytes), inside, len)
                    }
                    StagedChunk::Scratch { offset, .. } => {
                        // A scratch chunk with no image to pin cannot happen —
                        // the chunk was written to that image — but a hole is
                        // the honest answer if it ever did, and the pass then
                        // reports damage at bytes that *are* unreadable.
                        let Some(pin) = pin.as_ref() else {
                            continue;
                        };
                        HeldRun::scratch(
                            start,
                            std::sync::Arc::clone(pin),
                            offset.saturating_add(inside),
                            len,
                        )
                    }
                };
                runs.push(run);
            }
        }
        runs
    }

    // There is deliberately no accessor for the router's own routed map. It
    // records what routing *emitted*, spans whose write later failed included,
    // and reading it as coverage is what let a demotion sweep try to read a
    // byte back out of a file that never received it. Everything that needs to
    // know what reached disk asks `DirectSet`, which is told only about writes
    // that returned.
}
