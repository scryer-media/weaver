//! Continuation of the `impl DirectSetRouter` block from `direct_store/router.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl DirectSetRouter {
    // ---- Encrypted members: decrypt at write -------------------------------

    /// Routes one encrypted member slice, decrypting on the way in.
    ///
    /// CBC's structure makes this nearly stateless: decrypting cipher block
    /// *N* needs only cipher block *N−1*, so a router holding spans out of order
    /// can decrypt each one the moment its predecessor has landed. There is no
    /// chain checkpoint to maintain and no forward-only constraint.
    ///
    /// What is left is arithmetic on three pieces, because a slice's edges are
    /// article- and volume-shaped while AES is block-shaped:
    ///
    /// - a **head** partial block, when the slice does not start on a 16-byte
    ///   boundary — its first bytes belong to the previous article or the
    ///   previous *volume*, since a split member's parts are not individually
    ///   block-aligned;
    /// - the **aligned middle**, which is all of the slice for an aligned span
    ///   and is decrypted in one pass;
    /// - a **tail** partial block, symmetric with the head.
    ///
    /// Each edge block is resolved once, by whichever side reaches it first, and
    /// its plaintext is kept in [`MemberCrypt::edge_plain`] for the other side.
    /// That is what stops the two halves of a straddling block from deadlocking:
    /// a drain emits spans **only for the volume it is draining**, so without
    /// the shared plaintext each side would sit holding its half waiting for the
    /// other to route bytes it is not allowed to route.
    ///
    /// Anything that cannot be resolved is simply **not routed**: it stays
    /// `pending` in this volume's staging and rides the existing holds
    /// machinery, bounded by one article per member per gap.
    ///
    /// No new re-drain trigger is needed for that, and it is worth saying why,
    /// because the obvious reading is that a volume whose articles have all
    /// arrived would never be revisited. [`Self::route`] and
    /// [`Self::note_volume_complete`] both stage first and then drain **every**
    /// volume in the set, in ascending order, precisely so a header landing in
    /// one volume can release another's holds. A straddling block therefore
    /// resolves in whichever call brings its missing half: the earlier volume's
    /// drain reads the later one's freshly staged bytes through
    /// [`Self::member_cipher`], and the later volume's own drain — later in the
    /// same loop — finds the plaintext waiting for it.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn route_encrypted_slice(
        &mut self,
        volume_index: u32,
        cursor: u64,
        member_index: usize,
        logical_offset: u64,
        len: u64,
        replace: bool,
        spans: &mut Vec<RoutedSpan>,
        routed: &mut Vec<(u64, u64)>,
    ) -> Result<(), DemotionReason> {
        if len == 0 {
            return Ok(());
        }
        let Some(member_id) = self.member_id_for_layout(member_index) else {
            debug_assert!(
                false,
                "the layout mapped encrypted member {member_index} of {}, which the router never \
                 adopted",
                self.plan.set_name
            );
            return Ok(());
        };
        // The cipher extent, never `unpacked_size`: the stream runs to
        // `align16(unpacked_size)` and every length check that used the declared
        // size would be short by the tail padding.
        let sizes = self.members.get(&member_id).and_then(|member| {
            let crypt = member.crypt.as_ref()?;
            Some((crypt.cipher_size()?, member.unpacked_size))
        });
        let Some((cipher_size, unpacked_size)) = sizes else {
            // No declared size yet — the headers have not reached the one that
            // states it — or a member with no keys, which admission has already
            // demoted for. Either way the bytes stay pending.
            return Ok(());
        };
        let slice_end = logical_offset.saturating_add(len);
        debug_assert!(slice_end <= cipher_size);

        // The cache-invalidation guard, discharged before a byte of this span
        // is resolved. A `replace` span is a PAR2 repair of these very cipher
        // bytes, and both write-side caches still describe the damaged ones:
        // `encrypted_block_plain` below would hand back the *damaged* plaintext
        // for an edge block, and a checkpoint over the rewrite would seed the
        // overlay from cipher the volume no longer holds. Neither goes
        // structurally invalid, so nothing downstream could notice.
        if replace
            && let Some(crypt) = self
                .member_mut(member_id)
                .and_then(|member| member.crypt.as_mut())
        {
            crypt.invalidate_repaired(logical_offset, len);
        }

        let head_block =
            (!logical_offset.is_multiple_of(AES_BLOCK)).then(|| block_floor(logical_offset));
        let mid_start = block_ceil(logical_offset);
        let mid_end = block_floor(slice_end);
        let tail_block = (!slice_end.is_multiple_of(AES_BLOCK)
            && head_block != Some(block_floor(slice_end)))
        .then(|| block_floor(slice_end));

        // `(cipher offset, cipher bytes, plaintext bytes)`, ascending.
        let mut pieces: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();
        let mut held = false;

        for edge in [head_block, tail_block].into_iter().flatten() {
            let from = logical_offset.max(edge);
            let to = slice_end.min(edge.saturating_add(AES_BLOCK));
            let block = self.encrypted_block_plain(member_id, edge);
            let cipher =
                self.staged_bytes_at(volume_index, cursor + (from - logical_offset), to - from);
            match (block, cipher) {
                (Some(block), Some(cipher)) => {
                    let plain = block[(from - edge) as usize..(to - edge) as usize].to_vec();
                    pieces.push((from, cipher, plain));
                }
                _ => held = true,
            }
        }

        if mid_start < mid_end {
            let preceding = self.member_preceding_block(member_id, mid_start);
            let cipher = self.staged_bytes_at(
                volume_index,
                cursor + (mid_start - logical_offset),
                mid_end - mid_start,
            );
            match (preceding, cipher) {
                (Some(preceding), Some(cipher)) => {
                    let mut plain = cipher.clone();
                    let decrypted = self
                        .member_mut(member_id)
                        .and_then(|member| member.crypt.as_mut())
                        .is_some_and(|crypt| {
                            crypt.decrypt_range(mid_start, &preceding, &mut plain)
                        });
                    if decrypted {
                        pieces.push((mid_start, cipher, plain));
                    } else {
                        held = true;
                    }
                }
                _ => held = true,
            }
        }

        if held {
            #[cfg(test)]
            {
                self.blocks_held = self.blocks_held.saturating_add(1);
            }
            // The one new hold shape encryption introduces: a cipher block
            // whose other half has not arrived. Bounded by one article per
            // member per gap, so a large count here says the set is arriving
            // badly out of order, not that the transform is leaking.
            crate::runtime::perf_probe::record(
                "direct_store.encrypted.block_held",
                std::time::Duration::from_nanos(1),
            );
        }
        pieces.sort_by_key(|(start, _, _)| *start);
        for (start, cipher, plain) in pieces {
            let physical = cursor + (start - logical_offset);
            let piece_len = cipher.len() as u64;
            // Everything at or past the declared size is tail padding: real
            // cipher, never a destination byte.
            let destination_len = unpacked_size.saturating_sub(start).min(piece_len);
            self.note_encrypted_member_bytes(
                member_id,
                volume_index,
                start,
                &cipher,
                &plain,
                unpacked_size,
                replace,
            )?;
            if destination_len > 0 {
                self.record_routed_extent(
                    volume_index,
                    MemberExtent {
                        member_id,
                        physical_offset: physical,
                        logical_offset: start,
                        len: destination_len,
                    },
                );
                spans.push(RoutedSpan {
                    destination: DirectDestination::Member { member_id },
                    destination_offset: start,
                    volume_index,
                    source_offset: physical,
                    bytes: plain[..destination_len as usize].to_vec(),
                });
            }
            // The tail padding's **source** bytes. Their plaintext is never a
            // destination byte, but they are real posted bytes with a real
            // physical offset, and leaving them unrouted would stall the
            // volume's coverage floor forever — 0–15 bytes short, at the end of
            // the last part, for the life of the job. They go to the envelope,
            // which is a sparse image of the volume at true physical offsets,
            // so what lands there is exactly what was posted: the one place the
            // last cipher block still exists once the plaintext is on disk.
            if piece_len > destination_len {
                spans.push(RoutedSpan {
                    destination: DirectDestination::Envelope { volume_index },
                    destination_offset: physical + destination_len,
                    volume_index,
                    source_offset: physical + destination_len,
                    bytes: cipher[destination_len as usize..].to_vec(),
                });
            }
            routed.push((physical, piece_len));
        }
        Ok(())
    }

    /// The plaintext of one whole cipher block of an encrypted member.
    ///
    /// Answered from [`MemberCrypt::edge_plain`] when another volume's drain has
    /// already decrypted it, and otherwise assembled: the block's 16 cipher
    /// bytes — which may span two source volumes — plus its CBC predecessor.
    /// `None` means one of those is not here yet, which is a hold, not an error.
    pub(super) fn encrypted_block_plain(
        &mut self,
        member_id: u32,
        block_start: u64,
    ) -> Option<[u8; 16]> {
        if let Some(plain) = self
            .members
            .get(&member_id)
            .and_then(|member| member.crypt.as_ref())
            .and_then(|crypt| crypt.edge_plain(block_start))
        {
            return Some(plain);
        }
        let preceding = self.member_preceding_block(member_id, block_start)?;
        let cipher = self.member_cipher(member_id, block_start, AES_BLOCK)?;
        let mut plain = cipher;
        let crypt = self
            .member_mut(member_id)?
            .crypt
            .as_mut()
            .expect("an encrypted member's crypt state is created with the member");
        if !crypt.decrypt_range(block_start, &preceding, &mut plain) {
            return None;
        }
        let block: [u8; 16] = plain.try_into().ok()?;
        crypt.retain_edge(block_start, block);
        Some(block)
    }

    /// The 16 cipher bytes immediately before `block_start`: the member's IV at
    /// offset 0, a retained checkpoint at a decrypted run's frontier, or — for a
    /// block whose predecessor is still staged — the staged bytes themselves.
    pub(super) fn member_preceding_block(
        &self,
        member_id: u32,
        block_start: u64,
    ) -> Option<[u8; 16]> {
        if let Some(block) = self
            .members
            .get(&member_id)
            .and_then(|member| member.crypt.as_ref())
            .and_then(|crypt| crypt.preceding_block(block_start))
        {
            return Some(block);
        }
        let previous = block_start.checked_sub(AES_BLOCK)?;
        self.member_cipher(member_id, previous, AES_BLOCK)?
            .try_into()
            .ok()
    }

    /// Reads a member-logical (== cipher) range out of whatever source volumes
    /// hold it, through the layout's part table.
    ///
    /// Cross-volume by construction: the 16 bytes before a part's first byte are
    /// the tail of the previous volume's part, and that is the ordinary case for
    /// a split encrypted member. `None` when any byte of the range is not
    /// staged — routed bytes are gone from staging, which is exactly why
    /// [`MemberCrypt`] retains checkpoints and edge plaintext rather than
    /// re-reading them here.
    pub(super) fn member_cipher(
        &self,
        member_id: u32,
        logical_offset: u64,
        len: u64,
    ) -> Option<Vec<u8>> {
        let layout_index = self.layout_index_for_member(member_id)?;
        let member = self.layout_members().get(layout_index)?;
        let end = logical_offset.checked_add(len)?;
        let mut out = Vec::with_capacity(len as usize);
        let mut cursor = logical_offset;
        while cursor < end {
            let mut located = None;
            for part in &member.parts {
                let Some(start) = part.logical_offset else {
                    continue;
                };
                let part_end = start.saturating_add(part.data_size);
                if cursor >= start && cursor < part_end {
                    located = Some((part.volume, part.data_offset + (cursor - start), part_end));
                    break;
                }
            }
            let (volume, physical, part_end) = located?;
            let take = (part_end - cursor).min(end - cursor);
            out.extend_from_slice(&self.staged_bytes_at(volume, physical, take)?);
            cursor += take;
        }
        Some(out)
    }

    /// One volume's staged bytes, or `None` when the range is not wholly staged.
    pub(super) fn staged_bytes_at(
        &self,
        volume_index: u32,
        offset: u64,
        len: u64,
    ) -> Option<Vec<u8>> {
        if len == 0 {
            return Some(Vec::new());
        }
        self.staging
            .get(&volume_index)
            .and_then(|staging| staging.slice(offset, len, &self.scratch))
    }

    /// Feeds one decrypted run into the integrity gates.
    ///
    /// Two layers, two byte spaces, and that split is the whole point:
    ///
    /// - **Layer 1** composes the part's packed hash over **cipher** bytes,
    ///   before decryption. RARLAB `rar` leaves a split member's non-final
    ///   packed checksums *plain* even when it keys the whole-member one, so
    ///   this layer passes over ciphertext whatever the password was: it is a
    ///   wire-integrity check and **not** a wrong-password detector.
    /// - **Layer 2** composes plain CRC32 over the **plaintext** runs and folds
    ///   the result with the KDF hash key when the header keys it. That is the
    ///   real wrong-password backstop, and for a member whose header carries no
    ///   password check it is the *only* one.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn note_encrypted_member_bytes(
        &mut self,
        member_id: u32,
        volume_index: u32,
        cipher_offset: u64,
        cipher: &[u8],
        plain: &[u8],
        unpacked_size: u64,
        replace: bool,
    ) -> Result<(), DemotionReason> {
        let len = cipher.len() as u64;
        if len == 0 {
            return Ok(());
        }
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some(part) = self.part_for(layout_index, volume_index) else {
            return Ok(());
        };
        let (part_position, part_logical_offset, _, _) = part;
        let defer_gates = self.repair_draining;
        let Some(member) = self.member_mut(member_id) else {
            return Ok(());
        };
        let Some(crypt) = member.crypt.as_mut() else {
            return Ok(());
        };
        // Duplicate detection lives in **cipher** space here, not in the
        // destination coverage map: the tail padding is cipher the member routed
        // and destination bytes it never had, so a map that cannot name those
        // offsets cannot tell a duplicate of them from a first arrival.
        if crypt.note_emitted(cipher_offset, len) == 0 && !replace {
            return Ok(());
        }
        crypt.retain_tail_padding(unpacked_size, cipher_offset, plain);
        let destination_len = unpacked_size.saturating_sub(cipher_offset).min(len);
        let cipher_crc = par2_rs::checksum::crc32(cipher);
        let part_relative = cipher_offset.saturating_sub(part_logical_offset);
        if destination_len > 0 {
            let plain_crc = par2_rs::checksum::crc32(&plain[..destination_len as usize]);
            if replace {
                crypt
                    .plain_runs_mut()
                    .overwrite(cipher_offset, destination_len, plain_crc);
            } else {
                crypt
                    .plain_runs_mut()
                    .insert(cipher_offset, destination_len, plain_crc);
            }
        }
        if replace {
            let gaps = member.parts.entry(part_position).or_default().overwrite(
                part_relative,
                len,
                cipher_crc,
            );
            member.stale_gaps = subtract(&member.stale_gaps, cipher_offset, destination_len);
            for (start, end) in gaps {
                member.stale_gaps.insert(
                    start.saturating_add(part_logical_offset),
                    end.saturating_sub(start),
                );
            }
            member.checked_parts.remove(&part_position);
            member.verified = false;
        } else {
            member
                .parts
                .entry(part_position)
                .or_default()
                .insert(part_relative, len, cipher_crc);
        }
        if destination_len > 0 {
            member.covered.insert(cipher_offset, destination_len);
        }

        // A repair's pieces are recorded here and judged together afterwards;
        // see `repair_draining`.
        if defer_gates {
            return Ok(());
        }
        self.gate_part(member_id, volume_index)?;
        self.try_verify_member(member_id)
    }

    /// Layer 1 for the part of `member_id` that lives in `volume_index`: the
    /// part's packed CRC32, composed from the runs the part was fed, the moment
    /// the part is complete.
    ///
    /// Completeness is asked in the space the part's runs live in. An encrypted
    /// member's runs are cipher and cover the tail padding, which the
    /// destination coverage map cannot name, so they are asked of the emitted
    /// cipher coverage; a plain member's runs are its destination bytes, so the
    /// coverage map answers. A part that is not complete, or whose runs do not
    /// tile it — a repair's stale gaps, a hole an article never filled — has no
    /// value yet and is not judged.
    ///
    /// Guarded by that completeness rather than attempted on every run: the
    /// composition walks the runs it was fed instead of reading one merged
    /// value, so asking before the part is whole would be a scan per span for
    /// an answer that cannot exist yet.
    pub(super) fn gate_part(
        &mut self,
        member_id: u32,
        volume_index: u32,
    ) -> Result<(), DemotionReason> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some((part_position, part_logical_offset, part_len, packed_crc32)) =
            self.part_for(layout_index, volume_index)
        else {
            return Ok(());
        };
        let packed_uses_mac = self
            .layout_members()
            .get(layout_index)
            .and_then(|member| member.parts.get(part_position as usize))
            .is_some_and(|part| part.packed_hash_uses_mac);
        let Some(member) = self.member_mut(member_id) else {
            return Ok(());
        };
        let part_complete = match member.crypt.as_ref() {
            Some(crypt) => crypt.emitted_covers(part_logical_offset, part_len),
            None => member
                .covered
                .missing(part_logical_offset, part_len)
                .is_empty(),
        };
        if !part_complete {
            return Ok(());
        }
        let Some(value) = member
            .parts
            .get(&part_position)
            .and_then(|runs| runs.compose(0, part_len))
        else {
            return Ok(());
        };
        member.checked_parts.insert(part_position, value);
        let Some(expected) = packed_crc32 else {
            return Ok(());
        };
        // An encrypted member's value is folded with the hash key when the
        // part's header keys its checksum. A fold that refuses to answer is a
        // mismatch: `Some(expected)` is the only value that passes.
        let composed = member.crypt.as_ref().map_or(Some(value), |crypt| {
            crypt.fold_member_crc(value, packed_uses_mac)
        });
        if composed != Some(expected)
            && !self.record_part_checksum_damage(volume_index, member_id, part_position)
        {
            return Err(self.fail(DemotionReason::PartChecksumMismatch));
        }
        Ok(())
    }

    /// Runs every gate a repair's drain deferred, over the finished rewrite.
    ///
    /// Every member is visited, not only the ones the rewrite touched: the
    /// drain that carried the rewrite also drained every other staged volume,
    /// and a hold it released may have completed a part anywhere in the set.
    /// A part already judged is skipped; a member already verified returns
    /// from its own gate at once.
    pub(super) fn settle_repair_gates(&mut self) -> Result<(), DemotionReason> {
        let member_ids: Vec<u32> = self.members.keys().copied().collect();
        for member_id in member_ids {
            let Some(layout_index) = self.layout_index_for_member(member_id) else {
                continue;
            };
            let parts: Vec<(u32, u32)> = self
                .layout_members()
                .get(layout_index)
                .map(|member| {
                    member
                        .parts
                        .iter()
                        .enumerate()
                        .map(|(position, part)| (position as u32, part.volume))
                        .collect()
                })
                .unwrap_or_default();
            for (position, volume) in parts {
                let judged = self
                    .members
                    .get(&member_id)
                    .is_some_and(|member| member.checked_parts.contains_key(&position));
                if judged {
                    continue;
                }
                self.gate_part(member_id, volume)?;
            }
            self.try_verify_member(member_id)?;
        }
        Ok(())
    }

    /// `(position in chain, logical offset, packed length, packed CRC32)` for
    /// the part of the layout member at `layout_index` living in `volume_index`.
    pub(super) fn part_for(
        &self,
        layout_index: usize,
        volume_index: u32,
    ) -> Option<(u32, u64, u64, Option<u32>)> {
        let member = self.layout_members().get(layout_index)?;
        member
            .parts
            .iter()
            .enumerate()
            .find(|(_, part)| part.volume == volume_index)
            .map(|(position, part)| {
                (
                    position as u32,
                    part.logical_offset.unwrap_or(0),
                    part.data_size,
                    part.packed_crc32,
                )
            })
    }

    /// Layer 2: the whole-member CRC32, composed from the parts in logical
    /// order once every part is complete and the chain has closed.
    pub(super) fn try_verify_member(&mut self, member_id: u32) -> Result<(), DemotionReason> {
        let Some(layout_index) = self.layout_index_for_member(member_id) else {
            return Ok(());
        };
        let Some(layout_member) = self.layout_members().get(layout_index) else {
            return Ok(());
        };
        if !layout_member.chain_complete {
            return Ok(());
        }
        let Some(expected) = layout_member.data_crc32 else {
            // The chain closed with no whole-member CRC32, which the layout
            // reports as `Ineligible`; `check_eligibility` owns that demotion.
            return Ok(());
        };
        let part_lengths: Vec<u64> = layout_member
            .parts
            .iter()
            .map(|part| part.data_size)
            .collect();
        let unpacked_size = layout_member.unpacked_size.unwrap_or(0);
        if self
            .members
            .get(&member_id)
            .is_none_or(|member| member.verified)
        {
            return Ok(());
        }
        // The re-arm rule, stated as a refusal. A member carrying
        // restart-seeded coverage has bytes on disk that no `CrcRuns` in this
        // process ever saw, so there is no composed value for them — and there
        // must not be one until they are re-read. The composition below would
        // already stall on the missing `checked_parts` entry in every shape
        // this can take; saying it here means a future part-granularity change
        // cannot quietly turn "unverifiable" into "verified".
        //
        // The same is true about a repair's stale gaps, and for the same
        // reason: they are covered bytes whose composed value a rewrite threw
        // away, so composing around them would pass the member on the strength
        // of runs that describe a *different* span than the one on disk.
        if self.members.get(&member_id).is_some_and(|member| {
            !member.restart_seeded.is_empty() || !member.stale_gaps.is_empty()
        }) {
            return Ok(());
        }
        // Damage on record in a volume this member spans. The member's own
        // gate must not fire in either direction while that is true: a
        // `MemberChecksumMismatch` here would demote the set for damage the
        // repair is on its way to fix, and a *pass* would be worse still —
        // vouching for a member over bytes an archive-level checksum has
        // already called wrong. The gate re-arms when
        // [`Self::route_repaired`] clears the volume.
        if self.member_spans_damaged_volume(member_id) {
            return Ok(());
        }
        if unpacked_size == 0 {
            // A zero-length stored member. Nothing will ever be routed for it,
            // so the byte-driven gate below can never fire: the first shape
            // returned here and left `verified` false for the life of the job,
            // which is a set that never finalizes, never demotes and keeps its
            // suppressions armed — a permanent zombie.
            //
            // The CRC32 of no bytes is `0x00000000`, which is exactly what RAR
            // writes into an empty member's header, so the same gate closes it:
            // anything else is a header disagreeing with itself.
            if expected != 0 {
                return Err(self.fail(DemotionReason::MemberChecksumMismatch));
            }
            if let Some(member) = self.member_mut(member_id) {
                member.verified = true;
            }
            return Ok(());
        }
        let uses_mac = layout_member.data_hash_uses_mac;
        let Some(member) = self.members.get(&member_id) else {
            return Ok(());
        };
        if member.covered.contiguous_from_zero() < unpacked_size {
            return Ok(());
        }

        // Layer 2 for an encrypted member. Composed over **plaintext**,
        // member-wide rather than per part, then folded with the KDF hash key
        // when the header keys the checksum.
        //
        // This is the real wrong-password gate. Layer 1 above cannot be one: its
        // packed hashes cover cipher bytes and are plain CRC32s on the non-final
        // parts, so they pass identically whatever key the bytes were decrypted
        // with — a wrong password that got past admission (no check in the
        // header, or a forged one) reaches here with every earlier gate green.
        //
        // It deliberately does **not** read `checked_parts`. Those are cipher
        // values, and after a restart the cipher is gone: only the plaintext is
        // on disk, so a re-armed member composes exactly what its re-read
        // produced and nothing else.
        if let Some(crypt) = member.crypt.as_ref() {
            if !crypt.tail_padding_retained() {
                return Ok(());
            }
            let Some(composed) = crypt.plain_runs().compose(0, unpacked_size) else {
                return Ok(());
            };
            if crypt.fold_member_crc(composed, uses_mac) != Some(expected) {
                return Err(self.fail(DemotionReason::MemberChecksumMismatch));
            }
            if let Some(member) = self.member_mut(member_id) {
                member.verified = true;
            }
            return Ok(());
        }

        let mut composed = 0u32;
        for (position, len) in part_lengths.iter().enumerate() {
            let Some(value) = member.checked_parts.get(&(position as u32)).copied() else {
                return Ok(());
            };
            composed = weaver_yenc::crc32_combine(composed, value, *len);
        }
        if composed != expected {
            return Err(self.fail(DemotionReason::MemberChecksumMismatch));
        }
        if let Some(member) = self.member_mut(member_id) {
            member.verified = true;
        }
        Ok(())
    }
}
