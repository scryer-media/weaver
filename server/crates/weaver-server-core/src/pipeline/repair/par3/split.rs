use super::*;
use crate::pipeline::direct_store::{ByteRanges, provider::VirtualVolume, router::MemberExtent};
use std::collections::HashMap;

// Posted source IDs occupy the u32 range. A whole split image has its own
// namespace while retaining the carrier index for refresh and retirement.
const WHOLE_SPLIT: u64 = 1 << 32;

pub(super) fn source(file: u32) -> SourceId {
    SourceId(WHOLE_SPLIT | u64::from(file))
}

pub(super) fn file_index(source: SourceId) -> Option<u32> {
    u32::try_from(source.0 & !WHOLE_SPLIT).ok()
}

// A split 7z can still be routed to member partials. Preserve those immutable
// views, including envelope bytes and held runs, without materializing a copy.
fn append_virtual_part(
    image: &VirtualVolume,
    offset: u64,
    len: u64,
    extents: &mut Vec<MemberExtent>,
    partials: &mut HashMap<u32, PathBuf>,
    covered: &mut ByteRanges,
    held: &mut Vec<crate::pipeline::direct_store::provider::HeldRun>,
) -> EngineResult<()> {
    if !image.ciphers.is_empty() {
        return Err(EngineError::InvalidState("encrypted direct split 7z image"));
    }
    let mut ids = HashMap::new();
    for extent in &image.extents {
        if extent.physical_offset >= len {
            continue;
        }
        let member_id = if let Some(id) = ids.get(&extent.member_id) {
            *id
        } else {
            let path = image
                .partials
                .get(&extent.member_id)
                .ok_or(EngineError::InvalidState("split 7z member backing missing"))?;
            let id = u32::try_from(partials.len())
                .map_err(|_| budget::host_limit("embedded PAR3 split backings"))?;
            partials.insert(id, path.clone());
            ids.insert(extent.member_id, id);
            id
        };
        extents.push(MemberExtent {
            member_id,
            physical_offset: offset + extent.physical_offset,
            logical_offset: extent.logical_offset,
            len: extent.len.min(len - extent.physical_offset),
        });
    }
    let envelope_id = u32::try_from(partials.len())
        .map_err(|_| budget::host_limit("embedded PAR3 split backings"))?;
    partials.insert(envelope_id, image.envelope.clone());
    for &(start, end) in image.envelope_covered.ranges() {
        let end = end.min(len);
        let mut cursor = start;
        // Member extents take precedence over envelope bytes in the original
        // reader. Subtract them so the composed extent list stays disjoint.
        let first = image
            .extents
            .partition_point(|extent| extent.physical_offset.saturating_add(extent.len) <= start);
        for extent in image.extents[first..]
            .iter()
            .take_while(|extent| extent.physical_offset < end)
        {
            let next = extent.physical_offset.min(end);
            if cursor < next {
                extents.push(MemberExtent {
                    member_id: envelope_id,
                    physical_offset: offset + cursor,
                    logical_offset: cursor,
                    len: next - cursor,
                });
            }
            cursor = cursor.max(extent.physical_offset.saturating_add(extent.len));
        }
        if cursor < end {
            extents.push(MemberExtent {
                member_id: envelope_id,
                physical_offset: offset + cursor,
                logical_offset: cursor,
                len: end - cursor,
            });
        }
    }
    for &(start, end) in image.covered.ranges() {
        let end = end.min(len);
        if start < end {
            covered.insert(offset + start, end - start);
        }
    }
    for run in image.held.iter().filter(|run| run.start < len) {
        let mut run = run.clone();
        run.len = run.len.min(len - run.start);
        run.start += offset;
        held.push(run);
    }
    Ok(())
}

impl Pipeline {
    // Embedded metadata describes the concatenated archive. Keep the posted
    // parts as immutable backings and translate only their committed ranges;
    // a missing article remains a hole rather than becoming a zero-filled read.
    pub(super) fn split_par3_image(
        &self,
        job: JobId,
        carrier: NzbFileId,
        start: u64,
        local_start: bool,
    ) -> EngineResult<Option<(VirtualVolume, String, u64)>> {
        let Some(state) = self.jobs.get(&job) else {
            return Ok(None);
        };
        let Some(file) = state.assembly.file(carrier) else {
            return Ok(None);
        };
        let name = self.current_filename_for_file(job, file);
        let Some((base, _)) = name.rsplit_once('.') else {
            return Ok(None);
        };
        let FileRole::SevenZipSplit {
            number: carrier_number,
        } = file.role()
        else {
            return Ok(None);
        };
        let carrier_number = *carrier_number;
        let mut parts = BTreeMap::new();
        for part in state.assembly.files() {
            let FileRole::SevenZipSplit { number } = part.role() else {
                continue;
            };
            let number = *number;
            let name = self.current_filename_for_file(job, part);
            if name.rsplit_once('.').map(|(name, _)| name) != Some(base) {
                continue;
            }
            if number as usize >= MAX_CARRIERS {
                return Err(budget::host_limit("embedded PAR3 split parts"));
            }
            let path = state.working_dir.join(name);
            let virtual_part = self.par3_virtual_volume(part.file_id());
            let length = virtual_part.as_ref().map_or_else(
                || std::fs::metadata(&path).map_or(0, |metadata| metadata.len()),
                |volume| volume.len,
            );
            if parts
                .insert(number, (part, path, length, virtual_part))
                .is_some()
            {
                // Reposts need an unambiguous representative before they can
                // become one authenticated source image.
                return Ok(None);
            }
        }
        let Some((&last, (_, _, last_len, _))) = parts.last_key_value() else {
            return Ok(None);
        };
        let chunk = parts
            .iter()
            .filter(|(number, _)| **number < last)
            .map(|(_, (_, _, length, _))| *length)
            .max()
            .unwrap_or(0);
        if chunk == 0 {
            return Ok(None);
        }
        let length = chunk
            .checked_mul(u64::from(last))
            .and_then(|offset| offset.checked_add(*last_len))
            .ok_or(budget::host_limit("embedded PAR3 split length"))?;
        let start = if local_start {
            chunk
                .checked_mul(u64::from(carrier_number))
                .and_then(|offset| offset.checked_add(start))
                .ok_or(budget::host_limit("embedded PAR3 split offset"))?
        } else {
            start
        };
        let mut covered = ByteRanges::new();
        let mut extents = Vec::new();
        let mut partials = HashMap::new();
        let mut held = Vec::new();
        for (number, (part, path, disk_len, virtual_part)) in parts {
            let offset = chunk * u64::from(number);
            let part_len = if number == last { disk_len } else { chunk };
            if let Some(image) = virtual_part {
                append_virtual_part(
                    &image,
                    offset,
                    part_len,
                    &mut extents,
                    &mut partials,
                    &mut covered,
                    &mut held,
                )?;
                continue;
            }
            let id = part.file_id();
            let member_id = u32::try_from(partials.len())
                .map_err(|_| budget::host_limit("embedded PAR3 split backings"))?;
            partials.insert(member_id, path);
            extents.push(MemberExtent {
                member_id,
                physical_offset: offset,
                logical_offset: 0,
                len: part_len,
            });
            let mut ranges = Vec::new();
            if part.is_complete() {
                ranges.push(0..disk_len);
            } else {
                ranges.extend(part.protected_write_ranges().map(|(start, end)| start..end));
                if let Some(runtime) = self.par3_runtime.as_ref()
                    && let Some(materialized) =
                        runtime.materialized_ranges(job, SourceId(u64::from(id.file_index)))?
                {
                    ranges.extend(materialized.iter().cloned());
                }
            }
            for range in ranges {
                let end = range.end.min(disk_len).min(part_len);
                if range.start < end {
                    covered.insert(offset + range.start, end - range.start);
                }
            }
        }
        extents.sort_unstable_by_key(|extent| extent.physical_offset);
        held.sort_unstable_by_key(|run| run.start);
        Ok(Some((
            VirtualVolume {
                volume_index: 0,
                envelope: state.working_dir.join(base),
                extents,
                partials: Arc::new(partials),
                covered,
                envelope_covered: ByteRanges::new(),
                held: Arc::new(held),
                len: length,
                ciphers: Arc::new(HashMap::new()),
            },
            base.to_string(),
            start,
        )))
    }

    // Only an installed and independently verified whole archive can retire
    // the split inputs that supplied its authenticated embedded source.
    pub(super) fn retire_par3_split_inputs(
        &mut self,
        job: JobId,
        source: Option<SourceId>,
        installed: &[par3_rs::session_repair::InstalledFile],
        verified: &[readback::VerifiedOutput],
    ) {
        let Some(state) = self.jobs.get(&job) else {
            return;
        };
        let Some(source) = source else { return };
        let Some(file_index) = file_index(source) else {
            return;
        };
        let Some(carrier) = state.assembly.file(NzbFileId {
            job_id: job,
            file_index,
        }) else {
            return;
        };
        if !matches!(carrier.role(), FileRole::SevenZipSplit { .. }) {
            return;
        }
        let name = self.current_filename_for_file(job, carrier);
        let Some((base, _)) = name.rsplit_once('.') else {
            return;
        };
        let path = state.working_dir.join(base);
        if !installed.iter().any(|output| output.path == path)
            || !verified.iter().any(|output| output.path == path)
        {
            return;
        }
        let parts: Vec<_> = state
            .assembly
            .files()
            .filter_map(|part| {
                if !matches!(part.role(), FileRole::SevenZipSplit { .. }) {
                    return None;
                }
                let name = self.current_filename_for_file(job, part);
                (name.rsplit_once('.').map(|(base, _)| base) == Some(base))
                    .then_some((part.file_id(), name))
            })
            .collect();
        let base = base.to_string();
        self.direct_unpack_abort_set(
            job,
            &base,
            "embedded recovery produced the whole archive",
            crate::pipeline::direct_unpack::wiring::AbortLatch::Permanent,
            crate::pipeline::direct_unpack::wiring::DemotionReason::DownloadEnded,
        );
        let touched = parts.iter().map(|(_, name)| name.clone()).collect();
        self.invalidate_archive_set_for_identity_rebind(job, &base, &touched);
        if let Some(state) = self.jobs.get_mut(&job) {
            state.assembly.remove_archive_topology(&base);
        }
        self.recovery_unposted_outputs
            .entry(job)
            .or_default()
            .superseded
            .extend(parts.iter().map(|(file, _)| *file));
        self.par2_joined_split_sets
            .entry(job)
            .or_default()
            .insert(base, parts.into_iter().map(|(_, name)| name).collect());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::direct_store::provider::{HeldRun, VirtualVolumeReader};
    use std::io::{Read, Seek, SeekFrom};

    #[test]
    fn virtual_parts_preserve_members_envelopes_holds_and_holes() {
        let root = tempfile::tempdir().unwrap();
        let first_path = root.path().join("first");
        let second_path = root.path().join("second");
        let envelope = root.path().join("envelope");
        std::fs::write(&first_path, b"abcd").unwrap();
        std::fs::write(&second_path, b"EFGH").unwrap();
        std::fs::write(&envelope, b"HHxxxxTT").unwrap();
        let mut first = VirtualVolume {
            volume_index: 0,
            envelope: envelope.clone(),
            extents: vec![MemberExtent {
                member_id: 7,
                physical_offset: 2,
                logical_offset: 0,
                len: 4,
            }],
            partials: Arc::new(HashMap::from([(7, first_path)])),
            covered: ByteRanges::new(),
            envelope_covered: ByteRanges::new(),
            held: Arc::new(vec![HeldRun::memory(
                8,
                bytes::Bytes::from_static(b"zz"),
                0,
                2,
            )]),
            len: 10,
            ciphers: Arc::new(HashMap::new()),
        };
        first.covered.insert(0, 8);
        first.envelope_covered.insert(0, 8);
        let mut second = first.clone();
        second.extents = vec![MemberExtent {
            member_id: 7,
            physical_offset: 0,
            logical_offset: 1,
            len: 3,
        }];
        second.partials = Arc::new(HashMap::from([(7, second_path)]));
        second.covered = ByteRanges::new();
        second.covered.insert(0, 3);
        second.envelope_covered = ByteRanges::new();
        second.held = Arc::new(Vec::new());
        second.len = 5;
        let mut combined = second.clone();
        combined.extents.clear();
        combined.covered = ByteRanges::new();
        combined.len = 15;
        let mut paths = HashMap::new();
        let mut holds = Vec::new();
        for (image, offset) in [(&first, 0), (&second, 10)] {
            append_virtual_part(
                image,
                offset,
                image.len,
                &mut combined.extents,
                &mut paths,
                &mut combined.covered,
                &mut holds,
            )
            .unwrap();
        }
        combined
            .extents
            .sort_unstable_by_key(|extent| extent.physical_offset);
        combined.partials = Arc::new(paths);
        combined.held = Arc::new(holds);
        let mut reader = VirtualVolumeReader::<false>::new(combined, Arc::new(Default::default()));
        let mut bytes = [0; 13];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"HHabcdTTzzFGH");
        reader.seek(SeekFrom::Start(13)).unwrap();
        assert!(reader.read(&mut [0; 2]).is_err());
    }
}
