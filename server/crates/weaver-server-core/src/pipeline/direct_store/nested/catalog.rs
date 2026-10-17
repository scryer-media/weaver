use super::seek::Seek;
use super::*;
pub(super) use crate::pipeline::extraction::catalog::MemberCatalog;
use crate::pipeline::extraction::catalog::MemberFact;
use crate::pipeline::extraction::catalog::{
    InputProjection, InputRange, MemberProjection, MemberRange, ProjectionError,
};
use std::ops::Range;

impl MemberProjection for DirectSetRouter {
    fn project_member(
        &self,
        member: u32,
        range: MemberRange,
    ) -> Result<InputProjection, ProjectionError> {
        let end = range
            .offset
            .checked_add(range.len)
            .ok_or(ProjectionError::InvalidRange)?;
        range
            .write_offset
            .checked_add(range.len)
            .ok_or(ProjectionError::InvalidRange)?;
        let mut mapped = ByteRanges::new();
        let mut sources = Vec::new();
        for &volume in self.plan().volumes.keys() {
            for extent in self.volume_member_extents(volume) {
                if extent.member_id != member {
                    continue;
                }
                let start = range.offset.max(extent.logical_offset);
                let extent_end = extent
                    .logical_offset
                    .checked_add(extent.len)
                    .ok_or(ProjectionError::InvalidRange)?;
                let stop = end.min(extent_end);
                if start >= stop {
                    continue;
                }
                if mapped.missing(start, stop - start) != vec![(start, stop)] {
                    return Err(ProjectionError::Overlap);
                }
                mapped.insert(start, stop - start);
                sources.push(InputRange {
                    volume,
                    offset: extent
                        .physical_offset
                        .checked_add(start - extent.logical_offset)
                        .ok_or(ProjectionError::InvalidRange)?,
                    len: stop - start,
                    write_offset: range.write_offset + (start - range.offset),
                });
            }
        }
        if !mapped.missing(range.offset, range.len).is_empty() {
            return Err(ProjectionError::Unmapped);
        }
        Ok(InputProjection::Mapped(sources))
    }
}

impl MemberCatalog for DirectSetRouter {
    fn revision(&self) -> u64 {
        self.member_facts_revision()
    }

    fn members(&self) -> Vec<MemberFact<'_>> {
        let ciphers = self.member_ciphers();
        self.member_partials()
            .into_iter()
            .map(|(id, name, _)| MemberFact {
                id,
                name,
                routable: !ciphers.contains_key(&id),
            })
            .collect()
    }

    fn length(&self, member: u32) -> Option<u64> {
        self.member_declared_length(member)
    }
}

// Names and lengths are the only facts needed to recognize a child archive.
// The returned names are owned because discovery mutates the graph afterwards.
pub(crate) fn archive_candidates(
    catalog: &dyn MemberCatalog,
) -> Vec<(u32, String, SetFormat, u32, bool, Option<u64>)> {
    catalog
        .members()
        .into_iter()
        .filter(|member| member.routable)
        .filter_map(|member| {
            let role = FileRole::from_filename(member.name);
            let (format, volume, whole) = SetFormat::of_role(&role)?;
            Some((
                member.id,
                archive_base_name(member.name, &role)?,
                format,
                volume,
                whole,
                catalog.length(member.id),
            ))
        })
        .collect()
}

// Routing consumes a namespace and transaction admission. It does not need to
// know whether the namespace was decoded, stored, or read from durable files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NamespaceEpoch {
    Direct(u64),
    Decoded(crate::pipeline::extraction::output::channel::StreamOwner),
}

pub(crate) trait RoutingNamespace: MemberProjection {
    fn plan(&self) -> &DirectSetPlan;
    fn resources(&self) -> super::super::router::RouterResources;
    fn generation(&self) -> u64;
    fn live_epoch(&self) -> Result<NamespaceEpoch, ProjectionError>;
    fn validate_input(&self, source: InputRange) -> Result<(), ProjectionError>;
    fn project_write(&self, receipt: NodePlacement) -> Result<InputProjection, ProjectionError>;
    fn member_destination(&self, member: u32) -> Option<PathBuf>;
    // Where this namespace's source carries `range` of one member, in the
    // source's own volumes, or why it cannot say. A stored namespace that has
    // not read the headers placing the range asks for them as well. See
    // [`super::seek`].
    fn lift_seek(&self, member: u32, range: Range<u64>) -> Result<Vec<Seek>, DemotionReason>;
}

impl RoutingNamespace for DirectSetRouter {
    fn member_destination(&self, member: u32) -> Option<PathBuf> {
        self.member_partials()
            .into_iter()
            .find_map(|(id, _, partial)| {
                (id == member).then(|| self.plan().destination_path(partial))
            })
    }

    fn plan(&self) -> &DirectSetPlan {
        self.plan()
    }

    fn resources(&self) -> super::super::router::RouterResources {
        self.resources()
    }

    fn generation(&self) -> u64 {
        0
    }

    fn live_epoch(&self) -> Result<NamespaceEpoch, ProjectionError> {
        Ok(NamespaceEpoch::Direct(self.namespace_identity()))
    }

    fn validate_input(&self, source: InputRange) -> Result<(), ProjectionError> {
        source
            .offset
            .checked_add(source.len)
            .ok_or(ProjectionError::InvalidRange)?;
        source
            .write_offset
            .checked_add(source.len)
            .ok_or(ProjectionError::InvalidRange)?;
        if !self.plan().volumes.contains_key(&source.volume) {
            return Err(ProjectionError::Unmapped);
        }
        Ok(())
    }

    fn project_write(&self, receipt: NodePlacement) -> Result<InputProjection, ProjectionError> {
        let source = InputRange {
            volume: receipt.volume_index,
            offset: receipt.source_offset,
            len: receipt.len,
            write_offset: 0,
        };
        self.validate_input(source)?;
        Ok(InputProjection::Mapped(vec![source]))
    }

    fn lift_seek(&self, member: u32, range: Range<u64>) -> Result<Vec<Seek>, DemotionReason> {
        Ok(super::seek::member_seeks(self, member, range))
    }
}

pub(crate) struct DecodedNamespace<'a> {
    pub(crate) catalog: &'a crate::pipeline::extraction::catalog::decoded::DecodedCatalog,
    pub(crate) plan: &'a DirectSetPlan,
    pub(crate) resources: &'a super::super::router::RouterResources,
    pub(crate) destinations: &'a BTreeMap<u32, PathBuf>,
}

impl MemberCatalog for DecodedNamespace<'_> {
    fn revision(&self) -> u64 {
        self.catalog.revision()
    }

    fn members(&self) -> Vec<MemberFact<'_>> {
        self.catalog.members()
    }

    fn length(&self, member: u32) -> Option<u64> {
        self.catalog.length(member)
    }
}

impl MemberProjection for DecodedNamespace<'_> {
    fn project_member(
        &self,
        member: u32,
        range: MemberRange,
    ) -> Result<InputProjection, ProjectionError> {
        self.catalog.project_member(member, range)
    }
}

impl RoutingNamespace for DecodedNamespace<'_> {
    fn member_destination(&self, member: u32) -> Option<PathBuf> {
        self.destinations.get(&member).cloned()
    }

    fn plan(&self) -> &DirectSetPlan {
        self.plan
    }

    fn resources(&self) -> super::super::router::RouterResources {
        self.resources.clone()
    }

    fn generation(&self) -> u64 {
        self.catalog.generation()
    }

    fn live_epoch(&self) -> Result<NamespaceEpoch, ProjectionError> {
        self.catalog.live_epoch().map(NamespaceEpoch::Decoded)
    }

    fn validate_input(&self, _source: InputRange) -> Result<(), ProjectionError> {
        Err(ProjectionError::Unmapped)
    }

    fn project_write(&self, receipt: NodePlacement) -> Result<InputProjection, ProjectionError> {
        let DirectDestination::Member { member_id } = receipt.destination else {
            return Err(ProjectionError::Unmapped);
        };
        self.project_member(
            member_id,
            MemberRange {
                offset: receipt.destination_offset,
                len: receipt.len,
                write_offset: 0,
            },
        )
    }

    // Decoded bytes exist only once the decoder has produced everything
    // before them.
    fn lift_seek(&self, _member: u32, _range: Range<u64>) -> Result<Vec<Seek>, DemotionReason> {
        Err(DemotionReason::UnseekableParent)
    }
}
