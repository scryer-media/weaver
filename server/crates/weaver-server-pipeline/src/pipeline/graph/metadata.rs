// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{Kind, MemberId};
use weaver_model::files::FileRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFormat {
    Par2,
    Par3,
}

impl RecoveryFormat {
    pub(crate) fn signed_index(kind: Kind, front: &[u8]) -> Option<Self> {
        if kind == Kind::Par2 {
            return weaver_graph::sniff::par2_index(front).then_some(Self::Par2);
        }
        if kind != Kind::Par3 {
            return None;
        }
        use par3_rs::{PacketHeader, PacketType};
        let mut at = 0usize;
        let mut metadata = false;
        while let Some(bytes) = front.get(at..) {
            let Ok(header) = PacketHeader::parse(bytes, at as u64) else {
                break;
            };
            match header.packet_type {
                PacketType::RecoveryData | PacketType::RecoveryExternalData | PacketType::Data => {
                    return None;
                }
                PacketType::Creator
                | PacketType::Comment
                | PacketType::Start
                | PacketType::Root
                | PacketType::File
                | PacketType::ExternalData
                | PacketType::CauchyMatrix
                | PacketType::SparseRandomMatrix
                | PacketType::ExplicitMatrix
                | PacketType::FftMatrix => metadata = true,
                _ => {}
            }
            let Ok(length) = usize::try_from(header.length) else {
                break;
            };
            if length < par3_rs::HEADER_SIZE {
                break;
            }
            at = at.saturating_add(length);
        }
        metadata.then_some(Self::Par3)
    }

    pub fn declared_index(role: &FileRole) -> Option<Self> {
        match role {
            FileRole::Par2 { is_index: true, .. } => Some(Self::Par2),
            FileRole::Par3 { is_index: true } => Some(Self::Par3),
            _ => None,
        }
    }

    pub(crate) fn kind(self) -> Kind {
        match self {
            Self::Par2 => Kind::Par2,
            Self::Par3 => Kind::Par3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataState {
    Unresolved,
    Ready,
    Unavailable,
}

// Reconstructed from root progress and recovered objects. Each declared
// index settles independently, including in mixed-format jobs. Recovery
// volumes discovered without an index use the root's bounded head sampling
// and normal seeks; they never become payload prerequisites here.
#[derive(Default)]
pub struct RecoveryMetadata {
    candidates: Vec<(MemberId, RecoveryFormat, MetadataState)>,
}

impl RecoveryMetadata {
    pub fn observe(&mut self, member: MemberId, format: RecoveryFormat, state: MetadataState) {
        self.candidates.push((member, format, state));
    }

    pub fn bootstrap_files(&self) -> Option<Vec<MemberId>> {
        let mut unresolved: Vec<_> = self
            .candidates
            .iter()
            .filter(|(_, _, state)| *state == MetadataState::Unresolved)
            .map(|(member, _, _)| *member)
            .collect();
        unresolved.sort_unstable();
        unresolved.dedup();
        (!unresolved.is_empty()).then_some(unresolved)
    }
}
