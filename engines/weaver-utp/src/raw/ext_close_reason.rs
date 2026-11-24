// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-23.

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct LibTorrentCloseReason(pub u16);

impl std::fmt::Debug for LibTorrentCloseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "close reason: {} ({})", self.description(), self.0)
    }
}

impl LibTorrentCloseReason {
    /// How many groups [`Self::bucket`] sorts reasons into.
    pub const BUCKETS: usize = 19;

    /// The name of each group, by its index.
    pub const BUCKET_LABELS: [&'static str; Self::BUCKETS] = [
        "none",
        "duplicate_peer_id",
        "torrent_removed",
        "no_memory",
        "port_blocked",
        "blocked",
        "upload_to_upload",
        "not_interested_upload_only",
        "timeout",
        "timed_out_interest",
        "timed_out_activity",
        "timed_out_handshake",
        "timed_out_request",
        "protocol_blocked",
        "peer_churn",
        "too_many_connections",
        "too_many_files",
        "protocol_error",
        "other",
    ];

    /// The group a reason is counted under: itself for libtorrent's plain
    /// reasons (0 to 16), one group for its protocol errors (256 to 288),
    /// and one for every other value, so the set stays fixed whatever a
    /// peer sends.
    pub fn bucket(&self) -> usize {
        match self.0 {
            plain @ 0..=16 => plain as usize,
            256..=288 => 17,
            _ => 18,
        }
    }

    pub fn parse(buf: [u8; 4]) -> Self {
        Self(u32::from_be_bytes(buf) as u16)
    }

    pub fn as_bytes(&self) -> [u8; 4] {
        (self.0 as u32).to_be_bytes()
    }

    pub fn description(&self) -> &'static str {
        match self.0 {
            0 => "none",
            1 => "duplicate_peer_id",
            2 => "torrent_removed",
            3 => "no_memory",
            4 => "port_blocked",
            5 => "blocked",
            6 => "upload_to_upload",
            7 => "not_interested_upload_only",
            8 => "timeout",
            9 => "timed_out_interest",
            10 => "timed_out_activity",
            11 => "timed_out_handshake",
            12 => "timed_out_request",
            13 => "protocol_blocked",
            14 => "peer_churn",
            15 => "too_many_connections",
            16 => "too_many_files",
            256 => "encryption_error",
            257 => "invalid_info_hash",
            258 => "self_connection",
            259 => "invalid_metadata",
            260 => "metadata_too_big",
            261 => "message_too_big",
            262 => "invalid_message_id",
            263 => "invalid_message",
            264 => "invalid_piece_message",
            265 => "invalid_have_message",
            266 => "invalid_bitfield_message",
            267 => "invalid_choke_message",
            268 => "invalid_unchoke_message",
            269 => "invalid_interested_message",
            270 => "invalid_not_interested_message",
            271 => "invalid_request_message",
            272 => "invalid_reject_message",
            273 => "invalid_allow_fast_message",
            274 => "invalid_extended_message",
            275 => "invalid_cancel_message",
            276 => "invalid_dht_port_message",
            277 => "invalid_suggest_message",
            278 => "invalid_have_all_message",
            279 => "invalid_dont_have_message",
            280 => "invalid_have_none_message",
            281 => "invalid_pex_message",
            282 => "invalid_metadata_request_message",
            283 => "invalid_metadata_message",
            284 => "invalid_metadata_offset",
            285 => "request_when_choked",
            286 => "corrupt_pieces",
            287 => "pex_message_too_big",
            288 => "pex_too_frequent",
            _ => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LibTorrentCloseReason;

    #[test]
    fn every_reason_falls_in_a_named_group() {
        for plain in 0..=16u16 {
            let reason = LibTorrentCloseReason(plain);
            assert_eq!(
                LibTorrentCloseReason::BUCKET_LABELS[reason.bucket()],
                reason.description()
            );
        }
        for error in [256u16, 257, 288] {
            assert_eq!(
                LibTorrentCloseReason::BUCKET_LABELS[LibTorrentCloseReason(error).bucket()],
                "protocol_error"
            );
        }
        for other in [17u16, 255, 289, u16::MAX] {
            assert_eq!(
                LibTorrentCloseReason::BUCKET_LABELS[LibTorrentCloseReason(other).bucket()],
                "other"
            );
        }
    }
}
