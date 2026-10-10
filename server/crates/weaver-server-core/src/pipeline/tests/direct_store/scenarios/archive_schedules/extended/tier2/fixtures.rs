//! The archive sets tier two posts, built by the matrix's synthetic stored
//! volume writers and the 7z writer.
use super::super::super::super::sevenz_store::schedules::unrepeated_payload;
use super::super::super::super::sevenz_store::{Entry, build_7z_shaped, split_volumes};
pub(in super::super) use super::PASSWORD;
use super::*;

/// What the volumes are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in super::super) enum Container {
    Rar5,
    /// Encrypted data under plain headers.
    Rar5Encrypted,
    /// Encrypted headers: nothing is readable without the password.
    Rar5EncryptedHeaders,
    Rar4,
    SevenZip,
}

/// The member every single-member set carries.
pub(in super::super) const MEMBER: &str = "harbour/lantern.mkv";
/// A 7z entry names no directory in these fixtures.
pub(in super::super) const SEVENZ_MEMBER: &str = "lantern.mkv";

impl Container {
    pub(in super::super) fn member(self) -> &'static str {
        if self == Self::SevenZip {
            SEVENZ_MEMBER
        } else {
            MEMBER
        }
    }

    pub(in super::super) fn password(self) -> Option<String> {
        matches!(self, Self::Rar5Encrypted | Self::Rar5EncryptedHeaders)
            .then(|| PASSWORD.to_string())
    }

    /// `payload` as one member over `count` volumes.
    pub(in super::super) fn volumes(self, payload: &[u8], count: usize) -> Vec<(String, Vec<u8>)> {
        let member = self.member();
        let mut volumes = match self {
            Self::Rar5 => single_member_store_set(member, payload, count),
            Self::Rar5Encrypted => {
                encrypted_store_set(member, payload, count, PASSWORD, Some(PASSWORD), false)
            }
            Self::Rar5EncryptedHeaders => header_encrypted_store_set(
                member,
                payload,
                count,
                PASSWORD,
                HeaderCheck::For(PASSWORD),
            ),
            Self::Rar4 if count > 2 => {
                single_member_rar4_store_set_numbered(member, payload, count)
            }
            Self::Rar4 => single_member_rar4_store_set(member, payload, count),
            Self::SevenZip => {
                let archive = build_7z_shaped(
                    &[Entry::file(SEVENZ_MEMBER, payload.to_vec())],
                    sevenz_turbo::EncoderMethod::COPY,
                    None,
                    false,
                );
                split_volumes(&archive, count)
            }
        };
        if count == 1 && self != Self::SevenZip {
            volumes[0].0 = "silver.horizon.rar".to_string();
        }
        volumes
    }
}

/// Bytes in which no block recurs, so a recovery set has nothing to borrow.
pub(in super::super) fn payload(seed: u64, len: usize) -> Vec<u8> {
    unrepeated_payload(seed, len)
}
