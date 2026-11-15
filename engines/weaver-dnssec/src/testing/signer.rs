// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

use aws_lc_rs::{
    digest,
    encoding::{AsDer, PublicKeyX509Der},
    rand::{SecureRandom, SystemRandom},
    rsa::KeySize,
    signature::{self, KeyPair},
};

use crate::{Name, RecordType, TrustAnchor};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyAlgorithm {
    P256,
    Ed25519,
    Ed448,
    Rsa2048,
    MlDsa44,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EquipmentError {
    #[error("key generation or signing failed")]
    Crypto,
    #[error("invalid test record or key encoding")]
    Encoding,
    #[error("invalid zone tree")]
    ZoneTree,
}

enum PrivateKey {
    P256(signature::EcdsaKeyPair),
    Ed25519(signature::Ed25519KeyPair),
    Ed448(Box<ed448_goldilocks_plus::SigningKey>),
    Rsa(signature::RsaKeyPair),
    MlDsa44(signature::PqdsaKeyPair),
}

/// A generated key and its DNSKEY metadata. Metadata can be changed for attacks.
#[derive(Clone)]
pub struct SigningKey {
    private: Arc<PrivateKey>,
    pub algorithm: KeyAlgorithm,
    pub flags: u16,
    pub protocol: u8,
    pub algorithm_number: u8,
    pub public_key: Vec<u8>,
}

impl SigningKey {
    pub fn generate(algorithm: KeyAlgorithm, flags: u16) -> Result<Self, EquipmentError> {
        let (private, public_key, algorithm_number) = match algorithm {
            KeyAlgorithm::P256 => {
                let key =
                    signature::EcdsaKeyPair::generate(&signature::ECDSA_P256_SHA256_FIXED_SIGNING)
                        .map_err(|_| EquipmentError::Crypto)?;
                let public = key
                    .public_key()
                    .as_ref()
                    .get(1..)
                    .ok_or(EquipmentError::Encoding)?
                    .to_vec();
                (PrivateKey::P256(key), public, 13)
            }
            KeyAlgorithm::Ed25519 => {
                let key =
                    signature::Ed25519KeyPair::generate().map_err(|_| EquipmentError::Crypto)?;
                let public = key.public_key().as_ref().to_vec();
                (PrivateKey::Ed25519(key), public, 15)
            }
            KeyAlgorithm::Ed448 => {
                let mut seed = ed448_goldilocks_plus::SecretKey::default();
                SystemRandom::new()
                    .fill(seed.as_mut())
                    .map_err(|_| EquipmentError::Crypto)?;
                let key = ed448_goldilocks_plus::SigningKey::from(seed);
                let public = key.verifying_key().to_bytes().to_vec();
                (PrivateKey::Ed448(Box::new(key)), public, 16)
            }
            KeyAlgorithm::Rsa2048 => {
                let key = signature::RsaKeyPair::generate(KeySize::Rsa2048)
                    .map_err(|_| EquipmentError::Crypto)?;
                let der: PublicKeyX509Der<'static> = key
                    .public_key()
                    .as_der()
                    .map_err(|_| EquipmentError::Crypto)?;
                let public = rsa_dnskey(der.as_ref())?;
                (PrivateKey::Rsa(key), public, 8)
            }
            KeyAlgorithm::MlDsa44 => {
                let key = signature::PqdsaKeyPair::generate(&signature::ML_DSA_44_SIGNING)
                    .map_err(|_| EquipmentError::Crypto)?;
                let public = key.public_key().as_ref().to_vec();
                (PrivateKey::MlDsa44(key), public, 18)
            }
        };
        Ok(Self {
            private: Arc::new(private),
            algorithm,
            flags,
            protocol: 3,
            algorithm_number,
            public_key,
        })
    }

    pub fn dnskey_rdata(&self) -> Vec<u8> {
        let mut bytes = self.flags.to_be_bytes().to_vec();
        bytes.extend([self.protocol, self.algorithm_number]);
        bytes.extend(&self.public_key);
        bytes
    }

    pub fn key_tag(&self) -> u16 {
        let sum = self
            .dnskey_rdata()
            .iter()
            .enumerate()
            .fold(0_u32, |sum, (index, byte)| {
                sum + if index % 2 == 0 {
                    u32::from(*byte) << 8
                } else {
                    u32::from(*byte)
                }
            });
        (sum + ((sum >> 16) & 0xffff)) as u16
    }

    pub fn ds_rdata(&self, owner: &Name, digest_type: u8) -> Result<Vec<u8>, EquipmentError> {
        let algorithm = match digest_type {
            1 => &digest::SHA1_FOR_LEGACY_USE_ONLY,
            2 => &digest::SHA256,
            4 => &digest::SHA384,
            _ => return Err(EquipmentError::Encoding),
        };
        let mut input = lower_name(owner);
        input.extend(self.dnskey_rdata());
        let mut output = self.key_tag().to_be_bytes().to_vec();
        output.extend([self.algorithm_number, digest_type]);
        output.extend(digest::digest(algorithm, &input).as_ref());
        Ok(output)
    }

    pub fn anchor(&self, owner: &Name) -> Result<TrustAnchor, EquipmentError> {
        let ds = self.ds_rdata(owner, 2)?;
        Ok(TrustAnchor {
            zone: owner.clone(),
            key_tag: self.key_tag(),
            algorithm: self.algorithm_number,
            digest_type: 2,
            digest: ds.get(4..).ok_or(EquipmentError::Encoding)?.to_vec(),
        })
    }

    fn sign(&self, bytes: &[u8]) -> Result<Vec<u8>, EquipmentError> {
        let random = SystemRandom::new();
        match self.private.as_ref() {
            PrivateKey::P256(key) => key
                .sign(&random, bytes)
                .map(|s| s.as_ref().to_vec())
                .map_err(|_| EquipmentError::Crypto),
            PrivateKey::Ed25519(key) => Ok(key.sign(bytes).as_ref().to_vec()),
            PrivateKey::Ed448(key) => Ok(key.sign_raw(bytes).to_bytes().to_vec()),
            PrivateKey::Rsa(key) => {
                let mut signature = vec![0; key.public_modulus_len()];
                key.sign(&signature::RSA_PKCS1_SHA256, &random, bytes, &mut signature)
                    .map_err(|_| EquipmentError::Crypto)?;
                Ok(signature)
            }
            PrivateKey::MlDsa44(key) => {
                let mut signature = vec![0; signature::ML_DSA_44_SIGNING.signature_len()];
                let length = key
                    .sign(bytes, &mut signature)
                    .map_err(|_| EquipmentError::Crypto)?;
                signature.truncate(length);
                Ok(signature)
            }
        }
    }

    pub(crate) fn regenerate(&mut self) -> Result<(), EquipmentError> {
        let replacement = Self::generate(self.algorithm, self.flags)?;
        self.private = replacement.private;
        self.public_key = replacement.public_key;
        Ok(())
    }
}

/// An uncompressed IN-class test resource record, freely editable before encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub owner: Name,
    pub rtype: RecordType,
    pub ttl: u32,
    pub rdata: Vec<u8>,
}

impl Record {
    pub fn new(owner: Name, rtype: RecordType, ttl: u32, rdata: Vec<u8>) -> Self {
        Self {
            owner,
            rtype,
            ttl,
            rdata,
        }
    }

    pub(crate) fn encode(&self, output: &mut Vec<u8>) -> Result<(), EquipmentError> {
        output.extend(self.owner.as_wire());
        output.extend(self.rtype.0.to_be_bytes());
        output.extend(1_u16.to_be_bytes());
        output.extend(self.ttl.to_be_bytes());
        output.extend(
            u16::try_from(self.rdata.len())
                .map_err(|_| EquipmentError::Encoding)?
                .to_be_bytes(),
        );
        output.extend(&self.rdata);
        Ok(())
    }
}

pub(crate) fn sign_rrset(
    records: &[Record],
    signer: &Name,
    key: &SigningKey,
    inception: u32,
    expiration: u32,
) -> Result<Record, EquipmentError> {
    let first = records.first().ok_or(EquipmentError::Encoding)?;
    let labels =
        first.owner.labels().count() - usize::from(first.owner.labels().next() == Some(b"*"));
    let mut prefix = first.rtype.0.to_be_bytes().to_vec();
    prefix.extend([
        key.algorithm_number,
        u8::try_from(labels).map_err(|_| EquipmentError::Encoding)?,
    ]);
    prefix.extend(first.ttl.to_be_bytes());
    prefix.extend(expiration.to_be_bytes());
    prefix.extend(inception.to_be_bytes());
    prefix.extend(key.key_tag().to_be_bytes());
    prefix.extend(lower_name(signer));
    let mut data = Vec::with_capacity(records.len());
    for record in records {
        if record.owner != first.owner || record.rtype != first.rtype || record.ttl != first.ttl {
            return Err(EquipmentError::Encoding);
        }
        data.push(canonical_rdata(record.rtype.0, &record.rdata)?);
    }
    data.sort_unstable();
    data.dedup();
    let mut input = prefix.clone();
    for rdata in data {
        input.extend(lower_name(&first.owner));
        input.extend(first.rtype.0.to_be_bytes());
        input.extend(1_u16.to_be_bytes());
        input.extend(first.ttl.to_be_bytes());
        input.extend(
            u16::try_from(rdata.len())
                .map_err(|_| EquipmentError::Encoding)?
                .to_be_bytes(),
        );
        input.extend(rdata);
    }
    prefix.extend(key.sign(&input)?);
    Ok(Record::new(
        first.owner.clone(),
        RecordType::RRSIG,
        first.ttl,
        prefix,
    ))
}

fn lower_name(name: &Name) -> Vec<u8> {
    name.as_wire().iter().map(u8::to_ascii_lowercase).collect()
}

/// Independent canonical label ordering, used only by the signer equipment.
pub(crate) fn name_cmp(left: &Name, right: &Name) -> core::cmp::Ordering {
    let left: Vec<_> = left
        .labels()
        .map(|l| l.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>())
        .collect();
    let right: Vec<_> = right
        .labels()
        .map(|l| l.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>())
        .collect();
    left.iter().rev().cmp(right.iter().rev())
}

fn canonical_rdata(rtype: u16, input: &[u8]) -> Result<Vec<u8>, EquipmentError> {
    let (fixed, names) = match rtype {
        2 | 3 | 4 | 5 | 7 | 8 | 9 | 12 | 39 | 30 => (0, 1),
        14 | 17 => (0, 2),
        6 => (0, 2),
        15 | 18 | 21 | 36 => (2, 1),
        26 => (2, 2),
        33 => (6, 1),
        24 | 46 => (18, 1),
        35 => (4, 1),
        _ => return Ok(input.to_vec()),
    };
    let mut output = input.get(..fixed).ok_or(EquipmentError::Encoding)?.to_vec();
    let mut rest = input.get(fixed..).ok_or(EquipmentError::Encoding)?;
    if rtype == 35 {
        for _ in 0..3 {
            let length = usize::from(*rest.first().ok_or(EquipmentError::Encoding)?) + 1;
            output.extend(rest.get(..length).ok_or(EquipmentError::Encoding)?);
            rest = rest.get(length..).ok_or(EquipmentError::Encoding)?;
        }
    }
    for _ in 0..names {
        loop {
            let (&length, tail) = rest.split_first().ok_or(EquipmentError::Encoding)?;
            if length > 63 {
                return Err(EquipmentError::Encoding);
            }
            output.push(length);
            output.extend(
                tail.get(..usize::from(length))
                    .ok_or(EquipmentError::Encoding)?
                    .iter()
                    .map(u8::to_ascii_lowercase),
            );
            rest = tail
                .get(usize::from(length)..)
                .ok_or(EquipmentError::Encoding)?;
            if length == 0 {
                break;
            }
        }
    }
    output.extend(rest);
    Ok(output)
}

struct Der<'a>(&'a [u8]);
impl<'a> Der<'a> {
    fn take(&mut self, tag: u8) -> Result<&'a [u8], EquipmentError> {
        let (&actual, rest) = self.0.split_first().ok_or(EquipmentError::Encoding)?;
        if actual != tag {
            return Err(EquipmentError::Encoding);
        }
        let (&first, mut rest) = rest.split_first().ok_or(EquipmentError::Encoding)?;
        let length = if first & 0x80 == 0 {
            usize::from(first)
        } else {
            let count = usize::from(first & 0x7f);
            if count == 0 || count > 4 {
                return Err(EquipmentError::Encoding);
            }
            let bytes = rest.get(..count).ok_or(EquipmentError::Encoding)?;
            if bytes.first() == Some(&0) {
                return Err(EquipmentError::Encoding);
            }
            rest = rest.get(count..).ok_or(EquipmentError::Encoding)?;
            let mut length = 0_usize;
            for byte in bytes {
                length = length
                    .checked_mul(256)
                    .and_then(|n| n.checked_add(usize::from(*byte)))
                    .ok_or(EquipmentError::Encoding)?;
            }
            if length < 128 {
                return Err(EquipmentError::Encoding);
            }
            length
        };
        let (value, remaining) = rest
            .split_at_checked(length)
            .ok_or(EquipmentError::Encoding)?;
        self.0 = remaining;
        Ok(value)
    }
    fn finish(self) -> Result<(), EquipmentError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(EquipmentError::Encoding)
        }
    }
}

fn unsigned_integer(input: &[u8]) -> Result<&[u8], EquipmentError> {
    if input.first().is_none_or(|byte| byte & 0x80 != 0) {
        return Err(EquipmentError::Encoding);
    }
    let mut value = input;
    while value.first() == Some(&0) {
        value = value.get(1..).ok_or(EquipmentError::Encoding)?;
    }
    if value.is_empty() {
        Err(EquipmentError::Encoding)
    } else {
        Ok(value)
    }
}

fn rsa_dnskey(input: &[u8]) -> Result<Vec<u8>, EquipmentError> {
    let mut outer = Der(input);
    let mut spki = Der(outer.take(0x30)?);
    outer.finish()?;
    let mut algorithm = Der(spki.take(0x30)?);
    // rsaEncryption OID, RFC 3279 section 2.3.1.
    if algorithm.take(6)? != [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1]
        || !algorithm.take(5)?.is_empty()
    {
        return Err(EquipmentError::Encoding);
    }
    algorithm.finish()?;
    let (&unused, bits) = spki
        .take(3)?
        .split_first()
        .ok_or(EquipmentError::Encoding)?;
    if unused != 0 {
        return Err(EquipmentError::Encoding);
    }
    spki.finish()?;
    let mut bits = Der(bits);
    let mut rsa = Der(bits.take(0x30)?);
    bits.finish()?;
    let modulus = unsigned_integer(rsa.take(2)?)?;
    let exponent = unsigned_integer(rsa.take(2)?)?;
    rsa.finish()?;
    let mut output = Vec::new();
    if exponent.len() < 256 {
        output.push(u8::try_from(exponent.len()).map_err(|_| EquipmentError::Encoding)?);
    } else {
        output.push(0);
        output.extend(
            u16::try_from(exponent.len())
                .map_err(|_| EquipmentError::Encoding)?
                .to_be_bytes(),
        );
    }
    output.extend(exponent);
    output.extend(modulus);
    Ok(output)
}

pub fn nsec3_hash(name: &Name, salt: &[u8], iterations: u16) -> Vec<u8> {
    let mut input = lower_name(name);
    input.extend(salt);
    let mut hash = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, &input)
        .as_ref()
        .to_vec();
    for _ in 0..iterations {
        let mut input = hash;
        input.extend(salt);
        hash = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, &input)
            .as_ref()
            .to_vec();
    }
    hash
}

pub fn base32hex(input: &[u8]) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuv";
    let mut accumulator = 0_u32;
    let mut bits = 0_u32;
    let mut output = String::new();
    for byte in input {
        accumulator = (accumulator << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            if let Some(byte) = ALPHABET.get(((accumulator >> bits) & 31) as usize) {
                output.push(char::from(*byte));
            }
        }
    }
    if bits > 0
        && let Some(byte) = ALPHABET.get(((accumulator << (5 - bits)) & 31) as usize)
    {
        output.push(char::from(*byte));
    }
    output
}
