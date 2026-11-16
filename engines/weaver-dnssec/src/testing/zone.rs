// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};

use super::signer::{name_cmp, sign_rrset};
use super::{EquipmentError, KeyAlgorithm, Record, SigningKey, base32hex, nsec3_hash};
use crate::{Name, Query, RecordType, TrustAnchor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DenialMode {
    Nsec,
    Nsec3 {
        salt: Vec<u8>,
        iterations: u16,
        opt_out: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignatureWindow {
    pub inception: u32,
    pub expiration: u32,
}

impl Default for SignatureWindow {
    fn default() -> Self {
        Self {
            inception: 1_000_000,
            expiration: 2_000_000,
        }
    }
}

/// Mutable synthetic zone. A default zone has one combined signing key.
#[derive(Clone)]
pub struct Zone {
    pub apex: Name,
    pub keys: Vec<SigningKey>,
    pub records: Vec<Record>,
    /// Delegated owner -> DS RDATA values. An empty vector is unsigned.
    pub delegations: BTreeMap<Name, Vec<Vec<u8>>>,
    pub signature_window: SignatureWindow,
    pub denial: DenialMode,
    pub ttl: u32,
    pub unsigned: bool,
    /// Indices into keys; these keys sign the DNSKEY RRset.
    pub dnskey_signers: Vec<usize>,
    /// Indices into keys; these keys sign every other RRset.
    pub record_signers: Vec<usize>,
}

impl Zone {
    pub fn new(apex: Name, algorithm: KeyAlgorithm) -> Result<Self, EquipmentError> {
        let key = SigningKey::generate(algorithm, 257)?;
        let mut zone = Self {
            apex,
            keys: vec![key],
            records: Vec::new(),
            delegations: BTreeMap::new(),
            signature_window: SignatureWindow::default(),
            denial: DenialMode::Nsec,
            ttl: 300,
            unsigned: false,
            dnskey_signers: vec![0],
            record_signers: vec![0],
        };
        // Synthetic SOA/NS, not a published vector. No lookup is ever performed.
        let nameserver = prepend(b"ns", &zone.apex)?;
        let mailbox = prepend(b"hostmaster", &zone.apex)?;
        let mut soa = nameserver.as_wire().to_vec();
        soa.extend(mailbox.as_wire());
        for number in [1_u32, 3600, 300, 86400, 300] {
            soa.extend(number.to_be_bytes());
        }
        zone.add_record(zone.apex.clone(), RecordType::SOA, soa);
        zone.add_record(
            zone.apex.clone(),
            RecordType::NS,
            nameserver.as_wire().to_vec(),
        );
        Ok(zone)
    }

    pub fn add_record(&mut self, owner: Name, rtype: RecordType, rdata: Vec<u8>) -> &mut Self {
        self.records
            .push(Record::new(owner, rtype, self.ttl, rdata));
        self
    }

    pub fn add_key(
        &mut self,
        algorithm: KeyAlgorithm,
        flags: u16,
    ) -> Result<usize, EquipmentError> {
        let tags: BTreeSet<_> = self.keys.iter().map(SigningKey::key_tag).collect();
        let mut key = SigningKey::generate(algorithm, flags)?;
        while tags.contains(&key.key_tag()) {
            key.regenerate()?;
        }
        let index = self.keys.len();
        self.keys.push(key);
        Ok(index)
    }

    pub fn delegate(&mut self, child: Name, ds: Vec<Vec<u8>>) -> &mut Self {
        self.delegations.insert(child, ds);
        self
    }

    pub fn ds_records(&self, digest_type: u8) -> Result<Vec<Vec<u8>>, EquipmentError> {
        self.dnskey_signers
            .iter()
            .map(|index| {
                self.keys
                    .get(*index)
                    .ok_or(EquipmentError::Encoding)?
                    .ds_rdata(&self.apex, digest_type)
            })
            .collect()
    }

    pub fn anchors(&self) -> Result<Vec<TrustAnchor>, EquipmentError> {
        self.dnskey_signers
            .iter()
            .map(|index| {
                self.keys
                    .get(*index)
                    .ok_or(EquipmentError::Encoding)?
                    .anchor(&self.apex)
            })
            .collect()
    }

    pub fn signed_rrset(
        &self,
        owner: &Name,
        rtype: RecordType,
    ) -> Result<Vec<Record>, EquipmentError> {
        let records: Vec<_> = self
            .all_records()
            .into_iter()
            .filter(|record| record.owner == *owner && record.rtype == rtype)
            .collect();
        self.sign_records(records)
    }

    /// Sign supplied records independently, also useful for deliberately false proofs.
    pub fn sign_records(&self, mut records: Vec<Record>) -> Result<Vec<Record>, EquipmentError> {
        if records.is_empty() || self.unsigned {
            return Ok(records);
        }
        let indices = if records
            .first()
            .is_some_and(|record| record.rtype == RecordType::DNSKEY)
        {
            &self.dnskey_signers
        } else {
            &self.record_signers
        };
        let mut signatures = Vec::new();
        for index in indices {
            let key = self.keys.get(*index).ok_or(EquipmentError::Encoding)?;
            signatures.push(sign_rrset(
                &records,
                &self.apex,
                key,
                self.signature_window.inception,
                self.signature_window.expiration,
            )?);
        }
        records.extend(signatures);
        Ok(records)
    }

    fn all_records(&self) -> Vec<Record> {
        let mut records = self.records.clone();
        if !self.unsigned {
            records.extend(self.keys.iter().map(|key| {
                Record::new(
                    self.apex.clone(),
                    RecordType::DNSKEY,
                    self.ttl,
                    key.dnskey_rdata(),
                )
            }));
        }
        for (child, ds) in &self.delegations {
            records.push(Record::new(
                child.clone(),
                RecordType::NS,
                self.ttl,
                self.apex.as_wire().to_vec(),
            ));
            records.extend(
                ds.iter().map(|rdata| {
                    Record::new(child.clone(), RecordType::DS, self.ttl, rdata.clone())
                }),
            );
        }
        records
    }

    fn name_types(&self, empty_nonterminals: bool) -> BTreeMap<Name, BTreeSet<u16>> {
        let mut names: BTreeMap<Name, BTreeSet<u16>> = BTreeMap::new();
        for record in self.all_records() {
            if !record.owner.is_at_or_below(&self.apex) || self.below_delegation(&record.owner) {
                continue;
            }
            names
                .entry(record.owner.clone())
                .or_default()
                .insert(record.rtype.0);
            if empty_nonterminals {
                let mut parent = record.owner.parent();
                while let Some(name) = parent.filter(|name| name.is_at_or_below(&self.apex)) {
                    names.entry(name.clone()).or_default();
                    if name == self.apex {
                        break;
                    }
                    parent = name.parent();
                }
            }
        }
        names.entry(self.apex.clone()).or_default();
        names
    }

    fn below_delegation(&self, owner: &Name) -> bool {
        let mut parent = owner.parent();
        while let Some(name) = parent.filter(|name| name.is_at_or_below(&self.apex)) {
            if self.delegations.contains_key(&name) {
                return true;
            }
            parent = name.parent();
        }
        false
    }

    /// The complete signed denial chain. NSEC3 includes empty non-terminals.
    pub fn denial_records(&self) -> Result<Vec<Record>, EquipmentError> {
        let mut output = Vec::new();
        for record in self.denial_chain()? {
            output.extend(self.sign_records(vec![record])?);
        }
        Ok(output)
    }

    fn denial_chain(&self) -> Result<Vec<Record>, EquipmentError> {
        if self.unsigned {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        match &self.denial {
            DenialMode::Nsec => {
                let names = self.name_types(false);
                let mut names: Vec<_> = names.into_iter().collect();
                names.sort_unstable_by(|a, b| name_cmp(&a.0, &b.0));
                for ((owner, types), (next, _)) in names.iter().zip(names.iter().cycle().skip(1)) {
                    let mut types = types.clone();
                    types.extend([46, 47]);
                    let mut rdata = next.as_wire().to_vec();
                    rdata.extend(bitmap(&types));
                    output.push(Record::new(
                        owner.clone(),
                        RecordType::NSEC,
                        self.ttl,
                        rdata,
                    ));
                }
            }
            DenialMode::Nsec3 {
                salt,
                iterations,
                opt_out,
            } => {
                let names = self.name_types(true);
                let mut hashes = Vec::new();
                for (name, mut types) in names {
                    if *opt_out && self.delegations.get(&name).is_some_and(Vec::is_empty) {
                        continue;
                    }
                    if !types.is_empty()
                        && !(types.contains(&2) && !types.contains(&6) && !types.contains(&43))
                    {
                        types.insert(46);
                    }
                    hashes.push((nsec3_hash(&name, salt, *iterations), types));
                }
                hashes.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                for ((hash, types), (next, _)) in hashes.iter().zip(hashes.iter().cycle().skip(1)) {
                    let owner = prepend(base32hex(hash).as_bytes(), &self.apex)?;
                    let mut rdata = vec![1, u8::from(*opt_out)];
                    rdata.extend(iterations.to_be_bytes());
                    rdata.push(u8::try_from(salt.len()).map_err(|_| EquipmentError::Encoding)?);
                    rdata.extend(salt);
                    rdata.push(u8::try_from(next.len()).map_err(|_| EquipmentError::Encoding)?);
                    rdata.extend(next);
                    rdata.extend(bitmap(types));
                    output.push(Record::new(owner, RecordType::NSEC3, self.ttl, rdata));
                }
            }
        }
        Ok(output)
    }

    /// Pick only the exact or predecessor records needed for this name, its
    /// closest encloser, and its wildcard. Selection is independent of the
    /// validator's denial functions; the ordinary zone chain supplies intervals.
    fn denial_for(&self, name: &Name) -> Result<Vec<Record>, EquipmentError> {
        let chain = self.denial_chain()?;
        if chain.is_empty() {
            return Ok(Vec::new());
        }
        let mut selected = BTreeSet::new();
        match &self.denial {
            DenialMode::Nsec => {
                select_interval(&chain, name, &mut selected);
                let names = self.name_types(true);
                let mut ancestor = Some(name.clone());
                while let Some(candidate) = ancestor.filter(|n| n.is_at_or_below(&self.apex)) {
                    if names.contains_key(&candidate) {
                        if let Ok(wildcard) = prepend(b"*", &candidate) {
                            select_interval(&chain, &wildcard, &mut selected);
                        }
                        break;
                    }
                    ancestor = candidate.parent();
                }
                // A name below an unsigned cut needs that exact parent-side
                // denial rather than an interval speaking for the child.
                let mut ancestor = name.parent();
                while let Some(candidate) = ancestor.filter(|n| n.is_at_or_below(&self.apex)) {
                    if self.delegations.contains_key(&candidate) {
                        select_interval(&chain, &candidate, &mut selected);
                        break;
                    }
                    ancestor = candidate.parent();
                }
            }
            DenialMode::Nsec3 {
                salt, iterations, ..
            } => {
                let hashed_owner = |name: &Name| {
                    prepend(
                        base32hex(&nsec3_hash(name, salt, *iterations)).as_bytes(),
                        &self.apex,
                    )
                };
                select_interval(&chain, &hashed_owner(name)?, &mut selected);
                let mut ancestor = Some(name.clone());
                while let Some(candidate) = ancestor.filter(|n| n.is_at_or_below(&self.apex)) {
                    let owner = hashed_owner(&candidate)?;
                    if let Ok(index) =
                        chain.binary_search_by(|record| name_cmp(&record.owner, &owner))
                    {
                        selected.insert(index);
                        if candidate != *name {
                            let next_closer = name
                                .suffix(candidate.label_count() + 1)
                                .ok_or(EquipmentError::Encoding)?;
                            select_interval(&chain, &hashed_owner(&next_closer)?, &mut selected);
                        }
                        if let Ok(wildcard) = prepend(b"*", &candidate) {
                            select_interval(&chain, &hashed_owner(&wildcard)?, &mut selected);
                        }
                        break;
                    }
                    ancestor = candidate.parent();
                }
            }
        }
        let mut output = Vec::new();
        for index in selected {
            if let Some(record) = chain.get(index) {
                output.extend(self.sign_records(vec![record.clone()])?);
            }
        }
        Ok(output)
    }

    fn exists(&self, name: &Name) -> bool {
        self.name_types(true).contains_key(name)
    }
}

fn select_interval(chain: &[Record], name: &Name, selected: &mut BTreeSet<usize>) {
    let index = match chain.binary_search_by(|record| name_cmp(&record.owner, name)) {
        Ok(index) => Some(index),
        Err(0) => chain.len().checked_sub(1),
        Err(index) => index.checked_sub(1),
    };
    if let Some(index) = index {
        selected.insert(index);
    }
}

fn bitmap(types: &BTreeSet<u16>) -> Vec<u8> {
    let mut windows: BTreeMap<u8, Vec<u8>> = BTreeMap::new();
    for rtype in types {
        let [window, low] = rtype.to_be_bytes();
        let bytes = windows.entry(window).or_default();
        let index = usize::from(low / 8);
        bytes.resize(bytes.len().max(index + 1), 0);
        if let Some(byte) = bytes.get_mut(index) {
            *byte |= 0x80 >> (low % 8);
        }
    }
    let mut output = Vec::new();
    for (window, bytes) in windows {
        output.extend([window, bytes.len() as u8]);
        output.extend(bytes);
    }
    output
}

fn prepend(label: &[u8], suffix: &Name) -> Result<Name, EquipmentError> {
    let mut wire = vec![u8::try_from(label.len()).map_err(|_| EquipmentError::Encoding)?];
    wire.extend(label);
    wire.extend(suffix.as_wire());
    Name::from_wire(&wire).map_err(|_| EquipmentError::Encoding)
}

fn substitute(name: &Name, owner: &Name, target: &Name) -> Result<Name, EquipmentError> {
    let keep = name
        .label_count()
        .checked_sub(owner.label_count())
        .ok_or(EquipmentError::Encoding)?;
    let mut wire = Vec::new();
    for label in name.labels().take(keep) {
        wire.push(u8::try_from(label.len()).map_err(|_| EquipmentError::Encoding)?);
        wire.extend(label);
    }
    wire.extend(target.as_wire());
    Name::from_wire(&wire).map_err(|_| EquipmentError::Encoding)
}

/// Sections are public so attacks can be expressed without byte-offset coupling.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FakeReply {
    pub rcode: u16,
    pub truncated: bool,
    pub answer: Vec<Record>,
    pub authority: Vec<Record>,
    pub additional: Vec<Record>,
}

impl FakeReply {
    pub fn encode(&self, query: &Query) -> Result<Vec<u8>, EquipmentError> {
        let mut bytes = vec![0, 0];
        let flags = 0x8190_u16 | (self.rcode & 15) | if self.truncated { 0x0200 } else { 0 };
        bytes.extend(flags.to_be_bytes());
        bytes.extend(1_u16.to_be_bytes());
        for count in [
            self.answer.len(),
            self.authority.len(),
            self.additional.len() + 1,
        ] {
            bytes.extend(
                u16::try_from(count)
                    .map_err(|_| EquipmentError::Encoding)?
                    .to_be_bytes(),
            );
        }
        bytes.extend(query.name.as_wire());
        bytes.extend(query.rtype.0.to_be_bytes());
        bytes.extend(1_u16.to_be_bytes());
        for record in self
            .answer
            .iter()
            .chain(&self.authority)
            .chain(&self.additional)
        {
            record.encode(&mut bytes)?;
        }
        bytes.extend([0, 0, 41, 4, 208, (self.rcode >> 4) as u8, 0, 128, 0, 0, 0]);
        Ok(bytes)
    }

    pub fn strip_signatures(&mut self) {
        for section in [&mut self.answer, &mut self.authority, &mut self.additional] {
            section.retain(|r| r.rtype != RecordType::RRSIG);
        }
    }
}

type ReplyHook = Box<dyn Fn(&Query, &mut FakeReply) + Send + Sync>;
type BytesHook = Box<dyn Fn(&Query, &mut Vec<u8>) + Send + Sync>;

/// An in-memory recursive resolver. Construction joins each child to its nearest parent.
pub struct FakeResolver {
    zones: Vec<Zone>,
    apex_index: BTreeMap<Name, usize>,
    overrides: BTreeMap<Query, Option<Vec<u8>>>,
    reply_hooks: Vec<ReplyHook>,
    bytes_hooks: Vec<BytesHook>,
}

impl FakeResolver {
    pub fn new(mut zones: Vec<Zone>) -> Result<Self, EquipmentError> {
        let mut apex_index = BTreeMap::new();
        let mut tags = BTreeSet::new();
        let mut replacements: BTreeMap<Name, BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>> =
            BTreeMap::new();
        // DS remapping is scoped to an owner and the complete original digest.
        // Duplicate old keys may retain one copy and regenerate another; retain
        // every corresponding DS so either selected signer still proves the set.
        for (index, zone) in zones.iter_mut().enumerate() {
            if apex_index.insert(zone.apex.clone(), index).is_some() {
                return Err(EquipmentError::ZoneTree);
            }
            for key in &mut zone.keys {
                let old = key.clone();
                while !tags.insert(key.key_tag()) {
                    key.regenerate()?;
                }
                for digest_type in [1, 2, 4] {
                    replacements
                        .entry(zone.apex.clone())
                        .or_default()
                        .entry(old.ds_rdata(&zone.apex, digest_type)?)
                        .or_default()
                        .insert(key.ds_rdata(&zone.apex, digest_type)?);
                }
            }
        }
        for zone in &mut zones {
            for (child, entries) in &mut zone.delegations {
                if let Some(mapping) = replacements.get(child) {
                    let mut rewritten = Vec::new();
                    let mut seen = BTreeSet::new();
                    for entry in entries.iter() {
                        if !seen.insert(entry) {
                            continue;
                        }
                        if let Some(values) = mapping.get(entry) {
                            rewritten.extend(values.iter().cloned());
                        } else {
                            rewritten.push(entry.clone());
                        }
                    }
                    *entries = rewritten;
                }
            }
        }
        let mut links = Vec::new();
        for child in &zones {
            let mut ancestor = child.apex.parent();
            let mut parent_index = None;
            while let Some(candidate) = ancestor {
                if let Some(index) = apex_index.get(&candidate) {
                    parent_index = Some(*index);
                    break;
                }
                ancestor = candidate.parent();
            }
            if let Some(index) = parent_index {
                let ds = if child.unsigned {
                    Vec::new()
                } else {
                    child.ds_records(2)?
                };
                links.push((index, child.apex.clone(), ds));
            }
        }
        for (index, child, ds) in links {
            if let Some(parent) = zones.get_mut(index) {
                parent.delegations.entry(child).or_insert(ds);
            }
        }
        Ok(Self {
            zones,
            apex_index,
            overrides: BTreeMap::new(),
            reply_hooks: Vec::new(),
            bytes_hooks: Vec::new(),
        })
    }

    pub fn zones(&self) -> &[Zone] {
        &self.zones
    }
    pub fn zone_mut(&mut self, apex: &Name) -> Option<&mut Zone> {
        self.zones.get_mut(*self.apex_index.get(apex)?)
    }
    pub fn set_reply(&mut self, query: Query, reply: Option<Vec<u8>>) {
        self.overrides.insert(query, reply);
    }
    pub fn tamper(&mut self, hook: impl Fn(&Query, &mut FakeReply) + Send + Sync + 'static) {
        self.reply_hooks.push(Box::new(hook));
    }
    pub fn tamper_bytes(&mut self, hook: impl Fn(&Query, &mut Vec<u8>) + Send + Sync + 'static) {
        self.bytes_hooks.push(Box::new(hook));
    }

    pub fn answer(&self, query: &Query) -> Option<Vec<u8>> {
        if let Some(reply) = self.overrides.get(query) {
            return reply.clone();
        }
        let mut reply = self.reply(query).ok()?;
        for hook in &self.reply_hooks {
            hook(query, &mut reply);
        }
        let mut bytes = reply.encode(query).ok()?;
        for hook in &self.bytes_hooks {
            hook(query, &mut bytes);
        }
        Some(bytes)
    }

    /// Build a reply before applying hooks; can be reused under another query.
    pub fn reply(&self, query: &Query) -> Result<FakeReply, EquipmentError> {
        let mut reply = FakeReply::default();
        if query.rtype == RecordType::DS {
            let zone = query
                .name
                .parent()
                .as_ref()
                .and_then(|parent| self.zone_for(parent))
                .or_else(|| self.zone_for(&query.name))
                .ok_or(EquipmentError::ZoneTree)?;
            let ds = zone.signed_rrset(&query.name, RecordType::DS)?;
            if !ds.is_empty() {
                reply.answer = ds;
                return Ok(reply);
            }
            let cname = zone.signed_rrset(&query.name, RecordType::CNAME)?;
            if !cname.is_empty() {
                reply.answer = cname;
                return Ok(reply);
            }
            reply.authority = zone.denial_for(&query.name)?;
            return Ok(reply);
        }
        let mut current = query.name.clone();
        let mut seen = BTreeSet::new();
        for _ in 0..=crate::limits::MAX_ALIAS_LINKS + 1 {
            if !seen.insert(current.clone()) {
                return Ok(reply);
            }
            let zone = self.zone_for(&current).ok_or(EquipmentError::ZoneTree)?;
            let records = zone.signed_rrset(&current, query.rtype)?;
            if !records.is_empty() {
                reply.answer.extend(records);
                return Ok(reply);
            }
            let cname = zone.signed_rrset(&current, RecordType::CNAME)?;
            if let Some(record) = cname.first() {
                let target =
                    Name::from_wire(&record.rdata).map_err(|_| EquipmentError::Encoding)?;
                reply.answer.extend(cname);
                current = target;
                continue;
            }
            if let Some(dname) = zone
                .records
                .iter()
                .filter(|record| {
                    record.rtype == RecordType::DNAME
                        && record.owner != current
                        && current.is_at_or_below(&record.owner)
                })
                .max_by_key(|record| record.owner.label_count())
            {
                let target = Name::from_wire(&dname.rdata).map_err(|_| EquipmentError::Encoding)?;
                let target = substitute(&current, &dname.owner, &target)?;
                reply
                    .answer
                    .extend(zone.signed_rrset(&dname.owner, RecordType::DNAME)?);
                reply.answer.push(Record::new(
                    current,
                    RecordType::CNAME,
                    dname.ttl,
                    target.as_wire().to_vec(),
                ));
                current = target;
                continue;
            }
            let mut wildcard = None;
            if !zone.exists(&current) {
                let mut parent = current.parent();
                while let Some(ce) = parent.filter(|name| name.is_at_or_below(&zone.apex)) {
                    if zone.exists(&ce) {
                        wildcard = Some(prepend(b"*", &ce)?);
                        break;
                    }
                    parent = ce.parent();
                }
            }
            if let Some(wildcard) = wildcard.filter(|name| zone.exists(name)) {
                let mut records = zone.signed_rrset(&wildcard, query.rtype)?;
                if records.is_empty() {
                    records = zone.signed_rrset(&wildcard, RecordType::CNAME)?;
                }
                if !records.is_empty() {
                    let cname_target = records
                        .first()
                        .filter(|record| record.rtype == RecordType::CNAME)
                        .map(|record| Name::from_wire(&record.rdata))
                        .transpose()
                        .map_err(|_| EquipmentError::Encoding)?;
                    for record in &mut records {
                        record.owner = current.clone();
                    }
                    reply.answer.extend(records);
                    reply.authority.extend(zone.denial_for(&current)?);
                    if let Some(target) = cname_target {
                        current = target;
                        continue;
                    }
                    return Ok(reply);
                }
            } else if !zone.exists(&current) {
                reply.rcode = 3;
            }
            reply.authority.extend(zone.denial_for(&current)?);
            reply
                .authority
                .extend(zone.signed_rrset(&zone.apex, RecordType::SOA)?);
            return Ok(reply);
        }
        Ok(reply)
    }

    fn zone_for(&self, name: &Name) -> Option<&Zone> {
        let mut ancestor = Some(name.clone());
        while let Some(candidate) = ancestor {
            if let Some(index) = self.apex_index.get(&candidate) {
                return self.zones.get(*index);
            }
            ancestor = candidate.parent();
        }
        None
    }
}
