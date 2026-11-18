// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery volume whose front will not come, searched for a packet of a
// set its object holds in the clean bytes past the hole, as they are handed
// over in order. The bytes past a hole start anywhere, most often inside a
// recovery packet as long as a recovery block, so the first packet that
// starts past them can be most of a block away, far past what one offer
// carries. The search runs on across offers: it keeps where it stands and
// at most one header, hashes a packet as its bytes pass, and holds none of
// them. Only a packet of a set the object holds whose hash passes finds
// the volume; a magic alone, a packet of another set, or one whose hash
// fails finds nothing, and the search reads on past it.
//
// Its bound: the bytes start inside one packet at worst, so a packet starts
// within the longest packet the object's sets can hold, and that packet is
// no longer. Past the longest packet with no packet of the object's begun,
// and past the end of the one begun, the volume is none of the object's.

// What a format says of its packets, for a search of its own.
pub(crate) trait Packets {
    type Hasher;
    const MAGIC: &'static [u8];
    // The header's length, and where the hash a header carries begins.
    const HEADER: usize;
    const HASHED_FROM: usize;

    // A whole header, its magic first: the length of the packet it opens
    // when the packet is of a set the object holds; else none.
    fn ours(&self, header: &[u8]) -> Option<u64>;

    // The longest packet a set the object holds can hold. Asked once, as a
    // search starts: the search's bound.
    fn longest(&self) -> u64;

    fn hasher(&self) -> Self::Hasher;
    fn update(hasher: &mut Self::Hasher, bytes: &[u8]);
    // Whether the packet `header` opens, hashed into `hasher`, passes.
    fn passes(hasher: Self::Hasher, header: &[u8]) -> bool;
}

// What a search made of the bytes it was handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Searched {
    // A packet of a set the object holds, its hash passing.
    Found,
    // Nothing yet: the bytes that follow may hold one.
    More,
    // Nothing within the bound: the volume is none of the object's.
    Never,
}

pub(crate) struct Search<H> {
    // The offset of the next byte the search reads.
    next: u64,
    // Bytes read outside a packet being hashed, against the bound.
    read: u64,
    // The longest packet of the object's, as the search started.
    longest: u64,
    // While looking for a magic, the last bytes read, short of one, which a
    // magic may begin in; once a magic is found, the header as far as it
    // has come.
    held: Vec<u8>,
    header: bool,
    // The packet being hashed, and how many of its bytes are still to pass.
    hashing: Option<(H, u64)>,
}

impl<H> Search<H> {
    // A search of the bytes from `at`, for packets no longer than `longest`.
    pub(crate) fn at(at: u64, longest: u64) -> Self {
        Self {
            next: at,
            read: 0,
            longest,
            held: Vec::new(),
            header: false,
            hashing: None,
        }
    }

    // Where the bytes the search reads next start.
    pub(crate) fn next(&self) -> u64 {
        self.next
    }

    // The bytes that follow the ones read so far.
    pub(crate) fn read<P: Packets<Hasher = H>>(&mut self, bytes: &[u8], format: &P) -> Searched {
        self.next += bytes.len() as u64;
        let mut rest = bytes;
        loop {
            if let Some((hasher, left)) = &mut self.hashing {
                let take = (*left).min(rest.len() as u64) as usize;
                P::update(hasher, &rest[..take]);
                *left -= take as u64;
                rest = &rest[take..];
                if *left > 0 {
                    return Searched::More;
                }
                let Some((hasher, _)) = self.hashing.take() else {
                    return Searched::More;
                };
                if P::passes(hasher, &self.held) {
                    return Searched::Found;
                }
                // No packet: the bytes it covered are behind the search, and
                // a packet of the object's after it starts in what follows.
                self.held.clear();
                self.header = false;
                continue;
            }
            if self.past_bound::<P>() {
                return Searched::Never;
            }
            if self.header {
                let take = (P::HEADER - self.held.len()).min(rest.len());
                self.held.extend_from_slice(&rest[..take]);
                self.read += take as u64;
                rest = &rest[take..];
                if self.held.len() < P::HEADER {
                    return Searched::More;
                }
                match format.ours(&self.held) {
                    Some(length) if (P::HEADER as u64..=self.longest).contains(&length) => {
                        let mut hasher = format.hasher();
                        P::update(&mut hasher, &self.held[P::HASHED_FROM..]);
                        self.hashing = Some((hasher, length - P::HEADER as u64));
                    }
                    // Not a packet of the object's: a magic may begin in
                    // its header past its own.
                    _ => {
                        self.held.remove(0);
                        self.header = false;
                    }
                }
                continue;
            }
            // A magic wholly in what is held, or begun there.
            let held = self.held.len();
            let mut joint = std::mem::take(&mut self.held);
            joint.extend_from_slice(&rest[..rest.len().min(P::MAGIC.len() - 1)]);
            if let Some(at) = find(&joint, P::MAGIC).filter(|at| *at < held) {
                joint.truncate(held);
                self.held = joint.split_off(at);
                self.header = true;
                continue;
            }
            joint.truncate(held);
            if let Some(at) = find(rest, P::MAGIC) {
                self.read += at as u64;
                rest = &rest[at..];
                self.header = true;
                continue;
            }
            self.read += rest.len() as u64;
            joint.extend_from_slice(rest);
            let keep = joint.len().saturating_sub(P::MAGIC.len() - 1);
            self.held = joint.split_off(keep);
            return if self.past_bound::<P>() {
                Searched::Never
            } else {
                Searched::More
            };
        }
    }

    // Read past where a packet of the object's would have begun and shown
    // its header.
    fn past_bound<P: Packets>(&self) -> bool {
        self.read > self.longest.saturating_add(P::HEADER as u64)
    }
}

fn find(bytes: &[u8], magic: &[u8]) -> Option<usize> {
    bytes
        .windows(magic.len())
        .position(|window| window == magic)
}

// The searches an object runs, one per volume offered past a hole, by the
// volume's name: a claim that goes on from where one stands reads on, and
// any other starts it again from the bytes it brings. A volume the object
// takes, or one it finds none of its own, ends its search.
pub(crate) struct Searches<H> {
    by_name: std::collections::BTreeMap<String, Search<H>>,
}

impl<H> Default for Searches<H> {
    fn default() -> Self {
        Self {
            by_name: std::collections::BTreeMap::new(),
        }
    }
}

impl<H> Searches<H> {
    // The clean bytes `at` of the volume `name` past its hole, as a claim
    // hands them.
    pub(crate) fn read<P: Packets<Hasher = H>>(
        &mut self,
        name: &str,
        (at, bytes): (u64, &[u8]),
        format: &P,
    ) -> Searched {
        let search = match self.by_name.get_mut(name) {
            Some(search) if search.next() == at => search,
            _ => {
                self.by_name
                    .insert(name.to_owned(), Search::at(at, format.longest()));
                let Some(search) = self.by_name.get_mut(name) else {
                    return Searched::Never;
                };
                search
            }
        };
        let searched = search.read(bytes, format);
        if searched != Searched::More {
            self.by_name.remove(name);
        }
        searched
    }

    // The volume `name` was taken, or can be none of the object's.
    pub(crate) fn end(&mut self, name: &str) {
        self.by_name.remove(name);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_name.len()
    }
}

#[cfg(test)]
mod tests;
