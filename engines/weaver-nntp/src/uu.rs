//! uuencode article decoding.
//!
//! Usenet binaries are overwhelmingly yEnc, but a long tail of posts — and some
//! encoders that never moved on — still use classic uuencode. This module is the
//! decoder for that tail. It is deliberately kept off the yEnc hot path: the
//! fused article decoder only reaches for it on the `=ybegin`-miss branch, so a
//! yEnc article never pays for uuencode support.
//!
//! # Structural facts that shape the design
//!
//! uuencode carries far less information than yEnc:
//!
//! - no per-part byte offsets — a part says nothing about where it belongs,
//! - no per-part checksum — nothing local can be verified,
//! - no declared file size — the total is only known once the last part lands,
//! - no header on continuation parts — they open with bare data lines.
//!
//! Everything downstream follows from that: parts can only be assembled in
//! sequence, and correctness is ultimately PAR2's job rather than the decoder's.
//! This module therefore never rejects an article outright for damaged content.
//! A line it cannot decode sets [`UuDecoder::damaged`] and decoding continues,
//! keeping whatever bytes were recovered so the repair layer has the most
//! material to work with.
//!
//! # Attribution
//!
//! The per-line decode follows the classic UUDECODE formulation (Clem Dye's
//! `UUDECODE.c`, 1998, released under the GPL) by way of the two reference
//! decoders this crate is checked against: NZBGet's `Decoder::DecodeUx` and
//! sabctools' uuencode branch. The detection heuristics — the `M`-line shape,
//! the octal-validated `begin` line, and the short-final-part admission — follow
//! sabctools. Damage handling is stricter here than in either reference: see
//! [`UuDecoder::push_body_line`].

/// Lowest byte value a uuencode payload character may take (`' '`).
const UU_CHAR_MIN: u8 = 32;
/// Highest byte value a uuencode payload character may take (`` '`' ``).
const UU_CHAR_MAX: u8 = 96;

/// Bytes encoded by a full uuencode line. 45 bytes is 15 groups of 3, encoded
/// as 60 characters after the leading length character.
const UU_MAX_LINE_BYTES: usize = 45;

/// Characters a full uuencode line carries after its length character.
const UU_MAX_LINE_CHARS: usize = 60;

/// Width of the padded scratch buffer the vector kernels consume. The 60
/// payload characters of a full line are padded up to this with `' '`, which
/// decodes to zero and lands only in bytes the caller discards.
const UU_KERNEL_CHARS: usize = 64;

/// How many characters a line may be short of the bits it declares before the
/// shortfall is treated as damage rather than as stripped trailing whitespace.
///
/// # Why any shortfall is tolerated
///
/// A space is the sextet zero, so an encoder writing a run of zero bits emits a
/// run of trailing spaces — and mail-to-news gateways, quoted-printable hops
/// and well-meaning agents have been stripping trailing whitespace off text
/// lines for as long as uuencode has existed. The characters that go missing
/// are exactly the ones that decode to nothing, so restoring them as *virtual*
/// spaces reconstructs the original bytes exactly. CPython's `binascii.a2b_uu`
/// does precisely this, and says so in a comment older than most of Usenet:
/// it substitutes zero for the characters that ran out and calls it
/// "some spaces got eaten at end-of-line".
///
/// # Why the tolerance is bounded at three
///
/// Four characters is a whole group. Padding four would fabricate three bytes
/// out of no observed characters at all, which is not reconstruction but
/// invention. Bounding at three keeps at least one real character in every
/// group the decode emits, so every byte produced still rests on something the
/// wire actually carried. Beyond that the line is short for some other reason
/// and takes the salvage-and-flag path, which is what it did before.
///
/// The reference decoders split here, and neither takes the third option of
/// refusing the line: one silently emits a short line (its group loop simply
/// stops once fewer than four characters remain, with no flag), and the other
/// reads past the end of the line. Reconstructing the whitespace is strictly
/// better than both.
const UU_MAX_VIRTUAL_PAD_CHARS: usize = 3;

/// Width of the kernel output buffer. Only the first 48 bytes are meaningful
/// (64 characters decode to 48 bytes); the tail is slack so the x86 kernel's
/// final 16-byte store stays in bounds.
const UU_KERNEL_BYTES: usize = 64;

/// Decode one uuencode character to its 6-bit value.
///
/// Backtick needs no special case: `` '`' `` is 0x60, so `0x60 - 0x20 = 0x40`
/// and the mask takes it to 0 — the same answer the reference decoders reach
/// with an explicit branch. Keeping it branchless is what lets the whole
/// transform become a single vector expression.
#[inline]
const fn uu_sextet(c: u8) -> u8 {
    c.wrapping_sub(b' ') & 0x3F
}

/// Characters a canonical encoder emits for `bytes` bytes of payload: whole
/// four-character groups, the last one zero-padded out.
#[inline]
const fn padded_payload_chars(bytes: usize) -> usize {
    bytes.div_ceil(3) * 4
}

/// The fewest characters that can carry `bytes` bytes.
///
/// Each character carries six bits, so a one-byte tail needs two characters and
/// a two-byte tail needs three — `(bytes * 8).div_ceil(6)`, spelled here as
/// whole groups plus the tail so the group structure stays visible.
///
/// Canonical encoders pad the final group out to four characters, but a family
/// of broken encoders stops as soon as the bits run out. NZBGet decodes those
/// natively: its tail arms read `iptr[0..1]` for a one-byte tail and
/// `iptr[0..2]` for a two-byte tail, never demanding a fourth character. So
/// accepting an unpadded final group is parity with the reference decoders, not
/// leniency — and it matters, because that final line belongs to exactly the
/// vintage of post that uuencode support exists for, which typically ships with
/// no PAR2 to repair a dropped tail.
///
/// This range — `[min_payload_chars, padded_payload_chars]` — also subsumes the
/// broken-encoder length reading attributed to Fredrik Lundh. That reading,
/// `(v * 4 + 5) / 3`, is a *character* count, and it lands inside this envelope
/// wherever the two differ (3 characters for a one-byte tail against a
/// two-character minimum and a four-character padded form; 7 against 6 and 8 for
/// a four-byte payload). Deriving the envelope from the arithmetic covers those
/// shapes without a second speculative reading of the length character.
#[inline]
const fn min_payload_chars(bytes: usize) -> usize {
    (bytes / 3) * 4
        + match bytes % 3 {
            1 => 2,
            2 => 3,
            _ => 0,
        }
}

/// Strip a trailing `\r\n` or `\n` from a raw line.
fn trim_line_ending(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    if end > 0 && line[end - 1] == b'\n' {
        end -= 1;
        if end > 0 && line[end - 1] == b'\r' {
            end -= 1;
        }
    }
    &line[..end]
}

/// Undo NNTP dot-stuffing on a body line.
fn strip_dot_stuffing(line: &[u8]) -> &[u8] {
    if line.starts_with(b"..") {
        &line[1..]
    } else {
        line
    }
}

/// Convert bytes to a string, trying UTF-8 first and falling back to Latin-1.
///
/// Same rule the yEnc header parser applies to `name=`, so a uuencode `begin`
/// name and a yEnc `name=` reach file identity spelled the same way.
fn bytes_to_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        // Latin-1: each byte maps directly to its Unicode code point.
        Err(_) => bytes.iter().map(|&b| b as char).collect(),
    }
}

/// Parse a uuencode `begin <mode> <name>` header line.
///
/// Returns the filename bytes when the line is a well-formed header. The name
/// may be empty (a `begin` line with a mode and nothing after it), which is
/// still a valid signal that the body starts on the next line.
///
/// The mode token is validated as octal digits, which is what keeps ordinary
/// English prose starting with "begin " from being mistaken for a header. This
/// is the **detection** reading, used by [`looks_like_uu`]; once an article has
/// already been claimed as uuencode the mode is read by
/// [`parse_begin_line_engaged`] instead, which does not care.
fn parse_begin_line(line: &[u8]) -> Option<&[u8]> {
    parse_begin_line_with(line, |mode| mode.iter().all(|b| (b'0'..=b'7').contains(b)))
}

/// Parse a `begin` line inside an article that is already known to be uuencode,
/// accepting any digit run as the mode.
///
/// Both reference decoders split the mode reading exactly this way: their
/// *detectors* demand octal before claiming an article, and their *decode*
/// stages then skip the mode token without validating it at all. The strictness
/// is a confidence gate on "is this uuencode?", and once that question is
/// settled it has no further job — a poster who wrote `begin 999` or a decimal
/// mode should not cost the file its name.
///
/// Safe to consult on a body line, which is what weaver does: uuencode payload
/// characters live in `[' ', '`']`, so the lowercase letters in `begin` cannot
/// appear in a valid data line. A body line starting with `begin ` is therefore
/// never a data line that this could steal.
fn parse_begin_line_engaged(line: &[u8]) -> Option<&[u8]> {
    parse_begin_line_with(line, |mode| mode.iter().all(u8::is_ascii_digit))
}

fn parse_begin_line_with(line: &[u8], mode_is_valid: impl Fn(&[u8]) -> bool) -> Option<&[u8]> {
    let rest = line.strip_prefix(b"begin ")?;

    let mode_start = rest.iter().position(|b| *b != b' ')?;
    let rest = &rest[mode_start..];

    let mode_len = rest
        .iter()
        .position(|b| b.is_ascii_whitespace())
        .unwrap_or(rest.len());
    if mode_len == 0 {
        return None;
    }
    if !mode_is_valid(&rest[..mode_len]) {
        return None;
    }

    let after_mode = &rest[mode_len..];
    let name_start = after_mode
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(after_mode.len());
    // The rest of the line is the filename, spaces and all.
    Some(after_mode[name_start..].trim_ascii_end())
}

/// Does this line have the shape of a full uuencode data line?
///
/// A full line is `'M'` (45 bytes) plus 60 payload characters, so 61 characters
/// once the line ending is stripped. Lines down to 58 are admitted as well,
/// because a trailing run of spaces — the sextet zero, which is what a run of
/// zero bits encodes to — is exactly what whitespace-stripping agents eat off
/// the end of a text line. Three is the same bound the decoder reconstructs
/// under, for the same reason: see [`UU_MAX_VIRTUAL_PAD_CHARS`].
///
/// This is where the stripped-whitespace tolerance belongs rather than in
/// [`plausible_uu_data_line`], because here the evidence can carry it. Even at
/// 58 characters the charset check below is examining 57 of them, so what is
/// being admitted is a line that looks overwhelmingly like uuencode and is a
/// few pad characters short — not a two-character line with nothing to check.
///
/// The payload charset check is stricter than the reference detector, which
/// tests only the length and the leading `'M'`. Prose lines of exactly the
/// right length that happen to start with `M` almost always contain lowercase
/// letters, which are outside the uuencode charset, so the check costs nothing
/// and turns a plausible false positive into a miss.
fn full_uu_data_line(line: &[u8]) -> bool {
    const SHORTEST_STRIPPED_FULL_LINE: usize = UU_MAX_LINE_CHARS - UU_MAX_VIRTUAL_PAD_CHARS + 1;

    (SHORTEST_STRIPPED_FULL_LINE..=UU_MAX_LINE_CHARS + 1).contains(&line.len())
        && line[0] == b'M'
        && line[1..]
            .iter()
            .all(|c| (UU_CHAR_MIN..=UU_CHAR_MAX).contains(c))
}

/// Does this line plausibly encode a uuencode part?
///
/// The final part of a multi-part post is short, so it never matches the
/// full-line shape and has to be admitted on its own terms. The line is checked
/// against its own length character: the declared byte count implies a range of
/// acceptable payload lengths — from [`min_payload_chars`] up to
/// [`padded_payload_chars`] — and the line has to carry a payload inside that
/// envelope, made only of uuencode characters, with nothing but padding after
/// it.
///
/// Validating against the widest end of the envelope is enough on its own:
/// padding characters are themselves inside the uuencode charset, so a payload
/// that stops early and pads out still passes the charset check across the whole
/// window.
fn plausible_uu_data_line(line: &[u8]) -> bool {
    let line = strip_dot_stuffing(line);
    if line.len() <= 1 {
        return false;
    }

    let declared = uu_sextet(line[0]) as usize;
    // A zero-length data line carries no evidence that this is uuencode at all —
    // a line of spaces would otherwise claim the article. The terminator check
    // handles the one that legitimately appears.
    if declared == 0 {
        return false;
    }

    let available = line.len() - 1;
    // Deliberately NOT widened by [`UU_MAX_VIRTUAL_PAD_CHARS`], unlike the
    // decoder. Detection is a claim about the whole article and this arm covers
    // short lines, where the charset check has only a handful of characters to
    // work with: admitting a shortfall here would let a two-character line
    // whose first byte happens to declare one to three bytes claim an entire
    // article. The stripped-whitespace tolerance is applied where the evidence
    // can carry it — see [`full_uu_data_line`] — and inside the decoder, which
    // only ever runs on an article something else already claimed.
    if available < min_payload_chars(declared) {
        return false;
    }

    let payload_end = 1 + padded_payload_chars(declared).min(available);
    line[1..payload_end]
        .iter()
        .all(|c| (UU_CHAR_MIN..=UU_CHAR_MAX).contains(c))
        && line[payload_end..].iter().all(|c| *c == b' ' || *c == b'`')
}

/// Would this line start a uuencode article?
///
/// This is the detection entry point. The caller must have already ruled out
/// yEnc: `=ybegin` always wins, and this is only consulted for lines that are
/// not one.
///
/// It answers yes for all three ways a uuencode article can open — an explicit
/// `begin` header, a full data line, or a short data line — because a
/// continuation part of a multi-part post carries no header at all and opens
/// directly with data.
pub fn looks_like_uu(line: &[u8]) -> bool {
    let line = trim_line_ending(line);
    if line.is_empty() {
        return false;
    }
    parse_begin_line(line).is_some() || full_uu_data_line(line) || plausible_uu_data_line(line)
}

/// Is this the line that ends a uuencode body?
///
/// Canonical uuencode closes with a zero-length data line (a single backtick)
/// followed by `end`. Either is taken as the end, matching both reference
/// decoders.
fn end_of_body_line(line: &[u8]) -> bool {
    line == b"`" || line == b"end" || line.starts_with(b"end ")
}

/// Body lines that carry no data and must not be treated as damage.
fn ignorable_body_line(line: &[u8]) -> bool {
    line.is_empty() || line == b"-- " || line.starts_with(b"Posted via ")
}

/// What a uuencode article decoded to.
///
/// Deliberately thin. uuencode declares no offsets, no per-part checksum and no
/// file size, so this is the complete set of facts an article yields: how many
/// bytes came out, what the `begin` line called the file if it had one, and
/// whether anything failed to decode along the way.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UuOutcome {
    /// Bytes this article decoded to.
    pub decoded_len: u64,
    /// Filename from the `begin` header. `None` on a continuation part, which
    /// legitimately carries no header at all.
    pub filename: Option<String>,
    /// At least one line failed to decode. The bytes are still here — see the
    /// module docs for why that is the useful behaviour.
    pub damaged: bool,
    /// A body was entered, i.e. the article really did carry uuencode data.
    pub saw_body: bool,
    /// The body's terminator was seen, so this article ended a file.
    pub ended: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UuState {
    /// Before the body: skipping whatever the poster put above it.
    Preamble,
    /// Inside the body, decoding data lines.
    Body,
    /// Past the terminator; anything further is a trailer and is ignored.
    ///
    /// # Concatenated multi-file articles
    ///
    /// This is where weaver deliberately diverges from one of the reference
    /// decoders. uuencode has no framing above the file, so a single article
    /// can carry `begin … end` twice and hold two files. That decoder's `end`
    /// handling only clears its in-body flag, leaving a later `begin` free to
    /// re-enter the body and append the second file's bytes to the same output
    /// — the two files come back concatenated, with only the first one's name.
    ///
    /// Weaver stays ended. Its file model is one NZB file per assembly, and a
    /// second file's bytes have no assembly to go to: appending them would
    /// silently corrupt the first file's tail with content that belongs
    /// somewhere else, and there is nowhere else to put it. Multi-file
    /// uuencode articles are a vintage shape outside the scope this decoder
    /// exists for, so the honest answer is to decode the first file correctly
    /// and ignore the rest rather than to produce a file that is neither.
    ///
    /// The trailer is ignored without damage: signatures, server banners and a
    /// second `begin` all routinely follow `end`, and none of them says
    /// anything is wrong with the bytes already decoded.
    Ended,
}

/// A line-fed uuencode decoder.
///
/// Feed it whole lines with [`push_line`](Self::push_line), in article order.
/// It skips the preamble, decodes the body, and ignores the trailer. Decoded
/// bytes accumulate in an internal buffer that the caller can drain as it goes.
///
/// The decoder never fails. Lines it cannot make sense of set
/// [`damaged`](Self::damaged) and decoding continues; whether a damaged article
/// is worth keeping is the caller's decision, not the decoder's.
#[derive(Debug)]
pub struct UuDecoder {
    state: UuState,
    output: Vec<u8>,
    filename: Option<String>,
    damaged: bool,
    decoded_len: u64,
    saw_begin: bool,
}

impl Default for UuDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl UuDecoder {
    pub fn new() -> Self {
        Self {
            state: UuState::Preamble,
            output: Vec::new(),
            filename: None,
            damaged: false,
            decoded_len: 0,
            saw_begin: false,
        }
    }

    /// Feed one line. The line may carry its trailing `\r\n`.
    pub fn push_line(&mut self, raw: &[u8]) {
        let line = trim_line_ending(raw);
        match self.state {
            UuState::Preamble => self.push_preamble_line(line),
            UuState::Body => self.push_body_line(line),
            // Trailers — signatures, server banners — follow `end` routinely.
            UuState::Ended => {}
        }
    }

    /// Has the body's terminator been seen?
    pub fn is_ended(&self) -> bool {
        self.state == UuState::Ended
    }

    /// Has a body been entered — that is, did this article carry binary data?
    pub fn saw_body(&self) -> bool {
        self.state != UuState::Preamble
    }

    /// Was an explicit `begin` header seen? False for continuation parts, which
    /// legitimately have none.
    pub fn saw_begin(&self) -> bool {
        self.saw_begin
    }

    /// Did any line fail to decode cleanly?
    pub fn damaged(&self) -> bool {
        self.damaged
    }

    /// Total bytes decoded so far, including bytes already drained.
    pub fn decoded_len(&self) -> u64 {
        self.decoded_len
    }

    /// The filename from the `begin` header, if this article carried one.
    pub fn filename(&self) -> Option<&str> {
        self.filename.as_deref()
    }

    /// Decoded bytes not yet drained.
    pub fn output(&self) -> &[u8] {
        &self.output
    }

    /// Take the decoded bytes accumulated so far, leaving the decoder able to
    /// continue into the same article.
    pub fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    /// Everything this article established, as the product the pipeline routes.
    pub fn outcome(&self) -> UuOutcome {
        UuOutcome {
            decoded_len: self.decoded_len,
            filename: self.filename.clone(),
            damaged: self.damaged,
            saw_body: self.saw_body(),
            ended: self.is_ended(),
        }
    }

    fn push_preamble_line(&mut self, line: &[u8]) {
        if let Some(name) = parse_begin_line(line) {
            if !name.is_empty() {
                self.filename = Some(bytes_to_string(name));
            }
            self.saw_begin = true;
            self.state = UuState::Body;
            // A `begin` line carries no payload of its own.
            return;
        }

        // No header: a continuation part opens straight into data, so a line
        // that stands up to validation on its own enters the body and is then
        // decoded as the body's first line.
        if full_uu_data_line(line) || plausible_uu_data_line(line) {
            self.state = UuState::Body;
            self.push_body_line(line);
        }
        // Anything else is preamble and is skipped.
    }

    fn push_body_line(&mut self, line: &[u8]) {
        if end_of_body_line(line) {
            self.state = UuState::Ended;
            return;
        }
        if ignorable_body_line(line) {
            return;
        }

        // A `begin` line inside a body. Both reference decoders fall through to
        // the data path here and emit a couple of bytes of garbage from the
        // word "begin" itself; weaver takes the name instead and emits nothing,
        // because a valid data line cannot begin with lowercase letters — they
        // are outside the uuencode charset — so there is no real line to steal.
        //
        // The name is only taken if none was seen, which is what makes the
        // second `begin` of a concatenated multi-file article a no-op rather
        // than a rename. See the note on [`UuState::Ended`] for why weaver does
        // not concatenate.
        if let Some(name) = parse_begin_line_engaged(line) {
            self.saw_begin = true;
            if self.filename.is_none() && !name.is_empty() {
                self.filename = Some(bytes_to_string(name));
            }
            return;
        }

        let line = strip_dot_stuffing(line);
        let Some((&length_char, payload)) = line.split_first() else {
            return;
        };

        let declared = uu_sextet(length_char) as usize;

        // Acceptance is the true minimum, not the padded form: an encoder that
        // stops as soon as the bits run out has still emitted every bit of its
        // payload, and both reference decoders read such a line.
        let minimum = min_payload_chars(declared);
        if payload.len() >= minimum {
            self.decode_full_line(declared, payload);
            return;
        }

        // Short by less than one group: the missing characters are taken to be
        // trailing spaces a gateway ate, restored as virtual spaces by the
        // prefilled scratch buffer in `decode_full_line`. A space is the sextet
        // zero, so when that is what happened the reconstruction is exact.
        // See [`UU_MAX_VIRTUAL_PAD_CHARS`] for the bound and why it is three.
        if declared <= UU_MAX_LINE_BYTES && minimum - payload.len() <= UU_MAX_VIRTUAL_PAD_CHARS {
            self.decode_full_line(declared, payload);
            return;
        }

        // Further below the minimum the line cannot carry the bytes it claims.
        // Both reference decoders treat this as a bad line; one drops it
        // silently and the other drops it with a flag. This keeps the flag and
        // additionally salvages the whole groups that are present, so a
        // truncated line contributes its readable prefix instead of nothing —
        // the repair layer is better served by partial data than by a hole.
        self.damaged = true;
        self.decode_salvage(declared, payload);
    }

    /// Decode a line whose payload carries every bit it declares, or is short of
    /// them by less than one group's worth of stripped trailing whitespace.
    fn decode_full_line(&mut self, declared: usize, payload: &[u8]) {
        debug_assert!(
            payload.len() + UU_MAX_VIRTUAL_PAD_CHARS >= min_payload_chars(declared)
                || declared > UU_MAX_LINE_BYTES
        );

        if declared > UU_MAX_LINE_BYTES {
            // Over-long declared lengths are outside the format. They are rare
            // enough not to be worth a vector path, and the scalar loop handles
            // an arbitrary count.
            self.decode_salvage(declared, payload);
            return;
        }

        // The scratch buffer is prefilled with `' '`, which decodes to zero, so
        // an unpadded final group needs no special case: copying only the
        // characters that are there leaves the absent ones reading as the pad
        // the encoder omitted. The bytes those positions would have influenced
        // are past `declared` and never taken. Trailing garbage past the padded
        // form is clamped away by the same expression.
        let used = padded_payload_chars(declared).min(payload.len());
        let mut chars = [b' '; UU_KERNEL_CHARS];
        chars[..used].copy_from_slice(&payload[..used]);

        let mut decoded = [0u8; UU_KERNEL_BYTES];
        decode_kernel(&chars, &mut decoded);

        self.output.extend_from_slice(&decoded[..declared]);
        self.decoded_len += declared as u64;
    }

    /// Scalar group-at-a-time decode, used for truncated and over-long lines.
    ///
    /// Consumes only the characters each group's bits actually need, which is
    /// what NZBGet's decoder does: its tail arms read two characters for a
    /// one-byte tail and three for a two-byte tail rather than demanding a
    /// padded fourth. A group that cannot even meet that minimum is where the
    /// line was genuinely cut.
    fn decode_salvage(&mut self, declared: usize, payload: &[u8]) {
        let mut remaining = declared;
        let mut chars = payload;

        while remaining > 0 {
            let needed = min_payload_chars(remaining.min(3));
            if chars.len() < needed {
                break;
            }

            let a = uu_sextet(chars[0]);
            let b = uu_sextet(chars[1]);
            // Absent characters are the padding the encoder omitted, and the
            // bits they would have carried belong to bytes this group does not
            // produce.
            let c = chars.get(2).map_or(0, |ch| uu_sextet(*ch));
            let d = chars.get(3).map_or(0, |ch| uu_sextet(*ch));

            self.output.push((a << 2) | (b >> 4));
            if remaining > 1 {
                self.output.push((b << 4) | (c >> 2));
            }
            if remaining > 2 {
                self.output.push((c << 6) | d);
            }

            self.decoded_len += remaining.min(3) as u64;
            remaining = remaining.saturating_sub(3);
            chars = &chars[needed..];
        }

        if remaining > 0 {
            // Ran out of characters mid-line: the shortfall is real damage even
            // when the groups that were present decoded cleanly.
            self.damaged = true;
        }
    }
}

/// Decode 64 uuencode characters into 48 bytes.
///
/// The transform is a single branchless expression per character followed by a
/// 4-to-3 repack, which is exactly the shape base64 decoders vectorise. Only
/// the first 48 bytes of `output` are meaningful; the rest is slack for the x86
/// kernel's final store.
#[inline]
fn decode_kernel(input: &[u8; UU_KERNEL_CHARS], output: &mut [u8; UU_KERNEL_BYTES]) {
    // Exactly one of the three blocks below survives `cfg` on any given target.
    #[cfg(target_arch = "x86_64")]
    {
        if dispatch_x86_ssse3() {
            // SAFETY: the dispatcher returned true only after detecting SSSE3.
            unsafe { decode_kernel_ssse3(input, output) }
        } else {
            decode_kernel_scalar(input, output);
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is architecturally guaranteed on aarch64 targets.
        unsafe { decode_kernel_neon(input, output) }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        decode_kernel_scalar(input, output);
    }
}

/// Scalar reference implementation, and the fallback where no vector tier is
/// available. The vector kernels are checked against this directly.
///
/// NEON is architecturally guaranteed on aarch64, so there the twin is reached
/// only by the differential test; it stays compiled so that test has something
/// to compare against.
#[cfg_attr(target_arch = "aarch64", allow(dead_code))]
fn decode_kernel_scalar(input: &[u8; UU_KERNEL_CHARS], output: &mut [u8; UU_KERNEL_BYTES]) {
    for group in 0..UU_KERNEL_CHARS / 4 {
        let a = uu_sextet(input[group * 4]);
        let b = uu_sextet(input[group * 4 + 1]);
        let c = uu_sextet(input[group * 4 + 2]);
        let d = uu_sextet(input[group * 4 + 3]);

        output[group * 3] = (a << 2) | (b >> 4);
        output[group * 3 + 1] = (b << 4) | (c >> 2);
        output[group * 3 + 2] = (c << 6) | d;
    }
}

/// Resolve the x86 tier once per process rather than once per line, the same
/// shape the yEnc dispatcher uses.
#[cfg(target_arch = "x86_64")]
#[inline]
fn dispatch_x86_ssse3() -> bool {
    use std::sync::OnceLock;

    static DISPATCH: OnceLock<bool> = OnceLock::new();
    *DISPATCH.get_or_init(|| std::arch::is_x86_feature_detected!("ssse3"))
}

/// SSSE3 kernel: four 16-character steps, each producing 12 bytes.
///
/// The repack is the textbook base64 sequence — `maddubs` folds character pairs
/// into 12-bit halves, `madd` folds those into a 24-bit group, and one shuffle
/// pulls the three bytes out of each 32-bit lane in big-endian order. uuencode
/// needs no lookup table in front of it because its alphabet is contiguous.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn decode_kernel_ssse3(input: &[u8; UU_KERNEL_CHARS], output: &mut [u8; UU_KERNEL_BYTES]) {
    use std::arch::x86_64::*;

    unsafe {
        let bias = _mm_set1_epi8(b' ' as i8);
        let mask = _mm_set1_epi8(0x3F);
        let fold_pairs = _mm_set1_epi32(0x0140_0140_u32 as i32);
        let fold_quads = _mm_set1_epi32(0x0001_1000);
        #[rustfmt::skip]
        let pack = _mm_setr_epi8(
            2, 1, 0,
            6, 5, 4,
            10, 9, 8,
            14, 13, 12,
            -1, -1, -1, -1,
        );

        for step in 0..4 {
            let raw = _mm_loadu_si128(input.as_ptr().add(step * 16).cast());
            let sextets = _mm_and_si128(_mm_sub_epi8(raw, bias), mask);
            let pairs = _mm_maddubs_epi16(sextets, fold_pairs);
            let quads = _mm_madd_epi16(pairs, fold_quads);
            let packed = _mm_shuffle_epi8(quads, pack);
            // Writes 16 bytes at offsets 0/12/24/36; the last one runs to 52,
            // which is why the output buffer carries slack past 48.
            _mm_storeu_si128(output.as_mut_ptr().add(step * 12).cast(), packed);
        }
    }
}

/// NEON kernel: one pass over all 64 characters.
///
/// `vld4q_u8` deinterleaves the line into the four character positions of every
/// group, which is precisely the operand layout the repack wants, and
/// `vst3q_u8` interleaves the three output bytes back into order. The whole
/// kernel is therefore load, mask, three shift-or pairs, store.
#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn decode_kernel_neon(input: &[u8; UU_KERNEL_CHARS], output: &mut [u8; UU_KERNEL_BYTES]) {
    use std::arch::aarch64::*;

    unsafe {
        let groups = vld4q_u8(input.as_ptr());
        let bias = vdupq_n_u8(b' ');
        let mask = vdupq_n_u8(0x3F);

        let a = vandq_u8(vsubq_u8(groups.0, bias), mask);
        let b = vandq_u8(vsubq_u8(groups.1, bias), mask);
        let c = vandq_u8(vsubq_u8(groups.2, bias), mask);
        let d = vandq_u8(vsubq_u8(groups.3, bias), mask);

        let packed = uint8x16x3_t(
            vorrq_u8(vshlq_n_u8::<2>(a), vshrq_n_u8::<4>(b)),
            vorrq_u8(vshlq_n_u8::<4>(b), vshrq_n_u8::<2>(c)),
            vorrq_u8(vshlq_n_u8::<6>(c), d),
        );
        vst3q_u8(output.as_mut_ptr(), packed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode one uuencode line the way a canonical encoder would.
    ///
    /// Anchored against a hand-computed vector by
    /// [`encoder_matches_hand_computed_vector`], so the golden round-trips below
    /// are checked against arithmetic rather than against this decoder.
    fn uu_line(data: &[u8], pad: u8) -> Vec<u8> {
        assert!(data.len() <= UU_MAX_LINE_BYTES);
        let mut line = vec![encode_char(data.len() as u8, pad)];
        for group in data.chunks(3) {
            let b0 = group[0];
            let b1 = group.get(1).copied().unwrap_or(0);
            let b2 = group.get(2).copied().unwrap_or(0);
            line.push(encode_char(b0 >> 2, pad));
            line.push(encode_char(((b0 << 4) | (b1 >> 4)) & 0x3F, pad));
            line.push(encode_char(((b1 << 2) | (b2 >> 6)) & 0x3F, pad));
            line.push(encode_char(b2 & 0x3F, pad));
        }
        line
    }

    /// `pad` picks how a zero sextet is spelled: canonical encoders use a
    /// space, others use a backtick. Both must decode identically.
    fn encode_char(sextet: u8, pad: u8) -> u8 {
        if sextet == 0 { pad } else { sextet + b' ' }
    }

    fn article(lines: &[&[u8]]) -> Vec<Vec<u8>> {
        lines.iter().map(|line| line.to_vec()).collect()
    }

    fn decode_all(lines: &[Vec<u8>]) -> UuDecoder {
        let mut decoder = UuDecoder::new();
        for line in lines {
            let mut raw = line.clone();
            raw.extend_from_slice(b"\r\n");
            decoder.push_line(&raw);
        }
        decoder
    }

    #[test]
    fn encoder_matches_hand_computed_vector() {
        // "Cat" is the canonical three-byte uuencode example: length char '#'
        // for 3 bytes, then sextets 16, 54, 5, 52 spelled "0V%T".
        assert_eq!(uu_line(b"Cat", b' '), b"#0V%T");
    }

    #[test]
    fn decodes_single_line_file() {
        let lines = article(&[b"begin 644 silver-horizon.bin", b"#0V%T", b"`", b"end"]);
        let decoder = decode_all(&lines);

        assert_eq!(decoder.output(), b"Cat");
        assert_eq!(decoder.filename(), Some("silver-horizon.bin"));
        assert_eq!(decoder.decoded_len(), 3);
        assert!(decoder.is_ended());
        assert!(!decoder.damaged());
    }

    #[test]
    fn decodes_tail_lengths_across_all_residues() {
        // 0, 1 and 2 mod 3 tails exercise every partial-group shape.
        for len in [45usize, 43, 44, 3, 1, 2, 30, 31, 32] {
            let payload: Vec<u8> = (0..len).map(|i| (i * 7 + 11) as u8).collect();
            let mut lines = vec![b"begin 644 silver-horizon.bin".to_vec()];
            lines.push(uu_line(&payload, b' '));
            lines.push(b"`".to_vec());
            lines.push(b"end".to_vec());

            let decoder = decode_all(&lines);
            assert_eq!(decoder.output(), &payload[..], "length {len}");
            assert_eq!(decoder.decoded_len(), len as u64, "length {len}");
            assert!(!decoder.damaged(), "length {len}");
        }
    }

    #[test]
    fn backtick_and_space_padding_decode_identically() {
        // A zero sextet may be spelled either way; the mask erases the
        // difference without a branch.
        for len in [1usize, 2, 4, 5, 44, 45] {
            let payload = vec![0u8; len];
            let spaced = decode_all(&[uu_line(&payload, b' ')]);
            let ticked = decode_all(&[uu_line(&payload, b'`')]);

            assert_eq!(spaced.output(), &payload[..], "space padding, length {len}");
            assert_eq!(
                ticked.output(),
                &payload[..],
                "backtick padding, length {len}"
            );
        }
    }

    #[test]
    fn decodes_multi_line_body() {
        let first: Vec<u8> = (0..45u8).collect();
        let second: Vec<u8> = (45..90u8).collect();
        let third: Vec<u8> = (90..100u8).collect();

        let mut lines = vec![b"begin 644 silver-horizon.bin".to_vec()];
        lines.push(uu_line(&first, b' '));
        lines.push(uu_line(&second, b' '));
        lines.push(uu_line(&third, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        let expected: Vec<u8> = (0..100u8).collect();
        assert_eq!(decoder.output(), &expected[..]);
        assert_eq!(decoder.decoded_len(), 100);
        assert!(!decoder.damaged());
    }

    #[test]
    fn skips_preamble_before_begin() {
        let payload: Vec<u8> = (0..45u8).collect();
        let mut lines = vec![
            b"Hello, this article has a chatty preamble.".to_vec(),
            b"".to_vec(),
            b"Posted from a newsreader that likes to talk.".to_vec(),
            b"begin 644 silver-horizon.bin".to_vec(),
        ];
        lines.push(uu_line(&payload, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        assert_eq!(decoder.output(), &payload[..]);
        assert!(decoder.saw_begin());
        assert!(!decoder.damaged());
    }

    #[test]
    fn ignores_trailer_after_end() {
        let payload: Vec<u8> = (0..45u8).collect();
        let mut lines = vec![b"begin 644 silver-horizon.bin".to_vec()];
        lines.push(uu_line(&payload, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());
        lines.push(b"-- ".to_vec());
        lines.push(b"Posted via an entirely fictional news service".to_vec());
        // A stray line with data shape must not resurrect the body.
        lines.push(uu_line(&payload, b' '));

        let decoder = decode_all(&lines);
        assert_eq!(decoder.output(), &payload[..]);
        assert_eq!(decoder.decoded_len(), 45);
        assert!(decoder.is_ended());
    }

    #[test]
    fn decodes_headerless_continuation_from_first_line() {
        // Continuation parts of a multi-part post carry no `begin` line at all:
        // the article opens with data.
        let first: Vec<u8> = (10..55u8).collect();
        let second: Vec<u8> = (55..100u8).collect();
        let lines = vec![uu_line(&first, b' '), uu_line(&second, b' ')];

        let decoder = decode_all(&lines);
        let expected: Vec<u8> = (10..100u8).collect();
        assert_eq!(decoder.output(), &expected[..]);
        assert!(decoder.saw_body());
        assert!(!decoder.saw_begin());
        assert_eq!(decoder.filename(), None);
        assert!(!decoder.damaged());
    }

    #[test]
    fn short_final_part_is_admitted_without_a_header() {
        // The last part of a post is short, so it never matches the full-line
        // shape and has to be admitted by the length-character split.
        let payload = b"Silver Horizon".to_vec();
        let line = uu_line(&payload, b'`');
        assert!(looks_like_uu(&line), "line {:?} should sniff as uu", line);

        let decoder = decode_all(&[line, b"`".to_vec(), b"end".to_vec()]);
        assert_eq!(decoder.output(), &payload[..]);
        assert!(!decoder.damaged());
    }

    #[test]
    fn extracts_name_with_spaces_and_odd_modes() {
        for (line, expected) in [
            (
                &b"begin 644 Silver Horizon S01E01.mkv"[..],
                Some("Silver Horizon S01E01.mkv"),
            ),
            (&b"begin 600 plain.bin"[..], Some("plain.bin")),
            (&b"begin 0644   spaced.bin"[..], Some("spaced.bin")),
            (&b"begin 644 "[..], None),
        ] {
            let decoder = decode_all(&[line.to_vec()]);
            assert_eq!(decoder.filename(), expected, "line {line:?}");
            assert!(decoder.saw_begin(), "line {line:?}");
        }
    }

    #[test]
    fn latin1_name_survives_invalid_utf8() {
        let mut line = b"begin 644 ".to_vec();
        line.push(0xFC); // 'ü' in Latin-1, not valid UTF-8 on its own.
        line.extend_from_slice(b"ber.bin");

        let decoder = decode_all(&[line]);
        assert_eq!(decoder.filename(), Some("\u{00FC}ber.bin"));
    }

    #[test]
    fn decodes_empty_file() {
        let decoder = decode_all(&article(&[b"begin 644 empty.bin", b"`", b"end"]));

        assert!(decoder.output().is_empty());
        assert_eq!(decoder.decoded_len(), 0);
        assert_eq!(decoder.filename(), Some("empty.bin"));
        assert!(decoder.is_ended());
        assert!(!decoder.damaged());
    }

    #[test]
    fn non_octal_mode_is_not_a_begin_line() {
        // Prose is the false positive this guard exists for.
        for line in [
            &b"begin the beguine"[..],
            &b"begin 9999 nope.bin"[..],
            &b"begin  "[..],
        ] {
            assert!(parse_begin_line(line).is_none(), "line {line:?}");
        }
    }

    #[test]
    fn prose_line_of_uu_length_is_not_data() {
        // 61 characters starting with 'M', but lowercase letters put it outside
        // the uuencode charset.
        let prose = b"Meanwhile the rest of this line is ordinary english prose!!!!";
        assert!(matches!(prose.len(), UU_MAX_LINE_CHARS | 61));
        assert!(!full_uu_data_line(prose));
        assert!(!looks_like_uu(prose));
    }

    #[test]
    fn truncated_line_flags_damage_and_salvages_prefix() {
        // A full line whose tail was cut: the length character still claims 45
        // bytes but only 5 groups survive. Damage is a body-state notion, so
        // the article has to have entered the body — a truncated line on its
        // own does not look like uuencode and never engages the decoder at all,
        // which `truncated_first_line_does_not_engage` pins separately.
        let payload: Vec<u8> = (0..45u8).collect();
        let full = uu_line(&payload, b' ');
        let truncated = full[..1 + 20].to_vec();

        let decoder = decode_all(&[b"begin 644 silver-horizon.bin".to_vec(), truncated]);
        assert!(decoder.damaged(), "truncated line must be flagged");
        // Five whole groups decode to fifteen bytes, which is better than the
        // reference decoders' zero.
        assert_eq!(decoder.output(), &payload[..15]);
        assert_eq!(decoder.decoded_len(), 15);
    }

    #[test]
    fn truncated_first_line_does_not_engage() {
        // Detection and damage are separate concerns. A cut line offered as the
        // opening line of a headerless article fails validation outright, so it
        // is preamble rather than damaged data — the article is left to the
        // caller's ordinary non-uu handling instead of being half-claimed.
        let payload: Vec<u8> = (0..45u8).collect();
        let truncated = uu_line(&payload, b' ')[..1 + 20].to_vec();

        assert!(!looks_like_uu(&truncated));

        let decoder = decode_all(&[truncated]);
        assert!(!decoder.saw_body());
        assert!(!decoder.damaged());
        assert_eq!(decoder.decoded_len(), 0);
    }

    #[test]
    fn broken_encoder_unpadded_tail_decodes_cleanly() {
        // Encoders that stop as soon as the payload's bits run out, instead of
        // padding the final group out to four characters. Both reference
        // decoders read these, and the posts they come from usually ship
        // without PAR2, so a dropped tail here is unrecoverable.
        for (bytes, chars) in [(1usize, 2usize), (1, 3), (2, 3)] {
            let payload: Vec<u8> = (0..bytes).map(|i| (i * 53 + 29) as u8).collect();
            let padded = uu_line(&payload, b' ');
            let unpadded = padded[..1 + chars].to_vec();

            let decoder = decode_all(std::slice::from_ref(&unpadded));
            assert_eq!(
                decoder.output(),
                &payload[..],
                "{bytes} byte(s) in {chars} characters"
            );
            assert!(
                !decoder.damaged(),
                "an unpadded tail is not damage: {bytes} byte(s) in {chars} characters"
            );
            assert_eq!(decoder.decoded_len(), bytes as u64);

            // And it agrees with the padded spelling of the same payload.
            assert_eq!(decode_all(&[padded]).output(), decoder.output());
            assert!(
                looks_like_uu(&unpadded),
                "detection must admit the unpadded form too"
            );
        }
    }

    #[test]
    fn tail_short_by_a_whole_group_still_damages() {
        // The boundary the stripped-whitespace tolerance sits on. Short by less
        // than a group is reconstructed as eaten spaces; short by a whole group
        // would be inventing three bytes out of no observed characters, so it
        // stays damage-and-salvage.
        let payload: Vec<u8> = (0..45u8).collect();
        let full = uu_line(&payload, b' ');
        assert_eq!(full.len(), 61);

        // 57 payload characters: three short of the 60 the length char demands.
        let tolerated = full[..1 + 57].to_vec();
        let decoder = decode_all(&[b"begin 644 silver-horizon.bin".to_vec(), tolerated]);
        assert!(!decoder.damaged(), "three short is the tolerated shape");
        assert_eq!(decoder.decoded_len(), 45);

        // 56: a whole group short.
        let starved = full[..1 + 56].to_vec();
        let decoder = decode_all(&[b"begin 644 silver-horizon.bin".to_vec(), starved]);
        assert!(decoder.damaged(), "a whole group short is damage");
        assert_eq!(
            decoder.decoded_len(),
            42,
            "the groups that were whole are still salvaged"
        );
    }

    // ---- field tolerances ----

    #[test]
    fn in_body_junk_lines_are_ignored_without_damage() {
        // The three shapes a body is allowed to contain that are not data: a
        // blank line, the "-- " signature separator, and a service tagline.
        // Each is a fossil of a real article, and the list is adopted literally
        // rather than generalised to "any line that will not decode" — that
        // would mask the genuine damage the flag exists to report.
        //
        // Without the list, "-- " reads as a length character of '-' declaring
        // 13 bytes with two payload characters behind it, so a par2-less job
        // would die on an article that decodes perfectly well.
        let first: Vec<u8> = (0..45u8).collect();
        let second: Vec<u8> = (45..90u8).collect();

        let mut lines = vec![b"begin 644 silver-horizon.bin".to_vec()];
        lines.push(uu_line(&first, b' '));
        lines.push(b"".to_vec());
        lines.push(b"-- ".to_vec());
        lines.push(b"Posted via Silver Horizon News, the friendliest server".to_vec());
        lines.push(uu_line(&second, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        let expected: Vec<u8> = (0..90u8).collect();
        assert_eq!(decoder.output(), &expected[..]);
        assert!(!decoder.damaged(), "the in-body ignore list is not damage");
        assert!(decoder.is_ended());
    }

    #[test]
    fn a_signature_separator_would_otherwise_be_read_as_damage() {
        // Pins WHY the ignore list is load-bearing rather than cosmetic. The
        // separator's first character is a length character declaring 13 bytes,
        // and two payload characters follow it — sixteen short of the eighteen
        // those bytes need, which is far past the stripped-whitespace tolerance
        // and squarely in the damage-and-salvage arm.
        assert_eq!(uu_sextet(b'-') as usize, 13);
        let payload_chars = b"-- ".len() - 1;
        assert_eq!(min_payload_chars(13), 18);
        assert!(min_payload_chars(13) - payload_chars > UU_MAX_VIRTUAL_PAD_CHARS);

        // Which is exactly the fate the list spares it.
        assert!(ignorable_body_line(b"-- "));
    }

    #[test]
    fn stripped_trailing_spaces_are_reconstructed_byte_identically() {
        // The classic gateway gremlin. A space is the sextet zero, so a run of
        // zero bits encodes to a run of trailing spaces — exactly what agents
        // that tidy text lines eat. Restoring them as virtual spaces gives the
        // original bytes back exactly.
        //
        // The payload is chosen to end in zero bytes so the canonical encoding
        // really does end in spaces, which is the case this tolerance is for.
        let mut payload: Vec<u8> = (0..42u8).collect();
        payload.extend_from_slice(&[0, 0, 0]);
        assert_eq!(payload.len(), 45);

        let full = uu_line(&payload, b' ');
        assert_eq!(full.len(), 61);
        assert!(
            full.ends_with(b"    "),
            "the fixture must actually end in the spaces a gateway would strip"
        );

        for stripped in 1..=UU_MAX_VIRTUAL_PAD_CHARS {
            let mut line = full.clone();
            line.truncate(full.len() - stripped);

            let decoder = decode_all(&[b"begin 644 silver-horizon.bin".to_vec(), line.clone()]);
            assert_eq!(
                decoder.output(),
                &payload[..],
                "{stripped} character(s) eaten"
            );
            assert_eq!(decoder.decoded_len(), 45, "{stripped} character(s) eaten");
            assert!(!decoder.damaged(), "{stripped} character(s) eaten");

            // And such a line is still recognised as uuencode on its own, so a
            // continuation article opening with one is claimed rather than
            // scanned past as junk.
            assert!(looks_like_uu(&line), "{stripped} character(s) eaten");
        }
    }

    #[test]
    fn a_begin_line_in_an_engaged_body_yields_the_filename() {
        // Mode strictness is a confidence gate on "is this uuencode?", and it
        // has no further job once that is settled. Both reference decoders
        // split the reading the same way: octal in the detector, anything in
        // the decode stage.
        let payload: Vec<u8> = (0..30u8).collect();
        let lines = vec![
            uu_line(&payload, b' '),
            b"begin 999 silver-horizon.bin".to_vec(),
            b"`".to_vec(),
            b"end".to_vec(),
        ];

        let decoder = decode_all(&lines);
        assert_eq!(decoder.filename(), Some("silver-horizon.bin"));
        assert!(decoder.saw_begin());
        assert_eq!(
            decoder.output(),
            &payload[..],
            "the begin line contributes no bytes of its own"
        );
        assert!(!decoder.damaged());

        // Detection stays strict, which is what stops prose claiming articles.
        assert!(parse_begin_line(b"begin 999 silver-horizon.bin").is_none());
        assert!(!looks_like_uu(b"begin 999 silver-horizon.bin"));
    }

    #[test]
    fn a_second_begin_after_end_is_ignored_without_damage() {
        // A concatenated multi-file article. One reference decoder re-enters
        // the body here and appends the second file's bytes to the first file's
        // output; weaver stays ended and decodes only the first file. See the
        // note on `UuState::Ended` for why. The second header must not damage,
        // rename, or contribute bytes.
        let first: Vec<u8> = (0..45u8).collect();
        let second: Vec<u8> = (100..145u8).collect();

        let mut lines = vec![b"begin 644 first.bin".to_vec()];
        lines.push(uu_line(&first, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());
        lines.push(b"begin 644 second.bin".to_vec());
        lines.push(uu_line(&second, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        assert_eq!(decoder.output(), &first[..]);
        assert_eq!(decoder.decoded_len(), 45);
        assert_eq!(decoder.filename(), Some("first.bin"));
        assert!(!decoder.damaged());
        assert!(decoder.is_ended());
    }

    #[test]
    fn a_second_begin_inside_the_body_does_not_rename_or_damage() {
        // The other placement of the same gremlin: a second header with no
        // intervening `end`. Both reference decoders read the word "begin"
        // itself as a data line and emit two bytes of garbage; weaver emits
        // nothing and keeps the name it already had.
        let first: Vec<u8> = (0..45u8).collect();
        let second: Vec<u8> = (100..145u8).collect();

        let mut lines = vec![b"begin 644 first.bin".to_vec()];
        lines.push(uu_line(&first, b' '));
        lines.push(b"begin 644 second.bin".to_vec());
        lines.push(uu_line(&second, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        let expected: Vec<u8> = first.iter().chain(second.iter()).copied().collect();
        assert_eq!(decoder.output(), &expected[..]);
        assert_eq!(decoder.filename(), Some("first.bin"));
        assert!(!decoder.damaged());
    }

    #[test]
    fn a_final_line_padded_with_data_characters_still_decodes_exactly() {
        // Observed in real field articles: the last short line of a file pads
        // its final group with ordinary uuencode-alphabet characters instead of
        // spaces or backticks. Those positions carry bits that belong to bytes
        // past the declared length, so they are noise — but they are not the
        // padding the format nominates, and one reference implementation
        // (CPython's `binascii.a2b_uu`) rejects such a line outright as
        // trailing garbage and truncates the file at that point.
        //
        // Weaver consumes the bytes the length character declares and ignores
        // whatever follows, which decodes the line exactly. That rule already
        // held; this vector exists so it cannot regress, because the shape is
        // common enough in the wild to lose real files over.
        for len in [1usize, 2, 4, 5, 7, 8, 43, 44] {
            let payload: Vec<u8> = (0..len).map(|i| (i * 53 + 19) as u8).collect();
            let canonical = uu_line(&payload, b' ');

            // Overwrite the pad positions — everything past the characters the
            // declared bytes actually need — with data-like characters.
            let mut garbled = canonical.clone();
            let real_chars = 1 + min_payload_chars(len);
            assert!(
                real_chars <= garbled.len(),
                "the canonical form is at least the minimum"
            );
            for slot in garbled[real_chars..].iter_mut() {
                *slot = b'%';
            }

            let decoder = decode_all(&[
                b"begin 644 silver-horizon.bin".to_vec(),
                garbled.clone(),
                b"`".to_vec(),
                b"end".to_vec(),
            ]);
            assert_eq!(decoder.output(), &payload[..], "length {len}");
            assert_eq!(decoder.decoded_len(), len as u64, "length {len}");
            assert!(
                !decoder.damaged(),
                "data-like padding is not damage: length {len}"
            );
        }
    }

    #[test]
    fn every_tail_length_round_trips() {
        // The full sweep the reference suite parameterises over: one line
        // carrying 1..=45 bytes, which is every payload length a single
        // uuencode line can hold and therefore every partial-group shape.
        for len in 1..=UU_MAX_LINE_BYTES {
            let payload: Vec<u8> = (0..len).map(|i| (i * 37 + 5) as u8).collect();
            let line = uu_line(&payload, b' ');

            // Each length is also its own article, exactly as a single-line
            // post would arrive.
            let decoder = decode_all(&[
                b"begin 644 silver-horizon.bin".to_vec(),
                line.clone(),
                b"`".to_vec(),
                b"end".to_vec(),
            ]);
            assert_eq!(decoder.output(), &payload[..], "length {len}");
            assert_eq!(decoder.decoded_len(), len as u64, "length {len}");
            assert!(!decoder.damaged(), "length {len}");
            assert!(decoder.is_ended(), "length {len}");

            // And the same line standing alone is recognised as uuencode, which
            // is how a headerless continuation part gets claimed.
            assert!(looks_like_uu(&line), "length {len}");
        }
    }

    #[test]
    fn a_line_short_of_even_one_group_is_not_detected_as_uu() {
        // Detection stays strict where the evidence is thin: a two-character
        // line must never claim an article, however its first byte reads. The
        // decoder's tolerance is wider on purpose — it only ever runs inside an
        // article something else already claimed.
        let padded = uu_line(&[0x5Au8], b' ');
        let starved = padded[..1 + 1].to_vec();
        assert!(!looks_like_uu(&starved));
    }

    #[test]
    fn unpadded_tail_after_full_lines_decodes_cleanly() {
        // The realistic shape: a file whose last line is the broken one.
        let head: Vec<u8> = (0..45u8).collect();
        let tail = vec![0xA7u8];

        let mut lines = vec![b"begin 644 silver-horizon.bin".to_vec()];
        lines.push(uu_line(&head, b' '));
        lines.push(uu_line(&tail, b' ')[..3].to_vec());
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        let mut expected = head.clone();
        expected.extend_from_slice(&tail);
        assert_eq!(decoder.output(), &expected[..]);
        assert!(!decoder.damaged());
        assert_eq!(decoder.decoded_len(), 46);
    }

    #[test]
    fn trailing_garbage_beyond_declared_length_decodes_cleanly() {
        // The condition the broken-encoder workaround actually targets: a line
        // carrying more characters than its length character claims. Consuming
        // exactly what the declared length needs handles it by construction.
        let payload: Vec<u8> = (0..12u8).collect();
        let mut line = uu_line(&payload, b' ');
        line.extend_from_slice(b"````    ");

        let decoder = decode_all(&[line]);
        assert_eq!(decoder.output(), &payload[..]);
        assert!(!decoder.damaged());
    }

    #[test]
    fn damage_does_not_stop_later_lines() {
        // SAB keeps the bytes and lets PAR2 judge; a bad line in the middle
        // must not cost the good lines around it.
        let first: Vec<u8> = (0..45u8).collect();
        let third: Vec<u8> = (45..90u8).collect();

        let mut lines = vec![b"begin 644 silver-horizon.bin".to_vec()];
        lines.push(uu_line(&first, b' '));
        lines.push(b"M!!".to_vec()); // claims 45 bytes, carries two characters
        lines.push(uu_line(&third, b' '));
        lines.push(b"`".to_vec());
        lines.push(b"end".to_vec());

        let decoder = decode_all(&lines);
        assert!(decoder.damaged());
        let expected: Vec<u8> = (0..90u8).collect();
        assert_eq!(decoder.output(), &expected[..]);
    }

    #[test]
    fn dot_stuffing_is_undone() {
        // A 14-byte line's length character is '.', so its wire form arrives
        // dot-stuffed as "..".
        let payload: Vec<u8> = (0..14u8).collect();
        let line = uu_line(&payload, b' ');
        assert_eq!(line[0], b'.');

        let mut stuffed = b".".to_vec();
        stuffed.extend_from_slice(&line);

        let plain = decode_all(&[line]);
        let unstuffed = decode_all(&[stuffed]);

        assert_eq!(plain.output(), &payload[..]);
        assert_eq!(unstuffed.output(), &payload[..]);
        assert!(!unstuffed.damaged());
    }

    #[test]
    fn end_forms_both_terminate() {
        for terminator in [&b"`"[..], &b"end"[..], &b"end "[..]] {
            let payload: Vec<u8> = (0..9u8).collect();
            let lines = vec![uu_line(&payload, b' '), terminator.to_vec()];
            let decoder = decode_all(&lines);
            assert!(decoder.is_ended(), "terminator {terminator:?}");
            assert_eq!(decoder.output(), &payload[..]);
        }
    }

    #[test]
    fn empty_article_decodes_nothing() {
        let decoder = decode_all(&article(&[b"", b"Nothing to see here.", b""]));
        assert!(!decoder.saw_body());
        assert!(decoder.output().is_empty());
        assert_eq!(decoder.decoded_len(), 0);
        assert!(!decoder.damaged());
    }

    #[test]
    fn take_output_leaves_the_decoder_usable() {
        let first: Vec<u8> = (0..45u8).collect();
        let second: Vec<u8> = (45..90u8).collect();

        let mut decoder = UuDecoder::new();
        decoder.push_line(b"begin 644 silver-horizon.bin\r\n");
        decoder.push_line(&[uu_line(&first, b' '), b"\r\n".to_vec()].concat());
        let drained = decoder.take_output();
        decoder.push_line(&[uu_line(&second, b' '), b"\r\n".to_vec()].concat());

        assert_eq!(drained, first);
        assert_eq!(decoder.output(), &second[..]);
        assert_eq!(decoder.decoded_len(), 90);
    }

    #[test]
    fn ybegin_line_is_never_claimed_by_the_sniffer() {
        // yEnc precedence is the caller's job, but the sniffer must not claim a
        // yEnc control line even if it were asked.
        for line in [
            &b"=ybegin part=1 line=128 size=100 name=silver-horizon.bin"[..],
            &b"=ypart begin=1 end=100"[..],
            &b"=yend size=100 pcrc32=abcdef01"[..],
        ] {
            assert!(!looks_like_uu(line), "line {line:?}");
        }
    }

    /// A kernel under differential test, paired with the label used in failures.
    type LabelledKernel = (
        &'static str,
        fn(&[u8; UU_KERNEL_CHARS], &mut [u8; UU_KERNEL_BYTES]),
    );

    /// Every kernel that is usable on this host, paired with a label.
    ///
    /// The vector kernel is listed on its own rather than only through
    /// [`decode_kernel`], so a host that has the tier always compares it — a
    /// dispatcher that quietly declined a tier can never masquerade as a tested
    /// one.
    fn kernels_under_test() -> Vec<LabelledKernel> {
        let mut kernels: Vec<LabelledKernel> = vec![("dispatched", decode_kernel)];

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("ssse3") {
            fn ssse3(input: &[u8; UU_KERNEL_CHARS], output: &mut [u8; UU_KERNEL_BYTES]) {
                // SAFETY: guarded by the detection check above.
                unsafe { decode_kernel_ssse3(input, output) }
            }
            kernels.push(("ssse3", ssse3));
        }

        #[cfg(target_arch = "aarch64")]
        {
            fn neon(input: &[u8; UU_KERNEL_CHARS], output: &mut [u8; UU_KERNEL_BYTES]) {
                // SAFETY: NEON is architecturally guaranteed on aarch64.
                unsafe { decode_kernel_neon(input, output) }
            }
            kernels.push(("neon", neon));
        }

        kernels
    }

    #[test]
    fn vector_kernel_is_exercised_wherever_the_host_has_one() {
        // Guards the guard: the differential below must not quietly degrade
        // into scalar-against-scalar on a host that has a vector tier. This
        // also pins the test list and the dispatcher to the same predicate, so
        // a tier the dispatcher declined can never look tested.
        let labels: Vec<&str> = kernels_under_test()
            .into_iter()
            .map(|(label, _)| label)
            .collect();

        #[cfg(target_arch = "aarch64")]
        assert!(
            labels.contains(&"neon"),
            "NEON is baseline on aarch64, so it must always be under test: {labels:?}"
        );

        #[cfg(target_arch = "x86_64")]
        assert_eq!(
            labels.contains(&"ssse3"),
            dispatch_x86_ssse3(),
            "the SSSE3 kernel must be under test exactly when the dispatcher would pick it: {labels:?}"
        );

        assert!(labels.contains(&"dispatched"));
    }

    #[test]
    fn kernel_matches_scalar_over_randomised_lines() {
        // Differential check of every available kernel against the scalar twin
        // across the whole uuencode charset plus bytes outside it.
        let kernels = kernels_under_test();
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for iteration in 0..2_000 {
            let mut chars = [b' '; UU_KERNEL_CHARS];
            for slot in chars.iter_mut() {
                let value = next();
                *slot = if iteration % 4 == 0 {
                    // Whole-byte range, including values outside the charset,
                    // so the two paths agree on the mask as well as the repack.
                    (value & 0xFF) as u8
                } else {
                    UU_CHAR_MIN + (value % (UU_CHAR_MAX - UU_CHAR_MIN + 1) as u64) as u8
                };
            }

            let mut expected = [0u8; UU_KERNEL_BYTES];
            decode_kernel_scalar(&chars, &mut expected);

            for (label, kernel) in &kernels {
                let mut actual = [0u8; UU_KERNEL_BYTES];
                kernel(&chars, &mut actual);

                assert_eq!(
                    expected[..48],
                    actual[..48],
                    "{label} kernel mismatch on iteration {iteration}"
                );
            }
        }
    }

    #[test]
    fn kernel_decodes_a_known_full_line() {
        let payload: Vec<u8> = (0..45u8)
            .map(|i| i.wrapping_mul(37).wrapping_add(5))
            .collect();
        let line = uu_line(&payload, b' ');
        assert_eq!(line.len(), 61);

        let mut chars = [b' '; UU_KERNEL_CHARS];
        chars[..UU_MAX_LINE_CHARS].copy_from_slice(&line[1..]);
        let mut decoded = [0u8; UU_KERNEL_BYTES];
        decode_kernel(&chars, &mut decoded);

        assert_eq!(&decoded[..45], &payload[..]);
    }
}
