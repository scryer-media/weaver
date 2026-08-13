/// Header damage that was tolerated rather than rejected.
///
/// Reference decoders (SABnzbd/sabctools and nzbget) accept a range of
/// malformed-but-recoverable yEnc articles that a strict parser would reject.
/// Weaver accepts the same articles, but never silently: every field that was
/// absent or unparseable is recorded here so callers can log the damage.
///
/// A default (all-`false`) value means the headers were fully well-formed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct YencHeaderDefects {
    /// Bytes appeared before the `=ybegin` line and were skipped.
    pub junk_before_ybegin: bool,
    /// `=ybegin` had no `name=` field; [`YencMetadata::name`] is empty.
    pub missing_name: bool,
    /// `=ybegin` had no `size=` field; [`YencMetadata::size`] is `0`.
    pub missing_size: bool,
    /// `=ybegin` had no `line=` field; [`YencMetadata::line_length`] is `0`.
    pub missing_line: bool,
    /// `=ybegin size=` was present but not a valid integer; treated as absent.
    pub invalid_size: bool,
    /// `=ybegin line=` was present but not a valid integer; treated as absent.
    pub invalid_line: bool,
    /// `=ypart` declared `end=` past the `=ybegin size=` file size.
    pub ypart_end_exceeds_size: bool,
    /// `=yend size=` was present but not a valid integer; treated as absent.
    pub invalid_yend_size: bool,
    /// `=yend pcrc32=` was present but not valid hex; treated as absent.
    pub invalid_pcrc32: bool,
    /// `=yend crc32=` was present but not valid hex; treated as absent.
    pub invalid_crc32: bool,
}

impl YencHeaderDefects {
    /// True when at least one tolerated defect was recorded.
    pub fn any(&self) -> bool {
        *self != Self::default()
    }

    /// Union of two defect sets (used to fold `=yend` damage into the
    /// article-level result).
    pub fn merged(self, other: Self) -> Self {
        Self {
            junk_before_ybegin: self.junk_before_ybegin || other.junk_before_ybegin,
            missing_name: self.missing_name || other.missing_name,
            missing_size: self.missing_size || other.missing_size,
            missing_line: self.missing_line || other.missing_line,
            invalid_size: self.invalid_size || other.invalid_size,
            invalid_line: self.invalid_line || other.invalid_line,
            ypart_end_exceeds_size: self.ypart_end_exceeds_size || other.ypart_end_exceeds_size,
            invalid_yend_size: self.invalid_yend_size || other.invalid_yend_size,
            invalid_pcrc32: self.invalid_pcrc32 || other.invalid_pcrc32,
            invalid_crc32: self.invalid_crc32 || other.invalid_crc32,
        }
    }
}

/// Outcome of part-CRC verification for a decoded article.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CrcVerification {
    /// No usable expected CRC was present in `=yend` (absent, or present but
    /// unparseable and therefore ignored). Nothing was checked.
    #[default]
    Unverified,
    /// An expected CRC was present and matched the computed CRC.
    Verified,
    /// An expected CRC was present and did not match. Whole-article decode
    /// entry points report this as [`crate::YencError::CrcMismatch`] instead of
    /// returning a result, so this variant only appears on paths that collect
    /// the status without failing.
    Mismatch,
}

/// Metadata extracted from =ybegin and =ypart headers.
#[derive(Debug, Clone)]
pub struct YencMetadata {
    /// Original filename from the `name` field. Empty when the poster omitted
    /// `name=`; check [`YencHeaderDefects::missing_name`] to distinguish that
    /// from a genuinely empty name.
    pub name: String,
    /// Total file size in bytes (from `size` field). `0` when the poster
    /// omitted or mangled `size=` — see [`YencHeaderDefects::missing_size`] and
    /// [`YencHeaderDefects::invalid_size`].
    pub size: u64,
    /// Typical encoded line length (from `line` field). `0` means "not
    /// declared"; the decoder then runs without the line-length hint.
    pub line_length: u32,
    /// Part number (1-based), `None` for single-part articles.
    pub part: Option<u32>,
    /// Total number of parts, if specified.
    pub total: Option<u32>,
    /// Start byte offset in the original file (1-based), multi-part only.
    pub begin: Option<u64>,
    /// End byte offset in the original file (1-based, inclusive), multi-part only.
    pub end: Option<u64>,
    /// Header damage that was tolerated while parsing this article.
    pub defects: YencHeaderDefects,
}

impl YencMetadata {
    /// Absolute offset of this article's first decoded byte within the file it
    /// is part of: `=ypart begin` (1-based) mapped to a 0-based offset, and `0`
    /// for a single-part article, whose bytes are the whole file.
    ///
    /// This is where the decoder anchors its CRC checkpoint grid. The poster's
    /// `begin` is not trusted beyond that: an evidence collector reconciles the
    /// segment base against the offset the article was actually placed at, and
    /// a disagreement makes the segments tile nothing rather than publishing a
    /// block verdict computed on a misplaced grid.
    pub fn article_file_offset(&self) -> u64 {
        match self.begin {
            Some(begin) => begin.saturating_sub(1),
            None => 0,
        }
    }
}

/// Result of decoding a yEnc article body.
#[derive(Debug)]
pub struct DecodeResult {
    /// Parsed metadata from headers.
    pub metadata: YencMetadata,
    /// Number of decoded bytes written to the output buffer.
    pub bytes_written: usize,
    /// Computed CRC32 of the decoded data.
    pub part_crc: u32,
    /// Expected part CRC32 from `=yend` (`pcrc32` field), if present.
    pub expected_part_crc: Option<u32>,
    /// Expected full-file CRC32 from `=yend` (`crc32` field), if present.
    pub expected_file_crc: Option<u32>,
    /// Tri-state CRC outcome: verified, unverified (no usable expected CRC), or
    /// mismatch.
    ///
    /// This replaces an earlier `crc_valid: bool`, which read `true` both when
    /// a CRC was checked and matched *and* when there was no CRC to check —
    /// so an article with an absent or unparseable `crc32=` reported the same
    /// success as a verified one. Callers that only need "not known bad" should
    /// compare against [`CrcVerification::Mismatch`]; callers that need real
    /// verification should compare against [`CrcVerification::Verified`].
    pub crc_status: CrcVerification,
    /// Whether the `=yend` trailer was present. If false, the article may be
    /// incomplete (truncated download).
    pub has_trailer: bool,
    /// Header damage that was tolerated while decoding this article, folding
    /// together `=ybegin`/`=ypart` and `=yend` defects.
    pub defects: YencHeaderDefects,
    /// The decode pass's CRC32 segments, in file order, tiling exactly the
    /// bytes this article decoded to.
    ///
    /// [`Self::part_crc`] is their in-order combine-fold, so this never changes
    /// the article's own verdict; the records exist so an evidence collector
    /// above the decoder can fold the segments tiling a PAR2 block — which may
    /// span several articles — into that block's CRC32 without a second pass
    /// over the bytes. A decode with no segment plan declared (see
    /// [`crate::DecodeState::set_segment_plan`]) reports one segment covering
    /// the whole article.
    pub segments: Vec<crate::segment::Segment>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_clone() {
        let meta = YencMetadata {
            name: "test.bin".to_string(),
            size: 1024,
            line_length: 128,
            part: Some(1),
            total: Some(10),
            begin: Some(1),
            end: Some(100),
            defects: YencHeaderDefects::default(),
        };
        let cloned = meta.clone();
        assert_eq!(cloned.name, "test.bin");
        assert_eq!(cloned.size, 1024);
        assert_eq!(cloned.part, Some(1));
    }

    #[test]
    fn metadata_debug() {
        let meta = YencMetadata {
            name: "file.dat".to_string(),
            size: 500,
            line_length: 128,
            part: None,
            total: None,
            begin: None,
            end: None,
            defects: YencHeaderDefects::default(),
        };
        let debug = format!("{:?}", meta);
        assert!(debug.contains("file.dat"));
    }

    #[test]
    fn defects_default_is_clean_and_merges_as_union() {
        let clean = YencHeaderDefects::default();
        assert!(!clean.any());

        let junk = YencHeaderDefects {
            junk_before_ybegin: true,
            ..Default::default()
        };
        let bad_crc = YencHeaderDefects {
            invalid_crc32: true,
            ..Default::default()
        };
        let merged = junk.merged(bad_crc);

        assert!(merged.any());
        assert!(merged.junk_before_ybegin);
        assert!(merged.invalid_crc32);
        assert!(!merged.missing_name);
        assert_eq!(clean.merged(clean), clean);
    }

    #[test]
    fn crc_verification_defaults_to_unverified() {
        assert_eq!(CrcVerification::default(), CrcVerification::Unverified);
    }
}
