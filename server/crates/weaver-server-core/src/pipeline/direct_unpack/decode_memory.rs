//! How much decoder memory a 7z archive actually needs, read off its coders.
//!
//! A chase used to reserve the whole configured decoder allowance for its
//! entire life, parks included, so every chase in the process single-filed
//! through one reservation and a chase parked on a repair held the rest back
//! for as long as the park lasted. The allowance is a ceiling, not a
//! requirement: what a decode needs is written in the archive's own coder
//! properties, and for almost every archive it is a dictionary of a few dozen
//! megabytes.
//!
//! The model is for the **single-threaded** decoders. The multi-threaded LZMA2
//! reader is a different animal — it buffers a whole run of dependent chunks
//! before decoding any of it, so its footprint scales with the block rather
//! than the dictionary — and the chase does not use it (see
//! `SevenZipDecodeMemory` in the finalize extraction module). Nothing here is
//! meant to be exact to the byte; each entry is the decoder's dominant
//! allocation plus a margin that covers its state and buffers.

use sevenz_rust2::{Archive, EncoderMethod};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// Range decoder input buffer plus the LZMA state tables. The LZMA2 reader's
/// own accounting names 40 KiB of state and a 64 KiB compressed-chunk buffer;
/// LZMA's is the same order. Rounded up to a full megabyte.
const LZ_STATE_BYTES: u64 = MIB;
/// The PPMd model is one allocation of exactly the declared size; this covers
/// the decoder's own tables around it.
const PPMD_STATE_BYTES: u64 = MIB;
/// A bzip2 block is at most 900 KiB, and the decoder holds a few times that.
const BZIP2_BYTES: u64 = 8 * MIB;
/// Deflate's window is 32 KiB and the reader wraps its input in a buffer.
const DEFLATE_BYTES: u64 = MIB;
/// Brotli's largest standard window is 16 MiB.
const BROTLI_BYTES: u64 = 32 * MIB;
/// The zstd decoder refuses frames whose window exceeds 128 MiB unless told
/// otherwise, and nothing here tells it otherwise.
const ZSTD_BYTES: u64 = 160 * MIB;
/// An LZ4 frame block is at most 4 MiB, plus a 64 KiB dictionary.
const LZ4_BYTES: u64 = 16 * MIB;
/// Branch-call-jump filters and the delta filter keep a few hundred bytes of
/// state; AES keeps a block. One megabyte covers any of them with room.
const FILTER_BYTES: u64 = MIB;
/// BCJ2 reads four streams at once and keeps a range coder over one of them.
/// Its sub-streams' own decoders are separate coders in the same block and
/// are summed with it.
const BCJ2_BYTES: u64 = 16 * MIB;

/// A coder this module has no memory model for.
///
/// Sizing stops at the first one: a reservation built from a chain with an
/// unknown link in it would be a guess presented as a measurement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnsizedCoder {
    pub(crate) method_id: Vec<u8>,
    pub(crate) reason: &'static str,
}

impl std::fmt::Display for UnsizedCoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "coder {:02x?}: {}", self.method_id, self.reason)
    }
}

/// Bytes a single-threaded decode of `archive` needs for its decoders.
///
/// Blocks decode one after another, so the answer is the most expensive block,
/// not the sum of them. Within a block the coders are nested readers that are
/// all live at once, so a block costs the sum of its chain.
pub(crate) fn decoder_memory_bytes(archive: &Archive) -> Result<u64, UnsizedCoder> {
    let mut largest_block = 0u64;
    for block in &archive.blocks {
        let mut chain = 0u64;
        for coder in &block.coders {
            chain = chain.saturating_add(coder_memory_bytes(
                coder.encoder_method_id(),
                coder.properties(),
            )?);
        }
        largest_block = largest_block.max(chain);
    }
    Ok(largest_block)
}

/// Bytes one coder's decoder needs, from its method id and property bytes.
pub(crate) fn coder_memory_bytes(method_id: &[u8], properties: &[u8]) -> Result<u64, UnsizedCoder> {
    if method_id == EncoderMethod::ID_COPY {
        Ok(0)
    } else if method_id == EncoderMethod::ID_LZMA {
        let dict = lzma_dictionary_bytes(method_id, properties)?;
        Ok(dict.saturating_add(LZ_STATE_BYTES))
    } else if method_id == EncoderMethod::ID_LZMA2 {
        let dict = lzma2_dictionary_bytes(method_id, properties)?;
        Ok(dict.saturating_add(LZ_STATE_BYTES))
    } else if method_id == EncoderMethod::ID_PPMD {
        let model = ppmd_model_bytes(method_id, properties)?;
        Ok(model.saturating_add(PPMD_STATE_BYTES))
    } else if method_id == EncoderMethod::ID_BZIP2 {
        Ok(BZIP2_BYTES)
    } else if method_id == EncoderMethod::ID_DEFLATE {
        Ok(DEFLATE_BYTES)
    } else if method_id == EncoderMethod::ID_BROTLI {
        Ok(BROTLI_BYTES)
    } else if method_id == EncoderMethod::ID_ZSTD {
        Ok(ZSTD_BYTES)
    } else if method_id == EncoderMethod::ID_LZ4 {
        Ok(LZ4_BYTES)
    } else if method_id == EncoderMethod::ID_BCJ2 {
        Ok(BCJ2_BYTES)
    } else if method_id == EncoderMethod::ID_AES256_SHA256
        || method_id == EncoderMethod::ID_DELTA
        || method_id == EncoderMethod::ID_BCJ_X86
        || method_id == EncoderMethod::ID_BCJ_ARM
        || method_id == EncoderMethod::ID_BCJ_ARM64
        || method_id == EncoderMethod::ID_BCJ_ARM_THUMB
        || method_id == EncoderMethod::ID_BCJ_PPC
        || method_id == EncoderMethod::ID_BCJ_IA64
        || method_id == EncoderMethod::ID_BCJ_SPARC
        || method_id == EncoderMethod::ID_BCJ_RISCV
    {
        Ok(FILTER_BYTES)
    } else {
        Err(UnsizedCoder {
            method_id: method_id.to_vec(),
            reason: "no memory model for this coder",
        })
    }
}

/// LZMA properties are five bytes: lc/lp/pb, then the dictionary size as a
/// little-endian `u32`.
fn lzma_dictionary_bytes(method_id: &[u8], properties: &[u8]) -> Result<u64, UnsizedCoder> {
    if properties.len() < 5 {
        return Err(UnsizedCoder {
            method_id: method_id.to_vec(),
            reason: "LZMA properties shorter than five bytes",
        });
    }
    let dict = u32::from_le_bytes([properties[1], properties[2], properties[3], properties[4]]);
    Ok(u64::from(dict))
}

/// LZMA2 properties are one byte encoding the dictionary size: values up to 39
/// map to `(2 | (p & 1)) << (p / 2 + 11)`, and 40 means the 4 GiB maximum.
/// This is the decoder's own decoding of the byte; the decoder also rounds the
/// dictionary up to a multiple of sixteen before allocating it.
fn lzma2_dictionary_bytes(method_id: &[u8], properties: &[u8]) -> Result<u64, UnsizedCoder> {
    let Some(&bits) = properties.first() else {
        return Err(UnsizedCoder {
            method_id: method_id.to_vec(),
            reason: "LZMA2 properties empty",
        });
    };
    let bits = u64::from(bits);
    if bits & !0x3F != 0 {
        return Err(UnsizedCoder {
            method_id: method_id.to_vec(),
            reason: "LZMA2 property byte has reserved bits set",
        });
    }
    if bits > 40 {
        return Err(UnsizedCoder {
            method_id: method_id.to_vec(),
            reason: "LZMA2 dictionary larger than the 4 GiB maximum",
        });
    }
    let dict = if bits == 40 {
        u64::from(u32::MAX)
    } else {
        (2 | (bits & 1)) << (bits / 2 + 11)
    };
    Ok((dict + 15) & !15)
}

/// PPMd properties are five bytes: the model order, then the model memory
/// size as a little-endian `u32`. The decoder allocates exactly that.
fn ppmd_model_bytes(method_id: &[u8], properties: &[u8]) -> Result<u64, UnsizedCoder> {
    if properties.len() < 5 {
        return Err(UnsizedCoder {
            method_id: method_id.to_vec(),
            reason: "PPMd properties shorter than five bytes",
        });
    }
    let memory = u32::from_le_bytes([properties[1], properties[2], properties[3], properties[4]]);
    Ok(u64::from(memory))
}

#[cfg(test)]
mod tests {
    use std::io;

    use sevenz_rust2::encoder_options::{
        AesEncoderOptions, EncoderOptions, Lzma2Options, PpmdOptions,
    };
    use sevenz_rust2::{
        ArchiveEntry, ArchiveReader, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password,
        SourceReader,
    };

    use super::*;

    fn payload(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    /// Encode `members` with `methods` (library order: output end first) and
    /// hand back the parsed archive.
    fn archive_for(methods: Vec<EncoderConfiguration>, members: usize, solid: bool) -> Archive {
        archive_for_with_password(methods, members, solid, Password::empty())
    }

    fn archive_for_with_password(
        methods: Vec<EncoderConfiguration>,
        members: usize,
        solid: bool,
        password: Password,
    ) -> Archive {
        let mut writer = ArchiveWriter::new(io::Cursor::new(Vec::new())).expect("writer");
        writer.set_content_methods(methods);
        let entries: Vec<(String, Vec<u8>)> = (0..members)
            .map(|index| {
                (
                    format!("silver_horizon/part_{index:02}.bin"),
                    payload(256 * 1024, 7_000 + index as u64),
                )
            })
            .collect();
        if solid {
            let names = entries
                .iter()
                .map(|(name, _)| ArchiveEntry::new_file(name))
                .collect::<Vec<_>>();
            let readers = entries
                .iter()
                .map(|(_, bytes)| SourceReader::new(io::Cursor::new(bytes.clone())))
                .collect::<Vec<_>>();
            writer
                .push_archive_entries(names, readers)
                .expect("solid block");
        } else {
            for (name, bytes) in &entries {
                writer
                    .push_archive_entry(
                        ArchiveEntry::new_file(name),
                        Some(io::Cursor::new(bytes.clone())),
                    )
                    .expect("entry");
            }
        }
        let bytes = writer.finish().expect("finish").into_inner();
        let reader = ArchiveReader::new(io::Cursor::new(bytes), password)
            .expect("parse what the writer wrote");
        reader.archive().clone()
    }

    fn lzma2_with_dictionary(dict: u32) -> EncoderConfiguration {
        let mut options = Lzma2Options::from_level(5);
        options.set_dictionary_size(dict);
        EncoderConfiguration::from(options)
    }

    #[test]
    fn a_stored_archive_needs_nothing() {
        let archive = archive_for(
            vec![EncoderConfiguration::new(EncoderMethod::COPY)],
            1,
            false,
        );
        assert_eq!(decoder_memory_bytes(&archive).unwrap(), 0);
    }

    #[test]
    fn lzma2_is_its_dictionary_plus_state() {
        for dict in [64 * 1024, 1 << 20, 16 << 20, 64 << 20] {
            let archive = archive_for(vec![lzma2_with_dictionary(dict)], 1, false);
            assert_eq!(
                decoder_memory_bytes(&archive).unwrap(),
                u64::from(dict) + LZ_STATE_BYTES,
                "dictionary {dict}"
            );
        }
    }

    #[test]
    fn lzma2_property_bytes_decode_like_the_decoder() {
        // The decoder's table: p → (2 | (p & 1)) << (p / 2 + 11).
        assert_eq!(
            coder_memory_bytes(EncoderMethod::ID_LZMA2, &[0]).unwrap(),
            4096 + LZ_STATE_BYTES
        );
        assert_eq!(
            coder_memory_bytes(EncoderMethod::ID_LZMA2, &[1]).unwrap(),
            6144 + LZ_STATE_BYTES
        );
        assert_eq!(
            coder_memory_bytes(EncoderMethod::ID_LZMA2, &[24]).unwrap(),
            (16 << 20) + LZ_STATE_BYTES
        );
        assert_eq!(
            coder_memory_bytes(EncoderMethod::ID_LZMA2, &[40]).unwrap(),
            ((u64::from(u32::MAX) + 15) & !15) + LZ_STATE_BYTES
        );
        assert!(coder_memory_bytes(EncoderMethod::ID_LZMA2, &[41]).is_err());
        assert!(coder_memory_bytes(EncoderMethod::ID_LZMA2, &[0x40]).is_err());
        assert!(coder_memory_bytes(EncoderMethod::ID_LZMA2, &[]).is_err());
    }

    #[test]
    fn lzma_reads_its_dictionary_from_the_properties() {
        let dict = 8u32 << 20;
        let mut properties = vec![0x5D];
        properties.extend_from_slice(&dict.to_le_bytes());
        assert_eq!(
            coder_memory_bytes(EncoderMethod::ID_LZMA, &properties).unwrap(),
            u64::from(dict) + LZ_STATE_BYTES
        );
        assert!(coder_memory_bytes(EncoderMethod::ID_LZMA, &[0x5D, 0, 0]).is_err());
    }

    #[test]
    fn ppmd_is_its_declared_model_plus_state() {
        let options = PpmdOptions::from_order_memory_size(6, 24 << 20);
        let archive = archive_for(
            vec![
                EncoderConfiguration::new(EncoderMethod::PPMD)
                    .with_options(EncoderOptions::Ppmd(options)),
            ],
            1,
            false,
        );
        assert_eq!(
            decoder_memory_bytes(&archive).unwrap(),
            (24 << 20) + PPMD_STATE_BYTES
        );
    }

    #[test]
    fn fixed_size_codecs_cost_their_constants() {
        for (method, expected) in [
            (EncoderMethod::BZIP2, BZIP2_BYTES),
            (EncoderMethod::DEFLATE, DEFLATE_BYTES),
            (EncoderMethod::BROTLI, BROTLI_BYTES),
            (EncoderMethod::ZSTD, ZSTD_BYTES),
            (EncoderMethod::LZ4, LZ4_BYTES),
        ] {
            let archive = archive_for(vec![EncoderConfiguration::new(method)], 1, false);
            assert_eq!(
                decoder_memory_bytes(&archive).unwrap(),
                expected,
                "{method:?}"
            );
        }
    }

    #[test]
    fn a_chain_sums_its_coders() {
        let dict = 4u32 << 20;
        // Filter chains and encryption are separate coders in the same block,
        // every one of them live while the block decodes.
        let filtered = archive_for(
            vec![
                lzma2_with_dictionary(dict),
                EncoderConfiguration::new(EncoderMethod::BCJ_X86_FILTER),
            ],
            1,
            false,
        );
        assert_eq!(
            decoder_memory_bytes(&filtered).unwrap(),
            u64::from(dict) + LZ_STATE_BYTES + FILTER_BYTES
        );

        let delta = archive_for(
            vec![
                lzma2_with_dictionary(dict),
                EncoderConfiguration::new(EncoderMethod::DELTA_FILTER),
            ],
            1,
            false,
        );
        assert_eq!(
            decoder_memory_bytes(&delta).unwrap(),
            u64::from(dict) + LZ_STATE_BYTES + FILTER_BYTES
        );

        let encrypted = archive_for_with_password(
            vec![
                EncoderConfiguration::from(AesEncoderOptions::new(Password::new(
                    "SilverHorizonPass1",
                ))),
                lzma2_with_dictionary(dict),
            ],
            1,
            false,
            Password::new("SilverHorizonPass1"),
        );
        assert_eq!(
            decoder_memory_bytes(&encrypted).unwrap(),
            u64::from(dict) + LZ_STATE_BYTES + FILTER_BYTES
        );
    }

    #[test]
    fn blocks_take_the_largest_not_the_sum() {
        let dict = 2u32 << 20;
        let solid = archive_for(vec![lzma2_with_dictionary(dict)], 3, true);
        let separate = archive_for(vec![lzma2_with_dictionary(dict)], 3, false);
        assert_eq!(solid.blocks.len(), 1);
        assert_eq!(separate.blocks.len(), 3);
        let one_block = u64::from(dict) + LZ_STATE_BYTES;
        assert_eq!(decoder_memory_bytes(&solid).unwrap(), one_block);
        assert_eq!(
            decoder_memory_bytes(&separate).unwrap(),
            one_block,
            "three blocks decode one at a time, so they cost one block"
        );
    }

    #[test]
    fn an_unknown_coder_refuses_to_be_sized() {
        let error = coder_memory_bytes(EncoderMethod::ID_DEFLATE64, &[]).unwrap_err();
        assert_eq!(error.method_id, EncoderMethod::ID_DEFLATE64.to_vec());
        let error = coder_memory_bytes(&[0x7F, 0x7F], &[]).unwrap_err();
        assert_eq!(error.method_id, vec![0x7F, 0x7F]);
        assert!(error.to_string().contains("no memory model"));
    }
}
