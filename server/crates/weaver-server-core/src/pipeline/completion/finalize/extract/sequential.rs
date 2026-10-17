// Sequential archive adapters shared by download-time and completed-file extraction.

use super::*;
use crate::pipeline::direct_store::plan::StreamKind;
use std::io::Read;
use weaver_model::files::FileRole;

impl From<StreamKind> for SimpleArchiveKind {
    fn from(kind: StreamKind) -> Self {
        match kind {
            StreamKind::Xz => Self::Xz,
            StreamKind::Gz => Self::Gz,
            StreamKind::Bzip2 => Self::Bzip2,
            StreamKind::Zstd => Self::Zstd,
            StreamKind::Brotli => Self::Brotli,
            StreamKind::Deflate => Self::Deflate,
        }
    }
}

impl SimpleArchiveKind {
    pub(in crate::pipeline) fn from_role(role: &FileRole) -> Option<Self> {
        match ArchiveEngine::of(crate::jobs::assembly::ArchiveType::from_role(role)?) {
            ArchiveEngine::Simple(kind) => Some(kind),
            ArchiveEngine::Rar | ArchiveEngine::SevenZip => None,
        }
    }

    fn is_tar(self) -> bool {
        matches!(self, Self::Tar | Self::TarGz | Self::TarBz2 | Self::TarXz)
    }
}

pub(in crate::pipeline) struct SequentialExtractionContext<'a> {
    pub kind: SimpleArchiveKind,
    pub archive_path: &'a Path,
    pub root: &'a ExtractionRoot,
    pub budget: &'a Arc<JobExtractionBudget>,
    pub event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub job_id: JobId,
    pub set_name: &'a str,
}

// What a single-stream decode needs besides its bytes. Where its output lands
// is the output's business, so no extraction root is carried.
pub(in crate::pipeline) struct SingleStreamContext<'a> {
    pub kind: SimpleArchiveKind,
    // The input's path in the namespace its output lands in. The output keeps
    // its directory, as gunzip does, so two same-named streams in different
    // directories stay two outputs.
    pub input: &'a Path,
    pub budget: &'a Arc<JobExtractionBudget>,
    pub event_tx: &'a broadcast::Sender<PipelineEvent>,
    pub job_id: JobId,
    pub set_name: &'a str,
}

// A file on disk is its own namespace: its output lands beside it, under its
// own name, whatever directory the file was opened from. Plain parts are
// presented as one stream, with the topology's base name as the output name.
impl<'a> From<&SequentialExtractionContext<'a>> for SingleStreamContext<'a> {
    fn from(context: &SequentialExtractionContext<'a>) -> Self {
        let input = match context.kind {
            SimpleArchiveKind::Split => Path::new(context.set_name),
            _ => context
                .archive_path
                .file_name()
                .map_or(context.archive_path, Path::new),
        };
        Self {
            kind: context.kind,
            input,
            budget: context.budget,
            event_tx: context.event_tx,
            job_id: context.job_id,
            set_name: context.set_name,
        }
    }
}

// Decodes a sequential archive on disk into its root, reopening the file
// whenever the decode starts again.
pub(super) fn extract_file(
    context: &SequentialExtractionContext<'_>,
) -> Result<Vec<String>, ExtractionError> {
    extract_sequential(
        || {
            std::fs::File::open(context.archive_path).map_err(|error| {
                ExtractionError::from_io(
                    format!("failed to open {:?}: {error}", context.kind),
                    &error,
                )
            })
        },
        context,
    )
}

// Decodes a sequential archive into its root. `open` gives the archive's
// bytes from the start. A single stream restarts as
// [`decode_single_stream_to`] describes; a tar's decoder is held to the
// reservation its kind fixes, which no later stream outgrows. Input may block
// on download coverage; it must report EOF only when the entire source has
// actually been committed.
pub(in crate::pipeline) fn extract_sequential<R: Read>(
    mut open: impl FnMut() -> Result<R, ExtractionError>,
    context: &SequentialExtractionContext<'_>,
) -> Result<Vec<String>, ExtractionError> {
    let SequentialExtractionContext {
        kind,
        root,
        budget,
        event_tx,
        job_id,
        set_name,
        ..
    } = context;
    if !kind.is_tar() {
        // No part is deleted or overwritten here.
        return decode_single_stream_to(
            open,
            &FileOutput { root, budget },
            &SingleStreamContext::from(context),
        );
    }
    let memory =
        stream_decoder_memory(*kind, std::io::empty, budget.max_memory_bytes()).or_terminal()?;
    let _memory = budget.reserve_memory_wait(memory.reserved)?;
    let mut decoded = StreamDecoder::open(*kind, open()?, budget, memory, Integrity::NoChecksum)?;
    let extracted =
        extract_tar_from_reader(&mut decoded, root, budget, event_tx, *job_id, set_name)?;
    // TAR stops at its end marker, before the outer codec necessarily
    // validates its trailer. Verify that trailer before accepting output.
    let mut trailing_bytes = 0u64;
    let mut buffer = [0u8; 8192];
    loop {
        budget
            .check_active_io()
            .map_err(|error| ExtractionError::io(&error))?;
        let read = decoded.read(&mut buffer).map_err(|error| {
            ExtractionError::from_io(
                format!("failed to finish compressed tar stream: {error}"),
                &error,
            )
        })?;
        if read == 0 {
            break;
        }
        trailing_bytes = trailing_bytes
            .checked_add(read as u64)
            .ok_or_else(|| ExtractionError::terminal("tar trailing length overflow"))?;
        budget.check_member_metadata("tar trailing data", trailing_bytes)?;
    }
    Ok(extracted)
}

// Decodes one single-stream member as its bytes arrive and hands its one
// output to `output`, which may route an archive-shaped output to a child
// engine instead of to disk. `open` gives the stream's bytes from the start;
// it is called once to size the decoder from the stream's header and once per
// decode. The decoder's memory is reserved here, and when a later frame or
// block asks for more than the header declared, the decode starts again from
// the start under a reservation sized from what was asked, bounded by the
// job's ceiling.
pub(in crate::pipeline) fn decode_single_stream_to<R: Read, O: ArchiveOutput>(
    mut open: impl FnMut() -> Result<R, ExtractionError>,
    output: &O,
    context: &SingleStreamContext<'_>,
) -> Result<Vec<String>, ExtractionError> {
    let SingleStreamContext { kind, budget, .. } = *context;
    let ceiling = budget.max_memory_bytes();
    let header = open()?;
    let mut memory = stream_decoder_memory(kind, || header, ceiling).or_terminal()?;
    write_stream(output, context, |writer| {
        // The output stays open across attempts. A decoder is deterministic
        // over the same bytes, and those bytes never change, so a new attempt
        // passes over what the last one already wrote instead of writing it
        // again: an output that feeds a child engine cannot be taken back.
        let mut written = 0u64;
        loop {
            let _permit = budget.reserve_memory_wait(memory.reserved)?;
            let reader = open()?;
            let asked = match decode_attempt(reader, writer, kind, budget, memory, &mut written) {
                Ok(integrity) => return Ok((written, integrity)),
                Err(Attempt::Failed(error)) => return Err(error),
                Err(Attempt::Outgrew(asked)) => asked,
            };
            let Some(grown) = memory.grown(kind, asked, ceiling) else {
                return Err(refused_later(kind, asked, ceiling));
            };
            tracing::info!(
                job_id = context.job_id.0,
                format = ?kind,
                reserved = memory.reserved,
                grown = grown.reserved,
                written,
                "stream outgrew its header's decoder memory; decoding again from the start"
            );
            memory = grown;
        }
    })
}

// Writes the one member of an xz file on disk that the block-parallel decoder
// is decoding into its root. The parallel decoder takes a file only when it
// is one stream, so the integrity that stream's header declares is the
// file's.
pub(super) fn write_decoded_file<R: Read>(
    mut decoded: R,
    context: &SequentialExtractionContext<'_>,
) -> Result<Vec<String>, ExtractionError> {
    let file = std::fs::File::open(context.archive_path).map_err(|error| {
        ExtractionError::from_io(
            format!("failed to open {:?}: {error}", context.kind),
            &error,
        )
    })?;
    let header = read_stream_header(context.kind, file)?;
    let integrity = context.kind.header_integrity(&header);
    let kind = context.kind;
    write_stream(
        &FileOutput {
            root: context.root,
            budget: context.budget,
        },
        &SingleStreamContext::from(context),
        |writer| {
            std::io::copy(&mut decoded, writer)
                .map(|bytes| (bytes, integrity))
                .map_err(|error| decode_failed(kind, &error))
        },
    )
}

// A later frame or block that asks for more than the ceiling. An xz block
// header carries a CRC, so what it asks is what the encoder wrote. A zstd
// frame header carries none, so what it asks may be damage that repair of the
// input clears; only the first header's verdict is final, as it is the one
// the whole stream was sized from.
fn refused_later(
    kind: SimpleArchiveKind,
    asked: StreamDecoderMemory,
    ceiling: u64,
) -> ExtractionError {
    let message = format!(
        "{kind:?} stream asks for a {} byte window and {} bytes of decoder memory \
         past its first header, above the {ceiling} byte limit",
        asked.window, asked.reserved
    );
    match kind {
        SimpleArchiveKind::Zstd => ExtractionError::recoverable(message),
        _ => ExtractionError::terminal(message),
    }
}

// Why one attempt at a stream stopped short.
enum Attempt {
    Failed(ExtractionError),
    // A later frame or block asked for this much.
    Outgrew(StreamDecoderMemory),
}

// One decode of a stream from its start into `writer`, passing over the
// `written` bytes an earlier attempt already wrote, and advancing `written`
// past every byte this one writes.
fn decode_attempt<R: Read, W: std::io::Write>(
    mut reader: R,
    writer: &mut W,
    kind: SimpleArchiveKind,
    budget: &Arc<JobExtractionBudget>,
    memory: StreamDecoderMemory,
    written: &mut u64,
) -> Result<Integrity, Attempt> {
    // An xz stream's header declares its check, and is read ahead of the
    // decoder and handed back in front of the rest.
    let header = match kind {
        SimpleArchiveKind::Xz => read_stream_header(kind, &mut reader).map_err(Attempt::Failed)?,
        _ => Vec::new(),
    };
    let first = kind.header_integrity(&header);
    let reader = std::io::Cursor::new(header).chain(reader);
    let mut decoded =
        StreamDecoder::open(kind, reader, budget, memory, first).map_err(Attempt::Failed)?;
    let failed = |error: std::io::Error| match outgrown(memory, &error) {
        Some(asked) => Attempt::Outgrew(asked),
        None => Attempt::Failed(decode_failed(kind, &error)),
    };
    let retained = *written;
    let passed =
        std::io::copy(&mut (&mut decoded).take(retained), &mut std::io::sink()).map_err(failed)?;
    if passed != retained {
        return Err(Attempt::Failed(ExtractionError::terminal(format!(
            "{kind:?} stream decoded {passed} bytes where an earlier decode of the same bytes \
             wrote {retained}"
        ))));
    }
    let mut counted = Counted {
        inner: &mut decoded,
        bytes: 0,
    };
    let result = std::io::copy(&mut counted, writer);
    // A read error leaves every byte read before it written, so the count is
    // exactly what the output holds past `retained`.
    *written = retained + counted.bytes;
    result.map_err(failed)?;
    Ok(decoded.integrity())
}

fn decode_failed(kind: SimpleArchiveKind, error: &std::io::Error) -> ExtractionError {
    ExtractionError::from_io(format!("failed to decompress {kind:?}: {error}"), error)
}

struct Counted<R> {
    inner: R,
    bytes: u64,
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.bytes += read as u64;
        Ok(read)
    }
}

// What a later frame or block that outgrew `memory` asked for, if that is
// why `error` stopped the decode.
fn outgrown(memory: StreamDecoderMemory, error: &std::io::Error) -> Option<StreamDecoderMemory> {
    let inner = error.get_ref();
    if let Some(outgrew) = inner.and_then(|inner| inner.downcast_ref::<DecoderOutgrew>()) {
        return Some(outgrew.asked);
    }
    if let Some(lzma_turbo::XzErrorKind::MemoryLimit { needed, .. }) = inner
        .and_then(|inner| inner.downcast_ref::<lzma_turbo::XzError>())
        .map(|error| &error.kind)
    {
        return Some(StreamDecoderMemory::xz(*needed));
    }
    // A frame the walk could not follow reaches the decoder's own window
    // check, which says only that the window is too large.
    crate::pipeline::extraction::error::zstd_frame_exceeds_window(error).then_some(
        StreamDecoderMemory {
            window: memory.window.saturating_mul(2),
            reserved: memory.reserved,
        },
    )
}

fn read_stream_header(
    kind: SimpleArchiveKind,
    reader: impl Read,
) -> Result<Vec<u8>, ExtractionError> {
    let mut header = Vec::with_capacity(SINGLE_STREAM_HEADER_BYTES);
    reader
        .take(SINGLE_STREAM_HEADER_BYTES as u64)
        .read_to_end(&mut header)
        .map_err(|error| {
            ExtractionError::from_io(format!("failed to read {kind:?} header: {error}"), &error)
        })?;
    Ok(header)
}

// The one place a sequential kind becomes its decoder, reading through the
// job's budget. A tar or split kind reads its bytes as they stand. An xz or
// zstd decoder is built to stay within `memory`, so a later stream, block or
// frame that asks for more fails instead of growing past what was reserved.
enum StreamDecoder<R: Read> {
    Plain(BudgetedReader<R>),
    Gz(flate2::read::MultiGzDecoder<BudgetedReader<R>>),
    Bzip2(bzip2::read::MultiBzDecoder<BudgetedReader<R>>),
    Deflate(flate2::read::DeflateDecoder<BudgetedReader<R>>),
    Brotli(Box<brotli::Decompressor<BudgetedReader<R>>>),
    // With what the first stream's header declares.
    Xz(Box<lzma_turbo::xz::XzReader<BudgetedReader<R>>>, Integrity),
    Zstd(
        Box<
            zstd::stream::read::Decoder<'static, std::io::BufReader<ZstdFrames<BudgetedReader<R>>>>,
        >,
    ),
}

impl<R: Read> StreamDecoder<R> {
    fn open(
        kind: SimpleArchiveKind,
        reader: R,
        budget: &Arc<JobExtractionBudget>,
        memory: StreamDecoderMemory,
        first: Integrity,
    ) -> Result<Self, ExtractionError> {
        let reader = BudgetedReader::new(reader, Arc::clone(budget));
        Ok(match kind {
            SimpleArchiveKind::Tar | SimpleArchiveKind::Split => Self::Plain(reader),
            SimpleArchiveKind::TarGz | SimpleArchiveKind::Gz => {
                Self::Gz(flate2::read::MultiGzDecoder::new(reader))
            }
            SimpleArchiveKind::TarBz2 | SimpleArchiveKind::Bzip2 => {
                Self::Bzip2(bzip2::read::MultiBzDecoder::new(reader))
            }
            SimpleArchiveKind::TarXz | SimpleArchiveKind::Xz => Self::Xz(
                Box::new(
                    crate::ingest::xz_multistream_decoder(reader, memory.window).map_err(
                        |error| {
                            ExtractionError::terminal(format!("failed to open xz decoder: {error}"))
                        },
                    )?,
                ),
                first,
            ),
            SimpleArchiveKind::Deflate => Self::Deflate(flate2::read::DeflateDecoder::new(reader)),
            SimpleArchiveKind::Brotli => {
                Self::Brotli(Box::new(brotli::Decompressor::new(reader, 4096)))
            }
            SimpleArchiveKind::Zstd => {
                let frames = ZstdFrames::new(reader, memory);
                let mut decoder = zstd::stream::read::Decoder::new(frames).map_err(|error| {
                    ExtractionError::from_io(format!("failed to open zstd: {error}"), &error)
                })?;
                // The window was admitted whole, so its log rounds up.
                let window_log =
                    (64 - memory.window.saturating_sub(1).leading_zeros()).clamp(10, 31);
                decoder.window_log_max(window_log).map_err(|error| {
                    ExtractionError::from_io(
                        format!("failed to apply zstd memory limit: {error}"),
                        &error,
                    )
                })?;
                Self::Zstd(Box::new(decoder))
            }
            SimpleArchiveKind::Zip => {
                return Err(ExtractionError::terminal("ZIP requires a seekable reader"));
            }
        })
    }

    // What a decode that reached the end has checked. gzip and bzip2 always
    // carry a checksum of the data. An xz check and a zstd content checksum
    // are optional, so they count only where every stream or frame declared
    // one; brotli, raw deflate and plain bytes have none.
    //
    // The xz decoder does not say what a later stream's header declared, so
    // only a decode that ended after one stream is judged by the first.
    fn integrity(&self) -> Integrity {
        match self {
            Self::Gz(_) | Self::Bzip2(_) => Integrity::Verified,
            Self::Xz(decoder, first) if decoder.stream_index() == 1 => *first,
            Self::Zstd(decoder) => decoder.get_ref().get_ref().integrity(),
            Self::Xz(..) | Self::Plain(_) | Self::Deflate(_) | Self::Brotli(_) => {
                Integrity::NoChecksum
            }
        }
    }
}

impl<R: Read> Read for StreamDecoder<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(reader) => reader.read(buffer),
            Self::Gz(decoder) => decoder.read(buffer),
            Self::Bzip2(decoder) => decoder.read(buffer),
            Self::Deflate(decoder) => decoder.read(buffer),
            Self::Brotli(decoder) => decoder.read(buffer),
            Self::Xz(decoder, _) => decoder.read(buffer),
            Self::Zstd(decoder) => decoder.read(buffer),
        }
    }
}

// A later zstd frame that asks more of its decoder than was admitted.
#[derive(Debug)]
struct DecoderOutgrew {
    asked: StreamDecoderMemory,
}

impl std::fmt::Display for DecoderOutgrew {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "zstd frame asks for a {} byte window and {} bytes of decoder memory",
            self.asked.window, self.asked.reserved
        )
    }
}

impl std::error::Error for DecoderOutgrew {}

// Follows a zstd stream's frames as its bytes pass to the decoder. Every
// frame's header is read here before the decoder reads it, so a frame that
// asks for more than was admitted is refused before the decoder allocates for
// it, and whether every frame ends in a content checksum is known when the
// stream does. Structure the walk cannot follow is left to the decoder to
// refuse, and proves nothing.
struct ZstdFrames<R> {
    inner: R,
    admitted: StreamDecoderMemory,
    walk: FrameWalk,
    field: [u8; ZSTD_FRAME_HEADER_MAX_BYTES],
    gathered: usize,
    checksum: bool,
    frames: u64,
    unchecked: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameWalk {
    // Gathering a frame's first four bytes.
    Magic,
    // Gathering a skippable frame's length.
    SkippableLength,
    // Gathering a frame header of this length.
    Header(usize),
    // Gathering a block header.
    Block,
    // Passing over bytes: a skippable payload, a block, or a checksum.
    Pass { left: u64, then: AfterPass },
    Lost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AfterPass {
    Frame,
    Block,
    LastBlock,
}

impl<R> ZstdFrames<R> {
    fn new(inner: R, admitted: StreamDecoderMemory) -> Self {
        Self {
            inner,
            admitted,
            walk: FrameWalk::Magic,
            field: [0; ZSTD_FRAME_HEADER_MAX_BYTES],
            gathered: 0,
            checksum: false,
            frames: 0,
            unchecked: false,
        }
    }

    fn integrity(&self) -> Integrity {
        if self.frames > 0 && !self.unchecked && self.walk == FrameWalk::Magic && self.gathered == 0
        {
            Integrity::Verified
        } else {
            Integrity::NoChecksum
        }
    }

    // Gathers bytes into the field until it holds `len`.
    fn gather(&mut self, bytes: &mut &[u8], len: usize) -> bool {
        let take = (len - self.gathered).min(bytes.len());
        self.field[self.gathered..self.gathered + take].copy_from_slice(&bytes[..take]);
        self.gathered += take;
        *bytes = &bytes[take..];
        self.gathered == len
    }

    fn pass(&mut self, left: u64, then: AfterPass) {
        self.walk = FrameWalk::Pass { left, then };
        if left == 0 {
            self.passed(then);
        }
    }

    fn passed(&mut self, then: AfterPass) {
        self.walk = match then {
            AfterPass::Frame => FrameWalk::Magic,
            AfterPass::Block => FrameWalk::Block,
            AfterPass::LastBlock if self.checksum => {
                self.pass(4, AfterPass::Frame);
                return;
            }
            AfterPass::LastBlock => FrameWalk::Magic,
        };
    }

    fn observe(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        while !bytes.is_empty() {
            match self.walk {
                FrameWalk::Lost => return Ok(()),
                FrameWalk::Pass { left, then } => {
                    let take = left.min(bytes.len() as u64);
                    bytes = &bytes[take as usize..];
                    self.walk = FrameWalk::Pass {
                        left: left - take,
                        then,
                    };
                    if take == left {
                        self.passed(then);
                    }
                }
                FrameWalk::Magic => {
                    if !self.gather(&mut bytes, 4) {
                        continue;
                    }
                    let magic = [self.field[0], self.field[1], self.field[2], self.field[3]];
                    if magic == ZSTD_MAGIC {
                        self.walk = FrameWalk::Header(5);
                    } else if zstd_skippable_magic(magic) {
                        self.walk = FrameWalk::SkippableLength;
                    } else {
                        self.lose();
                    }
                }
                FrameWalk::SkippableLength => {
                    if !self.gather(&mut bytes, 8) {
                        continue;
                    }
                    let length = [self.field[4], self.field[5], self.field[6], self.field[7]];
                    self.gathered = 0;
                    self.pass(u64::from(u32::from_le_bytes(length)), AfterPass::Frame);
                }
                FrameWalk::Header(len) => {
                    if !self.gather(&mut bytes, len) {
                        continue;
                    }
                    let full = zstd_frame_header_len(self.field[4]);
                    if len < full {
                        self.walk = FrameWalk::Header(full);
                        continue;
                    }
                    self.gathered = 0;
                    let Ok(frame) = zstd_frame_header(&self.field[..len]) else {
                        self.lose();
                        continue;
                    };
                    self.frames += 1;
                    self.checksum = frame.checksum;
                    self.unchecked |= !frame.checksum;
                    let asked = StreamDecoderMemory::zstd(frame);
                    if asked.window > self.admitted.window
                        || asked.reserved > self.admitted.reserved
                    {
                        return Err(std::io::Error::other(DecoderOutgrew { asked }));
                    }
                    self.walk = FrameWalk::Block;
                }
                FrameWalk::Block => {
                    if !self.gather(&mut bytes, 3) {
                        continue;
                    }
                    self.gathered = 0;
                    let header =
                        u32::from_le_bytes([self.field[0], self.field[1], self.field[2], 0]);
                    let size = u64::from(header >> 3);
                    let then = if header & 1 != 0 {
                        AfterPass::LastBlock
                    } else {
                        AfterPass::Block
                    };
                    match (header >> 1) & 0b11 {
                        // Raw and compressed blocks carry `size` bytes; an
                        // RLE block carries the one byte it repeats.
                        0 | 2 => self.pass(size, then),
                        1 => self.pass(1, then),
                        _ => self.lose(),
                    }
                }
            }
        }
        Ok(())
    }

    fn lose(&mut self) {
        self.walk = FrameWalk::Lost;
        self.unchecked = true;
    }
}

impl<R: Read> Read for ZstdFrames<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.observe(&buffer[..read])?;
        Ok(read)
    }
}

// The output a stream's input names: its directory kept, its codec suffix
// dropped. A name that is nothing but the suffix names no output at all, and
// is refused before any output exists.
fn stream_output_name(context: &SingleStreamContext<'_>) -> Result<String, ExtractionError> {
    let kind = context.kind;
    let input = context.input.to_string_lossy();
    let (directory, file) = input
        .rsplit_once(['/', '\\'])
        .unwrap_or(("", input.as_ref()));
    let stem = derive_single_file_output_name(file, kind.single_stream_suffixes());
    if stem.is_empty() {
        return Err(ExtractionError::terminal(format!(
            "{kind:?} stream '{input}' names no output: its name is only its suffix"
        )));
    }
    Ok(if directory.is_empty() {
        stem.to_owned()
    } else {
        format!("{directory}/{stem}")
    })
}

// Opens the stream's one output and hands it to `decode`, which writes the
// decoded bytes and says how many there were and what they proved.
fn write_stream<O: ArchiveOutput>(
    output: &O,
    context: &SingleStreamContext<'_>,
    decode: impl FnOnce(&mut std::io::BufWriter<O::Member>) -> Result<(u64, Integrity), ExtractionError>,
) -> Result<Vec<String>, ExtractionError> {
    let SingleStreamContext {
        kind,
        budget,
        event_tx,
        job_id,
        set_name,
        ..
    } = *context;
    let name = stream_output_name(context)?;
    let path = ExtractionRoot::parse_relative_path(&name)
        .map_err(|error| budget.reject_unsafe_path(error))?;

    let _ = event_tx.send(PipelineEvent::ExtractionMemberStarted {
        job_id,
        set_name: set_name.to_string(),
        member: name.clone(),
    });
    let member = output
        .file(0, &path, None)
        .map_err(|error| ExtractionError::io(&error))?;
    let mut writer = buffered_extraction_output(member);
    let (bytes_written, integrity) = decode(&mut writer)?;
    writer
        .into_inner()
        .map_err(|error| error.into_error())
        .and_then(OutputMember::finish_data)
        .and_then(|pending| pending.integrity(integrity))
        .map_err(|error| decode_failed(kind, &error))?;
    let _ = event_tx.send(PipelineEvent::ExtractionMemberFinished {
        job_id,
        set_name: set_name.to_string(),
        member: name.clone(),
    });
    tracing::info!(job_id = job_id.0, member = %name, bytes_written, format = ?kind, "compressed file decompressed");

    Ok(vec![name])
}

// Enough of a stream to read what its header declares: an xz stream header
// is twelve bytes.
const SINGLE_STREAM_HEADER_BYTES: usize = 12;

impl SimpleArchiveKind {
    fn single_stream_suffixes(self) -> &'static [&'static str] {
        match self {
            Self::Gz => &[".gz"],
            Self::Bzip2 => &[".bz2"],
            Self::Xz => &[".xz"],
            Self::Deflate => &[".deflate"],
            Self::Brotli => &[".br"],
            Self::Zstd => &[".zstd", ".zst"],
            Self::Zip | Self::Tar | Self::TarGz | Self::TarBz2 | Self::TarXz | Self::Split => &[],
        }
    }

    // What a stream's `header`, its leading bytes, declares a decode that
    // reaches the end will have checked. gzip and bzip2 always carry a
    // checksum of the data; an xz stream carries one where its header
    // declares one. A zstd checksum is declared per frame, so a zstd stream
    // is judged by its decode, never by its first header.
    fn header_integrity(self, header: &[u8]) -> Integrity {
        let verified = match self {
            Self::Gz | Self::Bzip2 => true,
            Self::Xz => lzma_turbo::xz::probe(header).is_some_and(|stream| {
                let check = stream.flags.check;
                !matches!(check, lzma_turbo::xz::CheckType::None) && check.is_verifiable()
            }),
            _ => false,
        };
        if verified {
            Integrity::Verified
        } else {
            Integrity::NoChecksum
        }
    }
}

// A decoder's memory as its own header sizes it: what is reserved for it, and
// the window or dictionary its decoder is built to stay within, so a later
// frame or block that asks for more is refused instead of holding what the
// reservation never counted. A codec whose footprint is fixed has no window to
// grow, so its window is what is reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline) struct StreamDecoderMemory {
    pub(in crate::pipeline) reserved: u64,
    pub(super) window: u64,
}

// Everything an xz decoder holds besides its dictionary: its input buffer,
// its scratch, and the LZMA probability tables, which are a few hundred
// kilobytes together at most.
const XZ_DECODER_OVERHEAD_BYTES: u64 = 1024 * 1024;

// The largest window a zstd decoder admits: libzstd refuses a window log
// above 31 whatever memory it is given.
const ZSTD_WINDOW_MAX_BYTES: u64 = 1 << 31;

impl StreamDecoderMemory {
    // A zstd decoder admitted for `frame`: held to its window, rounded up to
    // the power of two a window log admits, and reserved what it buffers.
    fn zstd(frame: ZstdFrameHeader) -> Self {
        Self {
            window: frame
                .window
                .max(1024)
                .checked_next_power_of_two()
                .unwrap_or(u64::MAX),
            reserved: frame
                .held
                .max(1024)
                .saturating_add(ZSTD_DECODER_OVERHEAD_BYTES),
        }
    }

    // An xz decoder held to `dictionary`, and reserved that and the rest of
    // what it holds.
    fn xz(dictionary: u64) -> Self {
        Self {
            window: dictionary,
            reserved: dictionary.saturating_add(XZ_DECODER_OVERHEAD_BYTES),
        }
    }

    // Whether a decoder of `kind` may be admitted this much under `ceiling`.
    fn admissible(self, kind: SimpleArchiveKind, ceiling: u64) -> bool {
        let window_max = match kind {
            SimpleArchiveKind::Zstd => ZSTD_WINDOW_MAX_BYTES,
            SimpleArchiveKind::Xz => crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES,
            _ => u64::MAX,
        };
        self.reserved <= ceiling && self.window <= window_max
    }

    // What to admit after a later frame or block asked for `asked`, or `None`
    // when that is past `ceiling`. A dimension that grows at least doubles,
    // so a run of frames each asking a little more than the last restarts
    // the decode a bounded number of times.
    fn grown(self, kind: SimpleArchiveKind, asked: Self, ceiling: u64) -> Option<Self> {
        if !asked.admissible(kind, ceiling) {
            return None;
        }
        let grow = |current: u64, asked: u64, limit: u64| {
            if asked <= current {
                current
            } else {
                asked.max(current.saturating_mul(2)).min(limit)
            }
        };
        let grown = match kind {
            // The xz decoder's limit is its dictionary, and what is reserved
            // follows it.
            SimpleArchiveKind::Xz => Self::xz(grow(
                self.window,
                asked.window,
                crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES
                    .min(ceiling.saturating_sub(XZ_DECODER_OVERHEAD_BYTES)),
            )),
            _ => Self {
                window: grow(self.window, asked.window, ZSTD_WINDOW_MAX_BYTES),
                reserved: grow(self.reserved, asked.reserved, ceiling),
            },
        };
        (grown != self).then_some(grown)
    }
}

// Decoder memory for one archive of `kind`, sized from its own leading bytes
// where the format declares what its decoder will hold. `header` is called
// only for those formats; a reader over bytes still downloading waits for
// them, and the decode opens its own from the start.
pub(in crate::pipeline) fn stream_decoder_memory<R: Read>(
    kind: SimpleArchiveKind,
    header: impl FnOnce() -> R,
    ceiling: u64,
) -> Result<StreamDecoderMemory, String> {
    const MIB: u64 = 1024 * 1024;
    let fixed = |bytes: u64| {
        Ok(StreamDecoderMemory {
            reserved: bytes,
            window: bytes,
        })
    };
    match kind {
        SimpleArchiveKind::Zstd => {
            let frame = zstd_first_frame_header(header())?;
            let memory = StreamDecoderMemory::zstd(frame);
            if !memory.admissible(kind, ceiling) {
                return Err(format!(
                    "zstd frame needs a {} byte window and {} bytes of decoder memory, \
                     above the {ceiling} byte limit",
                    memory.window, memory.reserved
                ));
            }
            Ok(memory)
        }
        SimpleArchiveKind::Xz => {
            let memory = StreamDecoderMemory::xz(xz_dictionary_bytes(header())?);
            if !memory.admissible(kind, ceiling) {
                return Err(format!(
                    "xz block needs a {} byte dictionary and {} bytes of decoder memory, \
                     above the {} byte dictionary limit or the {ceiling} byte limit",
                    memory.window,
                    memory.reserved,
                    crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES
                ));
            }
            Ok(memory)
        }
        // A tar.xz is reserved the whole xz limit and its decoder is held to
        // the same, so no later stream can outgrow what was reserved.
        SimpleArchiveKind::TarXz => {
            fixed(crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES.min(ceiling))
        }
        SimpleArchiveKind::Brotli => fixed(32 * MIB),
        SimpleArchiveKind::Zip | SimpleArchiveKind::TarBz2 | SimpleArchiveKind::Bzip2 => {
            fixed(8 * MIB)
        }
        SimpleArchiveKind::Tar
        | SimpleArchiveKind::TarGz
        | SimpleArchiveKind::Gz
        | SimpleArchiveKind::Deflate
        | SimpleArchiveKind::Split => fixed(MIB),
    }
}

// The dictionary an xz stream's first block needs. A stream that opens on
// its index has no block, and is held to the smallest dictionary LZMA has.
// The decoder caps a block's dictionary to the uncompressed size its header
// declares, since no match reaches back past the block's first byte, so the
// first block is sized the same way.
fn xz_dictionary_bytes(mut source: impl Read) -> Result<u64, String> {
    use lzma_turbo::xz::{BlockHeader, StreamHeader, block, stream::STREAM_HEADER_SIZE};
    const LZMA_DICTIONARY_MIN_BYTES: u64 = 4096;

    let truncated = |error: std::io::Error| format!("failed to read xz header: {error}");
    let mut stream = [0u8; STREAM_HEADER_SIZE];
    source.read_exact(&mut stream).map_err(truncated)?;
    StreamHeader::parse(&stream).map_err(|error| format!("xz stream header: {error}"))?;
    let mut first = [0u8; 1];
    source.read_exact(&mut first).map_err(truncated)?;
    let Some(size) = block::header_size_from_first_byte(first[0]) else {
        return Ok(LZMA_DICTIONARY_MIN_BYTES);
    };
    let mut block = vec![0u8; size];
    block[0] = first[0];
    source.read_exact(&mut block[1..]).map_err(truncated)?;
    let block = BlockHeader::parse(&block).map_err(|error| format!("xz block header: {error}"))?;
    let dictionary = lzma2_dictionary_bytes(block.chain.dict_prop);
    Ok(block.uncompressed_size.map_or(dictionary, |size| {
        dictionary.min(size.max(LZMA_DICTIONARY_MIN_BYTES))
    }))
}

// The LZMA2 dictionary-size property as bytes (xz file format, section
// 5.3.1). lzma_turbo keeps its own conversion private to the crate.
fn lzma2_dictionary_bytes(property: u8) -> u64 {
    if property >= 40 {
        return u64::from(u32::MAX);
    }
    (2 | u64::from(property & 1)) << (u32::from(property) / 2 + 11)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::extraction::ExtractionLimits;
    use std::io::{Cursor, Write};

    const MIB: u64 = 1024 * 1024;

    fn budget(directory: &Path, max_memory_bytes: u64) -> Arc<JobExtractionBudget> {
        JobExtractionBudget::new(
            Arc::new(ExtractionLimits {
                max_job_bytes: 1 << 40,
                max_member_bytes: 1 << 40,
                max_entries: 1000,
                max_ratio: 1_000_000,
                max_seconds: 43_200,
                min_free_bytes: 1,
                max_memory_bytes,
            }),
            directory.to_path_buf(),
            1 << 30,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap()
    }

    fn patterned(len: usize, seed: u32) -> Vec<u8> {
        (0..len as u32)
            .map(|index| (index.wrapping_add(seed).wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect()
    }

    fn zstd_frame(bytes: &[u8], window_log: u32, checksum: bool) -> Vec<u8> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.window_log(window_log).unwrap();
        encoder.include_checksum(checksum).unwrap();
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    // A frame written by hand: a window descriptor of `window_exponent`, a
    // two-byte content size, and the payload as one raw block. No encoder
    // pairs a window this much larger than its content.
    fn raw_zstd_frame(payload: &[u8], window_exponent: u8) -> Vec<u8> {
        assert!((256..65_536 + 256).contains(&payload.len()));
        let mut frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0b0100_0000, window_exponent << 3];
        frame.extend_from_slice(&((payload.len() - 256) as u16).to_le_bytes());
        let block = 1 | ((payload.len() as u32) << 3);
        frame.extend_from_slice(&block.to_le_bytes()[..3]);
        frame.extend_from_slice(payload);
        frame
    }

    fn skippable_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = 0x184D_2A50u32.to_le_bytes().to_vec();
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn xz_stream(bytes: &[u8], preset: u32, check: bool) -> Vec<u8> {
        let props = lzma_turbo::LzmaEncProps::xz_preset(preset, false).unwrap();
        let mut writer = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
        if !check {
            writer.set_check(lzma_turbo::xz::CheckType::None).unwrap();
        }
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    }

    struct Decoded {
        directory: tempfile::TempDir,
        budget: Arc<JobExtractionBudget>,
        result: Result<Vec<String>, ExtractionError>,
    }

    // Decodes `stream` the way a stream engine does: sized from its header,
    // reserved, and held to that size, reopening it when it outgrows that.
    fn decode_named(kind: SimpleArchiveKind, stream: &[u8], ceiling: u64, input: &str) -> Decoded {
        let directory = tempfile::tempdir().unwrap();
        let budget = budget(directory.path(), ceiling);
        let root = ExtractionRoot::open(directory.path()).unwrap();
        let (event_tx, _) = broadcast::channel(8);
        let result = decode_single_stream_to(
            || Ok(Cursor::new(stream)),
            &FileOutput {
                root: &root,
                budget: &budget,
            },
            &SingleStreamContext {
                kind,
                input: Path::new(input),
                budget: &budget,
                event_tx: &event_tx,
                job_id: JobId(1),
                set_name: "payload",
            },
        );
        Decoded {
            directory,
            budget,
            result,
        }
    }

    fn decode(kind: SimpleArchiveKind, stream: &[u8], ceiling: u64) -> Decoded {
        let input = format!("dir/payload.bin{}", kind.single_stream_suffixes()[0]);
        decode_named(kind, stream, ceiling, &input)
    }

    // What a whole decode of `stream` proves, as sized from its header.
    fn integrity(kind: SimpleArchiveKind, stream: &[u8]) -> Integrity {
        let directory = tempfile::tempdir().unwrap();
        let budget = budget(directory.path(), 1 << 30);
        let memory = stream_decoder_memory(kind, || Cursor::new(stream), 1 << 30).unwrap();
        let mut reader = Cursor::new(stream);
        let header = match kind {
            SimpleArchiveKind::Xz => read_stream_header(kind, &mut reader).unwrap(),
            _ => Vec::new(),
        };
        let first = kind.header_integrity(&header);
        let mut decoded = StreamDecoder::open(
            kind,
            Cursor::new(header).chain(reader),
            &budget,
            memory,
            first,
        )
        .unwrap();
        std::io::copy(&mut decoded, &mut std::io::sink()).unwrap();
        decoded.integrity()
    }

    // Finding: a later frame that outgrows the first header's window used to
    // fail the job. It now decodes again from the start under what it asked
    // for, and the bytes already written are neither written nor charged a
    // second time.
    #[test]
    fn a_later_zstd_frame_past_the_first_frames_window_decodes_again_under_its_own() {
        let first = patterned(4096, 1);
        let second = patterned(1 << 21, 2);
        let mut stream = zstd_frame(&first, 10, false);
        stream.extend(zstd_frame(&second, 24, false));
        let decoded = decode(SimpleArchiveKind::Zstd, &stream, 1 << 30);
        assert_eq!(decoded.result.unwrap(), ["dir/payload.bin"]);
        let output = [first, second].concat();
        assert_eq!(
            std::fs::read(decoded.directory.path().join("dir/payload.bin")).unwrap(),
            output
        );
        assert_eq!(decoded.budget.total_written(), output.len() as u64);
    }

    #[test]
    fn a_later_xz_stream_past_the_first_streams_dictionary_decodes_again_under_its_own() {
        let first = patterned(4096, 8);
        let second = patterned(1 << 20, 9);
        let mut stream = xz_stream(&first, 0, true);
        stream.extend(xz_stream(&second, 6, true));
        let decoded = decode(SimpleArchiveKind::Xz, &stream, 1 << 30);
        assert_eq!(decoded.result.unwrap(), ["dir/payload.bin"]);
        let output = [first, second].concat();
        assert_eq!(
            std::fs::read(decoded.directory.path().join("dir/payload.bin")).unwrap(),
            output
        );
        assert_eq!(decoded.budget.total_written(), output.len() as u64);
    }

    // A zstd frame header carries no checksum, so a later one that asks for
    // more than the ceiling may be damage repair clears; the first header's
    // own refusal stays final.
    #[test]
    fn a_later_zstd_frame_past_the_ceiling_is_recoverable_and_a_first_one_terminal() {
        let mut stream = zstd_frame(&patterned(4096, 3), 10, false);
        stream.extend(zstd_frame(&patterned(1 << 21, 4), 24, false));
        let decoded = decode(SimpleArchiveKind::Zstd, &stream, 8 * MIB);
        let error = decoded
            .result
            .expect_err("frame 2 asks for more than 8 MiB");
        assert!(error.is(ExtractionErrorClass::Recoverable), "{error}");
        assert!(error.message().contains("above the"), "{error}");

        let wide = zstd_frame(&patterned(1 << 21, 5), 24, false);
        let decoded = decode(SimpleArchiveKind::Zstd, &wide, 8 * MIB);
        let error = decoded
            .result
            .expect_err("frame 1 asks for more than 8 MiB");
        assert!(error.is(ExtractionErrorClass::Terminal), "{error}");
    }

    // A later xz block's header carries a CRC, so what it asks for past the
    // ceiling is what was written.
    #[test]
    fn a_later_xz_block_past_the_ceiling_is_terminal() {
        let mut stream = xz_stream(&patterned(4096, 6), 0, true);
        stream.extend(xz_stream(&patterned(1 << 20, 7), 6, true));
        let decoded = decode(SimpleArchiveKind::Xz, &stream, MIB + 512 * 1024);
        let error = decoded
            .result
            .expect_err("stream 2 needs a 1 MiB dictionary");
        assert!(error.is(ExtractionErrorClass::Terminal), "{error}");
    }

    #[test]
    fn zstd_frames_within_the_first_frames_window_decode() {
        let first = patterned(1 << 20, 3);
        let second = patterned(1 << 19, 4);
        let mut stream = zstd_frame(&first, 20, true);
        stream.extend(zstd_frame(&second, 19, true));
        let decoded = decode(SimpleArchiveKind::Zstd, &stream, 1 << 30);
        assert_eq!(decoded.result.unwrap(), ["dir/payload.bin"]);
        assert_eq!(
            std::fs::read(decoded.directory.path().join("dir/payload.bin")).unwrap(),
            [first, second].concat()
        );
    }

    #[test]
    fn a_zstd_window_reservation_covers_the_whole_window_its_decoder_admits() {
        let stream = zstd_frame(&patterned(1 << 20, 5), 20, false);
        let memory =
            stream_decoder_memory(SimpleArchiveKind::Zstd, || Cursor::new(&stream), 1 << 30)
                .unwrap();
        assert_eq!(memory.window, 1 << 20);
        assert_eq!(memory.reserved, (1 << 20) + ZSTD_DECODER_OVERHEAD_BYTES);
    }

    // Finding: libzstd checks a frame's window descriptor, not its content
    // size, against the window log. A frame declaring an 8 MiB window over a
    // few hundred bytes is held to the 8 MiB window and reserved the few
    // hundred bytes, so it decodes under a ceiling below its window.
    #[test]
    fn a_zstd_window_descriptor_sets_the_window_and_the_content_the_reservation() {
        let payload = patterned(300, 6);
        let frame = raw_zstd_frame(&payload, 13);
        let memory =
            stream_decoder_memory(SimpleArchiveKind::Zstd, || Cursor::new(&frame), 4 * MIB)
                .unwrap();
        assert_eq!(memory.window, 8 * MIB);
        assert_eq!(memory.reserved, 1024 + ZSTD_DECODER_OVERHEAD_BYTES);
        let decoded = decode(SimpleArchiveKind::Zstd, &frame, 4 * MIB);
        assert_eq!(decoded.result.unwrap(), ["dir/payload.bin"]);
        assert_eq!(
            std::fs::read(decoded.directory.path().join("dir/payload.bin")).unwrap(),
            payload
        );
    }

    // Finding: pzstd opens its output with a twelve-byte skippable frame,
    // longer than any frame header, which a fixed peek refused as truncated.
    #[test]
    fn skippable_frames_ahead_of_the_first_frame_are_passed_over() {
        let payload = patterned(1 << 16, 7);
        let frame = zstd_frame(&payload, 17, true);
        for prefix in [
            skippable_frame(&[0x10, 0, 0, 0]),
            [skippable_frame(&patterned(40, 8)), skippable_frame(&[])].concat(),
        ] {
            let stream = [prefix, frame.clone()].concat();
            let memory =
                stream_decoder_memory(SimpleArchiveKind::Zstd, || Cursor::new(&stream), 1 << 30)
                    .unwrap();
            assert_eq!(memory.window, 1 << 17);
            let decoded = decode(SimpleArchiveKind::Zstd, &stream, 1 << 30);
            assert_eq!(decoded.result.unwrap(), ["dir/payload.bin"]);
            assert_eq!(
                std::fs::read(decoded.directory.path().join("dir/payload.bin")).unwrap(),
                payload
            );
            assert_eq!(
                integrity(SimpleArchiveKind::Zstd, &stream),
                Integrity::Verified
            );
        }
        let truncated = skippable_frame(&[0; 16]);
        assert!(
            stream_decoder_memory(
                SimpleArchiveKind::Zstd,
                || Cursor::new(&truncated[..20]),
                1 << 30
            )
            .is_err()
        );
    }

    // Finding: the decoder caps a block's dictionary to the uncompressed size
    // its header declares, so an `xz -9` file of a few kilobytes is sized to
    // those kilobytes, not to its 64 MiB dictionary.
    #[test]
    fn an_xz_reservation_is_the_first_blocks_capped_dictionary_and_the_decoder_overhead() {
        for (preset, len, dictionary) in [
            (0, 1 << 20, 256 << 10),
            (6, 1 << 16, 1 << 16),
            (9, 1000, 4096),
        ] {
            let stream = xz_stream(&patterned(len, preset), preset, true);
            let memory =
                stream_decoder_memory(SimpleArchiveKind::Xz, || Cursor::new(&stream), 1 << 30)
                    .unwrap();
            assert_eq!(memory.window, dictionary, "preset {preset}");
            assert_eq!(
                memory.reserved,
                dictionary + XZ_DECODER_OVERHEAD_BYTES,
                "preset {preset}"
            );
        }
        let payload = patterned(20_000, 9);
        let stream = xz_stream(&payload, 9, true);
        let decoded = decode(SimpleArchiveKind::Xz, &stream, 32 * MIB);
        assert_eq!(decoded.result.unwrap(), ["dir/payload.bin"]);
        assert_eq!(
            std::fs::read(decoded.directory.path().join("dir/payload.bin")).unwrap(),
            payload
        );
    }

    // Finding: the reservation was clamped to the ceiling while the decoder
    // held its dictionary and its overhead. What it holds must fit whole.
    #[test]
    fn an_xz_decoder_is_admitted_only_when_its_dictionary_and_overhead_fit() {
        let stream = xz_stream(&patterned(1 << 20, 7), 0, true);
        let whole = (256 << 10) + XZ_DECODER_OVERHEAD_BYTES;
        let memory =
            stream_decoder_memory(SimpleArchiveKind::Xz, || Cursor::new(&stream), whole).unwrap();
        assert_eq!(memory.reserved, whole);
        assert_eq!(memory.window, 256 << 10);
        assert!(
            stream_decoder_memory(SimpleArchiveKind::Xz, || Cursor::new(&stream), whole - 1)
                .is_err()
        );
    }

    #[test]
    fn a_completed_file_within_its_first_headers_window_decodes() {
        let first = patterned(1 << 19, 15);
        let second = patterned(1 << 18, 16);
        let mut zstd = zstd_frame(&first, 20, true);
        zstd.extend(zstd_frame(&second, 19, true));
        let mut xz = xz_stream(&first, 6, true);
        xz.extend(xz_stream(&second, 0, true));
        for (kind, stream) in [(SimpleArchiveKind::Zstd, zstd), (SimpleArchiveKind::Xz, xz)] {
            let (directory, result) = decode_file(kind, &stream, 1 << 30);
            assert_eq!(result.unwrap(), ["payload.bin"], "{kind:?}");
            assert_eq!(
                std::fs::read(directory.path().join("payload.bin")).unwrap(),
                [first.as_slice(), second.as_slice()].concat(),
                "{kind:?}"
            );
        }
    }

    // Decodes `stream` as a completed file on disk, the way the simple-archive
    // task does.
    fn decode_file(
        kind: SimpleArchiveKind,
        stream: &[u8],
        ceiling: u64,
    ) -> (tempfile::TempDir, Result<Vec<String>, ExtractionError>) {
        let directory = tempfile::tempdir().unwrap();
        let archive_path = directory
            .path()
            .join(format!("payload.bin{}", kind.single_stream_suffixes()[0]));
        std::fs::write(&archive_path, stream).unwrap();
        let budget = budget(directory.path(), ceiling);
        let root = ExtractionRoot::open(directory.path()).unwrap();
        let (event_tx, _) = broadcast::channel(8);
        let context = SequentialExtractionContext {
            kind,
            archive_path: &archive_path,
            root: &root,
            budget: &budget,
            event_tx: &event_tx,
            job_id: JobId(1),
            set_name: "payload",
        };
        let result = match kind {
            SimpleArchiveKind::Xz => extract_xz(&context, 1),
            _ => extract_file(&context),
        };
        (directory, result)
    }

    #[test]
    fn a_completed_file_past_its_first_headers_window_decodes_again_under_its_own() {
        let first = patterned(4096, 11);
        let second = patterned(1 << 20, 12);
        let mut zstd = zstd_frame(&first, 10, false);
        zstd.extend(zstd_frame(&second, 24, false));
        let mut xz = xz_stream(&first, 0, true);
        xz.extend(xz_stream(&second, 6, true));
        for (kind, stream) in [(SimpleArchiveKind::Zstd, zstd), (SimpleArchiveKind::Xz, xz)] {
            let (directory, result) = decode_file(kind, &stream, 1 << 30);
            assert_eq!(result.unwrap(), ["payload.bin"], "{kind:?}");
            assert_eq!(
                std::fs::read(directory.path().join("payload.bin")).unwrap(),
                [first.as_slice(), second.as_slice()].concat(),
                "{kind:?}"
            );
        }
    }

    // Finding: integrity was judged from the first frame or stream alone. A
    // zstd stream is verified only when every frame ends in a checksum; an xz
    // stream only when its one stream declares a check, since the decoder
    // does not say what a later stream's header declared.
    #[test]
    fn integrity_is_proved_by_every_frame_or_stream_not_the_first() {
        let payload = patterned(4096, 10);
        let checked = zstd_frame(&payload, 10, true);
        let unchecked = zstd_frame(&payload, 10, false);
        let xz_checked = xz_stream(&payload, 0, true);
        let xz_unchecked = xz_stream(&payload, 0, false);
        let cases = [
            (
                SimpleArchiveKind::Zstd,
                checked.clone(),
                Integrity::Verified,
            ),
            (
                SimpleArchiveKind::Zstd,
                unchecked.clone(),
                Integrity::NoChecksum,
            ),
            (
                SimpleArchiveKind::Zstd,
                [checked.clone(), checked.clone()].concat(),
                Integrity::Verified,
            ),
            (
                SimpleArchiveKind::Zstd,
                [checked.clone(), unchecked.clone()].concat(),
                Integrity::NoChecksum,
            ),
            (
                SimpleArchiveKind::Zstd,
                [unchecked, checked].concat(),
                Integrity::NoChecksum,
            ),
            (
                SimpleArchiveKind::Xz,
                xz_checked.clone(),
                Integrity::Verified,
            ),
            (
                SimpleArchiveKind::Xz,
                xz_unchecked.clone(),
                Integrity::NoChecksum,
            ),
            (
                SimpleArchiveKind::Xz,
                [xz_checked, xz_unchecked].concat(),
                Integrity::NoChecksum,
            ),
        ];
        for (index, (kind, stream, expected)) in cases.into_iter().enumerate() {
            assert_eq!(integrity(kind, &stream), expected, "case {index} {kind:?}");
        }
        for kind in [SimpleArchiveKind::Gz, SimpleArchiveKind::Bzip2] {
            assert_eq!(kind.header_integrity(&[]), Integrity::Verified);
        }
        for kind in [
            SimpleArchiveKind::Xz,
            SimpleArchiveKind::Zstd,
            SimpleArchiveKind::Brotli,
        ] {
            assert_eq!(kind.header_integrity(&[]), Integrity::NoChecksum);
        }
    }

    // Finding: a member named only `.gz` stripped to an empty name, and
    // `dir/.gz` to the directory itself. Neither names an output, and no
    // output is created for either.
    #[test]
    fn a_stream_named_only_its_suffix_is_refused_before_any_output() {
        let stream = {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(b"payload").unwrap();
            encoder.finish().unwrap()
        };
        for input in [".gz", "dir/.gz", "dir/.GZ"] {
            let decoded = decode_named(SimpleArchiveKind::Gz, &stream, 1 << 30, input);
            let error = decoded.result.expect_err(input);
            assert!(error.is(ExtractionErrorClass::Terminal), "{input}: {error}");
            assert!(
                error.message().contains("names no output"),
                "{input}: {error}"
            );
            assert_eq!(
                std::fs::read_dir(decoded.directory.path()).unwrap().count(),
                0,
                "{input}"
            );
        }
    }
}
