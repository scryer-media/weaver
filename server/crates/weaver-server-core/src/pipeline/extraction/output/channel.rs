//! Bounded bridge from blocking archive engines to their owning coordinator.
//! A frame retains admission until placement or an accounted transfer into
//! the coordinator's bounded holds/scratch storage, not just channel delivery.

use super::*;
use std::path::PathBuf;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::pipeline::extraction::safety::MemoryPermit;

const FRAME_OVERHEAD: usize = 256;
const CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct StreamOwner {
    pub root: u64,
    pub node: u32,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MemberId {
    pub owner: StreamOwner,
    pub occurrence: usize,
}

#[derive(Debug)]
pub(crate) enum Event {
    File { path: PathBuf, length: Option<u64> },
    // Bytes go directly to the generation's private output root. Completion
    // acknowledges physical placement, while integrity remains independent.
    PhysicalFile { path: PathBuf, length: Option<u64> },
    Directory { path: PathBuf },
    Bytes { offset: u64, bytes: bytes::Bytes },
    Metadata(Metadata),
    DataComplete { length: u64 },
    Integrity(Integrity),
}

struct Admission {
    memory: MemoryPermit,
    capacity: Arc<Semaphore>,
}

struct Control {
    budget: Arc<JobExtractionBudget>,
    revoked: watch::Sender<bool>,
}

// The generation owner retains this after individual member proofs succeed.
// Revocation wakes both queue-capacity and byte-capacity waits. It does not
// join workers or delete their files; those are coordinator responsibilities.
#[derive(Clone)]
pub(crate) struct Generation {
    owner: StreamOwner,
    control: Arc<Control>,
}

impl Generation {
    pub(crate) fn owner(&self) -> StreamOwner {
        self.owner
    }

    pub(crate) fn revoke(&self) {
        self.control.revoked.send_replace(true);
    }

    pub(crate) fn is_revoked(&self) -> bool {
        *self.control.revoked.borrow() || self.control.budget.stopped_failure().is_some()
    }
}

pub(crate) struct FrameLease {
    generation: Generation,
    _admission: Arc<Admission>,
    _bytes: OwnedSemaphorePermit,
}

struct WorkerAttempt {
    generation: Generation,
    succeeded: bool,
}

impl Drop for WorkerAttempt {
    fn drop(&mut self) {
        if !self.succeeded {
            self.generation.revoke();
        }
    }
}

impl FrameLease {
    pub(crate) fn is_revoked(&self) -> bool {
        self.generation.is_revoked()
    }
}

pub(crate) struct Frame {
    pub member: MemberId,
    pub event: Event,
    // Keep this through placement, or release it after the router has charged
    // retained bytes to its own holds/scratch admission. Retaining it until
    // placement when a later header is needed would deadlock the producer.
    pub lease: FrameLease,
}

#[derive(Clone)]
pub(crate) struct ChannelOutput {
    generation: Generation,
    admission: Arc<Admission>,
    sender: mpsc::Sender<Frame>,
    runtime: Handle,
    budget: Arc<JobExtractionBudget>,
}

impl ChannelOutput {
    // Reserve admission before starting a worker, while it holds no decoder
    // memory. If admission is unavailable, the coordinator defers that worker.
    pub(crate) fn new(
        owner: StreamOwner,
        memory: MemoryPermit,
        sender: mpsc::Sender<Frame>,
        runtime: Handle,
        budget: Arc<JobExtractionBudget>,
    ) -> io::Result<(Self, Generation)> {
        let capacity = usize::try_from(memory.bytes())
            .ok()
            .filter(|capacity| *capacity > FRAME_OVERHEAD && *capacity <= u32::MAX as usize)
            .ok_or_else(|| io::Error::other("invalid archive output admission"))?;
        let generation = Generation {
            owner,
            control: Arc::new(Control {
                budget: Arc::clone(&budget),
                revoked: watch::channel(false).0,
            }),
        };
        Ok((
            Self {
                generation: generation.clone(),
                admission: Arc::new(Admission {
                    memory,
                    capacity: Arc::new(Semaphore::new(capacity)),
                }),
                sender,
                runtime,
                budget,
            },
            generation,
        ))
    }

    // The worker boundary covers failures between members and unwinding, when
    // no pending member token remains to revoke earlier successful outputs.
    pub(crate) fn run<T, E>(&self, worker: impl FnOnce(&Self) -> Result<T, E>) -> Result<T, E> {
        let mut attempt = WorkerAttempt {
            generation: self.generation.clone(),
            succeeded: false,
        };
        let result = worker(self);
        attempt.succeeded = result.is_ok();
        result
    }

    fn send(
        &self,
        occurrence: usize,
        payload_bytes: usize,
        event: impl FnOnce() -> Event,
    ) -> io::Result<()> {
        // Only blocking decoder workers call this bridge. The actor drains it
        // asynchronously and must never join a worker synchronously.
        self.runtime
            .block_on(self.send_async(occurrence, payload_bytes, event))
    }

    async fn send_async(
        &self,
        occurrence: usize,
        payload_bytes: usize,
        event: impl FnOnce() -> Event,
    ) -> io::Result<()> {
        self.budget.check_active_io()?;
        let charge = payload_bytes
            .checked_add(FRAME_OVERHEAD)
            .filter(|bytes| *bytes as u64 <= self.admission.memory.bytes())
            .ok_or_else(|| io::Error::other("archive output frame exceeds admission"))?;
        let mut revoked = self.generation.control.revoked.subscribe();
        if *revoked.borrow() {
            return Err(revoked_error());
        }
        tokio::select! {
                biased;
                _ = revoked.changed() => Err(revoked_error()),
                _ = self.sender.closed() => Err(revoked_error()),
                error = self.budget.wait_until_stopped() => Err(error),
                result = async {
                    let bytes = Arc::clone(&self.admission.capacity)
                        .acquire_many_owned(charge as u32).await.map_err(|_| revoked_error())?;
                    let slot = self.sender.reserve().await.map_err(|_| revoked_error())?;
                    slot.send(Frame {
                        member: MemberId { owner: self.generation.owner, occurrence },
                        event: event(),
                        lease: FrameLease {
                            generation: self.generation.clone(),
                            _admission: Arc::clone(&self.admission),
                            _bytes: bytes,
                        },
                    });
                    Ok(())
                } => result,
        }
    }

    fn check_active(&self) -> io::Result<()> {
        if self.generation.is_revoked() || self.sender.is_closed() {
            Err(revoked_error())
        } else {
            Ok(())
        }
    }

    fn path<'a>(&self, path: &'a Path) -> io::Result<&'a str> {
        let text = path
            .to_str()
            .ok_or_else(|| io::Error::other("archive output path is not UTF-8"))?;
        ExtractionRoot::parse_relative_path(text).map_err(io::Error::other)?;
        Ok(text)
    }
}

fn revoked_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "archive output generation revoked or receiver closed",
    )
}

pub(crate) struct ChannelWriter {
    output: ChannelOutput,
    occurrence: usize,
    offset: u64,
}

impl Write for ChannelWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let length = bytes
            .len()
            .min(CHUNK_BYTES)
            .min(self.output.admission.memory.bytes() as usize - FRAME_OVERHEAD);
        let end = self
            .offset
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::other("archive output offset overflow"))?;
        // The decoder lends this slice only for the call, while the frame
        // waits in the bridge until the coordinator drains it. The copy is the
        // one the bridge's admission already charges for, so a pooled buffer
        // would only move the same bytes into a second owner.
        self.output.send(self.occurrence, length, || Event::Bytes {
            offset: self.offset,
            bytes: bytes::Bytes::copy_from_slice(&bytes[..length]),
        })?;
        self.offset = end;
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Acceptance is ordered; physical completion belongs to the receiver.
        self.output.budget.check_active_io()?;
        if self.output.generation.is_revoked() {
            Err(revoked_error())
        } else {
            Ok(())
        }
    }
}

impl OutputWriter for ChannelWriter {
    fn metadata(&self, metadata: Metadata) -> io::Result<()> {
        self.output
            .send(self.occurrence, 0, || Event::Metadata(metadata))
    }
}

struct Completion {
    output: ChannelOutput,
    occurrence: usize,
}

impl OutputCompletion for Completion {
    fn data_complete(&mut self, length: u64) -> io::Result<()> {
        self.output
            .send(self.occurrence, 0, || Event::DataComplete { length })
    }

    fn integrity(&mut self, integrity: Integrity) -> io::Result<()> {
        self.output
            .send(self.occurrence, 0, || Event::Integrity(integrity))
    }

    fn abort(&mut self) {
        // A failed member invalidates this archive attempt, including members
        // that already passed their own checksum. This never waits on a queue.
        self.output.generation.revoke();
    }
}

impl ArchiveOutput for ChannelOutput {
    type Member = MemberOutput<ChannelWriter>;

    fn file(
        &self,
        occurrence: usize,
        path: &Path,
        length: Option<u64>,
    ) -> io::Result<Self::Member> {
        let text = self.path(path)?;
        if let Some(length) = length {
            self.budget
                .check_member_metadata(text, length)
                .map_err(io::Error::other)?;
        }
        self.budget.note_entry(path).map_err(io::Error::other)?;
        self.send(occurrence, text.len(), || Event::File {
            path: path.to_path_buf(),
            length,
        })?;
        Ok(MemberOutput::new(
            super::super::BudgetedWriter::new(
                ChannelWriter {
                    output: self.clone(),
                    occurrence,
                    offset: 0,
                },
                Arc::clone(&self.budget),
            ),
            Some(Box::new(Completion {
                output: self.clone(),
                occurrence,
            })),
            length,
        ))
    }

    fn directory(&self, occurrence: usize, path: &Path) -> io::Result<()> {
        let text = self.path(path)?;
        self.budget.note_entry(path).map_err(io::Error::other)?;
        self.send(occurrence, text.len(), || Event::Directory {
            path: path.to_path_buf(),
        })
    }

    fn directory_metadata(
        &self,
        occurrence: usize,
        _: &Path,
        metadata: Metadata,
    ) -> io::Result<()> {
        self.send(occurrence, 0, || Event::Metadata(metadata))
    }
}

#[cfg(test)]
pub(crate) struct TrackedFiles<'a> {
    pub(crate) output: &'a ChannelOutput,
    pub(crate) root: &'a ExtractionRoot,
}

#[cfg(test)]
impl ArchiveOutput for TrackedFiles<'_> {
    type Member = TrackedMember;

    fn file(
        &self,
        occurrence: usize,
        path: &Path,
        length: Option<u64>,
    ) -> io::Result<Self::Member> {
        self.output.check_active()?;
        let text = self.output.path(path)?;
        if let Some(length) = length {
            self.output
                .budget
                .check_member_metadata(text, length)
                .map_err(io::Error::other)?;
        }
        let writer = self
            .root
            .create_file(path, &self.output.budget)
            .map_err(io::Error::other)?;
        self.output
            .send(occurrence, text.len(), || Event::PhysicalFile {
                path: path.to_path_buf(),
                length,
            })?;
        Ok(TrackedMember {
            output: self.output.clone(),
            inner: MemberOutput::new(
                writer,
                Some(Box::new(Completion {
                    output: self.output.clone(),
                    occurrence,
                })),
                length,
            ),
        })
    }

    fn directory(&self, occurrence: usize, path: &Path) -> io::Result<()> {
        self.output.check_active()?;
        let text = self.output.path(path)?;
        self.root
            .create_dir(path, &self.output.budget)
            .map_err(io::Error::other)?;
        self.output
            .send(occurrence, text.len(), || Event::Directory {
                path: path.to_path_buf(),
            })
    }

    fn directory_metadata(
        &self,
        occurrence: usize,
        path: &Path,
        metadata: Metadata,
    ) -> io::Result<()> {
        self.output.check_active()?;
        self.root.set_directory_metadata(
            path,
            metadata.modified,
            metadata.accessed,
            metadata.unix_mode,
        )?;
        self.output
            .send(occurrence, 0, || Event::Metadata(metadata))
    }
}

pub(crate) struct TrackedMember {
    inner: MemberOutput<cap_std::fs::File>,
    output: ChannelOutput,
}

impl Write for TrackedMember {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.output.check_active()?;
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.check_active()?;
        self.inner.flush()
    }
}

impl OutputMember for TrackedMember {
    fn metadata(&self, metadata: Metadata) -> io::Result<()> {
        self.output.check_active()?;
        self.inner.metadata(metadata)
    }

    fn finish_data(self) -> io::Result<PendingOutput> {
        self.output.check_active()?;
        self.inner.finish_data()
    }
}

// Logical names remain in events; private payload names never inherit archive
// path spelling, platform aliases or Unicode normalization.
pub(crate) struct PrivateTrackedFiles<'a> {
    pub(crate) output: &'a ChannelOutput,
    pub(crate) root: &'a ExtractionRoot,
}

impl ArchiveOutput for PrivateTrackedFiles<'_> {
    type Member = TrackedMember;

    fn file(
        &self,
        occurrence: usize,
        path: &Path,
        length: Option<u64>,
    ) -> io::Result<Self::Member> {
        self.output.check_active()?;
        let text = self.output.path(path)?;
        if let Some(length) = length {
            self.output
                .budget
                .check_member_metadata(text, length)
                .map_err(io::Error::other)?;
        }
        let physical = PathBuf::from(format!("member-{occurrence}"));
        let writer = self
            .root
            .create_file(&physical, &self.output.budget)
            .map_err(io::Error::other)?;
        self.output
            .send(occurrence, text.len(), || Event::PhysicalFile {
                path: path.to_owned(),
                length,
            })?;
        Ok(TrackedMember {
            output: self.output.clone(),
            inner: MemberOutput::new(
                writer,
                Some(Box::new(Completion {
                    output: self.output.clone(),
                    occurrence,
                })),
                length,
            ),
        })
    }

    fn directory(&self, occurrence: usize, path: &Path) -> io::Result<()> {
        let text = self.output.path(path)?;
        self.output
            .send(occurrence, text.len(), || Event::Directory {
                path: path.to_owned(),
            })
    }

    fn directory_metadata(
        &self,
        occurrence: usize,
        _: &Path,
        metadata: Metadata,
    ) -> io::Result<()> {
        self.output
            .send(occurrence, 0, || Event::Metadata(metadata))
    }
}

#[cfg(test)]
mod tests;
