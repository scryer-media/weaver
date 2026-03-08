use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use tokio::sync::Semaphore;

/// Size tiers for buffer allocation.
///
/// Usenet articles are almost always under 1MB. Typical decoded sizes are
/// ~380KB (750KB yEnc segments) or ~760KB (~1.4MB segments).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferTier {
    /// 512 KB — most segments.
    Small,
    /// 1 MB — large segments.
    Medium,
    /// 4 MB — PAR2 recovery blocks, unusual articles.
    Large,
}

impl BufferTier {
    pub fn size_bytes(self) -> usize {
        match self {
            BufferTier::Small => 512 * 1024,
            BufferTier::Medium => 1024 * 1024,
            BufferTier::Large => 4 * 1024 * 1024,
        }
    }

    /// Pick the smallest tier that fits `needed` bytes.
    pub fn for_size(needed: usize) -> Self {
        if needed <= Self::Small.size_bytes() {
            Self::Small
        } else if needed <= Self::Medium.size_bytes() {
            Self::Medium
        } else {
            Self::Large
        }
    }
}

/// Configuration for a [`BufferPool`].
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    pub small_count: usize,
    pub medium_count: usize,
    pub large_count: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            small_count: 256,
            medium_count: 32,
            large_count: 8,
        }
    }
}

impl BufferPoolConfig {
    /// Total memory this pool will allocate.
    pub fn total_bytes(&self) -> usize {
        self.small_count * BufferTier::Small.size_bytes()
            + self.medium_count * BufferTier::Medium.size_bytes()
            + self.large_count * BufferTier::Large.size_bytes()
    }

    /// Scale buffer pool configuration based on available system memory.
    ///
    /// Uses the effective available memory (respecting cgroup limits) to pick
    /// appropriate buffer counts. The pool targets roughly 5% of available RAM,
    /// clamped between a minimum floor (for 512MB systems) and the default
    /// ceiling (for 8GB+ systems).
    pub fn for_available_memory(available_bytes: u64) -> Self {
        let available_mb = (available_bytes / (1024 * 1024)) as usize;

        let (small, medium, large) = if available_mb < 1024 {
            // < 1 GB: minimal (~24 MB)
            (32, 4, 1)
        } else if available_mb < 2048 {
            // 1–2 GB: small (~48 MB)
            (64, 8, 2)
        } else if available_mb < 4096 {
            // 2–4 GB: moderate (~96 MB)
            (128, 16, 4)
        } else if available_mb < 8192 {
            // 4–8 GB: standard (~144 MB)
            (192, 24, 6)
        } else {
            // 8 GB+: full default (~192 MB)
            (256, 32, 8)
        };

        Self {
            small_count: small,
            medium_count: medium,
            large_count: large,
        }
    }

    /// Recommended write buffer max_pending for this memory tier.
    /// Fewer pending segments per file on constrained systems.
    pub fn write_buffer_max_pending(&self) -> usize {
        if self.small_count <= 64 {
            4
        } else if self.small_count <= 128 {
            8
        } else {
            16
        }
    }
}

/// Metrics for monitoring buffer pool utilization.
#[derive(Debug)]
pub struct BufferPoolMetrics {
    pub small_in_use: usize,
    pub small_total: usize,
    pub medium_in_use: usize,
    pub medium_total: usize,
    pub large_in_use: usize,
    pub large_total: usize,
    pub wait_count: usize,
}

/// A tiered, slab-backed buffer pool with backpressure.
///
/// Buffers are pre-allocated at startup and reused via lock-free queues.
/// When all buffers of a tier are in use, [`acquire`](BufferPool::acquire)
/// awaits until one is returned — this is the primary backpressure mechanism
/// in the pipeline.
pub struct BufferPool {
    small: TierPool,
    medium: TierPool,
    large: TierPool,
    wait_count: AtomicUsize,
}

struct TierPool {
    slots: ArrayQueue<BufferSlot>,
    semaphore: Arc<Semaphore>,
    tier: BufferTier,
    total: usize,
    in_use: AtomicUsize,
}

#[derive(Debug)]
struct BufferSlot {
    data: Vec<u8>,
}

impl BufferPool {
    /// Create a new buffer pool with the given configuration.
    ///
    /// All buffers are allocated upfront.
    pub fn new(config: BufferPoolConfig) -> Arc<Self> {
        Arc::new(Self {
            small: TierPool::new(BufferTier::Small, config.small_count),
            medium: TierPool::new(BufferTier::Medium, config.medium_count),
            large: TierPool::new(BufferTier::Large, config.large_count),
            wait_count: AtomicUsize::new(0),
        })
    }

    /// Acquire a buffer from the specified tier.
    ///
    /// If no buffers are available, this will wait until one is returned.
    /// This is the backpressure mechanism: downstream stages must release
    /// buffers before upstream stages can proceed.
    pub async fn acquire(self: &Arc<Self>, tier: BufferTier) -> BufferHandle {
        let tier_pool = self.tier_pool(tier);

        // Try to acquire without waiting first.
        if let Ok(permit) = tier_pool.semaphore.clone().try_acquire_owned() {
            let slot = tier_pool.slots.pop().expect("semaphore/queue mismatch");
            tier_pool.in_use.fetch_add(1, Ordering::Relaxed);
            return BufferHandle {
                inner: Arc::new(BufferInner {
                    slot,
                    len: AtomicUsize::new(0),
                    tier,
                    pool: Arc::clone(self),
                    _permit: permit,
                }),
            };
        }

        // Must wait — record the wait for metrics.
        self.wait_count.fetch_add(1, Ordering::Relaxed);
        let permit = tier_pool
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let slot = tier_pool.slots.pop().expect("semaphore/queue mismatch");
        tier_pool.in_use.fetch_add(1, Ordering::Relaxed);

        BufferHandle {
            inner: Arc::new(BufferInner {
                slot,
                len: AtomicUsize::new(0),
                tier,
                pool: Arc::clone(self),
                _permit: permit,
            }),
        }
    }

    /// Try to acquire a buffer without waiting. Returns `None` if all
    /// buffers of the requested tier are in use.
    pub fn try_acquire(self: &Arc<Self>, tier: BufferTier) -> Option<BufferHandle> {
        let tier_pool = self.tier_pool(tier);
        let permit = tier_pool.semaphore.clone().try_acquire_owned().ok()?;
        let slot = tier_pool.slots.pop().expect("semaphore/queue mismatch");
        tier_pool.in_use.fetch_add(1, Ordering::Relaxed);

        Some(BufferHandle {
            inner: Arc::new(BufferInner {
                slot,
                len: AtomicUsize::new(0),
                tier,
                pool: Arc::clone(self),
                _permit: permit,
            }),
        })
    }

    /// Current pool metrics.
    pub fn metrics(&self) -> BufferPoolMetrics {
        BufferPoolMetrics {
            small_in_use: self.small.in_use.load(Ordering::Relaxed),
            small_total: self.small.total,
            medium_in_use: self.medium.in_use.load(Ordering::Relaxed),
            medium_total: self.medium.total,
            large_in_use: self.large.in_use.load(Ordering::Relaxed),
            large_total: self.large.total,
            wait_count: self.wait_count.load(Ordering::Relaxed),
        }
    }

    /// Number of available (not in use) buffers for a tier.
    pub fn available(&self, tier: BufferTier) -> usize {
        let tp = self.tier_pool(tier);
        tp.total - tp.in_use.load(Ordering::Relaxed)
    }

    fn tier_pool(&self, tier: BufferTier) -> &TierPool {
        match tier {
            BufferTier::Small => &self.small,
            BufferTier::Medium => &self.medium,
            BufferTier::Large => &self.large,
        }
    }

    fn return_buffer(&self, tier: BufferTier, slot: BufferSlot) {
        let tier_pool = self.tier_pool(tier);
        tier_pool.in_use.fetch_sub(1, Ordering::Relaxed);
        tier_pool
            .slots
            .push(slot)
            .expect("returned buffer to full queue");
        // Semaphore permit is dropped by the BufferInner's _permit field,
        // which automatically wakes any waiting acquire().
    }
}

impl TierPool {
    fn new(tier: BufferTier, count: usize) -> Self {
        let slots = ArrayQueue::new(count.max(1));
        for _ in 0..count {
            slots
                .push(BufferSlot {
                    data: vec![0u8; tier.size_bytes()],
                })
                .expect("queue overflow during init");
        }
        Self {
            slots,
            semaphore: Arc::new(Semaphore::new(count)),
            tier,
            total: count,
            in_use: AtomicUsize::new(0),
        }
    }
}

/// Handle to a pooled buffer. Clone is cheap (Arc).
///
/// When the last handle is dropped, the buffer is returned to the pool.
#[derive(Clone)]
pub struct BufferHandle {
    inner: Arc<BufferInner>,
}

struct BufferInner {
    slot: BufferSlot,
    len: AtomicUsize,
    tier: BufferTier,
    pool: Arc<BufferPool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl BufferHandle {
    /// The capacity of this buffer (determined by its tier).
    pub fn capacity(&self) -> usize {
        self.inner.slot.data.len()
    }

    /// The amount of valid data written to this buffer.
    pub fn len(&self) -> usize {
        self.inner.len.load(Ordering::Acquire)
    }

    /// Whether the buffer contains no valid data.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set the valid data length after writing.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the buffer capacity.
    pub fn set_len(&self, len: usize) {
        assert!(len <= self.capacity(), "len exceeds buffer capacity");
        self.inner.len.store(len, Ordering::Release);
    }

    /// Immutable view of the valid data.
    pub fn as_slice(&self) -> &[u8] {
        &self.inner.slot.data[..self.len()]
    }

    /// Mutable view of the entire buffer capacity (for writing decoded data).
    ///
    /// Returns `None` if there are multiple handles to this buffer (i.e.,
    /// it's been cloned). Only the sole owner can write.
    pub fn as_mut_slice(&mut self) -> Option<&mut [u8]> {
        let inner = Arc::get_mut(&mut self.inner)?;
        Some(&mut inner.slot.data)
    }

    /// The tier this buffer belongs to.
    pub fn tier(&self) -> BufferTier {
        self.inner.tier
    }
}

impl Drop for BufferInner {
    fn drop(&mut self) {
        // Take the slot out and return it to the pool.
        // We need to move the slot data, so we swap with an empty vec.
        let slot = BufferSlot {
            data: std::mem::take(&mut self.slot.data),
        };
        self.pool.return_buffer(self.tier, slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> Arc<BufferPool> {
        BufferPool::new(BufferPoolConfig {
            small_count: 4,
            medium_count: 2,
            large_count: 1,
        })
    }

    #[tokio::test]
    async fn acquire_and_release() {
        let pool = test_pool();
        assert_eq!(pool.available(BufferTier::Small), 4);

        let buf = pool.acquire(BufferTier::Small).await;
        assert_eq!(pool.available(BufferTier::Small), 3);
        assert_eq!(buf.capacity(), 512 * 1024);
        assert_eq!(buf.len(), 0);

        drop(buf);
        assert_eq!(pool.available(BufferTier::Small), 4);
    }

    #[tokio::test]
    async fn write_and_read() {
        let pool = test_pool();
        let mut buf = pool.acquire(BufferTier::Small).await;

        let data = b"hello, world!";
        let slice = buf.as_mut_slice().unwrap();
        slice[..data.len()].copy_from_slice(data);
        buf.set_len(data.len());

        assert_eq!(buf.as_slice(), data);
        assert_eq!(buf.len(), data.len());
    }

    #[tokio::test]
    async fn clone_prevents_mut_access() {
        let pool = test_pool();
        let mut buf = pool.acquire(BufferTier::Small).await;
        buf.set_len(5);

        let _clone = buf.clone();
        assert!(buf.as_mut_slice().is_none());

        // But read access works on both.
        assert_eq!(buf.len(), 5);
        assert_eq!(_clone.len(), 5);
    }

    #[tokio::test]
    async fn try_acquire_exhaustion() {
        let pool = BufferPool::new(BufferPoolConfig {
            small_count: 2,
            medium_count: 0,
            large_count: 0,
        });

        let _a = pool.try_acquire(BufferTier::Small).unwrap();
        let _b = pool.try_acquire(BufferTier::Small).unwrap();
        assert!(pool.try_acquire(BufferTier::Small).is_none());

        drop(_a);
        assert!(pool.try_acquire(BufferTier::Small).is_some());
    }

    #[tokio::test]
    async fn backpressure_blocks_then_unblocks() {
        let pool = BufferPool::new(BufferPoolConfig {
            small_count: 1,
            medium_count: 0,
            large_count: 0,
        });

        let buf = pool.acquire(BufferTier::Small).await;
        assert_eq!(pool.available(BufferTier::Small), 0);

        // Spawn a task that will acquire after a delay.
        let pool2 = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let _buf = pool2.acquire(BufferTier::Small).await;
            assert_eq!(_buf.capacity(), 512 * 1024);
        });

        // Give the spawned task time to start waiting.
        tokio::task::yield_now().await;
        assert_eq!(pool.metrics().wait_count, 1);

        // Release our buffer — the waiting task should unblock.
        drop(buf);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn tier_for_size() {
        assert_eq!(BufferTier::for_size(100), BufferTier::Small);
        assert_eq!(BufferTier::for_size(512 * 1024), BufferTier::Small);
        assert_eq!(BufferTier::for_size(512 * 1024 + 1), BufferTier::Medium);
        assert_eq!(BufferTier::for_size(1024 * 1024), BufferTier::Medium);
        assert_eq!(BufferTier::for_size(1024 * 1024 + 1), BufferTier::Large);
    }

    #[tokio::test]
    async fn metrics() {
        let pool = test_pool();
        let metrics = pool.metrics();
        assert_eq!(metrics.small_total, 4);
        assert_eq!(metrics.small_in_use, 0);
        assert_eq!(metrics.medium_total, 2);
        assert_eq!(metrics.large_total, 1);

        let _a = pool.acquire(BufferTier::Small).await;
        let _b = pool.acquire(BufferTier::Medium).await;
        let metrics = pool.metrics();
        assert_eq!(metrics.small_in_use, 1);
        assert_eq!(metrics.medium_in_use, 1);
    }

    #[test]
    fn config_total_bytes() {
        let config = BufferPoolConfig::default();
        let expected = 256 * 512 * 1024 + 32 * 1024 * 1024 + 8 * 4 * 1024 * 1024;
        assert_eq!(config.total_bytes(), expected);
    }

    #[test]
    #[should_panic(expected = "len exceeds buffer capacity")]
    fn set_len_panics_on_overflow() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = test_pool();
            let buf = pool.acquire(BufferTier::Small).await;
            buf.set_len(buf.capacity() + 1);
        });
    }
}
