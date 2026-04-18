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
fn runtime_memory_caps_uncapped_hosts_but_respects_cgroup_limits() {
    let uncapped = BufferPoolConfig::for_runtime_memory(32 * 1024 * 1024 * 1024, None);
    assert_eq!(uncapped.small_count, 192);
    assert_eq!(uncapped.medium_count, 24);
    assert_eq!(uncapped.large_count, 6);

    let capped =
        BufferPoolConfig::for_runtime_memory(32 * 1024 * 1024 * 1024, Some(1536 * 1024 * 1024));
    assert_eq!(capped.small_count, 64);
    assert_eq!(capped.medium_count, 8);
    assert_eq!(capped.large_count, 2);
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
