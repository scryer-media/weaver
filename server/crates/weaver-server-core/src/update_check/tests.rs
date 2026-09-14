use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// Definitely newer than any version this project will ship from this branch.
const FUTURE_VERSION: &str = "99.0.0";
/// Definitely older than the running build.
const ANCIENT_VERSION: &str = "0.0.1";

/// Scripted [`ReleaseFetcher`]: hands back queued outcomes in order, repeating
/// the last one once the script is exhausted, and records what it was called
/// with. No socket is ever opened.
struct ScriptedFetcher {
    outcomes: Mutex<VecDeque<Result<FetchOutcome, String>>>,
    last: Mutex<Result<FetchOutcome, String>>,
    calls: AtomicUsize,
    observed_etags: Mutex<Vec<Option<String>>>,
}

impl ScriptedFetcher {
    fn new(outcomes: Vec<Result<FetchOutcome, String>>) -> Arc<Self> {
        let last = outcomes
            .last()
            .cloned()
            .unwrap_or(Ok(FetchOutcome::NotModified));
        Arc::new(Self {
            outcomes: Mutex::new(outcomes.into()),
            last: Mutex::new(last),
            calls: AtomicUsize::new(0),
            observed_etags: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn observed_etags(&self) -> Vec<Option<String>> {
        self.observed_etags.lock().unwrap().clone()
    }
}

impl ReleaseFetcher for ScriptedFetcher {
    fn fetch_latest<'a>(
        &'a self,
        etag: Option<&'a str>,
    ) -> BoxFuture<'a, Result<FetchOutcome, String>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observed_etags
            .lock()
            .unwrap()
            .push(etag.map(str::to_string));
        let next = self.outcomes.lock().unwrap().pop_front();
        let outcome = match next {
            Some(outcome) => {
                *self.last.lock().unwrap() = outcome.clone();
                outcome
            }
            None => self.last.lock().unwrap().clone(),
        };
        Box::pin(async move { outcome })
    }
}

fn fetched(version: &str) -> Result<FetchOutcome, String> {
    Ok(FetchOutcome::Fetched {
        release: Some(ReleaseInfo {
            version: version.to_string(),
            url: Some(format!(
                "https://github.com/scryer-media/weaver/releases/tag/weaver-v{version}"
            )),
            published_at_epoch_ms: Some(1_700_000_000_000),
        }),
        etag: Some(format!("\"etag-{version}\"")),
    })
}

fn service_with(
    outcomes: Vec<Result<FetchOutcome, String>>,
) -> (UpdateCheckService, Arc<ScriptedFetcher>) {
    let db = Database::open_in_memory().expect("in-memory database");
    let fetcher = ScriptedFetcher::new(outcomes);
    let service = UpdateCheckService::with_fetcher(db, fetcher.clone());
    (service, fetcher)
}

// --- version comparison -------------------------------------------------

#[test]
fn stable_tags_parse_with_and_without_release_prefix() {
    assert_eq!(
        parse_stable_version("weaver-v1.2.3"),
        Some(semver::Version::new(1, 2, 3))
    );
    assert_eq!(
        parse_stable_version("v1.2.3"),
        Some(semver::Version::new(1, 2, 3))
    );
    assert_eq!(
        parse_stable_version("1.2.3"),
        Some(semver::Version::new(1, 2, 3))
    );
    assert_eq!(parse_stable_version("not-a-version"), None);
}

#[test]
fn prerelease_tags_are_never_stable_versions() {
    assert_eq!(parse_stable_version("weaver-v1.2.3-rc.1"), None);
    assert_eq!(parse_stable_version("weaver-v1.2.3-beta"), None);
    // Build metadata is not a prerelease and stays eligible.
    assert!(parse_stable_version("weaver-v1.2.3+build.7").is_some());
}

#[test]
fn only_strictly_newer_stable_versions_count_as_updates() {
    assert!(is_newer_than_current(Some("weaver-v1.2.4"), "1.2.3"));
    assert!(is_newer_than_current(Some("weaver-v2.0.0"), "1.9.9"));
    assert!(!is_newer_than_current(Some("weaver-v1.2.3"), "1.2.3"));
    assert!(!is_newer_than_current(Some("weaver-v1.2.2"), "1.2.3"));
    // A newer prerelease must not trigger an update on a stable build.
    assert!(!is_newer_than_current(Some("weaver-v1.3.0-rc.1"), "1.2.3"));
    assert!(!is_newer_than_current(None, "1.2.3"));
    assert!(!is_newer_than_current(Some("garbage"), "1.2.3"));
}

// --- payload parsing ----------------------------------------------------

#[test]
fn release_payload_yields_version_url_and_publish_time() {
    let body = br#"{
        "tag_name": "weaver-v1.4.0",
        "html_url": "https://github.com/scryer-media/weaver/releases/tag/weaver-v1.4.0",
        "published_at": "2026-01-02T03:04:05Z",
        "draft": false,
        "prerelease": false
    }"#;
    let release = parse_release_payload(body)
        .expect("parses")
        .expect("stable");
    assert_eq!(release.version, "1.4.0");
    assert_eq!(
        release.url.as_deref(),
        Some("https://github.com/scryer-media/weaver/releases/tag/weaver-v1.4.0")
    );
    assert_eq!(release.published_at_epoch_ms, Some(1_767_323_045_000));
}

#[test]
fn draft_and_prerelease_payloads_are_ignored() {
    let draft = br#"{"tag_name": "weaver-v1.4.0", "draft": true, "prerelease": false}"#;
    assert_eq!(parse_release_payload(draft).expect("parses"), None);

    let prerelease = br#"{"tag_name": "weaver-v1.4.0", "draft": false, "prerelease": true}"#;
    assert_eq!(parse_release_payload(prerelease).expect("parses"), None);

    let pre_tag = br#"{"tag_name": "weaver-v1.4.0-rc.1", "draft": false, "prerelease": false}"#;
    assert_eq!(parse_release_payload(pre_tag).expect("parses"), None);

    assert!(parse_release_payload(b"not json").is_err());
}

// --- retry-after --------------------------------------------------------

#[test]
fn retry_after_accepts_delta_seconds_and_http_dates() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::RETRY_AFTER, "120".parse().unwrap());
    assert_eq!(
        retry_after_from_headers(&headers),
        Some(Duration::from_secs(120))
    );

    // HTTP-date form: httpdate parses it and we take the remaining delta.
    let when = SystemTime::now() + Duration::from_secs(600);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::RETRY_AFTER,
        httpdate::fmt_http_date(when).parse().unwrap(),
    );
    let parsed = retry_after_from_headers(&headers).expect("http-date retry-after");
    assert!(
        parsed > Duration::from_secs(540) && parsed <= Duration::from_secs(600),
        "unexpected retry-after {parsed:?}"
    );

    // A past date clamps to zero rather than underflowing.
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::RETRY_AFTER,
        httpdate::fmt_http_date(SystemTime::now() - Duration::from_secs(600))
            .parse()
            .unwrap(),
    );
    assert_eq!(retry_after_from_headers(&headers), Some(Duration::ZERO));

    // Falls back to GitHub's reset epoch when Retry-After is absent.
    let reset = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-ratelimit-reset", reset.to_string().parse().unwrap());
    assert!(
        retry_after_from_headers(&headers).expect("reset fallback") <= Duration::from_secs(300)
    );

    assert_eq!(
        retry_after_from_headers(&reqwest::header::HeaderMap::new()),
        None
    );
}

// --- watch-channel semantics --------------------------------------------

#[tokio::test(start_paused = true)]
async fn subscriber_sees_initial_snapshot_before_any_change() {
    let (service, _fetcher) = service_with(vec![fetched(FUTURE_VERSION)]);

    let receiver = service.subscribe();
    let initial = receiver.borrow().clone();
    assert_eq!(initial.current_version, env!("CARGO_PKG_VERSION"));
    assert!(!initial.update_available);
    assert_eq!(initial.latest_version, None);
    assert!(!initial.checking);
    assert_eq!(initial, service.status());
}

#[tokio::test(start_paused = true)]
async fn every_transition_is_published_to_subscribers() {
    let (service, _fetcher) = service_with(vec![fetched(FUTURE_VERSION)]);
    let mut receiver = service.subscribe();

    service.run_check().await.expect("check succeeds");

    assert!(receiver.changed().await.is_ok());
    let status = receiver.borrow_and_update().clone();
    assert!(status.update_available);
    assert_eq!(status.latest_version.as_deref(), Some(FUTURE_VERSION));
    assert!(!status.checking, "checking must be cleared once settled");
}

// --- check outcomes -----------------------------------------------------

#[tokio::test(start_paused = true)]
async fn newer_stable_release_sets_update_available() {
    let (service, _fetcher) = service_with(vec![fetched(FUTURE_VERSION)]);

    service.run_check().await.expect("check succeeds");

    let status = service.status();
    assert!(status.update_available);
    assert_eq!(status.latest_version.as_deref(), Some(FUTURE_VERSION));
    assert!(status.release_url.is_some());
    assert_eq!(status.published_at_epoch_ms, Some(1_700_000_000_000));
    assert!(status.last_error.is_none());
    assert!(status.last_successful_check_at_epoch_ms.is_some());
}

#[tokio::test(start_paused = true)]
async fn older_release_does_not_set_update_available() {
    let (service, _fetcher) = service_with(vec![fetched(ANCIENT_VERSION)]);

    service.run_check().await.expect("check succeeds");

    let status = service.status();
    assert!(!status.update_available);
    assert_eq!(status.latest_version.as_deref(), Some(ANCIENT_VERSION));
}

#[tokio::test(start_paused = true)]
async fn fetch_error_preserves_the_last_good_result() {
    let (service, _fetcher) = service_with(vec![
        fetched(FUTURE_VERSION),
        Err("connection reset".to_string()),
    ]);

    service.run_check().await.expect("first check succeeds");
    let good = service.status();
    let first_success_at = good.last_successful_check_at_epoch_ms;

    service.run_check().await.expect_err("second check fails");

    let after_failure = service.status();
    assert_eq!(
        after_failure.last_error.as_deref(),
        Some("connection reset")
    );
    // Everything discovered by the good check is untouched.
    assert_eq!(after_failure.latest_version, good.latest_version);
    assert_eq!(after_failure.update_available, good.update_available);
    assert_eq!(after_failure.release_url, good.release_url);
    assert_eq!(
        after_failure.published_at_epoch_ms,
        good.published_at_epoch_ms
    );
    assert_eq!(
        after_failure.last_successful_check_at_epoch_ms, first_success_at,
        "a failure must not advance the last-successful marker"
    );
    assert!(!after_failure.checking);
}

#[tokio::test(start_paused = true)]
async fn rate_limit_backs_off_without_discarding_state() {
    let (service, _fetcher) = service_with(vec![
        fetched(FUTURE_VERSION),
        Ok(FetchOutcome::RateLimited {
            retry_after: Some(Duration::from_secs(1800)),
        }),
    ]);

    service.run_check().await.expect("first check succeeds");
    let retry_after = service
        .run_check()
        .await
        .expect("rate limit is not an error");

    assert_eq!(retry_after, Some(Duration::from_secs(1800)));
    let status = service.status();
    assert_eq!(status.latest_version.as_deref(), Some(FUTURE_VERSION));
    assert!(status.update_available);
    assert!(status.last_error.is_some());
}

#[tokio::test(start_paused = true)]
async fn not_modified_refreshes_timestamps_and_reuses_the_etag() {
    let (service, fetcher) =
        service_with(vec![fetched(FUTURE_VERSION), Ok(FetchOutcome::NotModified)]);

    service.run_check().await.expect("first check succeeds");
    service.run_check().await.expect("revalidation succeeds");

    let observed = fetcher.observed_etags();
    assert_eq!(observed[0], None, "first check has no validator yet");
    assert_eq!(
        observed[1].as_deref(),
        Some(format!("\"etag-{FUTURE_VERSION}\"").as_str()),
        "the stored validator must be replayed as If-None-Match"
    );

    let status = service.status();
    assert_eq!(status.latest_version.as_deref(), Some(FUTURE_VERSION));
    assert!(status.last_error.is_none());
    assert!(status.last_successful_check_at_epoch_ms.is_some());
}

// --- persistence --------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn last_good_result_survives_a_restart() {
    let db = Database::open_in_memory().expect("in-memory database");
    let service = UpdateCheckService::with_fetcher(
        db.clone(),
        ScriptedFetcher::new(vec![fetched(FUTURE_VERSION)]),
    );
    service.run_check().await.expect("check succeeds");

    // A fresh service over the same database is the restart.
    let restarted = UpdateCheckService::with_fetcher(
        db.clone(),
        ScriptedFetcher::new(vec![Ok(FetchOutcome::NotModified)]),
    );
    let status = restarted.status();
    assert_eq!(status.latest_version.as_deref(), Some(FUTURE_VERSION));
    assert!(status.update_available);
    assert!(status.release_url.is_some());
    assert!(status.last_successful_check_at_epoch_ms.is_some());
    assert!(
        status.last_error.is_none() && !status.checking,
        "transient fields must not be restored"
    );

    // The persisted validator is replayed, so the first post-restart request is
    // conditional.
    restarted.run_check().await.expect("revalidation succeeds");
}

#[tokio::test(start_paused = true)]
async fn a_release_no_longer_newer_than_the_build_clears_update_available() {
    let db = Database::open_in_memory().expect("in-memory database");
    // Persist a state whose recorded release is older than the running build,
    // as it would be after the user upgrades into that release.
    let stale = serde_json::json!({
        "latest_version": ANCIENT_VERSION,
        "release_url": "https://github.com/scryer-media/weaver/releases/tag/weaver-v0.0.1",
        "published_at_epoch_ms": 1_700_000_000_000i64,
        "last_successful_check_at_epoch_ms": 1_700_000_000_000i64,
        "etag": "\"stale\""
    });
    db.set_setting(UPDATE_CHECK_SETTING_KEY, &stale.to_string())
        .expect("seed persisted state");

    let service = UpdateCheckService::with_fetcher(
        db,
        ScriptedFetcher::new(vec![Ok(FetchOutcome::NotModified)]),
    );

    let status = service.status();
    assert_eq!(status.latest_version.as_deref(), Some(ANCIENT_VERSION));
    assert!(
        !status.update_available,
        "update_available is recomputed against the running binary on load"
    );
}

// --- loop scheduling ----------------------------------------------------
//
// The startup delay carries jitter, so every check has an *interval* of valid
// firing times rather than a single instant. These tests assert against the
// bounds of those intervals — anchored to absolute deadlines from a fixed
// origin, never to cumulative relative sleeps — so a wide or narrow jitter draw
// cannot flip the result.

/// Margin used to step just inside or just past a bound.
const MARGIN: Duration = Duration::from_secs(2);

/// Earliest and latest instant at which check `n` (1-based) can fire, given a
/// steady-state gap of `gap` between checks.
fn check_window(n: u32, gap: Duration) -> (Duration, Duration) {
    let earliest = STARTUP_DELAY + gap * (n - 1);
    let latest = STARTUP_DELAY + STARTUP_JITTER + gap * (n - 1);
    (earliest, latest)
}

#[tokio::test(start_paused = true)]
async fn background_loop_waits_out_the_startup_delay_then_repeats_on_the_interval() {
    let (service, fetcher) = service_with(vec![Ok(FetchOutcome::NotModified)]);

    let origin = tokio::time::Instant::now();
    let task = service.start_background_loop();

    // Nothing fires before the startup delay elapses.
    let (first_earliest, first_latest) = check_window(1, CHECK_INTERVAL);
    tokio::time::sleep_until(origin + first_earliest - MARGIN).await;
    assert_eq!(
        fetcher.calls(),
        0,
        "startup delay must gate the first check"
    );

    // Past the delay plus the maximum jitter, exactly one check has run.
    tokio::time::sleep_until(origin + first_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 1);

    // The steady-state cadence is CHECK_INTERVAL, not a tight loop: nothing
    // extra fires before the second window opens.
    let (second_earliest, second_latest) = check_window(2, CHECK_INTERVAL);
    tokio::time::sleep_until(origin + second_earliest - MARGIN).await;
    assert_eq!(fetcher.calls(), 1, "loop must not re-check early");

    tokio::time::sleep_until(origin + second_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 2);

    let (third_earliest, third_latest) = check_window(3, CHECK_INTERVAL);
    tokio::time::sleep_until(origin + third_earliest - MARGIN).await;
    assert_eq!(fetcher.calls(), 2, "cadence must hold across iterations");

    tokio::time::sleep_until(origin + third_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 3);

    task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_failing_check_still_waits_a_full_interval() {
    let (service, fetcher) = service_with(vec![Err("dns failure".to_string())]);

    let origin = tokio::time::Instant::now();
    let task = service.start_background_loop();

    let (_, first_latest) = check_window(1, CHECK_INTERVAL);
    tokio::time::sleep_until(origin + first_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 1);

    // A failure must never degrade into a retry storm.
    let (second_earliest, second_latest) = check_window(2, CHECK_INTERVAL);
    tokio::time::sleep_until(origin + second_earliest - MARGIN).await;
    assert_eq!(fetcher.calls(), 1, "failures must not tighten the loop");

    tokio::time::sleep_until(origin + second_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 2);

    task.abort();
}

#[tokio::test(start_paused = true)]
async fn a_short_retry_after_is_floored_to_the_minimum_interval() {
    let (service, fetcher) = service_with(vec![Ok(FetchOutcome::RateLimited {
        retry_after: Some(Duration::from_secs(1)),
    })]);

    let origin = tokio::time::Instant::now();
    let task = service.start_background_loop();

    let (_, first_latest) = check_window(1, MIN_CHECK_INTERVAL);
    tokio::time::sleep_until(origin + first_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 1);

    // A one-second Retry-After must not be honoured literally: the next check
    // is held back to the floor, not the value the server asked for.
    let (second_earliest, second_latest) = check_window(2, MIN_CHECK_INTERVAL);
    tokio::time::sleep_until(origin + second_earliest - MARGIN).await;
    assert_eq!(fetcher.calls(), 1, "retry-after must be floored");

    tokio::time::sleep_until(origin + second_latest + MARGIN).await;
    assert_eq!(fetcher.calls(), 2);

    task.abort();
}

#[test]
fn startup_delay_always_falls_inside_the_jitter_window() {
    for _ in 0..64 {
        let delay = startup_delay();
        assert!(delay >= STARTUP_DELAY);
        assert!(delay < STARTUP_DELAY + STARTUP_JITTER);
    }
}
