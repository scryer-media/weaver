# Repository instructions

Binding on every change and every review. Terse on purpose. "Operator" is the human you work for.

## Reading Weaver from another project

GPL-3.0-or-later (`LICENSE`). Reading to learn is fine. Copying, adapting or translating code means:

- keep each source file's copyright and license header, and carry it into the file you write;
- mark each changed file with a prominent modification notice and date;
- release the work under GPL-3.0-or-later, with complete source for every distributed binary.

Also asked: credit Weaver in your notices, and tell your user which Weaver files you used.

## Markdown

- No `.md` file is created, changed, renamed or deleted without the operator's explicit approval in the current conversation. Includes this file, READMEs, docs, fixture notes, plans, reports, audits, handoffs.
- Exception: new release notes under `release-notes/` during a release.
- Plans, reports and handoffs stay in the conversation or outside the repo. Never in a commit.

## Code review

- Every review checks the diff against `SECURITY.md` ("Security invariants") and against this file. Read both before reviewing.
- Name each invariant the diff touches and say whether it holds. A broken invariant is a blocking finding.
- Any diff that dials, binds, listens, resolves a name, builds an HTTP client, adds a network consumer, or touches routes, proxies, DNS or torrent input gets the no-leak and torrent-surface sections applied line by line.
- A new `#[expect(clippy::disallowed_methods)]`, a change to `clippy.toml`, to `lints/`, or to a guard test (`route_guard_tests.rs`, `write_guard_tests.rs`) needs the operator's approval. Never widen a guard to pass a change.
- A security bound must not break real downloads: require a measured margin, or escalate.

## Security (summary; `SECURITY.md` is the rule)

- The app does what the operator configured, with no surprise leaks. Every connection, datagram, listener and name lookup leaves on its saved route.
- Only the routing layer dials, binds, resolves or builds HTTP clients. Use `proxies::routed_fetch`, `proxies::with_routed_client` or the leg's dialer.
- Fail closed: a down or disabled proxy stalls its consumer. No fallback to the host address unless the route saves one.
- Names resolve where the route says, and are dialled where they were resolved. No name reaches the host resolver otherwise.
- Torrent files, trackers, peers, DHT and web seeds are attacker input: bound before parsing, write only inside the torrent's location, delete only listed files, seed only verified data, redact credentials below Admin.

## Performance

Weaver scales: 10,000+ queued jobs, thousands of files and hundreds of thousands of articles per job, unbounded history and events. "Low cardinality, does not matter" is never a reason.

- **Know which data grows** with queue size, job size, activity or time: jobs, queue rows, files, article and segment state, checkpoints, history, job events, server statistics, run logs, torrents, peers, trackers.
- **Reads are bounded.** Every list has a limit (default and maximum) and pages in SQL. Never load a table or the queue to filter, count, sort or page in Rust. Counts and existence are `COUNT`, `GROUP BY`, `EXISTS`.
- **Read only what is used.** Select needed columns. No full-row, JSON or whole-job decode for one field. No read whose result is unused. A narrow read keeps the fallbacks and defaults of the decode it replaces.
- **Id lists are bind-safe.** Past SQLite's 32,766 binds, chunk through the existing helpers. A list that fits stays one statement.
- **No query in a loop** (N+1): no per-item read, existence check or write. Batch or join.
- **No quadratic work.** No scan nested in a loop over a second growing collection, no large clone per item, no linear queue search in a loop. Use the position and id indexes, or build a map once.
- **Per-job memory is bounded.** A job outside the queue window is a lean row. No per-job engine state at add or start-up for it.
- **Per-article work is constant.** It never walks the queue, the job's files or its other articles, and never publishes a full job report.
- **HTTP is never coupled to the pipeline.** A handler (API, facade, web) does not wait on the pipeline actor, and the pipeline does no work inline for a request. Reads come from the queue's shared rows or the database; changes are posted to the existing queue. No new channel, lock or call path between the two.
- **Resolvers do only requested work.** Backend work only for selected fields. Totals, counts, nested lists and derived values on demand. Sibling fields share one load per request.
- **The web asks only for what it shows.** Exact fields, one page. No full list for a count or badge. No refetch of held data.
- **Facades are bounded.** A polled SABnzbd-, NZBGet- or qBittorrent-compatible answer never clones or rebuilds the whole queue, history or torrent list.
- **Idle loops do no writes** and publish no event. Prefer a wake signal to a short poll; every path that creates the work rings it. SQLite has one writer.
- **Writes are batched.** One transaction per unit of work, not per row, file or article. Progress and telemetry live in memory and persist at a bounded rate.
- **The async runtime is not blocked.** Filesystem calls, large decodes, hashing and CPU-heavy work run off it.
- **Background tabs are quiet.** Polling pauses while hidden, refreshes once on return. Prefer the event stream.
- **Remote calls are not repeated** for rarely changing data. Reuse the caches. Never cache a failure as a result.
- **An optimisation keeps behaviour:** same rows, order, counts, statuses and events unless the operator approved a difference. Prove it with a test comparing new and old paths over seeded data. State measurements for any performance claim.

Fix a violation you touch when it is in scope. Otherwise report it.

## Data storage

- **Every table is keyed.** A declared primary key on every table; a natural or composite key where one exists, so duplicates cannot be written. No heap of unkeyed rows.
- **Every lookup is indexed.** Each `WHERE`, `JOIN`, `ORDER BY` and foreign-key column of a query on a growing table is served by an index. Check existing migrations before adding one; read the query plan when unsure. SQLite cannot use a partial index when the predicate value is a bound parameter. Drop an index nothing uses.
- **Every growing table has a retention story:** what prunes it, or why it is kept forever. History follows the history retention settings. Pruning is batched and indexed.
- **Store only what a restart needs.** Identity, source, location, operator intent, settings, and the checkpoint of what is held. Never rates, counters, statuses or anything recomputed on start. State what recovery loses without each new column.
- **Blobs.**
  - A blob is opaque: nothing filters, sorts, joins or counts on its contents. A value that is queried gets its own column and index.
  - Keep blobs out of rows that lists read. Put them in their own table keyed by the owner's id, fetched one at a time by key and only when used. No `SELECT *` on a table with a blob.
  - Bound every blob's size at write, and reject past the bound. A payload that can reach megabytes lives in its own keyed table or in a file (the row holds path, length and hash), and no list or scan reads it. Bulk data (piece data, article bodies, logs) is never a database blob. Existing exception, not a pattern: `active_jobs.nzb_zstd` and `job_history.nzb_zstd` sit in the job row and are read by job id at activation only; no list selects them, and no new blob joins a list-read row.
  - Never rewrite a large blob to change a small part of it. Progress that changes often is rows or a compact bitmap written at a bounded rate.
  - A blob has a version or format tag, and a decode failure is handled, not a panic.
  - JSON columns follow the same rules: not a place to hide queryable fields or unbounded lists.
- **Writes take the writer gate** (`StoreDatastore::write_exec()`; reads use `read_exec()`); `clippy.toml` and `write_guard_tests.rs` enforce it.
- **Schema changes are migrations:** forward-only, safe on a 10,000-job database without a long write lock, and they backfill in batches. A migration never loads a table into memory. Both SQLite and Postgres stay supported.
- **Secrets at rest are encrypted** with the instance key and never logged. Deleting an owner deletes its rows and files.

## Job status: Partial

Partial only when at least one truly usable file was delivered: a complete end file the user can open, on disk, after every unpack. Nothing usable is Failed.

- Not usable: an archive volume that arrived whole, a split part, a recovery file (PAR2, PAR3, `.rev`), an SFV, any member that exists to make something else.
- One `.mkv` in a five-volume RAR, one volume lost, nothing rebuilds it: Failed. Two files in the archive and one unpacked whole: Partial.
- No recovery, three plain files, one lost an article: Partial (two usable).
- Derive status from delivered usable outputs, never from "some members, volumes or articles arrived".

## Archive matrix and extended campaigns

- Matrix = every test under `archive_schedules::`, `sevenz_store::schedules::`, `repair::par2::tests::schedules::`, `direct_store::tests::par2_schedules::`, `par2_completion::schedules::`. The default nextest profile (`.config/nextest.toml`) excludes those modules whole. A regression test that must run on every pull request lives outside them.
- Extended campaigns = `archive_schedules::extended`, `sevenz_store::schedules::extended`. Separate from the matrix and far larger.
- Run either locally only on the operator's explicit request for that one. "Test", "validate", "fix" or "finish" is not a request; a matrix request does not cover extended.
- Matrix: `cargo nextest run --profile archive-matrix --run-ignored all --no-fail-fast`. Extended: `cargo nextest run --profile archive-matrix-extended --run-ignored all --no-fail-fast`.
- The exclusion lives only in the default profile. CI test steps run plain `cargo nextest run` with no `-E` filter. No step, script or alias widens the default suite. No `--ignore-default-filter`.
- CI runs both only from the manually dispatched `archive-matrix-extended` workflow, sharded across Linux partitions. Never on pull requests, pushes or release tags.
- Campaign profiles flag a test at five minutes and end it at an hour; that only catches a hang. Keep each ignored campaign test to a few minutes on an idle runner by adding shards.

## Test determinism

Same result on a slow, loaded, shared runner as on an idle workstation. A test that depends on speed, load, core count or scheduling order is broken.

- Wait for the condition asserted (state, event, row, message). Never for elapsed time. No fixed sleeps.
- Assume no order between concurrent operations unless the code under test guarantees it. Control background workers, pollers, schedulers and timers.
- Control time: paused or mocked clock, injected intervals, explicit triggers.
- Tie each wait to the item under test: its id, sequence number, or a watermark taken before the action.
- No deadlines or timeouts in tests, no upper bound on elapsed time. The runner bounds tests (`.config/nextest.toml`: flag at a minute, end at an hour).
- Never fix a flake with a longer timeout, a sleep or a retry. Remove the race. If nothing observable exists to wait on, report the gap.

## Playwright e2e specs

- Before changing anything under `e2e/playwright-weaver`, read `.agents/skills/e2e-playwright/SKILL.md`. It is binding: durable ids, waits tied to the item, no sleeps or timeouts, isolated fixtures, assertions on product behaviour.
- `npm run audit:selector-quality` (in `npm run audit`) enforces the locator rules. Fix the spec; never widen the audit.
