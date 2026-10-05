# Repository instructions

## Markdown files

- Do not create, modify, rename, or delete any Markdown file without the operator's explicit approval in the current conversation. This covers every `.md` file in the repository, including this one, READMEs, docs, fixture notes, and any plan, report, proposal, audit, or handoff file.
- The one exception is creating release notes under `release-notes/` as part of the release process.
- Keep plans, reports, and handoffs in the conversation or outside the repository. They never belong in a commit.

## Local archive matrix execution

- Do not run the large archive schedule matrix during normal local development, routine validation, hygiene passes, or pre-commit checks. Run it locally only when an operator explicitly requests the matrix; a general request to test, validate, fix, or finish work is not authorization.
- Keep ordinary smoke and regression tests in the default suite. Do not bypass the matrix exclusions with `--ignore-default-filter` or enable its ignored tests without that explicit request.
- The opt-in local command is `cargo nextest run --profile archive-matrix --run-ignored all --no-fail-fast`. This covers the combined direct-store, chase, and conventional extraction campaigns for RAR and 7z.
- In CI the matrix runs only from the manually dispatched `archive-matrix-extended` workflow, which spreads it and the extended campaigns together over 128 Linux partitions. Pull requests, pushes, and release tags run the ordinary test suite and must not run the matrix.

## Extended archive campaigns

- The extended campaigns live under `archive_schedules::extended` and `sevenz_store::schedules::extended`. They widen the matrix with further formats, PAR3 recovery, every forceable demotion reason, and deeper schedules over the core direct-store RAR layouts.
- They are separate from the archive matrix and far larger. Never run them locally unless an operator explicitly requests the extended campaigns; a request to run the archive matrix does not cover them.
- The opt-in local command is `cargo nextest run --profile archive-matrix-extended --run-ignored all --no-fail-fast`.
- In CI they run only from the manually dispatched `archive-matrix-extended` workflow, sharing its 128 Linux partitions with the archive matrix. They must never run on pull requests, pushes, or release tags, and nothing added to them may match the archive matrix filter.
- Their smoke tests are not ignored and stay in the default suite. The campaign profiles flag a test every five minutes and end it after an hour. That limit only catches a hang; it is set far above any real run so a loaded runner cannot fail a test. Keep every ignored campaign test to a few minutes on an idle runner by adding shards.

## Test determinism

A test must give the same result on a slow, loaded, or shared CI runner as on a fast idle workstation. Passing locally proves nothing if the result depends on machine speed, load, core count, or scheduling order. A test that can fail that way is broken and must be fixed before it merges.

- Wait for the condition the assertion depends on, such as a state change, an event, a row, or a message. Never wait for elapsed time. Fixed sleeps are forbidden.
- Do not assume one concurrent operation finishes before another unless the code under test guarantees that order. Background workers, pollers, schedulers, and timers race the test unless the test controls them.
- Control time and scheduling explicitly. Use the paused or mocked clock, injected intervals, or explicit triggers instead of racing real timers.
- Tie every wait to the specific item under test, using its ID, sequence number, or a watermark taken before the action. Earlier or unrelated activity must not be able to satisfy the wait.
- Do not put deadlines or timeouts in tests, and never assert an upper bound on elapsed time. The test runner bounds every test instead: `.config/nextest.toml` flags a test after a minute and ends it after an hour, so a missed condition fails the run instead of hanging it. That limit is set far above any real run so a loaded runner cannot fail a test; it only catches a hang.
- Never fix a flaky test by raising a timeout, adding a sleep, or adding a retry. Find and remove the race. If the product exposes nothing observable to wait on, report that gap instead of working around it.
