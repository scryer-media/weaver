# Repository instructions

## Markdown files

- Do not create, modify, rename, or delete any Markdown file without the operator's explicit approval in the current conversation. This covers every `.md` file in the repository, including this one, READMEs, docs, fixture notes, and any plan, report, proposal, audit, or handoff file.
- The one exception is creating release notes under `release-notes/` as part of the release process.
- Keep plans, reports, and handoffs in the conversation or outside the repository. They never belong in a commit.

## Test determinism

A test must give the same result on a slow, loaded, or shared CI runner as on a fast idle workstation. Passing locally proves nothing if the result depends on machine speed, load, core count, or scheduling order. A test that can fail that way is broken and must be fixed before it merges.

- Wait for the condition the assertion depends on, such as a state change, an event, a row, or a message. Never wait for elapsed time. Fixed sleeps are forbidden.
- Do not assume one concurrent operation finishes before another unless the code under test guarantees that order. Background workers, pollers, schedulers, and timers race the test unless the test controls them.
- Control time and scheduling explicitly. Use the paused or mocked clock, injected intervals, or explicit triggers instead of racing real timers.
- Tie every wait to the specific item under test, using its ID, sequence number, or a watermark taken before the action. Earlier or unrelated activity must not be able to satisfy the wait.
- Do not put deadlines or timeouts in tests, and never assert an upper bound on elapsed time. The test runner bounds every test instead: `.config/nextest.toml` flags a test after a minute and ends it after ten, so a missed condition fails the run instead of hanging it.
- Never fix a flaky test by raising a timeout, adding a sleep, or adding a retry. Find and remove the race. If the product exposes nothing observable to wait on, report that gap instead of working around it.
