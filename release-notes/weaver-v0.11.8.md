# Weaver 0.11.8 release notes

## Highlights

Weaver 0.11.8 fixes a set of stalls in which a job could stop making progress
without failing: downloads waiting on retries that were themselves waiting on
the job to move, a restored job whose connections sat idle behind hundreds of
parked retries, and archive jobs that halted after verification while putting
misnamed files back in place.

It also makes that last step, placing files under the names their PAR2 set
records, safe to interrupt. A restart in the middle of it now resumes or rolls
back instead of leaving a job half-renamed.

And Weaver now tells you when a newer release is out.

## What changed

### Download reliability

- **Retries no longer wait on the progress they are meant to unblock.** A job
  whose remaining articles were all parked for retry could count those parked
  retries, and idle connections, as progress toward its next checkpoint, while
  the retries in turn waited for that checkpoint. Neither side moved. Parked
  retries and idle connections no longer count, and one article at a time is
  now reserved for making real progress until its result is handled, across
  dispatch, refills and spillover alike. A job restored with many parked
  retries lends its connections back to work that can run.
- **Retries against an unavailable provider are not parked forever.** A
  recovery download aimed at a provider that could not serve it could leave
  hundreds of retries waiting indefinitely. They are now released so the job
  can move on to the next recovery wave.
- **Abandoned connections cannot corrupt a job's bookkeeping.** When a
  connection is retired mid-request, its outstanding work is returned to the
  queue exactly once, events that arrive from it afterwards are ignored, and
  its reservation is refunded before it is cleared. Metrics also keep
  refreshing during long stretches of heavy result traffic.
- **Direct unpack no longer outruns its scratch space.** When an archive's
  header arrived late, direct-to-destination extraction could read ahead far
  enough to exhaust its scratch space and fall back to the slower staged path
  for an archive that was perfectly valid. Lookahead is now bounded while the
  header is outstanding, and the header probe no longer stalls behind a blocked
  article at the head of the queue.

### Archive verification and repair

- **Placing renamed files cannot stall or lose data.** After PAR2 verification,
  files whose names were scrambled are moved to the names the PAR2 set records.
  Where those moves form a cycle, such as two files that must swap names, the
  move could not complete and the job stopped. Every source is now moved aside
  before any destination is installed, a collision is refused before any file
  is touched, and a failed move rolls back without deleting the only copy.
- **An interrupted placement recovers on restart.** Placement is now journaled,
  together with each file's identity, so a process that stops partway through
  resumes or rolls back when Weaver starts again. Any earlier verification or
  repair verdict for that job is discarded and the job is verified afresh,
  because the files on disk may no longer be the ones that verdict described.
- **A placement that cannot be recovered pauses the job instead of blocking
  startup.** The job stays in the queue, paused, with the error saved and shown,
  and can be resumed on its own once the cause is dealt with.
- **A rename-only repair result is not reported as damage.** A post-repair
  check that found files merely needing to be placed under their proper names
  could fail the job as though they were still damaged.

### Update notifications

- **A link appears when a newer Weaver release is available.** Weaver checks
  the project's public GitHub releases about every six hours, starting a few
  minutes after launch, and shows a "New version" link in the navigation when a
  newer stable release exists. Prereleases are ignored. Checks use conditional
  requests and honour the rate limits GitHub sends back, a failed check keeps
  the last result rather than clearing it, and nothing about your installation
  is sent beyond the request itself, which identifies the Weaver version.

## Upgrade notes

- No database migration or settings change is required.
- The GraphQL schema gains an update-status query and subscription. The change
  is additive; existing clients are unaffected.
- Weaver now makes an outbound HTTPS request to `api.github.com` about every six
  hours. If your network blocks it, the check fails quietly and downloads are
  unaffected.
