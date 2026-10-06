# Weaver 0.14.5 release notes

Everything below is new since 0.14.4.

## Highlights

- 0.14.5 is the published build of 0.14.4. The 0.14.4 tag was cut but its
  release pipeline stopped before publishing: the test job ran past its
  limit, so no binaries, images, or packages shipped. Every change in the
  0.14.4 release notes ships here.

## What changed

- Completion checks now skip a job that is queued for or running
  post-processing. A late completion checkpoint during scripts previously
  ran the final move a second time, parked the output under `<name>.#<id>`,
  and repointed the job's output directory out from under the running
  script.
- The extended archive campaigns' schedule smokes and demotion-reason
  campaigns no longer run in the default test suite. They run only from the
  manually dispatched `archive-matrix-extended` workflow, alongside the
  archive matrix. The small regression tests in those modules stay in the
  default suite.
- The release pipeline's test job limit now leaves headroom for a loaded
  runner.

## Upgrade notes

There are no configuration or database migration changes in this release.
