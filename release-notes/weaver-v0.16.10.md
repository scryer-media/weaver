# Weaver 0.16.10 release notes

Everything below is new since 0.16.9.

## Highlights

- **Decode more RAR members while a download is still arriving.** Eligible
  files later in a non-solid archive can decode on spare workers while an
  earlier file streams on the run's own thread. Output is still delivered
  in archive order, using the existing thread and memory budgets.
- **Correct Windows release validation.** The directory-replacement cleanup
  fixture closes its child file before renaming the parent directory, as
  Windows requires, while retaining its checks that unrelated files survive
  cleanup.

## RAR extraction

- **Read ahead through complete volumes.** Between decoder writes, the RAR
  walk can read headers from volumes already available in full and hand
  eligible later members to workers. It no longer has to finish the current
  streamed member before starting that work.
- **Keep workers on verified input.** A volume is ready for parallel work
  only after it has closed and every byte is verified. A closed volume with
  missing or damaged ranges stays out of the worker pool.
- **Account for memory held by parallel work.** Admission includes decoded
  output waiting to be delivered and each decoder's dictionary, alongside
  the window held by the streamed member. Solid archives retain sequential
  decoding, and encrypted members need an available password before they
  can start alongside other work.
- **Preserve ordering and failure behavior.** Completed worker output waits
  for preceding members. A failed earlier member prevents later speculative
  output from being delivered. Damage reporting follows the volumes the
  failing decoder actually opened, including continuations beyond the
  headers copied when decoding began.
- **Keep output batching.** Streamed RAR output retains the megabyte batching
  introduced in 0.16.9 while the walk schedules work between writes.

## Regression coverage and release validation

- **Compare sequential and concurrent RAR results.** The ordinary tests
  exercise archive fixtures and writer layouts with one thread and multiple
  threads, including volume continuations, failures and constrained budgets.
- **Wait for a durable restart point.** The completed-download restart
  fixture holds extraction while it waits for healthy checkpoint coverage
  for every posted file. It no longer mistakes in-memory completion for a
  committed checkpoint after write batching.
- **Accept in-flight work at a feed-verdict checkpoint.** Restart assertions
  allow refused articles that have not settled yet, while still checking
  any recorded lost ranges and their bounds.
- **Exercise cleanup portably.** The Windows directory-replacement fixture
  reopens the original file after moving its parent, preserving the identity
  mismatch needed to prove that cleanup leaves the replacement untouched.
