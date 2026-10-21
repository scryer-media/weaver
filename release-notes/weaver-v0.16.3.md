# Weaver 0.16.3 release notes

## Downloads and queue

- **Responsive downloads on slow storage.** Decoded batches now write on the
  disk owner thread without blocking the pipeline, keeping dispatch, status
  updates, and the UI responsive while writes finish.
- **Fix a queue stall under write pressure.** When the highest-priority job
  cannot use a server, that server can continue downloading other eligible
  jobs. Soft pressure limits each handout to one article instead of leaving
  the connection idle against a backlog it cannot drain.
- **Safer write completion and repair.** Pending writes finish before archive
  demotion, PAR3 source publication, or shutdown proceeds. Damaged-segment
  writes avoid clean ranges still being written, removed jobs retain their
  memory accounting until writes finish, and failed handback writes fail the
  job instead of only logging an error.
- **Correct paused-job responses.** Jobs added paused, including duplicates
  held back by duplicate handling, immediately report `Paused` even before
  the queue has admitted them.

## Security and compatibility

- **Windows tunnel revocation.** Refresh the connection state of duplicated
  sockets so revoking a tunnel closes connections that have just finished
  connecting.
- **Certificate trust updates.** Update certificate validation and root stores
  to remove distrusted emSign intermediates and address
  RUSTSEC-2026-0381, RUSTSEC-2026-0382, and RUSTSEC-2026-0383.
- **Older Linux kernels.** Enable the secure-randomness fallback in the static
  musl builds for systems without the required entropy syscall support.

## Archives and validation

- **Unified accelerated CRC checks.** Use `crc-turbo` for yEnc, archive, and
  recovery checksums, including CRC combination and zero-filled ranges. Kernel
  diagnostics report the selected tier and any `CRC_TURBO_TIER` override.
- **Archive library updates.** Update the RAR, 7z, LZMA, PAR2, and PAR3 engines,
  including explicit wiping of RAR member cipher keys when they are dropped.
- **More reliable validation.** Build and pull end-to-end test images for the
  host architecture by default, and remove timing races from disk-write
  accounting and proxy fallback tests.
