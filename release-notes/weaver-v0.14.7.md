# Weaver 0.14.7 release notes

Everything below is new since 0.14.6.

## Highlights

- Updated repair libraries reduce reads during PAR2 verification and improve
  RAR3 recovery-volume restoration.
- Release image builds no longer copy root-level tarballs or ZIP files into
  the Docker build context.

## What changed

### Repair and verification

- `par2-rs` 0.10.8 reads a damaged file once during strict verification.
- `reedsolomon-rs` 0.4.9 and `unrar-rs` 0.10.9 restore RAR3 recovery volumes
  using a derived decode matrix across whole regions rather than one byte
  column at a time.
- `par3-rs` 0.5.0 adds internal support for restoring a Derived carrier and
  an experimental RAR5 PAR-inside module. This release does not enable a new
  Weaver workflow for that experimental module.

### Builds and tests

- The release Docker build excludes root-level tarballs and ZIP files. Such
  files are not part of the image and could previously consume build-host
  storage when copied with the repository.
- PAR3 tests now expect unchanged source reopens to avoid rereading data on
  Windows as they do on other platforms. A capacity-limited test provider
  now counts only admitted sessions, so refused connections cannot inflate
  its active-session count under load.

## Upgrade notes

There are no configuration or database migration changes in this release.
