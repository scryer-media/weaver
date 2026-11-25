# Weaver 0.17.5 release notes

Nothing in Weaver itself changed since 0.17.4. The 0.17.3 and 0.17.4 tag
runs did not publish a container image, so read the 0.17.0 to 0.17.4
notes with these.

## Build

- **The container image is published again.** A release step added for
  0.17.3 let a release be approved past failed tests. With it in place
  the image job was skipped, on 0.17.3 and on 0.17.4. That step is
  removed: a release needs every test job to pass, as before.
- **The Apple Silicon macOS package builds with thin LTO**, as the Intel
  package already did.
