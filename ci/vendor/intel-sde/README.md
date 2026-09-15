# Intel SDE CI Fixture

This directory contains the pinned Linux Intel Software Development Emulator archive used by the release CI SIMD emulation lane.

- Archive: `sde-external-9.48.0-2024-11-25-lin.tar.xz`
- SHA-256: `3173d2a5369e3385226b488d8b75403951bc14af601435fe707d9f83e0b533e6`
- Original Intel URL: `https://downloadmirror.intel.com/843185/sde-external-9.48.0-2024-11-25-lin.tar.xz`

The workflow verifies the archive hash before extracting it. Keeping the pinned archive in Git LFS avoids release failures when Intel's live download endpoint challenges automated CI traffic.
