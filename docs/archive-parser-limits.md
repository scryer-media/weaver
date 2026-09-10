# Archive parser limits

ZIP and 7z entry-count and declared-size checks run after the locked libraries
have constructed archive metadata. ZIP central-directory tables and 7z header
structures can therefore allocate memory before Weaver rejects an archive.
The extraction memory setting is not a hard process RSS limit.

This limitation is accepted for now. ZIP 8.6.0 and sevenz-rust2 0.22.2 remain
unchanged: no vendored copies, local patches, dependency upgrades, or codec
feature changes are part of this work. Enforcing limits before those libraries
allocate metadata remains deferred.
