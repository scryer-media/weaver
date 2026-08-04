# Weaver release signing policy

Official release tags are named `weaver-vX.Y.Z`. CI accepts a tag only when `git verify-tag` trusts the SSH identity in [`release-signing-allowed-signers`](release-signing-allowed-signers) and its target is reachable from `release-X.Y.Z`.

Each future GitHub release includes `weaver-checksums.txt`, its keyless Cosign bundle `weaver-checksums.txt.sigstore.json`, and `weaver-provenance.intoto.jsonl`. The checksum bundle is issued by the tag-triggered `deploy.yml` workflow through GitHub Actions OIDC.

Historic release backfills are explicitly marked as post-publication maintainer checksum signatures. They do not claim recreated build provenance.
