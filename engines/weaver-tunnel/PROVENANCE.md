# Tunnel source provenance

The initial SSH and WireGuard protocols, userspace IP stack, SOCKS front,
configuration validation and protocol fixtures were adapted from
`scryer-media/scryer`, `crates/scryer-tunnel`, at commit
`81e8de95848a31f024edd86d026db8867c296091` (`release-NEXT`).
The original repository's GPL v3 license is preserved in `LICENSE`.

SSH, WireGuard, HTTP/3 CONNECT, the userspace IP stack and their protocol fixtures come
from `scryer-media/proxy-tunnels`, through signed tag `v0.1.0` in this crate's
`Cargo.toml`. The workspace `Cargo.lock` records the exact resolved Git commit. The local protocol copies have
been removed. The shared extraction began at
`2b537101b58d2cad11664b401b82a726b8958db1`; its key-only SSH authentication,
per-channel host-key authorization, DNS admission and closing-socket accounting
are preserved. Download tuning is opt-in, leaving Scryer's defaults unchanged.

The Weaver adaptation replaces implicit runtime ownership with an explicit
executor, adds revocable direct streams and bridges, HTTP CONNECT/SOCKS5 upstream transports and
bounded DNS-over-TCP, and extends lifecycle, trust and consumer fixtures.
Persistence, routing policy, authorization and UI remain outside this crate.
The s2n direct-stream integration lives in `weaver-nntp` and uses its existing
s2n callback APIs and dependencies.

The tag resolves to `01481be7fca27a7864fe40e9a4b95fcfa9698ce2`. HTTP/3 uses
Quinn 0.11, rustls 0.23, and the shared repository's vendored h3 0.0.8 and
h3-quinn 0.0.10. The two MIT-licensed vendored libraries preserve upstream
provenance; their narrow patch encodes ordinary CONNECT without scheme/path
pseudo-headers. TLS 1.3, certificate verification and QUIC packet protection use
AWS-LC explicitly. The public root bundle is webpki-roots (MPL-2.0).
Local HTTP/3 fixtures reuse this stack and Weaver's existing AWS-LC rcgen
configuration. Their generated private trust root is only installed in tests.

Registry dependencies are pinned by the workspace `Cargo.lock`, with registry
checksums. The main networking additions resolve as follows:

| Dependency | Version | Upstream | Declared license | Enabled features |
| --- | --- | --- | --- | --- |
| russh | 0.63.1 | warp-tech/russh | Apache-2.0 | aws-lc-rs |
| gotatun | 0.9.2 | mullvad/gotatun | MPL-2.0 | aws-lc-rs, device, socket |
| smoltcp | 0.13.1 | smoltcp-rs/smoltcp | 0BSD | std, alloc, async, medium-ip, IPv4/IPv6, DNS, TCP, UDP |
| ipnetwork | 0.21.1 | achanda/ipnetwork | MIT OR Apache-2.0 | default |

Reqwest retains version 0.13.4 and gains SOCKS support. Existing package
versions were not replaced. Required additional registry packages carry
MIT, Apache-2.0, BSD, 0BSD or MPL-2.0 license declarations. Neither `tun`,
`pcap`, `daita` nor `windows-gro` is enabled on Gotatun. Russh does not enable
its default RSA or compression features. The resolved TLS implementation
continues to use `aws-lc-rs`.

Fixed private keys under `test_support` are public, disposable test material.
They are excluded from normal product builds.
