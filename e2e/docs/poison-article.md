# Poison-article cases

`TestPoisonArticleE2E` runs a native Weaver binary against its own loopback NNTP
fixture, configured with exactly one server, and proves that one article cannot
stop that server's downloads.

From the repository root, with the locked frontend assets already built:

```sh
cargo build --locked -p weaver
cd e2e
WEAVER_POISON_ARTICLE_E2E_BIN="$(cd .. && pwd)/target/debug/weaver" \
  go test ./internal/weaver -run '^TestPoisonArticleE2E$' -count=1 -v -timeout 25m
```

Requirements: the repository's Rust and Go toolchains and `par2`. No published
corpus or Docker stack is needed. Without the binary environment variable the
test reports a skip.

## Scenarios and assertions

- **trailer-junk.** Every article of the job carries a line between its yEnc
  trailer and the NNTP terminator. The job must complete with the expected
  output, and no fetch may be reported as a malformed terminator.
- **connection-breaker.** One article of a PAR2-protected job ends the
  connection partway through its body on every fetch, and a clean job is
  queued behind it. The clean job must complete within three minutes. The
  poisoned job must complete by PAR2 repair, and the fixture must see a bounded
  number of requests for the poisoned article: the run of transport failures
  has to end in the article's own retry budget, not repeat for as long as the
  job exists.

The second case waits out the retry holds that keep a repeating article away
from the server's recovery probe, so it takes several minutes by design.
