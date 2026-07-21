# Contributing to Weaver

[ARCHITECTURE.md](ARCHITECTURE.md) is the architectural contract. Update it when
a change intentionally alters that contract.

## Scope

- Keep changes focused. Exclude unrelated cleanup, dependency churn,
  formatting, generated-file churn, and refactors.
- Extend an existing coherent path before creating another subsystem or generic
  layer. Prefer less code when behavior remains clear.
- Preserve public behavior unless changing it is the stated goal. Update tests
  and operator-facing documentation for intentional changes.
- Do not release, deploy, tag, or alter release artifacts unless a maintainer
  explicitly requests it. Repository automation belongs in `cargo xtask`.

## Pull Request Process

1. Open the pull request against `main`.
2. A maintainer will review it and respond with feedback on the request and its
   implementation.
3. If the request is accepted for potential adoption, a maintainer will name
   the release branch and direct you to retarget the pull request to it. Do not
   choose or create a release branch yourself.

## Pipeline Correctness

- Preserve the explicit flow from ingest to durable job state, byte-pipeline
  execution, finalization, and history.
- Do not report download, decode, repair, extraction, or import success before
  the relevant output has been verified and durable state updated.
- Design active work for pause, cancellation, retry, shutdown, and restart.
- Keep passwords, server credentials, API keys, and decrypted secrets out of
  logs, errors, GraphQL responses, metrics, and persisted plaintext.
- Keep filesystem operations bounded to validated roots and treat cleanup or
  replacement as destructive operations requiring verified preconditions.

## Local Validation

Use `xtask` for repository-owned checks and discover the current surface with:

```bash
cargo xtask --help
cargo xtask ci clippy
cargo xtask ci audit
cargo nextest run --workspace --locked
```

Run affected tests while iterating. For frontend, platform, release, or
performance changes, run the applicable checks shown by `xtask --help` and
current CI. Disclose anything relevant that was not run.

## Tests And Fixtures

- Add regression coverage for bug fixes and test failure, cancellation,
  restart, and persistence behavior where relevant.
- Keep tests beside the owning domain in parallel test modules. Inline test
  modules are exceptions, not the default.
- Prefer small deterministic synthetic fixtures with documented generation.
  Sourced fixtures require known provenance and redistribution rights.
- Store binary fixtures through Git LFS. Do not commit personal downloads,
  private NZBs, credentials, databases, logs, or ordinary media collections.
- Performance claims require reproducible workloads, equivalent work, verified
  output, and enough measurements to support the claim.

## AI-Assisted Contributions

- The human submitter must understand and review every change and personally
  run claimed validation. AI cannot review itself or fabricate test results.
- Disclose substantive AI assistance and describe the human verification.
- Ground architectural, protocol, persistence, and GraphQL behavior in the
  repository and validated references, not a model's memory.
- Verify the origin and license of suggested material. Do not provide AI
  services with secrets, production data, private NZBs, databases, logs,
  restricted fixtures, or unpublished security reports.
- AI tools must not bypass hooks, signing, branch protections, or safety checks.
  Publishing, deployment, migrations against real data, and destructive actions
  require explicit maintainer authorization and human supervision.

## Repository Policy

- Use the versioned hooks and satisfy commit and tag signature requirements.
- Keep local paths, usernames, build output, logs, and credentials out of
  commits.
- Contributions must be original or compatible with [LICENSE](LICENSE).
  Third-party code and test data require clear origin and licensing.
