# Weaver Architecture Manifesto

## Purpose

This document is the architectural contract for Weaver.

It is written for humans and agents alike. If you are adding, changing, or reviewing a feature, this document is the default source of truth for how that work should fit into the system.

This is intentionally opinionated. Weaver should feel coherent because the architecture is coherent. We do not let each feature invent its own model, boundary rules, or source of truth.

If the code and this document diverge, stop and resolve the mismatch deliberately. Do not silently accept architectural drift. If the architecture changes intentionally, update this document in the same work.

## What Weaver Is

Weaver is a single-node Usenet downloader and post-processing system.

It accepts NZB input, downloads articles over NNTP, decodes yEnc payloads, verifies and repairs with PAR2, extracts archives, tracks job state, exposes a GraphQL API, and serves a web UI for operators.

The backend is authoritative. The web app is a projection client, not a second source of truth.

Weaver is intentionally:

- a single deployable binary
- SQLite-backed
- GraphQL-first
- optimized for one homelab node rather than distributed deployment
- built around a byte pipeline, not a fleet of cooperating services
- organized around a small number of product crates plus a few real engine crates

## Architectural Priorities

When tradeoffs appear, prefer:

- the smallest coherent change over the largest ambitious change
- coherence over local cleverness
- durability over convenience
- explicit flows over hidden coupling
- semantic models over storage-shaped models
- one strong path over multiple overlapping paths
- typed boundaries over stringly glue
- bounded memory over convenience shortcuts
- reducing code over growing code when functionality is preserved
- simple homelab operations over premature distributed complexity

## Non-Negotiable Principles

### 0. Xtask Is The Canonical Task Interface

Repository automation lives behind `cargo xtask`.

For humans and agents alike, `cargo xtask` is the default interface for:

- release automation
- CI-like local validation
- local dev and deploy harnesses
- profiling and benchmark entrypoints
- other repo-owned operational commands

Compatibility scripts may remain during migration, but they are wrappers around xtask rather than the source of truth. New repo automation belongs in xtask, not in new shell glue.

The only intentional shell holdouts should be true runtime/container entrypoints under `docker/`. Developer workflows, release flows, profiling, and benchmark orchestration belong in xtask.

### 1. The Backend Is Authoritative

All durable truth lives in the backend.

That includes:

- settings and server configuration
- categories and bandwidth policy
- active job state
- history and recovery state
- authentication and API keys
- backup and restore state

The web app may project, filter, and present that state, but it does not invent durable truth.

If a feature cannot clearly answer "who is authoritative for this state?", the design is not ready.

### 2. Dependency Direction Is Strict

Weaver should converge on this dependency direction:

```text
weaver-model + engine crates
          ^
weaver-server-core
          ^
weaver-server-api
          ^
weaver app crate
```

And outside the Rust dependency graph:

```text
weaver-web
   |
GraphQL
   |
weaver-server-api
```

Rules:

- low-level shared model code stays in the small shared crate
- engine crates stay below the server product layer
- server core owns product behavior and persistence orchestration
- the API crate maps transport to server-core behavior
- the app crate owns composition, process startup, and HTTP wiring

Never add an upward dependency. Never make an engine crate depend on server-core. Never let the API crate or app crate become a second domain layer.

### 3. Weaver Does Not Require a Durable Domain Event Spine

Weaver is the important exception to the event-spine rule used elsewhere.

Weaver may emit runtime pipeline events for:

- live progress
- subscriptions
- metrics
- logs
- internal coordination

Those events are useful, but they are not the product's canonical durable truth.

We do not force Weaver into event sourcing. We do not require every durable state change to become a first-class domain event. Durable truth may live directly in explicit domain state and SQLite-backed records as long as the system can recover, explain current state, and rebuild operator-facing views correctly.

### 4. Durable Before Live

Live updates matter, but durability comes first.

The rule is:

- authoritative state changes happen first
- persisted state is updated before live fanout is treated as complete
- live updates are allowed to be projections over current state rather than the primary record of state change

This means subscriptions and UI refreshes should always be able to recover from durable state, not depend on transient in-memory delivery.

### 5. The API Describes Intent, Not Storage

Weaver uses a semantic GraphQL API.

Queries should describe the views the UI and integrations need. Mutations should describe business actions. Subscriptions should describe meaningful live updates.

We do not expose raw SQLite tables, ad hoc status strings, or storage-only details merely because doing so is easy.

The API should reflect how operators think about:

- jobs
- queue state
- history
- settings
- servers
- categories
- RSS automation
- system health

It should not reflect how the tables happen to be laid out.

### 6. Single-Node SQLite Runtime Is Deliberate

Weaver is SQLite-first by design.

This is not a temporary placeholder for a future distributed system. It is the deliberate operating model for the product.

That means:

- all durable product state lives in SQLite
- we do not add Redis, message brokers, or extra persistence systems
- backup, restore, migration, and portability matter
- serialized writes and simple operations matter more than multi-node ambition

### 7. Memory Must Be Bounded

Every in-memory queue, channel, cache, buffer, and retry structure must have an explicit bound and a clear lifecycle.

That means:

- download, decode, and write backlogs have budgets
- channels have explicit capacity
- caches have eviction or reset points
- background loops have shutdown behavior
- no feature may assume the dataset is always small enough

If memory growth is unbounded, the feature is not done.

### 8. Shared Mutable Runtime State Must Be Explicit

Shared mutable runtime coordination must stay explicit and centralized.

We do not spread product behavior across module-level singletons, hidden stateful helpers, or incidental cross-thread mutation.

Runtime state such as:

- pipeline coordination
- pause and speed-limit state
- log buffers
- connection pools
- metrics accumulators
- retry queues

must have obvious ownership and obvious construction.

### 9. Typed Boundaries Are Mandatory

Weaver should not rely on free-floating string identifiers at internal boundaries.

Statuses, enum-like values, schedule actions, bandwidth-cap modes, queue states, auth scopes, and GraphQL-exposed state should be represented by:

- Rust enums with serde where appropriate
- typed constants defined once where enum modeling is not appropriate
- explicit boundary mappings at persistence and API edges

There should be no stringly typed enums floating around inside application code. Internal modules should pass typed values and only serialize at real boundaries.

### 10. Frontend Boundaries Are Real

The frontend is not allowed to become a second application layer.

The rules are:

- the backend owns durable truth and policy
- the web app owns presentation, user interaction, and local projection behavior
- GraphQL is the boundary
- backend internal types do not leak into frontend architecture

Weaver-web should feel rich, but it should still be a projection client over backend truth.

### 11. Engine Crates Are Real Boundaries, Not Product Homes

NNTP, NZB, yEnc, PAR2, and RAR are real engine boundaries.

Those crates should own protocol and algorithm concerns. They should not own product semantics such as:

- queue policy
- job lifecycle
- category behavior
- API-facing models
- SQLite orchestration

If shared low-level contracts are needed between engines and the server, they belong in the small shared model crate, not in a generic junk-drawer core crate.

### 12. Security and Permissions Are Core Behavior

Authentication, authorization, API keys, encrypted secrets, and backup access control are part of the real system.

Every feature must have a clear answer to:

- who can do this
- on whose behalf it runs
- what secrets it can touch
- what operator-facing state it can reveal
- how it behaves after restart or restore

### 13. Solve Problems With the Least Necessary Code

Weaver should not equate progress with code growth.

New features should be implemented with the smallest amount of code that cleanly solves the problem and fits the architecture.

That means:

- prefer extending an existing coherent path over creating a parallel subsystem
- prefer deleting, simplifying, or consolidating code when that preserves behavior
- prefer a narrow solution that fits the real requirement over a broad framework for hypothetical future needs
- treat reduction in overall codebase size, while retaining functionality, as a success

This is not a license for clever compression or unreadable shortcuts. The goal is less unnecessary code, less duplication, and less surface area to maintain.

## Durable Domain Commitments

These are the product areas that must remain explicit as Weaver grows.

They are here so the codebase does not collapse into a giant generic downloader blob.

### Ingest, Jobs, and Pipeline Stages Are Distinct Domains

Accepting input, turning it into a job, running the byte pipeline, and presenting final history are related, but they are not the same concern.

Weaver must keep these boundaries explicit:

- ingest turns external input into typed job intent
- jobs own durable job definitions and lifecycle state
- pipeline owns runtime execution of download, decode, verify, repair, extract, and finalize work
- history owns the durable operator-facing record of what happened

### Settings, Servers, Categories, and Bandwidth Policy Stay Explicit

Operational configuration is not one undifferentiated settings blob.

Weaver should keep explicit homes for:

- global settings
- NNTP server configuration
- categories and output routing
- schedules and bandwidth caps

These areas may interact, but they should not collapse into one generic config bucket or one giant `config.rs`.

### History, Metrics, and Logs Are Projections, Not Alternate Truth

Operator-facing history, metrics, and logs are important, but they are projections over authoritative backend state and runtime behavior.

They must not quietly become alternate sources of truth that diverge from real job, auth, or settings state.

### Archive and Repair Semantics Stay Explicit

RAR, PAR2, split files, extraction readiness, and archive topology are not incidental helpers. They are part of the product's core behavior.

That means:

- archive classification stays explicit
- repair readiness stays explicit
- extraction rules stay explicit
- file-role and archive-topology logic should live in coherent modules, not be scattered through unrelated helpers

### Engine Boundaries Stay Explicit

Engine crates should remain sharp and boring:

- NNTP
- NZB
- yEnc
- PAR2
- RAR

If product logic starts migrating into those crates, the architecture is drifting in the wrong direction.

## Patterns

### 1. Serde Defines Enum Boundaries

Rust enums are the canonical way to represent discrete state in Weaver.

Their serde form is the default boundary contract for:

- SQLite persistence where enum state must be stored
- GraphQL where enum state is genuinely part of the public contract

Rules:

- internal state stays strongly typed
- persistence uses the serde form
- API exposure uses the serde form when that state is part of the product contract
- if an enum is too implementation-shaped to expose cleanly, keep it internal rather than leaking a backend detail

### 2. Real Boundaries Belong at Persistence and API Layers

Weaver should not create artificial boundaries inside the codebase by repeatedly normalizing or cleansing internal values as though every module call were untrusted input.

The default rule is:

- domain modules talk to each other with typed values
- persistence code maps domain values to stored records
- API code maps domain values to transport types

Internal component-to-component communication should prefer typed domain values over DTO churn.

### 3. Organize by Domain, Not by Technical Function

Do not default to buckets like:

- `services/`
- `repositories/`
- `dto/`
- `usecases/`

Those shapes tend to create sink files and blurred ownership.

Prefer domain folders such as:

- `auth`
- `settings`
- `servers`
- `categories`
- `ingest`
- `jobs`
- `pipeline`
- `history`
- `rss`
- `bandwidth`
- `operations`

### 4. Default Domain File Responsibilities

Inside a domain-oriented crate, the normal shape of a domain is:

```text
src/
└── jobs/
    ├── mod.rs
    ├── model.rs
    ├── service.rs
    ├── repository.rs
    ├── persistence.rs
    ├── record.rs
    ├── queries.rs
    └── commands.rs
```

These files have distinct purposes:

- `mod.rs`: the domain entrypoint and export guide
- `model.rs`: core domain types and enums
- `service.rs`: orchestration and business rules
- `repository.rs`: persistence-facing traits or contracts
- `persistence.rs`: concrete persistence implementation for the domain
- `record.rs`: storage-shaped types that should not leak into the domain model
- `queries.rs`: SQL or storage query helpers
- `commands.rs`: semantic input types for domain actions

Not every tiny domain needs every file on day one. The point is to keep responsibilities obvious and to split by meaning rather than growing giant cross-domain files.

### 5. Tests Should Live Beside Domains, Not Inside Production Files

Weaver follows the parallel test directory convention.

The default is:

- crate-level `tests/` for integration and domain behavior
- domain-oriented test files beside the crate, not inline in production modules

Inline `#[cfg(test)]` modules should be rare exceptions, not the default.

## Canonical Feature Flow

The normal path for product work in Weaver should look like this:

1. An operator action, CLI command, RSS trigger, or HTTP request enters through the app or API boundary.
2. The request is mapped into typed server-core intent.
3. The owning domain validates and applies business rules.
4. Durable state is persisted if the feature changes authoritative state.
5. Runtime pipeline coordination is invoked if the feature affects active execution.
6. Live updates are emitted as projections if the UI or operators need them.
7. Queries and subscriptions read from authoritative state and explicit runtime projections, not from hidden side channels.

If a feature cannot explain its flow in those terms, it probably does not fit the architecture yet.

## Red Flags

These are signs that a change is likely heading in the wrong direction:

- adding another generic `core`, `state`, `scheduler`, or `assembly` style crate instead of giving behavior a real domain home
- putting business logic in the binary app crate
- putting business logic in the GraphQL crate
- making an engine crate depend on server product crates
- introducing stringly typed statuses where enums should exist
- leaking storage-shaped records into domain logic
- adding inline tests by default instead of using parallel test files
- creating new giant sink files like `facade.rs`, `types.rs`, or `service.rs` at crate root
- forcing event-sourcing style machinery onto features that do not need it just to imitate another product

## Contributor and Agent Checklist

Before merging a feature, confirm:

- the authoritative owner of the new state is clear
- the feature has a real domain home
- persistence changes stay in the owning domain
- API changes expose product semantics, not storage details
- internal calls use typed values instead of stringly glue
- memory growth is bounded
- engine crates remain below the product layer
- tests live in the parallel test structure
- the change uses the least necessary code that cleanly solves the problem

## Final Rule

Weaver should read as one product with a few real engines, not as a pile of technical buckets.

If a future change forces a choice between:

- adding another helper-oriented layer or generic crate
- or putting the behavior inside the domain that actually owns it

the default answer should be domain ownership.

The only explicit exception from the Volt and Scryer model is that Weaver does not require a durable domain event spine. Everything else should still follow the same architectural discipline.
