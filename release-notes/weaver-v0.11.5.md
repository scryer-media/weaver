# Weaver 0.11.5 release notes

## Highlights

Weaver 0.11.5 makes completed-output cleanup available to trusted integrations.
Control-scoped API keys can now remove completed files when they delete history,
matching the capabilities needed by download-client integrations.

## What changed

### History cleanup for integrations

- **Control-scoped callers can delete completed output.** All history-delete
  mutations now permit a control-scoped caller to use `deleteFiles: true`,
  including single-item, batch, and all-history deletion, as well as durable
  history-delete requests.
- **Read-only callers remain protected.** Read-scoped API keys still cannot
  delete completed files, and receive the existing control-scope authorization
  error instead.
- **Durable history deletion removes the output.** The history-delete worker
  now removes completed output directories for accepted `deleteFiles: true`
  requests, keeping the asynchronous operation consistent with direct history
  deletion.

## Upgrade notes

- This broadens the authority of existing control-scoped API keys: integrations
  using those keys may now remove completed output files, not only history
  records. Use read-scoped keys for clients that must remain unable to delete
  files.
- No database migration or GraphQL schema change is required.
