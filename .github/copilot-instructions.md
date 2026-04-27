Use the `agent-context` MCP first for repository exploration, caller tracing, symbol lookup, implementation discovery, and file discovery in this repo.

Call `list_scopes` first. Use `scope = "/Users/jeremy/dev/scryer-media/weaver"` for this repo, or `scope = "workspace"` for cross-repo searches. MCP `pathPrefix` and `file` filters are repo-relative; never include `/Users/jeremy/dev/scryer-media` or `scryer-media` in those filters.

Use `search_symbols` for exact definitions and `search_code` for broader semantic or hybrid discovery. Use shell search only after MCP has identified candidate files, or when checking an exact known string in live uncommitted files.

The MCP endpoint is `http://127.0.0.1:8765/mcp`, served by Homebrew service `agent-context`. If MCP appears unavailable, check `http://127.0.0.1:8765/health` and `brew services list` before falling back.
