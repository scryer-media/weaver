# Token discipline

A Weaver queue holds thousands of jobs, a job thousands of files, and history
grows without end. Keep bulk data on disk; read only what a decision needs.

## Rules

1. **Every response to a file.** `gql.sh op.graphql vars.json > out.json`,
   then summarise with `jq`. Never `cat` a response.
2. **Counts first.** Read a total or `length` before any items. Look at
   items only when a count looks wrong.
3. **Sample, don't list.** A handful: `jq -c '.data.X[:5][]'`.
4. **Only mismatches reach the conversation.** In a verification loop, write
   every result to a TSV or JSON file; print only failing rows and a total.
   Cap at 20 rows and say how many more.
5. **Use the shipped operations.** Each selects only the fields its step
   needs. No fields "just in case"; never fetch or read the GraphQL schema.
   Missing a field: add that one field to a copy of the file.
6. **Let the server do the work.** Use its filters and paging; do not pull a
   whole list to filter it locally.
7. **Poll sparingly.** Poll every 5 to 15 seconds; print one short line per
   poll, or nothing until done.
8. **Page with limits.** Queue and history take `pageIndex` (from 0) and
   `pageSize` (at most 500); torrents take `first` (at most 500) and
   `after`; job events take `limit` and `before` (an event's `cursor`). Use
   a small page first. Page into files, merge with `jq -s`.
9. **Keep secrets out of files you read back.** Server settings, logs and
   torrent details can hold host names, user names and tracker passkeys.
   Keep them in a mode-700 directory. Never print them whole.

## jq patterns

```sh
jq -r '.errors[]?.extensions.code' out.json      # error codes only
jq -r '.data.version' out.json                   # one value
jq -s 'map(.data.X) | add | length' page-*.json  # merge pages
```
