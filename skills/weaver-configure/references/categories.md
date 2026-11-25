# Categories

Web UI: **Settings → Categories**. One category covers usenet jobs and
torrents.

## CategoryInput

`addCategory(input)` and `updateCategory(id, input)` take the same input.
This skill sends only `name`, `destDir` and `aliases`.

| Field | Type | Default | Left out on update | Meaning |
|---|---|---|---|---|
| `name` | String! | – | required | Trimmed; empty is refused. Unique, ignoring letter case. Without `destDir` it must be one safe folder name (below). |
| `destDir` | String | null | **cleared** | The category's own folder. Empty = none. Not checked: Weaver trusts it as admin input. |
| `aliases` | String! | `""` | → `""` | Comma-separated patterns, `*` and `?` wildcards, letter case ignored. |
| `seeding`, `torrentRoute`, `unpack`, `uploadCap` | – | inherit / off | kept | Torrent settings. Out of scope: never send them. |

Update replaces name, folder and aliases: send all three, from
`ListCategories`, changing only what was agreed.

```sh
jq --argjson id 2 '.data.categories[] | select(.id == $id)' categories.json > before.json
jq '{id: .id, input: {name, destDir, aliases}} | .input.aliases = "tv*, series"' \
  before.json > vars.json
```

## Name rules

Without `destDir` the name becomes a folder, so it must be one name that
is safe on every platform; otherwise `INVALID_INPUT`.

- Pass: `movies`, `TV HD`, `movies-4k`, `日本語`.
- Refused: `movies/4k`, `movies\4k`, `.`, `..`, `C:`, `CON`, `NUL.txt`,
  `trailing.`, `bad*name`, 241 or more characters.

With `destDir` set, the name is not checked this way.

## Which category a download gets

A category string sent with a download (API, RSS, a client) is matched:

1. a category name, ignoring letter case;
2. else the first alias pattern that matches, in category order.

Example: aliases `film*, cinema` on `Movies` → `FILM-4K` and `cinema` file
under `Movies`.

## Where its files go

- `destDir` set: that folder.
- `destDir` null: `<completed folder>/<name>`.
- No category: the completed folder.

Changing `destDir` or `name` only changes the setting: no file already on
disk is moved.

## Removing a category

Never through this skill. The operator does it: **Settings → Categories** →
the category → **Remove category** → confirm **Remove category**. Downloads
already filed under it keep their folder; new ones land in the completed
folder.
