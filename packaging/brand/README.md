# Brand sources

The vector originals every brand asset in the tree is generated from.
Eight files, three forms × the colourways each form ships in:

| File | Form |
| --- | --- |
| `weaver-mark-color.svg` | the mark alone, brand gradient |
| `weaver-mark-dark.svg` | the mark alone, `#262324` |
| `weaver-mark-white.svg` | the mark alone, white |
| `weaver-wordmark-dark.svg` | WEAVER alone, `#262324` |
| `weaver-wordmark-white.svg` | WEAVER alone, white |
| `weaver-lockup-color.svg` | mark + WEAVER, gradient mark |
| `weaver-lockup-dark.svg` | mark + WEAVER, `#262324` |
| `weaver-lockup-white.svg` | mark + WEAVER, white |

## Regenerating

```sh
brew install librsvg imagemagick
packaging/brand/generate-assets.sh          # every asset below
packaging/macos/assets/generate-assets.sh   # then weaver.icns and the DMG art
```

`--check` on either script compares the committed assets against a fresh run
without writing to the tree. Both are deterministic, so a clean `--check` is
what says the committed artwork still matches these sources.

The order matters: `weaver.icns` is cut from
`apps/weaver-web/public/app-icon-dark-512.png`, which the first script writes.

What comes out, and from which source:

- `apps/weaver-web/public/` — favicons, the `.ico`, and the installed-app icons,
  from the colour mark. The plated ones sit on `#323232` or `#b9b9b9` because a
  launcher composites them over wallpaper. The manifest's maskable pair draws the
  mark smaller, inside the circle Android promises not to crop, and the Apple
  home-screen icons come in 180, 167, 152 and 120, each rendered at its size.
- `server/app/weaver/resources/macos/menubar-*.png` — the mono marks. Each file
  is named for the menu-bar appearance it serves, so the dark-named one holds
  the white drawing.
- `server/app/weaver/resources/windows/weaver.ico` — the colour mark. One icon
  resource serves both the executable and the notification area.
- `docs/img/weaver-hero.webp` — the colour mark, plated.
- `docs/img/weaver-lockup-on-light.svg` and `weaver-lockup-on-dark.svg` — the
  colour lockup for the repository README, with the viewBox cut to the ink. A
  README cannot restyle an image the way the interfaces restyle their inline
  lockup, so it picks one of the two by colour scheme; the dark one draws
  WEAVER in white.
- `packaging/macos/assets/` — `weaver.icns` and the DMG background's wordmark.

## Two traps

Rasterize with **librsvg**, never ImageMagick's own SVG renderer: these sources
declare their fill through a CSS class, which ImageMagick ignores, and it emits
a black silhouette with no warning.

Write `.ico` frames as **TrueColorAlpha**. An `.ico` otherwise carries a
one-bit transparency mask, which throws away every antialiased edge pixel — at
16px that closed the mark's interior lines into a solid blob and it stopped
reading as a W at all.

## In the web app

The interfaces draw the lockup live rather than loading an image, so the
wordmark can take `currentColor` and follow the theme while the mark keeps its
gradient. That copy of the geometry is `apps/weaver-web/src/lib/brand.tsx`, and
it is lifted from `weaver-lockup-color.svg` — a change to the lockup means
regenerating it too.
