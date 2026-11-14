// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/**
 * Characters in a one-time setup code, not counting its hyphen. It matches
 * `SETUP_CODE_LENGTH` in the server, which generates the code; everything on
 * the setup page takes the length from here.
 */
export const SETUP_CODE_CHARACTERS = 8;

/** Characters on each side of the hyphen. */
const SETUP_CODE_GROUP = SETUP_CODE_CHARACTERS / 2;

/** The code as displayed, `K7PM-2XQR`: eight characters and one hyphen. */
export const SETUP_CODE_DISPLAY_LENGTH = SETUP_CODE_CHARACTERS + 1;

/** An empty field's placeholder, in the code's displayed shape. */
export const SETUP_CODE_PLACEHOLDER = `${"·".repeat(SETUP_CODE_GROUP)}-${"·".repeat(SETUP_CODE_GROUP)}`;

/**
 * Formats what has been typed into a setup code field the way Weaver prints
 * the code: capitals, grouped four and four around a hyphen. The hyphen is
 * added once a fifth character arrives, so deleting back past it still works.
 */
export function formatSetupCode(input: string): string {
  const characters = input
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, SETUP_CODE_CHARACTERS);
  return characters.length > SETUP_CODE_GROUP
    ? `${characters.slice(0, SETUP_CODE_GROUP)}-${characters.slice(SETUP_CODE_GROUP)}`
    : characters;
}
