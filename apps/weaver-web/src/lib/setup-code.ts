/** Characters in a one-time setup code, not counting its hyphen. */
export const SETUP_CODE_CHARACTERS = 6;

/** The code as displayed, `K7P-M2X`: six characters and one hyphen. */
export const SETUP_CODE_DISPLAY_LENGTH = SETUP_CODE_CHARACTERS + 1;

/**
 * Formats what has been typed into a setup code field the way Weaver prints
 * the code: capitals, grouped three and three around a hyphen. The hyphen is
 * added once a fourth character arrives, so deleting back past it still works.
 */
export function formatSetupCode(input: string): string {
  const characters = input
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, SETUP_CODE_CHARACTERS);
  const group = SETUP_CODE_CHARACTERS / 2;
  return characters.length > group
    ? `${characters.slice(0, group)}-${characters.slice(group)}`
    : characters;
}
