// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn par2_ignore_extensions_default_to_the_baked_metadata_set() {
    assert_eq!(
        parse_par2_ignore_extensions(None),
        DEFAULT_PAR2_IGNORE_EXTENSIONS
            .iter()
            .map(|extension| (*extension).to_string())
            .collect::<Vec<_>>()
    );
    assert!(parse_par2_ignore_extensions(None).contains(&"nfo".to_string()));
    assert!(parse_par2_ignore_extensions(None).contains(&"sfv".to_string()));
}

#[test]
fn an_empty_par2_ignore_extension_override_disables_the_rule() {
    assert!(parse_par2_ignore_extensions(Some("")).is_empty());
    assert!(parse_par2_ignore_extensions(Some("   ")).is_empty());
    assert!(!par2_damage_ignorable(
        "silver.horizon.nfo",
        &parse_par2_ignore_extensions(Some(""))
    ));
}

#[test]
fn par2_ignore_extensions_accept_both_separators_and_normalize_entries() {
    assert_eq!(
        parse_par2_ignore_extensions(Some(" .NFO, sfv ;nfo;.Srr ")),
        vec!["nfo".to_string(), "sfv".to_string(), "srr".to_string()]
    );
}

#[test]
fn par2_damage_is_ignorable_by_extension_only() {
    let extensions = parse_par2_ignore_extensions(None);
    assert!(par2_damage_ignorable("silver.horizon.nfo", &extensions));
    assert!(par2_damage_ignorable("SILVER.HORIZON.SFV", &extensions));
    // The extension is the file's own, not a substring of its name: a
    // payload that merely mentions one of these words is still payload.
    assert!(!par2_damage_ignorable(
        "silver.horizon.nfo.mkv",
        &extensions
    ));
    assert!(!par2_damage_ignorable("silver.horizon.mkv", &extensions));
    assert!(!par2_damage_ignorable("nfo", &extensions));
}
