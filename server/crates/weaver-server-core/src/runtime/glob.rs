//! Small glob helpers for operator-supplied patterns.

/// Case-insensitive glob matcher supporting `*` (any sequence) and `?` (any
/// single character). Other characters are literals.
pub(crate) fn glob_match_ci(pattern: &str, input: &str) -> bool {
    if !pattern.contains(['*', '?']) {
        return pattern.eq_ignore_ascii_case(input);
    }

    let (mut pattern_index, mut input_index) = (0, 0);
    let (mut star_index, mut star_input_index) = (None, 0);

    while input_index < input.len() {
        let pattern_char = pattern[pattern_index..].chars().next();
        let input_char = input[input_index..]
            .chars()
            .next()
            .expect("input index is on a UTF-8 boundary");
        if pattern_char == Some('*') {
            star_index = Some(pattern_index);
            star_input_index = input_index;
            pattern_index += '*'.len_utf8();
        } else if pattern_char == Some('?')
            || pattern_char.is_some_and(|character| character.eq_ignore_ascii_case(&input_char))
        {
            pattern_index += pattern_char.expect("checked above").len_utf8();
            input_index += input_char.len_utf8();
        } else if let Some(star) = star_index {
            pattern_index = star + '*'.len_utf8();
            let star_input_char = input[star_input_index..]
                .chars()
                .next()
                .expect("star input index is on a UTF-8 boundary");
            star_input_index += star_input_char.len_utf8();
            input_index = star_input_index;
        } else {
            return false;
        }
    }

    while pattern[pattern_index..].starts_with('*') {
        pattern_index += '*'.len_utf8();
    }
    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::glob_match_ci;

    #[test]
    fn matches_ascii_case_insensitive_globs() {
        assert!(glob_match_ci("r??", "R42"));
        assert!(glob_match_ci("zip*", "ZiP64"));
        assert!(!glob_match_ci("r??", "r007"));
    }

    #[test]
    fn wildcard_matches_one_unicode_character_without_allocating() {
        assert!(glob_match_ci("?x", "éx"));
        assert!(!glob_match_ci("?x", "ééx"));
    }
}
