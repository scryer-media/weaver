//! Small, allocation-bounded glob helpers for operator-supplied patterns.

/// Case-insensitive glob matcher supporting `*` (any sequence) and `?` (any
/// single character). Other characters are literals.
pub(crate) fn glob_match_ci(pattern: &str, input: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let input: Vec<char> = input.chars().collect();
    let (mut pattern_index, mut input_index) = (0, 0);
    let (mut star_index, mut star_input_index) = (usize::MAX, 0);

    while input_index < input.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            star_index = pattern_index;
            star_input_index = input_index;
            pattern_index += 1;
        } else if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?'
                || pattern[pattern_index].eq_ignore_ascii_case(&input[input_index]))
        {
            pattern_index += 1;
            input_index += 1;
        } else if star_index != usize::MAX {
            pattern_index = star_index + 1;
            star_input_index += 1;
            input_index = star_input_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == '*' {
        pattern_index += 1;
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
}
