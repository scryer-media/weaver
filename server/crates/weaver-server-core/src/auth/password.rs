const ARGON2_M_COST: u32 = 19 * 1024;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;
const ARGON2_OUTPUT_LEN: usize = 32;

fn pinned_argon2() -> argon2::Argon2<'static> {
    let params = argon2::Params::new(
        ARGON2_M_COST,
        ARGON2_T_COST,
        ARGON2_P_COST,
        Some(ARGON2_OUTPUT_LEN),
    )
    .expect("pinned argon2 parameters must be valid");
    argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params)
}

pub fn hash_password(password: &str) -> Result<String, String> {
    use argon2::password_hash::{PasswordHasher, Salt, SaltString};
    // Salt bytes come straight from `getrandom` (already a direct dependency)
    // rather than `password_hash::rand_core::OsRng`: that re-export only
    // compiles when some other crate in the build graph happens to enable
    // `rand_core/getrandom`, which held for workspace builds but broke
    // standalone `-p weaver-server-core` builds after the 2026-08 dep bumps.
    let mut salt_bytes = [0u8; Salt::RECOMMENDED_LENGTH];
    getrandom::fill(&mut salt_bytes).map_err(|error| format!("salt generation failed: {error}"))?;
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|error| format!("salt encoding failed: {error}"))?;
    pinned_argon2()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| format!("argon2 hash failed: {error}"))
}

fn is_argon2_hash(hash: &str) -> bool {
    hash.starts_with("$argon2")
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    if is_argon2_hash(hash) {
        pinned_argon2()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    } else {
        false
    }
}

#[cfg(test)]
mod tests;
