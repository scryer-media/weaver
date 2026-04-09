pub mod api_keys;
pub mod model;
pub mod password;
pub mod repository;
pub mod service;

pub use api_keys::ApiKeyRow;
pub use model::{CachedLoginAuth, CallerScope, LoginAuthCache};
pub use password::{hash_password, needs_rehash, verify_password};
pub use repository::AuthCredentials;
pub use service::{
    Claims, JWT_TTL_SECS, JwtError, create_jwt, derive_jwt_secret, generate_api_key, hash_api_key,
    verify_jwt,
};
