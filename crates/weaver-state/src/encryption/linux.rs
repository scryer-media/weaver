use super::keystore::KeyStore;

const DOCKER_SECRET_PATH: &str = "/run/secrets/weaver_encryption_key";

pub struct DockerSecret;

impl KeyStore for DockerSecret {
    fn get_key(&self) -> Result<Option<String>, String> {
        match std::fs::read_to_string(DOCKER_SECRET_PATH) {
            Ok(contents) => {
                let trimmed = contents.trim().to_string();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!(
                "failed to read Docker secret at {DOCKER_SECRET_PATH}: {e}"
            )),
        }
    }

    fn set_key(&self, _key_base64: &str) -> Result<(), String> {
        Err("Docker secrets are read-only".into())
    }

    fn delete_key(&self) -> Result<(), String> {
        Err("Docker secrets are read-only".into())
    }

    fn name(&self) -> &'static str {
        "Docker secret"
    }
}
