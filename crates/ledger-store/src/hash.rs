use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub fn sha256_file(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    let file =
        File::open(path).with_context(|| format!("opening {} for sha256", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 1024 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hashes_bytes() {
        assert_eq!(
            sha256_bytes(b"ledger"),
            "fe14010b4fe83303852f0467c919ef9a7ca089b91e96e3aad7d426dd87079297"
        );
    }
}
