//! Seeds and database keys at rest.
//!
//! Both are encrypted with age's scrypt recipient under the wallet password. The
//! database key is random and only wrapped by the password, so changing the
//! password rewrites two small files instead of re-keying the database.

use std::io::{Read, Write};

use age::secrecy::SecretString;
use bip0039::{Count, English, Mnemonic};
use rand::RngExt;
use secrecy::{ExposeSecret, SecretVec};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("wrong password")]
    WrongPassword,
    #[error("not a valid 24-word recovery phrase")]
    BadPhrase,
    #[error("the password is empty")]
    EmptyPassword,
    #[error("encrypted file: {0}")]
    Format(String),
}

/// A recovery phrase held in memory only as long as needed.
pub struct Phrase(Zeroizing<String>);

impl Phrase {
    pub fn generate() -> Self {
        Phrase(Zeroizing::new(Mnemonic::<English>::generate(Count::Words24).phrase().to_string()))
    }

    /// Accepts only 24 English words (ZIP 315), normalising spacing and case.
    pub fn parse(text: &str) -> Result<Self, KeyError> {
        let normalised = Zeroizing::new(
            text.split_whitespace().map(str::to_lowercase).collect::<Vec<_>>().join(" "),
        );
        if normalised.split(' ').count() != 24 {
            return Err(KeyError::BadPhrase);
        }
        Mnemonic::<English>::from_phrase(normalised.as_str()).map_err(|_| KeyError::BadPhrase)?;
        Ok(Phrase(normalised))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The BIP 39 seed with an empty passphrase, as Zcash wallets use it.
    pub fn seed(&self) -> SecretVec<u8> {
        let m = Mnemonic::<English>::from_phrase(self.0.as_str()).expect("validated at construction");
        let mut seed = m.to_seed("");
        let out = SecretVec::new(seed.to_vec());
        seed.zeroize();
        out
    }
}

fn passphrase(password: &str) -> Result<SecretString, KeyError> {
    if password.is_empty() {
        return Err(KeyError::EmptyPassword);
    }
    Ok(SecretString::from(password.to_string()))
}

/// Encrypts with the password; `work_factor` overrides age's ~1 s scrypt target (tests only).
pub fn seal(plaintext: &[u8], password: &str, work_factor: Option<u8>) -> Result<Vec<u8>, KeyError> {
    let mut recipient = age::scrypt::Recipient::new(passphrase(password)?);
    if let Some(log_n) = work_factor {
        recipient.set_work_factor(log_n);
    }
    let enc = age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
        .map_err(|e| KeyError::Format(e.to_string()))?;
    let mut out = Vec::new();
    let mut w = enc.wrap_output(&mut out).map_err(|e| KeyError::Format(e.to_string()))?;
    w.write_all(plaintext).map_err(|e| KeyError::Format(e.to_string()))?;
    w.finish().map_err(|e| KeyError::Format(e.to_string()))?;
    Ok(out)
}

pub fn open(ciphertext: &[u8], password: &str) -> Result<Zeroizing<Vec<u8>>, KeyError> {
    let dec = age::Decryptor::new(ciphertext).map_err(|e| KeyError::Format(e.to_string()))?;
    if !dec.is_scrypt() {
        return Err(KeyError::Format("not a password-encrypted file".into()));
    }
    let identity = age::scrypt::Identity::new(passphrase(password)?);
    let mut r = dec
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .map_err(|_| KeyError::WrongPassword)?;
    let mut out = Zeroizing::new(Vec::new());
    r.read_to_end(&mut out).map_err(|e| KeyError::Format(e.to_string()))?;
    Ok(out)
}

pub fn new_db_key() -> Zeroizing<[u8; 32]> {
    let mut k = Zeroizing::new([0u8; 32]);
    rand::rng().fill(&mut k[..]);
    k
}

pub fn seal_phrase(p: &Phrase, password: &str, wf: Option<u8>) -> Result<Vec<u8>, KeyError> {
    seal(p.as_str().as_bytes(), password, wf)
}

pub fn open_phrase(ct: &[u8], password: &str) -> Result<Phrase, KeyError> {
    let bytes = open(ct, password)?;
    let s = std::str::from_utf8(&bytes).map_err(|_| KeyError::Format("phrase is not UTF-8".into()))?;
    Phrase::parse(s)
}

pub fn open_db_key(ct: &[u8], password: &str) -> Result<Zeroizing<[u8; 32]>, KeyError> {
    let bytes = open(ct, password)?;
    let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| KeyError::Format("bad key length".into()))?;
    Ok(Zeroizing::new(arr))
}

/// The seed's first bytes in hex, to tell two wallets apart in tests and logs.
pub fn seed_tag(seed: &SecretVec<u8>) -> String {
    hex::encode(&seed.expose_secret()[..4])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phrase_round_trip() {
        let p = Phrase::generate();
        assert_eq!(p.as_str().split(' ').count(), 24);
        let ct = seal_phrase(&p, "pw", Some(10)).unwrap();
        assert!(matches!(open_phrase(&ct, "nope"), Err(KeyError::WrongPassword)));
        let back = open_phrase(&ct, "pw").unwrap();
        assert_eq!(back.as_str(), p.as_str());
        assert_eq!(seed_tag(&back.seed()), seed_tag(&p.seed()));
    }

    #[test]
    fn phrase_rules() {
        let p = Phrase::generate();
        let messy = format!("  {}  ", p.as_str().to_uppercase().replace(' ', "\n "));
        assert_eq!(Phrase::parse(&messy).unwrap().as_str(), p.as_str());
        let twelve = Mnemonic::<English>::generate(Count::Words12);
        assert!(Phrase::parse(twelve.phrase()).is_err());
        assert!(Phrase::parse("abandon ".repeat(24).trim()).is_err());
        assert!(matches!(seal(b"x", "", Some(10)), Err(KeyError::EmptyPassword)));
    }

    #[test]
    fn db_key_round_trip() {
        let k = new_db_key();
        let ct = seal(&k[..], "pw", Some(10)).unwrap();
        assert_eq!(*open_db_key(&ct, "pw").unwrap(), *k);
    }
}
