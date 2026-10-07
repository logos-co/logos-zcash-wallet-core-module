//! Who may call the core. Only the wallet backend by default; a `callers.json`
//! in the instance's persistence directory can widen that for tests.

use serde::Deserialize;

pub const DEFAULT_CALLER: &str = "zcash_wallet_backend";

/// The caller as the host reported it, mirrored so this file needs no SDK.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    Unknown,
    Host,
    Module(String),
    Other,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Callers {
    #[serde(default)]
    pub modules: Vec<String>,
    /// Admits calls that carry a host anchor, such as `logosctl call`. Tests only.
    #[serde(default)]
    pub allow_host: bool,
}

impl Default for Callers {
    fn default() -> Self {
        Self { modules: vec![DEFAULT_CALLER.into()], allow_host: false }
    }
}

impl Callers {
    /// Absent file: defaults. Unreadable file: nobody, so a typo never opens the door.
    pub fn from_file(contents: Option<&str>) -> Self {
        match contents {
            None => Self::default(),
            Some(text) => serde_json::from_str(text).unwrap_or(Self { modules: vec![], allow_host: false }),
        }
    }

    pub fn admits(&self, caller: &Caller) -> bool {
        match caller {
            Caller::Module(name) => !name.is_empty() && self.modules.iter().any(|m| m == name),
            Caller::Host => self.allow_host,
            Caller::Unknown | Caller::Other => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_admit_only_the_backend() {
        let c = Callers::default();
        assert!(c.admits(&Caller::Module("zcash_wallet_backend".into())));
        assert!(!c.admits(&Caller::Module("zcash_wallet_ui".into())));
        assert!(!c.admits(&Caller::Host));
        assert!(!c.admits(&Caller::Unknown));
    }

    #[test]
    fn file_rules() {
        assert_eq!(Callers::from_file(None), Callers::default());
        let c = Callers::from_file(Some(r#"{"modules":["probe"],"allowHost":true}"#));
        assert!(c.admits(&Caller::Host) && c.admits(&Caller::Module("probe".into())));
        assert!(!c.admits(&Caller::Module("zcash_wallet_backend".into())));
        let broken = Callers::from_file(Some(r#"{"modules":["probe"],"typo":1}"#));
        assert!(!broken.admits(&Caller::Module("probe".into())));
    }
}
