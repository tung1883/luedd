use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub download_dir: PathBuf,
    pub max_concurrent_downloads: usize,
    pub per_download_concurrency: usize,
    /// UI font key: "system" | "georgia" | "ibm-plex-mono" | "excalifont" | "cascadia-mono".
    /// Defaulted so settings files written before this field still load.
    #[serde(default = "default_font")]
    pub font: String,
    /// UI language: "en" | "de". Defaulted for the same reason.
    #[serde(default = "default_language")]
    pub language: String,
}

fn default_font() -> String {
    "system".to_string()
}

fn default_language() -> String {
    "en".to_string()
}

impl Settings {
    fn defaults(data_dir: &Path) -> Self {
        Self {
            download_dir: data_dir.join("downloads"),
            max_concurrent_downloads: 2,
            per_download_concurrency: 8,
            font: default_font(),
            language: default_language(),
        }
    }
}

pub struct SettingsStore {
    path: PathBuf,
    data: RwLock<Settings>,
}

impl SettingsStore {
    pub async fn open(path: impl Into<PathBuf>, data_dir: &Path) -> Result<Self> {
        let path = path.into();
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).context("corrupt settings file")?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Settings::defaults(data_dir),
            Err(e) => return Err(e.into()),
        };
        Ok(Self { path, data: RwLock::new(data) })
    }

    pub async fn get(&self) -> Settings {
        self.data.read().await.clone()
    }

    pub async fn set(&self, settings: Settings) -> Result<()> {
        *self.data.write().await = settings;
        let json = serde_json::to_vec_pretty(&*self.data.read().await)?;
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&self.path, json).await?;
        Ok(())
    }
}

pub fn default_settings_path(data_dir: &Path) -> PathBuf {
    data_dir.join("settings.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn defaults_when_missing() {
        let dir = std::env::temp_dir().join(format!("luedd-settings-missing-{}", std::process::id()));
        let store = SettingsStore::open(dir.join("settings.json"), &dir).await.unwrap();
        let settings = store.get().await;
        assert_eq!(settings.download_dir, dir.join("downloads"));
        assert_eq!(settings.max_concurrent_downloads, 2);
    }

    #[tokio::test]
    async fn persists_and_reloads() {
        let dir = std::env::temp_dir().join(format!("luedd-settings-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("settings.json");

        {
            let store = SettingsStore::open(&path, &dir).await.unwrap();
            store
                .set(Settings {
                    download_dir: dir.join("custom-downloads"),
                    max_concurrent_downloads: 5,
                    per_download_concurrency: 16,
                    font: "cascadia-mono".to_string(),
                    language: "de".to_string(),
                })
                .await
                .unwrap();
        }

        let reopened = SettingsStore::open(&path, &dir).await.unwrap();
        let settings = reopened.get().await;
        assert_eq!(settings.download_dir, dir.join("custom-downloads"));
        assert_eq!(settings.max_concurrent_downloads, 5);
        assert_eq!(settings.per_download_concurrency, 16);

        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
